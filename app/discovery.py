"""IPv4 mDNS / DNS-SD 服务公告。

ai-relay 只在局域网公告自己的 HTTP 地址和无敏感元数据，从不保存 ERP 地址、
token 或回调 URL。ERP 侧发现代理通过标准 mDNS 主动解析并健康探测该服务。
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import uuid
from pathlib import Path
from typing import Optional

from zeroconf import IPVersion, ServiceInfo, Zeroconf

logger = logging.getLogger("ai-relay.discovery")

AI_RELAY_SERVICE_TYPE = "_amz-ai-relay._tcp.local."
METADATA_PATH = "/.well-known/amazon-service"
READINESS_PATH = "/v1/readiness"


def _private_ipv4_candidates() -> list[str]:
    """尽量收集本机可在 LAN 上被访问的私有 IPv4 地址。"""
    candidates: list[str] = []
    try:
        _, _, addresses = socket.gethostbyname_ex(socket.gethostname())
        candidates.extend(addresses)
    except OSError:
        pass
    # UDP connect 不发送数据，只借路由表选出默认出站网卡。
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("8.8.8.8", 80))
            candidates.insert(0, probe.getsockname()[0])
    except OSError:
        pass

    result: list[str] = []
    for candidate in candidates:
        try:
            parsed = __import__("ipaddress").ip_address(candidate)
        except ValueError:
            continue
        if parsed.version == 4 and parsed.is_private and not parsed.is_loopback and candidate not in result:
            result.append(candidate)
    return result


def load_or_create_service_id(path: Path) -> str:
    """为服务实例建立跨重启/DHCP 变化稳定的 UUID 身份。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        service_id = str(data.get("service_id") or "").strip()
        if service_id:
            uuid.UUID(service_id)
            return service_id
    except (OSError, ValueError, json.JSONDecodeError):
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    service_id = str(uuid.uuid4())
    path.write_text(json.dumps({"service_id": service_id}, indent=2) + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return service_id


class MdnsPublisher:
    """生命周期绑定到 ai-relay 进程的 IPv4 DNS-SD 服务发布器。"""

    def __init__(self, *, service_id: str, port: int, instance_name: str = "",
                 advertise_address: str = "") -> None:
        self.service_id = service_id
        self.port = port
        self.instance_name = instance_name.strip() or socket.gethostname()
        self.advertise_address = advertise_address.strip()
        self._zeroconf: Optional[Zeroconf] = None
        self._info: Optional[ServiceInfo] = None

    @property
    def service_name(self) -> str:
        safe_name = "-".join(part for part in self.instance_name.split() if part) or "ai-relay"
        return f"{safe_name}-{self.service_id[:8]}.{AI_RELAY_SERVICE_TYPE}"

    def start(self) -> bool:
        addresses = [self.advertise_address] if self.advertise_address else _private_ipv4_candidates()
        if not addresses:
            logger.warning("未找到可公告的私有 IPv4 地址，mDNS 服务发现未启动")
            return False
        try:
            packed_addresses = [socket.inet_aton(address) for address in addresses]
        except OSError as error:
            logger.warning("mDNS 广播地址非法，服务发现未启动: %s", error)
            return False

        properties = {
            b"service_type": b"ai-relay",
            b"service_id": self.service_id.encode("utf-8"),
            b"api_version": b"1",
            b"metadata_path": METADATA_PATH.encode("utf-8"),
            b"readiness_path": READINESS_PATH.encode("utf-8"),
        }
        try:
            self._zeroconf = Zeroconf(ip_version=IPVersion.V4Only)
            self._info = ServiceInfo(
                type_=AI_RELAY_SERVICE_TYPE,
                name=self.service_name,
                addresses=packed_addresses,
                port=self.port,
                properties=properties,
                server=f"{socket.gethostname()}.local.",
            )
            # allow_name_change：快速重启或旧实例记录未过期时，实例名冲突不应
            # 让整个 LAN 发现失效。服务身份是 TXT 中的稳定 service_id，不是
            # 实例名，因此改名不影响 ERP 识别同一服务。
            self._zeroconf.register_service(
                self._info, cooperating_responders=True, allow_name_change=True
            )
        except Exception as error:
            # 某些 zeroconf 异常（如 NonUniqueNameException）不带 message，
            # 只记录 str(error) 会得到空白日志，故同时记录异常类型。
            logger.warning(
                "mDNS 服务公告启动失败(%s): %s", type(error).__name__, error or "无错误信息"
            )
            self.stop()
            return False
        logger.info("mDNS 已公告 %s，IPv4=%s，端口=%s", self.service_name, ",".join(addresses), self.port)
        return True

    def stop(self) -> None:
        if self._zeroconf is not None:
            try:
                if self._info is not None:
                    self._zeroconf.unregister_service(self._info)
            except Exception:
                logger.debug("mDNS 服务注销失败", exc_info=True)
            finally:
                self._zeroconf.close()
        self._info = None
        self._zeroconf = None


async def start_publisher_async(publisher: MdnsPublisher) -> bool:
    """在线程中执行阻塞的 mDNS 注册。

    zeroconf 的同步 register_service 会阻塞调用线程的事件循环，在 ASGI
    lifespan 中直接调用会抛 EventLoopBlocked；因此必须放到工作线程。
    """
    return await asyncio.to_thread(publisher.start)


async def stop_publisher_async(publisher: MdnsPublisher) -> None:
    """在线程中执行阻塞的 mDNS 注销（事件循环不能阻塞）。"""
    await asyncio.to_thread(publisher.stop)
