"""WorkerPool 单元测试：FakeWorker，不碰真实浏览器。"""
import asyncio

import pytest

from app.config import Config
from app.pool import QueueFull, WorkerPool
from app.schemas import TaskStatus, WorkerState
from app.store import TaskStore


class FakeWorker:
    """模拟 worker：被指派后立即把任务标记完成。"""

    def __init__(self, worker_id, site, store, on_retry=None, fail_times=0):
        self.worker_id = worker_id
        self.site = site
        self.model = ""
        self.state = WorkerState.idle
        self.detail = None
        self.current_task_id = None
        self.done_count = 0
        self.fail_count = 0
        self.store = store
        self.on_retry = on_retry
        self.fail_times = fail_times  # 前 N 次指派失败
        self.ran = []
        self._task = None

    async def start(self):
        pass

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()

    def start_task(self, task):
        self.state = WorkerState.busy
        self.current_task_id = task.task_id
        self._task = asyncio.create_task(self.run_task(task))

    async def run_task(self, task):
        self.store.mark_running(task.task_id, worker_id=self.worker_id,
                                actual_site=self.site)
        await asyncio.sleep(0.01)  # 模拟执行耗时
        self.ran.append(task.task_id)
        if self.fail_times > 0:
            self.fail_times -= 1
            error = f"fake fail on {self.site}"
            if self.on_retry is not None:
                retried = self.on_retry(task, error, "fake_error")
                if not retried:
                    self.fail_count += 1
            else:
                self.store.mark_failed(task.task_id, error)
                self.fail_count += 1
        else:
            self.store.mark_done(task.task_id, f"答案 from {self.worker_id}")
            self.done_count += 1
        self.current_task_id = None
        self.state = WorkerState.idle


def make_pool(worker_sites=(), queue_size=100, history_size=10,
              max_retries=3, retry_switch_site=True, fail_map=None):
    """fail_map: {site: fail_times} 控制各站点 worker 前几次失败。"""
    config = Config.model_validate({
        "workers": [],  # worker 由测试注入
        "queue": {"max_size": queue_size},
        "dashboard": {"history_size": history_size},
        "task": {
            "max_retries": max_retries,
            "retry_switch_site": retry_switch_site,
            "retry_initial_delay_seconds": 0.01,
            "retry_max_delay_seconds": 0.01,
        },
    })
    store = TaskStore(history_size)
    pool = WorkerPool(config, store, client=None, dispatch_interval=0.01)
    fail_map = fail_map or {}
    for i, site in enumerate(worker_sites, 1):
        pool.workers.append(FakeWorker(
            f"w{i}", site, store,
            on_retry=pool._on_retry_needed,
            fail_times=fail_map.get(site, 0)))
    return pool, store


async def wait_done(store, task_id, timeout=3.0):
    """轮询直到任务终态。"""
    for _ in range(int(timeout / 0.02)):
        status = store.get(task_id).status
        if status in (TaskStatus.done, TaskStatus.failed):
            return status
        await asyncio.sleep(0.02)
    raise AssertionError(f"任务 {task_id[:8]} 未在 {timeout}s 内完成")


async def test_submit_并入队():
    pool, store = make_pool(["kimi"])
    task_id = await pool.submit("你好", "kimi")
    task = store.get(task_id)
    assert task is not None
    assert task.status == TaskStatus.queued
    assert task.site == "kimi"
    assert pool.queue.qsize() == 1


async def test_dispatch_按site匹配():
    pool, store = make_pool(["kimi", "deepseek"])
    await pool.start()
    try:
        tid = await pool.submit("问 deepseek", "deepseek")
        assert await wait_done(store, tid) == TaskStatus.done
        assert pool.workers[1].ran == [tid]
        assert pool.workers[0].ran == []
        assert store.get(tid).result == "答案 from w2"
    finally:
        await pool.stop()


async def test_dispatch_site为None时任一空闲():
    pool, store = make_pool(["kimi"])
    await pool.start()
    try:
        tid = await pool.submit("任意站点", None)
        assert await wait_done(store, tid) == TaskStatus.done
        assert pool.workers[0].ran == [tid]
    finally:
        await pool.stop()


async def test_无空闲worker时排队_按序执行():
    pool, store = make_pool(["kimi"])
    await pool.start()
    try:
        t1 = await pool.submit("第一", None)
        t2 = await pool.submit("第二", None)
        await wait_done(store, t1)
        await wait_done(store, t2)
        # 单 worker 串行执行，顺序与提交一致
        assert pool.workers[0].ran == [t1, t2]
    finally:
        await pool.stop()


async def test_队列满抛QueueFull():
    pool, store = make_pool(["kimi"], queue_size=1)
    # worker 置忙，dispatcher 不会取走任务
    pool.workers[0].state = WorkerState.busy
    await pool.start()
    try:
        await pool.submit("一", None)
        with pytest.raises(QueueFull):
            await pool.submit("二", None)
        # 被拒绝的任务不留记录
        assert len(store._tasks) == 1
    finally:
        await pool.stop()


async def test_workers_info与queue快照():
    pool, store = make_pool(["kimi"])
    pool.workers[0].state = WorkerState.busy
    await pool.submit("排队中", None)
    infos = pool.workers_info()
    assert infos[0].worker_id == "w1"
    assert infos[0].site == "kimi"
    assert infos[0].state == WorkerState.busy
    snap = pool.queue_snapshot()
    assert len(snap) == 1
    assert snap[0].prompt_preview == "排队中"


async def test_跨站点重试_失败后换站成功():
    """kimi 失败一次 → 重新入队 → 派到 deepseek 成功。"""
    pool, store = make_pool(
        ["kimi", "deepseek"],
        fail_map={"kimi": 1},
        max_retries=3,
        retry_switch_site=True,
    )
    await pool.start()
    try:
        tid = await pool.submit("跨站", "kimi")
        assert await wait_done(store, tid) == TaskStatus.done
        task = store.get(tid)
        assert task.retries == 1
        assert "kimi" in task.tried_sites
        assert tid in pool.workers[0].ran  # kimi 试过
        assert tid in pool.workers[1].ran  # deepseek 成功
        assert task.result == "答案 from w2"
        assert pool.workers[0].fail_count == 0  # 中间重试不计 fail
    finally:
        await pool.stop()


async def test_重试耗尽后最终失败():
    pool, store = make_pool(
        ["kimi", "deepseek"],
        fail_map={"kimi": 99, "deepseek": 99},
        max_retries=2,
        retry_switch_site=True,
    )
    await pool.start()
    try:
        tid = await pool.submit("必败", None)
        assert await wait_done(store, tid, timeout=5.0) == TaskStatus.failed
        task = store.get(tid)
        assert task.retries == 2
        assert "已重试 2 次" in (task.error or "")
    finally:
        await pool.stop()


async def test_retry_switch_site_false_不跨站():
    """关闭换站后，指定 kimi 的任务失败重试仍只派给 kimi。"""
    pool, store = make_pool(
        ["kimi", "deepseek"],
        fail_map={"kimi": 1},
        max_retries=3,
        retry_switch_site=False,
    )
    await pool.start()
    try:
        tid = await pool.submit("只走 kimi", "kimi")
        assert await wait_done(store, tid) == TaskStatus.done
        assert tid in pool.workers[0].ran
        assert pool.workers[1].ran == []  # deepseek 未接到
        assert store.get(tid).result == "答案 from w1"
    finally:
        await pool.stop()


def test_find_idle_worker_避开tried_sites():
    pool, store = make_pool(["kimi", "deepseek", "minimax"],
                            retry_switch_site=True)
    # 模拟 kimi 已失败
    w = pool._find_idle_worker("kimi", tried_sites=["kimi"])
    assert w is not None
    assert w.site != "kimi"


def test_find_idle_worker_按请求模型匹配():
    pool, _ = make_pool(["kimi", "deepseek"])
    pool.workers[0].model = "K2.6"
    pool.workers[1].model = "深度思考"
    assert pool._find_idle_worker("kimi", model="K2.6") is pool.workers[0]
    assert pool._find_idle_worker("kimi", model="不存在") is None


async def test_submit_请求未配置模型被拒绝():
    pool, _ = make_pool(["kimi"])
    pool.workers[0].model = "K2.6"
    with pytest.raises(ValueError, match="模型"):
        await pool.submit_with_metadata("模型", "kimi", model="K3")


async def test_target_submission_routes_to_exact_non_browser_target():
    config = Config.model_validate({
        "targets": [{
            "id": "api-one", "type": "openai_compatible", "base_url": "http://api.invalid/v1",
            "api_key_env": "TEST_API_ONE", "model": "m1", "count": 2,
        }],
    })
    store = TaskStore()
    pool = WorkerPool(config, store, client=None, dispatch_interval=0.01)
    task, reused = await pool.submit_with_metadata("API 任务", target_id="api-one")
    assert reused is False
    assert task.target_id == "api-one"
    assert task.backend_type.value == "openai_compatible"
    assert task.model == "m1"
    assert pool.queue.qsize() == 1


async def test_target_rejects_site_or_unauthorized_model_override():
    config = Config.model_validate({
        "targets": [{
            "id": "api-one", "type": "openai_compatible", "base_url": "http://api.invalid/v1",
            "api_key_env": "TEST_API_ONE", "model": "m1",
        }],
    })
    pool = WorkerPool(config, TaskStore(), client=None)
    with pytest.raises(ValueError, match="不能同时"):
        await pool.submit_with_metadata("x", "kimi", target_id="api-one")
    with pytest.raises(ValueError, match="不允许覆盖"):
        await pool.submit_with_metadata("x", target_id="api-one", model="other")


def test_find_idle_worker_关闭换站():
    pool, store = make_pool(["kimi", "deepseek"], retry_switch_site=False)
    w = pool._find_idle_worker("kimi", tried_sites=["kimi"])
    assert w is not None
    assert w.site == "kimi"
    # deepseek 忙时指定 kimi 仍只返回 kimi
    pool.workers[0].state = WorkerState.busy
    assert pool._find_idle_worker("kimi", tried_sites=["kimi"]) is None
