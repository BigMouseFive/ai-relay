"""站点适配器工厂。"""
from __future__ import annotations

from ..webbridge import WebbridgeClient
from .base import SiteAdapter
from .deepseek import DeepseekAdapter
from .kimi import KimiAdapter
from .minimax import MinimaxAdapter

_ADAPTERS: dict[str, type[SiteAdapter]] = {
    "kimi": KimiAdapter,
    "deepseek": DeepseekAdapter,
    "minimax": MinimaxAdapter,
}


def create_adapter(site: str, client: WebbridgeClient, session: str) -> SiteAdapter:
    try:
        cls = _ADAPTERS[site]
    except KeyError:
        raise ValueError(f"未知站点: {site}") from None
    return cls(client, session)
