from __future__ import annotations

from .base import SourceResolver
from .resolvers import (
    HjwzwResolver,
    Novel543Resolver,
    OneQxsResolver,
    Shuba69Resolver,
    SpudNovelResolver,
    TtkanResolver,
    Wa01Resolver,
)


class ResolverRegistry:
    def __init__(self) -> None:
        self._resolvers: dict[str, SourceResolver] = {}

    def register(self, resolver: SourceResolver) -> None:
        self._resolvers[resolver.source_id] = resolver

    def get(self, source_id: str) -> SourceResolver:
        try:
            return self._resolvers[source_id]
        except KeyError as exc:
            raise ValueError(f"Unsupported source resolver: {source_id}") from exc


def build_default_registry() -> ResolverRegistry:
    registry = ResolverRegistry()
    registry.register(HjwzwResolver())
    registry.register(Novel543Resolver())
    registry.register(OneQxsResolver())
    registry.register(Shuba69Resolver())
    registry.register(SpudNovelResolver())
    registry.register(TtkanResolver())
    registry.register(Wa01Resolver())
    return registry
