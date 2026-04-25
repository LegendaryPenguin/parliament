"""The callable registry for ``python`` nodes.

A node names a callable; the registry resolves the name. The obvious alternative is to
send code and ``eval`` it, which would make the set of things a workflow can do equal to
the set of things the interpreter can do — at which point the permission model on the
tool executor is decoration, since any node could bypass it. A registry makes the
available callables enumerable, and enumerable is the property the whole control plane is
built on.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

__all__ = ["UnknownCallableError", "DuplicateCallableError", "PythonCallable", "CallableRegistry"]

#: A node callable takes the resolved node input and returns a JSON-encodable value.
PythonCallable = Callable[[Any], Any]


class UnknownCallableError(LookupError):
    """The named callable is not registered. A definition error, so never retryable."""

    def __init__(self, name: str, known: tuple[str, ...]) -> None:
        listed = ", ".join(known) if known else "none"
        super().__init__(f"callable {name!r} is not registered (registered: {listed})")
        self.name = name
        self.known = known


class DuplicateCallableError(ValueError):
    """Two callables claimed one name."""

    def __init__(self, name: str) -> None:
        super().__init__(f"callable {name!r} is already registered")
        self.name = name


@dataclass(frozen=True)
class Registration:
    name: str
    fn: PythonCallable
    description: str


class CallableRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, Registration] = {}

    def register(self, name: str, fn: PythonCallable, description: str = "") -> None:
        # Rejected rather than overwritten: silent replacement means whichever module
        # imported last decides what the workflow does, and import order is not a contract.
        if name in self._entries:
            raise DuplicateCallableError(name)
        self._entries[name] = Registration(name=name, fn=fn, description=description)

    def resolve(self, name: str) -> PythonCallable:
        entry = self._entries.get(name)
        if entry is None:
            raise UnknownCallableError(name, tuple(sorted(self._entries)))
        return entry.fn

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._entries))

    def describe(self) -> tuple[Registration, ...]:
        return tuple(self._entries[name] for name in sorted(self._entries))
