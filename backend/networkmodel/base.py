from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, ClassVar, Dict, Iterable, List, Mapping, Optional, Tuple, Type, TypeVar

T = TypeVar("T", bound="RowModel")


@dataclass
class RowModel:
    table_name: ClassVar[str] = ""
    primary_key: ClassVar[Tuple[str, ...]] = ()
    network_id: Optional[str] = None

    def to_record(self) -> Dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}

    @classmethod
    def columns(cls) -> List[str]:
        return [field.name for field in fields(cls)]

    @classmethod
    def from_record(cls: Type[T], record: Mapping[str, Any]) -> T:
        data = {name: record.get(name) for name in cls.columns()}
        return cls(**data)

    def pk(self) -> Tuple[Any, ...]:
        return tuple(getattr(self, key) for key in self.primary_key)

    @classmethod
    def from_records(cls: Type[T], records: Iterable[Mapping[str, Any]]) -> List[T]:
        return [cls.from_record(record) for record in records]


@dataclass
class IndexModel(RowModel):
    index: Optional[int] = None


@dataclass
class IndexPhaseModel(IndexModel):
    phase: Optional[int] = None


@dataclass
class IndexCircuitModel(IndexModel):
    circuit: Optional[int] = None


@dataclass
class IndexSequenceModel(IndexModel):
    sequence: Optional[int] = None


@dataclass
class IndexBusCircuitModel(IndexModel):
    bus: Optional[int] = None
    circuit: Optional[int] = None
