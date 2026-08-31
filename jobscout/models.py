"""Data models for collected items."""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
import json


@dataclass
class CollectedItem:
    source: str
    data: Dict[str, Any]
    collected_at: str = ""
    tags: List[str] = field(default_factory=list)
    item_id: str = ""

    def __post_init__(self):
        if not self.collected_at:
            self.collected_at = datetime.now().isoformat()
        if not self.item_id:
            self.item_id = f"{self.source}_{hash(json.dumps(self.data, sort_keys=True, default=str)) & 0xFFFFFFFF:08x}"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)

    @classmethod
    def from_dict(cls, d: dict) -> "CollectedItem":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def matches(self, criteria: Dict[str, Any]) -> bool:
        for key, value in criteria.items():
            if key == "tags":
                if not set(value).issubset(set(self.tags)):
                    return False
            elif key in self.data:
                if self.data[key] != value:
                    return False
            else:
                return False
        return True
