# pylint: disable=missing-module-docstring missing-function-docstring missing-class-docstring
import hashlib
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FactDataQuality:
    # pylint: disable=invalid-name
    Date_Key: str
    EntityType_Key: Optional[str]
    Property_Key: Optional[str]
    Tag_Key: Optional[str]
    Metric_Key: str
    Value: float
    Key: str = field(init=False)
    # pylint: enable=invalid-name

    def __post_init__(self):
        entity_type_key_str = self.EntityType_Key or ''
        property_key_str = self.Property_Key or ''
        tag_key_str = self.Tag_Key or ''

        key_components = '|'.join([
            self.Date_Key,
            entity_type_key_str,
            property_key_str,
            tag_key_str,
            self.Metric_Key
        ])

        self.Key = hashlib.sha256(key_components.encode()).hexdigest()[:16]
