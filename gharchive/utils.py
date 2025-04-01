"""Utilities for gharchive + license scraping."""


import dataclasses
import datetime


@dataclasses.dataclass
class LicenseSnapshot:
    license: str
    start: datetime
    end: datetime
    license_source: str = ""

    def active(self, time):
        return self.start <= time <= self.end

    @classmethod
    def from_json(cls, json):
        return cls(**json)


@dataclasses.dataclass
class LicenseInfo:
    licenses: list[LicenseSnapshot]
    license_type: str = ""

    def license(self, time):
        for l in self.licenses:
            if l.active(time):
                return True
        return False

    @classmethod
    def from_json(cls, json):
        licenses = [LicenseSnapshot(**d) for d in json.pop("licenses")]
        return cls(licenses=licenses, **json)
