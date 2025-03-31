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


@dataclasses.dataclass
class LicenseInfo:
    licenses: list[LicenseSnapshot]
    license_type: str = ""

    def license(self, time):
        for l in self.licenses:
            if l.active(time):
                return True
        return False
