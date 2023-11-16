"""Enumeration of licenses."""

from enum import Enum


class StringEnum(Enum):
    def __str__(self):
        return self.value


class PermissiveLicenses(StringEnum):
    PD = "Public Domain"
    CC0 = "Creative Commons Zero - Public Domain"
    CC_BY = "Creative Commons - Attribution"
    CC_BY_SA = "Creative Commons - Attribution Share-Alike"
    GFDL = "GNU Free Documentation License"
    APACHE_2 = "Apache 2 License"
    MIT = "MIT License"
    BSD = "BSD License"


class RestrictiveLicenses(StringEnum):
    CC_BY_NC = "Creative Commons - Attribution Non-Commercial"
    CC_BY_NC_SA = "Creative Commons - Attribution Non-Commercial Share-Alike"
    CC_BY_ND = "Creative Commons - Attribution No Derivatives"
    CC_BY_NC_ND = "Creative Commons - Attribution Non-Commercial No Derivatives"
    GPL = "GNU General Public License"
