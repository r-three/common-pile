"""Enumeration of licenses."""

import re
from enum import Enum


class StringEnum(Enum):
    def __str__(self):
        return self.value

# {'CDLA Sharing 1.0', 'MPL 2.0', 'CDLA Permissive 1.0', 'Custom', 'No License', 'OANC', 
# 'GNU General Public License v3.0', 'MIT License', 'Unspecified', 'CC BY 4.0', 'CC0 1.0', 
# 'BSD 2-Clause License', 'Apache License 2.0', 'ISC License', 'EPL 1.0', 'GNU General Public License v2.0', 
# 'LGPL 2.1', 'CC BY-SA', 'C-UDA', 'CC BY 3.0', 'CC BY-SA 3.0', 'Artistic License 2.0', 'CC BY-SA 4.0', 'BSD 3-Clause License'}

class PermissiveLicenses(StringEnum):
    PD = "Public Domain"
    CC0 = "Creative Commons Zero - Public Domain - https://creativecommons.org/publicdomain/zero/1.0/"
    CC_BY = (
        "Creative Commons - Attribution - https://creativecommons.org/licenses/by/4.0/"
    )
    CC_BY_3 = (
        "Creative Commons - Attribution - https://creativecommons.org/licenses/by/3.0/"
    )
    CC_BY_SA = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/4.0/"
    CC_BY_SA_3 = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/3.0/"
    CC_BY_SA_2_5 = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/2.5/"
    GFDL = "GNU Free Documentation License"
    APACHE_2 = "Apache 2 License - https://www.apache.org/licenses/LICENSE-2.0"
    MIT = "MIT License"
    BSD = "BSD License"

    CDLA = "CDLA Sharing 1.0"
    CDLA_P = "CDLA Permissive 1.0"
    MPL = "MPL 2.0"
    CUSTOM = "Custom"
    NO_LICENSE = "No License"
    OANC = "OANC"
    GPL_V3 = "GNU General Public License v3.0"
    ISC = "ISC License"
    EPL = "EPL 1.0"
    GPL_V2 = "GNU General Public License v2.0"
    LGPL_2_1 = "LGPL 2.1"
    C_UDA = "C-UDA"
    ARTISTIC_2 = "Artistic License 2.0"




    # TODO: Fill out this function to match in more cases.
    # Note: This kind of function will always be messy and probably require
    # multiple checks that are common across branches. Instead of trying to
    # clean on the implementation, which would get complex (like the compositional
    # solution to fizzbuzz https://themonadreader.files.wordpress.com/2014/04/fizzbuzz.pdf)
    # we should just have a bit of a mess and lots of unittests.
    @classmethod
    def from_string(cls, s: str) -> "PermissiveLicenses":
        s = s.lower().strip()
        if re.match(r".*/publicdomain/zero/1.0/?$", s):
            return cls.CC0
        if m := re.match(r".*/licenses/by(?P<share>-sa)?/(?P<version>\d).0/?$", s):
            if m.group("version") == "4":
                if m.group("share") is None:
                    return cls.CC_BY_SA
                return cls.CC_BY
            elif m.group(1) == "3":
                if m.group("share") is None:
                    return cls.CC_BY_SA_3
                return cls.CC_BY_3
            else:
                raise ValueError(f"Unable to understand license {s}")
        raise ValueError(f"Unable to understand license {s}")


class RestrictiveLicenses(StringEnum):
    CC_BY_NC = "Creative Commons - Attribution Non-Commercial"
    CC_BY_NC_SA = "Creative Commons - Attribution Non-Commercial Share-Alike"
    CC_BY_ND = "Creative Commons - Attribution No Derivatives"
    CC_BY_NC_ND = "Creative Commons - Attribution Non-Commercial No Derivatives"
    GPL = "GNU General Public License"

    # TODO: Fill this in if we ever need to process restrictive license information.
    @classmethod
    def from_string(cls, s: str) -> "RestrictiveLicenses":
        pass
