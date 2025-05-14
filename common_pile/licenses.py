"""Enumeration of licenses."""

import re
from enum import Enum


class StringEnum(Enum):
    def __str__(self):
        return self.value


# TODO: With all the different versions that are out in the wild, this flat enum
# is getting hard to use. We should re-thing how to do this.
class PermissiveLicenses(StringEnum):
    """By 'Permissive' we mean licenses that are in the Gold, Silver, or Bronze
    lists of the Blue Oak Countil (https://blueoakcouncil.org/list), even if
    they have copyleft requirements.
    """

    PD = "Public Domain"
    CC0 = "Creative Commons Zero - Public Domain - https://creativecommons.org/publicdomain/zero/1.0/"
    CC_PDM = "Creative Commons Public Domain Mark - https://creativecommons.org/publicdomain/mark/1.0/"
    CC_BY = (
        "Creative Commons - Attribution - https://creativecommons.org/licenses/by/4.0/"
    )
    CC_BY_3 = (
        "Creative Commons - Attribution - https://creativecommons.org/licenses/by/3.0/"
    )
    CC_BY_2_5 = (
        "Creative Commons - Attribution - https://creativecommons.org/licenses/by/2.5/"
    )
    CC_BY_2 = (
        "Creative Commons - Attribution - https://creativecommons.org/licenses/by/2.0/"
    )
    CC_BY_SA = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/4.0/"
    CC_BY_SA_3 = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/3.0/"
    CC_BY_SA_2_5 = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/2.5/"
    CC_BY_SA_2_1 = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/2.1/"
    CC_BY_SA_1 = "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/1.0/"
    GFDL = "GNU Free Documentation License"
    APACHE_2 = "Apache 2 License - https://www.apache.org/licenses/LICENSE-2.0"
    MIT = "MIT License"
    BSD_2 = "BSD 2-Clause"
    BSD_3 = "BSD 3-Clause"

    ISC = "ISC License"
    ARTISTIC_2 = "Artistic License 2.0"

    # Not in the Blue Oak Council list, but open source compliant.
    CDLA_P = "Community Data License Agreement - Permissive 1.0 - https://cdla.dev/"

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
        if re.match(r".*/publicdomain/mark/1.0/?$", s):
            return cls.CC_PDM
        if re.match(r".*/publicdomain/.*", s):
            return cls.PD
        if m := re.search(r"(?:/licenses/)?by(?P<share>-sa)?/(?P<version>\d.\d)/?", s):
            if m.group("version") == "4.0":
                if m.group("share") is not None:
                    return cls.CC_BY_SA
                return cls.CC_BY
            elif m.group("version") == "3.0":
                if m.group("share") is not None:
                    return cls.CC_BY_SA_3
                return cls.CC_BY_3
            elif m.group("version") == "2.5":
                if m.group("share") is not None:
                    return cls.CC_BY_SA_2_5
                return cls.CC_BY_2_5
            elif m.group("version") == "2.1":
                if m.group("share") is not None:
                    return cls.CC_BY_SA_2_1
            elif m.group("version") == "2.0":
                return cls.CC_BY_2
            elif m.group("version") == "1.0":
                if m.group("share") is not None:
                    return cls.CC_BY_SA_1
            else:
                raise ValueError(f"Unable to understand license {s}")
        if s == "gfdl" or "gnu_free_documentation_license" in s:
            return cls.GFDL
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
