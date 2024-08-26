import datetime
from glob import glob
import os
import subprocess

from licensed_pile.licenses import PermissiveLicenses, RestrictiveLicenses


LICENSE_MAP = {
        "http://arxiv.org/licenses/nonexclusive-distrib/1.0/": RestrictiveLicenses.ARXIV,
        "http://creativecommons.org/licenses/by-nc-nd/4.0/": RestrictiveLicenses.CC_BY_NC_ND,
        "http://creativecommons.org/licenses/by-nc-sa/3.0/": RestrictiveLicenses.CC_BY_NC_SA,
        "http://creativecommons.org/licenses/by-nc-sa/4.0/": RestrictiveLicenses.CC_BY_NC_SA,
        "http://creativecommons.org/licenses/by-sa/4.0/": PermissiveLicenses.CC_BY_SA,
        "http://creativecommons.org/licenses/by/3.0/":  PermissiveLicenses.CC_BY_3,
        "http://creativecommons.org/licenses/by/4.0/": PermissiveLicenses.CC_BY,
        "http://creativecommons.org/licenses/publicdomain/": PermissiveLicenses.PD,
        "http://creativecommons.org/publicdomain/zero/1.0/": PermissiveLicenses.CC0
}


def is_permissive(license_str):
    return isinstance(LICENSE_MAP.get(license_str), PermissiveLicenses)  


def parse_date(date):
    return datetime.datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z").isoformat()


def get_main_tex_file(paper_dir):
    tex_files = glob(os.path.join(paper_dir, "**", "*.tex"), recursive=True)
    # Run `latexpand` and determine main .tex file to be the file with the longest expanded source code
    latexpand_lengths = [(f, len(subprocess.run(["latexpand", os.path.relpath(f, start=paper_dir)], cwd=paper_dir, capture_output=True).stdout)) for f in tex_files]
    return max(latexpand_lengths, key=lambda e: e[1])[0]


