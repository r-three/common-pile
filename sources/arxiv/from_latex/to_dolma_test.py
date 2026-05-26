"""Tests for arXiv source conversion."""

import gzip
import importlib.util
import io
import sys
import tarfile
import types
from pathlib import Path


MODULE_DIR = Path(__file__).parent
sys.path.insert(0, str(MODULE_DIR))
SPEC = importlib.util.spec_from_file_location("to_dolma", MODULE_DIR / "to-dolma.py")
to_dolma = importlib.util.module_from_spec(SPEC)


class Logger:
    def debug(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class BulkDownloader:
    pass


def load_to_dolma_module():
    modules = {
        "bulk_download": types.ModuleType("bulk_download"),
        "common_pile": types.ModuleType("common_pile"),
        "common_pile.logs": types.ModuleType("common_pile.logs"),
        "common_pile.licenses": types.ModuleType("common_pile.licenses"),
        "common_pile.write": types.ModuleType("common_pile.write"),
    }
    modules["bulk_download"].BulkDownloader = BulkDownloader
    modules["common_pile.logs"].get_logger = lambda *args, **kwargs: Logger()
    modules["common_pile.logs"].configure_logging = lambda *args, **kwargs: None
    modules["common_pile"].logs = modules["common_pile.logs"]
    modules["common_pile.licenses"].PermissiveLicenses = types.SimpleNamespace(
        CC_BY_SA="CC_BY_SA",
        CC_BY_3="CC_BY_3",
        CC_BY="CC_BY",
        PD="PD",
        CC0="CC0",
    )
    modules["common_pile.write"].to_dolma = lambda *args, **kwargs: None

    previous_modules = {name: sys.modules.get(name) for name in modules}
    sys.modules.update(modules)
    try:
        SPEC.loader.exec_module(to_dolma)
    finally:
        for name, module in previous_modules.items():
            if module is None:
                del sys.modules[name]
            else:
                sys.modules[name] = module


load_to_dolma_module()


def add_text_file(tar, name: str, text: str):
    data = text.encode("utf-8")
    info = tarfile.TarInfo(name)
    info.size = len(data)
    tar.addfile(info, io.BytesIO(data))


def test_is_commented_out_ignores_escaped_percent():
    contents = r"\% \input{section}"
    assert not to_dolma.is_commented_out(contents, contents.index(r"\input"))


def test_is_commented_out_detects_percent_on_same_line():
    contents = r"Some text % \input{section}"
    assert to_dolma.is_commented_out(contents, contents.index(r"\input"))


def test_load_article_src_handles_uppercase_tex_and_commented_input(tmp_path):
    article_path = tmp_path / "article.gz"
    main = "\n".join(
        [
            r"\begin{document}",
            "Intro",
            r"% \input{commented}",
            r"\input{included}",
            r"\end{document}",
        ]
    )
    with tarfile.open(article_path, "w:gz") as tar:
        add_text_file(tar, "main.TEX", main)
        add_text_file(tar, "commented.tex", "Commented input should not be inserted.")
        add_text_file(tar, "included.TEX", "Included text.")

    documents = list(to_dolma.load_article_src(str(article_path), "1234.5678"))

    assert documents == [
        (
            "\n".join(
                [
                    r"\begin{document}",
                    "Intro",
                    r"% \input{commented}",
                    "Included text.",
                    r"\end{document}",
                ]
            ),
            "main.TEX",
        )
    ]


def test_load_article_src_decodes_single_gzip_file(tmp_path):
    article_path = tmp_path / "article.gz"
    with gzip.open(article_path, "wb") as f:
        f.write(b"plain gzip contents")

    documents = list(to_dolma.load_article_src(str(article_path), "1234.5678"))

    assert documents == [("plain gzip contents", None)]
