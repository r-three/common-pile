import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Iterator

import lxml
import lxml.etree as ET
import regex

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Collect UK-Hansard into Dolma format")
parser.add_argument(
    "--base_folder",
    default="hansard/uk_parlparse/scrapedxml",
    help="Path to the directory of UK-Hansard XML files.",
)
parser.add_argument(
    "--output_dir",
    default="hansard/dolma_outputs",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)

parser.add_argument("--count", action="store_true", help="Count the number of words.")

FILE_NAMES = {
    "debates": {"source": "commons-debates"},
    "london-mayors-questions": {
        "source": "london-mayors-questions",
    },
    "lordspages": {"source": "lords-debates"},
    "lordswms": {
        "source": "lords-written-ministerial-statements",
    },
    "lordswrans": {
        "source": "lords-written-answers",
    },
    "ni": {"source": "northern-ireland-assembly"},
    "sp": {"source": "scottish-parliament"},
    "sp-written": {
        "source": "scottish-parliament-written-answers",
    },
    "standing": {"source": "standing-committees"},
    "westminister": {"source": "westminister-hall"},
    "wms": {"source": "written-ministerial-statements"},
    "wrans": {"source": "written-answers"},
    "senedd": {
        "cy": {"source": "senedd-cy"},
        "en": {"source": "senedd-en"},
        "source": "senedd",
    },
}
WHITESPACE = regex.compile(r"\w+|[^\w\s]+")
COUNT = 0

PARSER = lxml.etree.XMLParser(
    encoding="utf-8",
    recover=True,
)


def get_subfolders(folder_path: str | Path) -> list[Path]:
    if isinstance(folder_path, str):
        folder_path = Path(folder_path)
    subfolders = [f for f in folder_path.iterdir() if f.is_dir()]
    return subfolders


def parse_hansard_xml_file(root: ET._Element) -> str:
    parsed_text = []
    for element in root.iter():
        if element.tag == "major-heading":
            text = ET.tostring(element, method="text", encoding="unicode").strip()
            parsed_text.append(f"{text}")
        elif element.tag in ["oral-heading", "minor-heading"]:
            text = ET.tostring(element, method="text", encoding="unicode").strip()
            parsed_text.append(f"{text}")
        elif element.tag in ["division"]:
            continue
        elif element.tag in [
            "speech",
            "ques",
            "reply",
            "question",
        ]:
            speaker = (
                element.attrib.get("speakername", "") + ": "
                if element.attrib.get("speakername", "")
                else ""
            )
            speech_text = (
                ET.tostring(element, method="text", encoding="unicode")
                .strip()
                .replace("    ", "")
            )
            if not "table" in [x.tag for x in element]:
                parsed_text.append(f"{speaker + speech_text}")

    # The space character in the text is a non-breaking space character "NBSP"
    return "\n\n".join(parsed_text).replace(" ", "")


def process_files_in_folder(
    folder_path: Path, source: str, count: bool = False
) -> dict:
    global COUNT
    language = "en" if not source == "senedd-cy" else "cy"
    date_match = re.compile(r"\d{4}-\d{2}-\d{2}")
    for file in folder_path.iterdir():
        if file.suffix == ".xml" and file.stem != "tmp":
            root = ET.parse(file, parser=PARSER).getroot()
            parsed_text = parse_hansard_xml_file(root)
            if parsed_text:
                if count:
                    COUNT += len(WHITESPACE.split(parsed_text))
                date_ = date_match.search(file.stem)
                if date_:
                    date = date_.group()
                else:
                    date = "9999-01-01"
                yield {
                    "id": file.stem,
                    "text": parsed_text,
                    "created": date,
                    "source": f"uk-hansard-{source}",
                    "added": str(datetime.now().date()),
                    "metadata": {
                        "license": str(PermissiveLicenses.OPL),
                        "language": language,
                        "year": date.split("-")[0],
                    },
                }


def process_folder(folder_path: Path, count: bool = False) -> Iterator[dict]:
    for subfolder in get_subfolders(folder_path):
        if subfolder.name in FILE_NAMES:
            source = FILE_NAMES[subfolder.name]["source"]
            if source == "senedd":
                for nested_subfolder in get_subfolders(subfolder):
                    source = FILE_NAMES["senedd"][nested_subfolder.name]["source"]
                    yield from process_files_in_folder(
                        nested_subfolder, source, count=count
                    )
            else:
                yield from process_files_in_folder(subfolder, source)


def main(args):
    base_folder = Path(args.base_folder)
    to_dolma(
        examples=process_folder(base_folder, count=args.count),
        path=args.output_dir,
        shard_size=args.shard_size,
        filename="ukhansard.jsonl.gz",
    )


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
    if args.count:
        print(COUNT)
