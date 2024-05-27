import os

import lxml.etree as ET
from tqdm import tqdm


def parse_hansard_xml(root, speech: list):
    parsed_text = []
    for element in root.iter():
        if element.tag == "major-heading":
            text = ET.tostring(element, method="text", encoding="unicode").strip()
            parsed_text.append(f"{text}")
        elif element.tag in ["oral-heading", "minor-heading"]:
            text = ET.tostring(element, method="text", encoding="unicode").strip()
            parsed_text.append(f"{text}")
        elif element.tag in speech:
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
            parsed_text.append(f"{speaker + speech_text}")

    return "\n\n".join(parsed_text)


if __name__ == "__main__":
    DIR = ""
    # loop over each xml file in the directory and save the parsed text to a new file
    for file in tqdm(os.listdir(DIR)):
        if file.endswith(".xml"):
            root = ET.parse(os.path.join(DIR, file)).getroot()
            parsed_text = parse_hansard_xml(root, speech=["question", "reply"]).replace(
                " ", ""
            )
            with open(
                f"/Users/baber/PycharmProjects/licensed-pile/hansard/uk_processed/london-mayors-questions/{file.replace('.xml', '.txt')}",
                "w",
            ) as f:
                f.write(parsed_text)
    # root = ET.parse('/Users/baber/Downloads/uk_parlparse/scrapedxml/debates/debates2021-03-09c.xml').getroot()
    # print(parse_hansard_xml(root))
