import os
from typing import Dict, List, Optional


def parse_record(record: Dict[str, str]):
    postprocess_fns = {
        "Content Files": parse_files,
        "Attachment Files": parse_files,
        "Topics": parse_topics,
    }

    keep_fields = {
        "Notice": [
            "Document ID",
            "Document Type",
            "Posted Date",
            "Title",
            "Content Files",
        ],
        "Rule": [
            "Document ID",
            "Document Type",
            "Posted Date",
            "Title",
            "Topics",
            "Content Files",
        ],
        "Proposed Rule": [
            "Document ID",
            "Document Type",
            "Posted Date",
            "Title",
            "Content Files",
        ],
        "Supporting & Related Material": [
            "Document ID",
            "Document Type",
            "Posted Date",
            "Content Files",
        ],
    }

    rename_fields = {
        "Notice": {},
        "Rule": {},
        "Proposed Rule": {},
        "Supporting & Related Material": {},
    }

    doc_type = record["Document Type"]
    if doc_type in keep_fields and doc_type in rename_fields:
        parsed_record = parse(
            record,
            keep_fields=keep_fields[doc_type],
            rename_fields=rename_fields[doc_type],
        )
        parsed_record = {
            k: (postprocess_fns[k](v) if k in postprocess_fns else v)
            for k, v in parsed_record.items()
        }

        doc_id = parsed_record["Document ID"]
        return doc_id, parsed_record
    else:
        return None


def parse(
    record: Dict[str, str],
    keep_fields: List[str] = [],
    rename_fields: Dict[str, str] = {},
):
    filtered_record = {k: v for k, v in record.items() if k in keep_fields}
    renamed_filtered_record = {
        (rename_fields[k] if k in rename_fields else k): v
        for k, v in filtered_record.items()
    }
    return renamed_filtered_record


def parse_files(files: Optional[str]):
    return (
        [{"URL": f, "File Type": os.path.splitext(f)[1]} for f in files.split(",")]
        if files is not None
        else []
    )


def parse_topics(topics: Optional[str]):
    return topics.split(",") if topics is not None else []
