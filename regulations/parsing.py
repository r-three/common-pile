import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel


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
        # "Public Submission": ["Comment on Document ID", "Document Type", "Posted Date", "Comment", "Attachment Files"],
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
        # "Public Submission": {"Comment on Document ID": "Document ID"},
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


def parse_notice(record: Dict[str, str]):
    parsed_record = {
        k: v
        for k, v in record.items()
        if k in ["Document ID", "Posted Date", "Title", "Content Files"]
    }
    parsed_record["Content Files"] = parse_files(parsed_records["Content Files"])
    return parsed_record


def parse_rule(record: Dict[str, str]):
    parsed_record = {
        k: v
        for k, v in record.items()
        if k in ["Document ID", "Posted Date", "Title", "Content Files"]
    }
    return Rule(
        document_id=record["Document ID"],
        posted_date=record["Posted Date"],
        title=record["Title"],
        topics=parse_topics(record["Topics"]),
        content_files=parse_files(record["Content Files"]),
    )


def parse_proposed_rule(record: pd.Series):
    return ProposedRule(
        document_id=record["Document ID"],
        posted_date=record["Posted Date"],
        title=record["Title"],
        content_files=parse_files(record["Content Files"]),
    )


def parse_public_submission(record: pd.Series):
    return PublicSubmission(
        document_id=record["Comment on Document ID"],
        posted_date=record["Posted Date"],
        text=record["Comment"],
        attachment_files=parse_files(record["Attachment Files"]),
    )


def parse_supporting_material(record: pd.Series):
    return SupportingAndRelatedMaterial(
        document_id=record["Document ID"],
        posted_date=record["Posted Date"],
        content_files=parse_files(record["Content Files"]),
    )


class File(BaseModel):
    url: str
    filetype: str


class Notice(BaseModel):
    document_id: str
    posted_date: datetime
    title: str
    content_files: List[File]


class Rule(BaseModel):
    document_id: str
    posted_date: datetime
    title: str
    topics: List[str]
    content_files: List[File]


class ProposedRule(BaseModel):
    document_id: str
    posted_date: datetime
    title: str
    content_files: List[File]


class PublicSubmission(BaseModel):
    document_id: str
    posted_date: datetime
    text: str
    attachment_files: List[File]


class SupportingAndRelatedMaterial(BaseModel):
    document_id: str
    posted_date: datetime
    content_files: List[File]


class Other(BaseModel):
    document_id: str
    content_files: List[File]
