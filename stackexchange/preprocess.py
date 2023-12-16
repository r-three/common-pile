"""Preprocess stack exchange data."""

import argparse
import collections
import dataclasses
import datetime
import functools
import itertools
import logging
import multiprocessing as mp
import os
import shelve
import urllib.parse
from dataclasses import dataclass
from typing import List
from xml.dom import minidom
from xml.etree import ElementTree as ET

import bs4
import commonmark
import tqdm
from markdown_it import MarkdownIt

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Parse a stack exchange dump.")
parser.add_argument("--input", help="Path to the dump, data/dump/${site}")
parser.add_argument(
    "--output", help="Path to the output, data/stack-exchange/v0/${site}/documents"
)
parser.add_argument(
    "--processes",
    default=mp.cpu_count(),
    help="The number of multicore processors to use.",
)
parser.add_argument("--shelve", action="store_true", help="...")

# Use over commonmark library as that is deprecated and has errors parsing stack overflow.
MD = MarkdownIt("commonmark", {"breaks": True, "html": True})


@dataclass
class Post:
    text: str


@dataclass
class Comment(Post):
    author: str


@dataclass
class Answer(Post):
    authors: List[str]
    comments: List[Comment]


@dataclass
class Question(Post):
    id: str
    authors: List[str]
    comments: List[Comment]
    date: datetime.datetime
    answers: List[Answer] = dataclasses.field(default_factory=list)


def parse_document(path: str):
    """Iterable version of xml parsing, lets us not load the whole thing at once.

    See https://web.archive.org/web/20201111201837/http://effbot.org/zone/element-iterparse.htm
    form more details on what it is doing.
    """
    context = ET.iterparse(path, events=("start", "end"))
    context = iter(context)
    event, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == "row":
            yield elem
            root.clear()


def get_attr(xml_obj, key):
    if key in xml_obj.attrib:
        return xml_obj.attrib[key]
    return None


def get_html_text(html):
    soup = bs4.BeautifulSoup(html, "html.parser")
    return soup.get_text()


def get_body_text(xml_obj):
    return get_html_text(get_attr(xml_obj, "Body"))


def get_markdown_text(xml_obj):
    return get_html_text(MD.render(get_attr(xml_obj, "Text")))
    # return get_html_text(commonmark.commonmark(get_attr(xml_obj, "Text")))


def process_user(user, site):
    user_id = get_attr(user, "Id")
    if user_id == -1:
        return None, None
    return user_id, {
        stackexchange_url(site, user_id, "users"),
        get_attr(user, "DisplayName"),
    }


def process_revision(revision):
    user_id = get_attr(revision, "Id")
    if user_id in (-1, None):
        return None, None
    return get_attr(revision, "PostId"), user_id


def process_comment(comment):
    return (
        get_attr(comment, "PostId"),
        get_attr(comment, "UserId"),
        get_markdown_text(comment),
    )


def process_question(question):
    if get_attr(question, "PostTypeId") != "1":
        return None, None, None
    post_id = get_attr(question, "Id")
    text = f"{get_attr(question, 'Title')}\n{get_body_text(question)}"
    date = datetime.datetime.fromisoformat(
        get_attr(question, "CreationDate").split(".")[0]
    )
    return post_id, text, date


def process_answer(answer):
    if get_attr(answer, "PostTypeId") != "2":
        return None, None, None
    question_id = get_attr(answer, "ParentId")
    answer_id = get_attr(answer, "Id")
    text = get_body_text(answer)
    return question_id, answer_id, text


def stackexchange_license(date):
    """See https://stackoverflow.com/help/licensing"""
    # Use datetime instead of date so we can do a comparison.
    if date < datetime.datetime(2011, 4, 8, 0, 0, 0):
        # Update to 2.5
        return PermissiveLicenses.CC_BY_SA_3
    if date < datetime.datetime(2018, 5, 2, 0, 0, 0):
        return PermissiveLicenses.CC_BY_SA_3
    return PermissiveLicenses.CC_BY_SA


def stackexchange_url(site, id, collection: str = "questions"):
    return urllib.parse.quote(f"https://{site}/{collection}/{id}", safe=":/")


def format_dolma(question, site):
    all_authors = set(
        itertools.chain(
            # Authors of the questions
            question.authors,
            # Authors for each answer
            *(ans.authors for ans in question.answers),
            # Authors for each comment on the question
            *(c.author for c in question.comments if c.author is not None),
            # Authors for each comment on answers for the questions
            *(c.author for a in question.answers for c in a.comments),
        )
    )
    text = "\n".join(
        itertools.chain(
            # Question text
            (question.text,),
            # Text for each comment on the question
            (c.text for c in question.comments),
            # Answer text + comment on answer text for each answer
            *(
                itertools.chain((a.text,), (c.text for c in a.comments))
                for a in question.answers
            ),
        )
    )
    return {
        "id": question.id,
        "text": text,
        # Source is more than just "Stack Exchange" as we want to use the question
        # id as the id which needs to be unique *per* source*.
        "source": "Stack Exchange",
        "added": datetime.datetime.utcnow().isoformat(),
        "created": question.date.isoformat(),
        "metadata": {
            "license": str(stackexchange_license(question.date)),
            "site": site,
            "url": stackexchange_url(site, question.id),
            "authors": sorted(all_authors),
        },
    }


def main(args):
    # Note: The Stack Exchage data doesn't lend itself to being shared into the
    # dolma format before the preprocessing is done, therefore we manually use
    # multiprocessing as we go to generate examples in parallel which are
    # eventually stored in the shared format.
    site = os.path.basename(args.input)
    os.makedirs(args.output, exist_ok=True)
    # TODO: Does setting the start method to `spawn` help reduce memory usage?
    # Note: We use iterables through out this to reduce memory usage, however,
    # we need to be sure that we *consume* the iterable output of the
    # multiprocessing pool *within* the pool context manager, otherwise the
    # pool will be "finalized" (deleted) before all the data is processed and
    # the program will hang.
    with mp.Pool(processes=args.processes) as pool:
        print("Building Lookup from user id -> user names")
        user_xml = parse_document(os.path.join(args.input, "Users.xml"))
        author_display = collections.defaultdict(set)
        for user_id, user_names in pool.imap(
            functools.partial(process_user, site=site), user_xml, chunksize=100
        ):
            if user_id is None:
                continue
            author_display[user_id].update(user_names)

        print("Building Lookup from post id -> authors")
        history_xml = parse_document(os.path.join(args.input, "PostHistory.xml"))
        # It would probably be better/faster to use a database to store these
        # intermediate lookups instead of a shelve (which requires multiple
        # pickle serialization/deserialization) but I didn't want to implement
        # a database based key-value store that supports list values, set values
        # and scalar values.
        if args.shelve:
            post_authors = shelve.open(os.path.join(args.output, "authors.shelve"))
        else:
            post_authors = {}
        for post_id, user_id in pool.imap(process_revision, history_xml, chunksize=100):
            if post_id is None:
                continue
            authors = post_authors.get(post_id, set())
            authors.update(author_display[user_id])
            # Get and assign so that values are written back to the shelve.
            post_authors[post_id] = authors

        print("Building Lookup from post/answer id -> comments")
        if args.shelve:
            comments = shelve.open(os.path.join(args.output, "comments.shelve"))
        else:
            comments = {}
        comment_xml = parse_document(os.path.join(args.input, "Comments.xml"))
        for post_id, user_id, text in pool.imap(
            process_comment, comment_xml, chunksize=100
        ):
            if post_id is None:
                continue
            comment = comments.get(post_id, [])
            comment.append(
                Comment(
                    text=text,
                    author=author_display[user_id],
                )
            )
            # Get and assign so that values are written back to the shelve.
            comments[post_id] = comment

        if args.shelve:
            parsed_dump = shelve.open(os.path.join(args.output, "questions.shelve"))
        else:
            parsed_dump = {}
        print("Parsing Questions")
        post_xml = parse_document(os.path.join(args.input, "Posts.xml"))
        for post_id, text, date in pool.imap(process_question, post_xml, chunksize=100):
            if post_id is None:
                continue
            parsed_dump[post_id] = Question(
                text=text,
                id=post_id,
                authors=post_authors[post_id],
                comments=comments.get(post_id, []),
                date=date,
            )

        print("Parsing Answers")
        # Reinitialize the iterator over the Posts
        post_xml = parse_document(os.path.join(args.input, "Posts.xml"))
        for question_id, answer_id, answer in pool.imap(
            process_answer, post_xml, chunksize=100
        ):
            if question_id is None:
                continue
            question = parsed_dump[question_id]
            question.answers.append(
                Answer(
                    text=answer,
                    authors=post_authors[answer_id],
                    comments=comments.get(answer_id, []),
                )
            )
            # Get and assign so that values are written back to the shelve.
            parsed_dump[question_id] = question

        print("Formatting Questions")
        examples = map(functools.partial(format_dolma, site=site), parsed_dump.values())
        to_dolma(examples, os.path.join(args.output, "documents"), "se.jsonl.gz")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
