"""Preprocess stack exchange data."""

import argparse
import collections
import dataclasses
import datetime
import functools
import itertools
import logging
import multiprocessing as mp
import operator as op
import os
import shelve
import urllib.parse
from dataclasses import dataclass
from typing import List

import bs4
import tqdm
from markdown_it import MarkdownIt

import licensed_pile.xml as xml
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
parser.add_argument(
    "--shelve",
    action="store_true",
    help="Save lookup tables as shelves so we don't need to keep them all in memory.",
)

# Use over commonmark library as that is deprecated and has errors parsing stack overflow.
MD = MarkdownIt("commonmark", {"breaks": True, "html": True})


LICENSES = {
    "CC BY-SA 2.5": PermissiveLicenses.CC_BY_SA_2_5,
    "CC BY-SA 3.0": PermissiveLicenses.CC_BY_SA_3,
    "CC BY-SA 4.0": PermissiveLicenses.CC_BY_SA,
}


@dataclass
class Post:
    text: str
    date: datetime.datetime


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
    license: PermissiveLicenses
    answers: List[Answer] = dataclasses.field(default_factory=list)


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
    # The original commonmark library used is not maintained anymore and has
    # issues with some of the data.
    # return get_html_text(commonmark.commonmark(get_attr(xml_obj, "Text")))


def process_user(user, site):
    """Extract user information from xml.

    Returns:
      The url to the user's page on stack exchange, the username.
    """
    user_id = get_attr(user, "Id")
    if user_id == -1:
        return None, None
    return user_id, {
        stackexchange_url(site, user_id, "users"),
        get_attr(user, "DisplayName"),
    }


def process_revision(revision):
    """Extract post revision information from xml.

    Returns:
      The id of the post and the id of the user who made the post.
    """
    user_id = get_attr(revision, "Id")
    if user_id in (-1, None):
        return None, None
    return get_attr(revision, "PostId"), user_id


def process_comment(comment):
    """Extract comment information from xml.

    Returns:
      The id for the comment
      The id for the user who made the comment
      The text of the comment
      The date the comment as created
    """
    return (
        get_attr(comment, "PostId"),
        get_attr(comment, "UserId"),
        get_markdown_text(comment),
        get_date(get_attr(comment, "CreationDate")),
    )


def get_date(ts: str) -> datetime.datetime:
    # TODO: Add better error handling?
    return datetime.datetime.fromisoformat(ts.split(".")[0])


def process_question(question):
    """Extract question information from xml.

    Returns:
      The id of the question
      The text of the question (title + content)
      The date the question was posted
      The license that applies to the question
    """
    if get_attr(question, "PostTypeId") != "1":
        return None, None, None, None
    post_id = get_attr(question, "Id")
    text = f"{get_attr(question, 'Title')}\n{get_body_text(question)}"
    date = get_date(get_attr(question, "CreationDate"))
    license = stackexchange_license(get_attr(question, "ContentLicense"))
    return post_id, text, date, license


def process_answer(answer):
    """Extract answer information from xml.

    Returns:
      The id of the question this answer is for
      The id of the answer
      The text of the answer
      The date the answer was given
    """
    if get_attr(answer, "PostTypeId") != "2":
        return None, None, None, None
    question_id = get_attr(answer, "ParentId")
    answer_id = get_attr(answer, "Id")
    text = get_body_text(answer)
    date = get_date(get_attr(answer, "CreationDate"))
    return question_id, answer_id, text, date


def stackexchange_license(license):
    """For a rough idea of date based licenses see
       https://stackoverflow.com/help/licensing.

    Note:
      Each comment, answer, and question have an attached ContentLicense,
      but we are currently just using the Question License for the document
      license.

    TODO: Add filtering based on license type (do any answer/comment/question
      have licenses that aren't permissive?)
    """
    return LICENSES.get(license, license)


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
            "license": str(question.license),
            "site": site,
            "url": stackexchange_url(site, question.id),
            "authors": sorted(all_authors),
        },
    }


def main(args):
    # Note: The Stack Exchage data doesn't lend itself to being shared into the
    # dolma format before the preprocessing is done, therefore we manually use
    # multiprocessing as we go to generate examples in parallel which are
    # eventually stored in the dolma format.
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
        user_xml = xml.iterate_xml(os.path.join(args.input, "Users.xml"), "row")
        # This table is fairly small so we don't need to create a shelve for it.
        author_display = collections.defaultdict(set)
        for user_id, user_names in pool.imap_unordered(
            functools.partial(process_user, site=site), user_xml, chunksize=100
        ):
            if user_id is None:
                continue
            author_display[user_id].update(user_names)

        print("Building Lookup from post id -> authors")
        history_xml = xml.iterate_xml(
            os.path.join(args.input, "PostHistory.xml"), "row"
        )
        # It would probably be better/faster to use a database to store these
        # intermediate lookups instead of a shelve (which requires multiple
        # pickle serialization/deserialization) but I didn't want to implement
        # a database based key-value store that supports list values, set values
        # and scalar values.
        if args.shelve:
            post_authors = shelve.open(os.path.join(args.output, "authors.shelve"))
        else:
            post_authors = {}
        for post_id, user_id in pool.imap_unordered(
            process_revision, history_xml, chunksize=100
        ):
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
        comment_xml = xml.iterate_xml(os.path.join(args.input, "Comments.xml"), "row")
        for post_id, user_id, text, date in pool.imap_unordered(
            process_comment, comment_xml, chunksize=100
        ):
            if post_id is None:
                continue
            comment = comments.get(post_id, [])
            comment.append(
                Comment(
                    text=text,
                    author=author_display[user_id],
                    date=date,
                )
            )
            # Get and assign so that values are written back to the shelve.
            comments[post_id] = comment
        # Sort comments based on creation date, then when we add them to the text
        # we know that they will be in the correct order, even if they are out
        # of order in the dump/from multiprocessing.
        # Explicit loop instead of a comprehension because it might be a shelve :(
        for cid, cs in comments.items():
            comments[cid] = sorted(cs, key=op.attrgetter("date"))

        if args.shelve:
            parsed_dump = shelve.open(os.path.join(args.output, "questions.shelve"))
        else:
            parsed_dump = {}

        # Questions are the "document" level for this dataset, therefore we do
        # no need to sort them.
        print("Parsing Questions")
        post_xml = xml.iterate_xml(os.path.join(args.input, "Posts.xml"), "row")
        for post_id, text, date, license in pool.imap_unordered(
            process_question, post_xml, chunksize=100
        ):
            if post_id is None:
                continue
            parsed_dump[post_id] = Question(
                text=text,
                id=post_id,
                authors=post_authors[post_id],
                # Comments are sorted in chronological order.
                comments=comments.get(post_id, []),
                date=date,
                license=license,
            )

        print("Parsing Answers")
        # Reinitialize the iterator over the Posts as it was consumed when
        # looking for questions. We do this as a second pass so we know that
        # there will always be a question we can attach this answer to.
        post_xml = xml.iterate_xml(os.path.join(args.input, "Posts.xml"), "row")
        for question_id, answer_id, answer, date in pool.imap_unordered(
            process_answer, post_xml, chunksize=100
        ):
            if question_id is None:
                continue
            question = parsed_dump[question_id]
            question.answers.append(
                Answer(
                    text=answer,
                    authors=post_authors[answer_id],
                    # Comments are sorted in chronological order.
                    comments=comments.get(answer_id, []),
                    date=date,
                )
            )
            # Get and assign so that values are written back to the shelve.
            parsed_dump[question_id] = question

        # Sort answers to questions based on creation date, when when they are
        # added to the question text we know they will be in the correct order,
        # even if they are out of order in the dump/from multiprocessing.
        # Explicit loop instead of a compreshension because it might be a shelve :(
        for qid, q in parsed_dump.items():
            q.answers = sorted(q.answers, key=op.attrgetter("date"))
            parsed_dump[qid] = q

        # Use iterators so we don't need to have the full dataset loaded at once.
        print("Formatting Questions as Dolma Documents")
        # Even on rather large datasets, such as askubuntu.com, it faster to
        # do the comment/answer sorting and run format dolma in the main process
        # I assume the cost to serialize and decerialize the question is large
        # and especially when the main process is the only writer.
        examples = map(functools.partial(format_dolma, site=site), parsed_dump.values())
        to_dolma(examples, os.path.join(args.output, "documents"), "se.jsonl.gz")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
