"""Preprocess stack exchange data."""

import argparse
import collections
import dataclasses
import datetime
import functools
import itertools
import multiprocessing as mp
import operator as op
import os
import re
import shelve
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Sequence

import bs4
import tqdm
from markdown_it import MarkdownIt

import licensed_pile.xml as xml
from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Parse a stack exchange dump.")
parser.add_argument("--input", help="Path to the dump, data/dump/${site}")
parser.add_argument(
    "--output", help="Path to the output, data/stackexchange/v0/${site}/documents"
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
parser.add_argument(
    "--skip_comments",
    action="store_false",
    dest="include_comments",
    help="Should we skip including the comments in the text?",
)
parser.add_argument(
    "--sort",
    choices=("time", "votes"),
    default="votes",
    help="How should answers be sorted?",
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
    license: PermissiveLicenses


@dataclass
class Comment(Post):
    author: str


@dataclass
class Answer(Post):
    authors: List[str]
    comments: List[Comment]
    score: int
    accepted: bool


@dataclass
class Question(Post):
    id: str
    authors: List[str]
    comments: List[Comment]
    accepted_answer: int
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
      The license that applies to the comment
    """
    return (
        get_attr(comment, "PostId"),
        get_attr(comment, "UserId"),
        get_markdown_text(comment),
        get_date(get_attr(comment, "CreationDate")),
        stackexchange_license(get_attr(comment, "ContentLicense")),
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
      The id of the accepted answer
    """
    if get_attr(question, "PostTypeId") != "1":
        return None, None, None, None, None
    post_id = get_attr(question, "Id")
    text = f"{get_attr(question, 'Title')}\n{get_body_text(question)}"
    date = get_date(get_attr(question, "CreationDate"))
    license = stackexchange_license(get_attr(question, "ContentLicense"))
    accepted = get_attr(question, "AcceptedAnswerId")
    return post_id, text, date, license, accepted


def process_answer(answer):
    """Extract answer information from xml.

    Returns:
      The id of the question this answer is for
      The id of the answer
      The text of the answer
      The date the answer was given
      The score the answer got
      The license the applies to the Answer.
    """
    if get_attr(answer, "PostTypeId") != "2":
        return None, None, None, None, None, None
    question_id = get_attr(answer, "ParentId")
    answer_id = get_attr(answer, "Id")
    text = get_body_text(answer)
    date = get_date(get_attr(answer, "CreationDate"))
    # TODO: Is it possible to have a score that isn't an int (e.g. None)?
    score = int(get_attr(answer, "Score"))
    license = stackexchange_license(get_attr(answer, "ContentLicense"))
    return question_id, answer_id, text, date, score, license


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


def format_dolma(question, site, extra_metadata: Dict[str, str]):
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
    all_licenses = itertools.chain(
        (question.license,),
        (c.license for c in question.comments),
        *(
            itertools.chain((a.license,), (c.license for c in a.comments))
            for a in question.answers
        ),
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
            "all_licenses": sorted(map(str, set(all_licenses))),
            **extra_metadata,
        },
    }


def vote_sort(answers: Sequence[Answer]) -> List[Answer]:
    """Sort based on votes.

    Note: Not a stable sort.
    """

    def _cmp_answers(a, b):
        # If one of the answers is accepted, that should go first.
        if a.accepted:
            return -1
        if b.accepted:
            return 1
        return b.score - a.score

    return sorted(answers, key=functools.cmp_to_key(_cmp_answers))


def find_file(directory: str, file_name: str) -> str:
    """Some dumps use lowercase files names :/"""
    for f in (file_name, file_name.lower()):
        if os.path.exists(path := os.path.join(directory, f)):
            return path
    logger = logs.configure_logging("stackexchange")
    logger.error(f"Filed to find {file_name} in {directory}")
    raise ValueError(f"Failed to find {file_name} in {directory}")


def main(args):
    logger = logs.configure_logging("stackexchange")
    # Note: The Stack Exchage data doesn't lend itself to being shared into the
    # dolma format before the preprocessing is done, therefore we manually use
    # multiprocessing as we go to generate examples in parallel which are
    # eventually stored in the dolma format.
    # Make sure the ending the input dir with a `/` doesn't results in an empty
    # string as the site value.
    site = os.path.basename(re.sub(r"/$", "", args.input))
    os.makedirs(args.output, exist_ok=True)

    date_sort = functools.partial(sorted, key=op.attrgetter("date"))
    # Comments are always sorted by date
    sort_comments = date_sort
    if args.sort == "time":
        logger.info("Answers will be sorted based on the date.")
        sort_answers = date_sort
    else:
        logger.info("Answers will be sorted based on votes (accepted answer first).")
        sort_answers = vote_sort

    # TODO: Does setting the start method to `spawn` help reduce memory usage?
    # Note: We use iterables through out this to reduce memory usage, however,
    # we need to be sure that we *consume* the iterable output of the
    # multiprocessing pool *within* the pool context manager, otherwise the
    # pool will be "finalized" (deleted) before all the data is processed and
    # the program will hang.
    with mp.Pool(processes=args.processes) as pool:
        logger.info("Building Lookup from user id -> user names")
        user_xml = xml.iterate_xml(find_file(args.input, "Users.xml"), "row")
        # This table is fairly small so we don't need to create a shelve for it.
        author_display = collections.defaultdict(set)
        for user_id, user_names in pool.imap_unordered(
            functools.partial(process_user, site=site), user_xml, chunksize=100
        ):
            if user_id is None:
                continue
            author_display[user_id].update(user_names)

        logger.info("Building Lookup from post id -> authors")
        history_xml = xml.iterate_xml(find_file(args.input, "PostHistory.xml"), "row")
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

        # Even if we are going to skip including the comments in the output, we
        # still create the comment lookup date. Accesses to it later have
        # default values of empty lists so an empty look up table will result
        # in no comments being included. Even though we make the lookup table,
        # we do skip filling it with processed comments if they are going to be
        # skipped later.
        if args.shelve:
            comments = shelve.open(os.path.join(args.output, "comments.shelve"))
        else:
            comments = {}
        if args.include_comments:
            logger.info("Building Lookup from post/answer id -> comments")
            comment_xml = xml.iterate_xml(find_file(args.input, "Comments.xml"), "row")
            for post_id, user_id, text, date, license in pool.imap_unordered(
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
                        license=license,
                    )
                )
                # Get and assign so that values are written back to the shelve.
                comments[post_id] = comment
            # Sort comments based on creation date, then when we add them to the text
            # we know that they will be in the correct order, even if they are out
            # of order in the dump/from multiprocessing.
            # Explicit loop instead of a comprehension because it might be a shelve :(
            for cid, cs in comments.items():
                comments[cid] = sort_comments(cs)
        else:
            logger.info("Comments will not be included in the text output.")

        if args.shelve:
            parsed_dump = shelve.open(os.path.join(args.output, "questions.shelve"))
        else:
            parsed_dump = {}

        # Questions are the "document" level for this dataset, therefore we do
        # no need to sort them.
        logger.info("Parsing Questions")
        post_xml = xml.iterate_xml(find_file(args.input, "Posts.xml"), "row")
        for post_id, text, date, license, accepted_id in pool.imap_unordered(
            process_question, post_xml, chunksize=100
        ):
            if post_id is None:
                continue
            if post_id not in post_authors:
                logger.warning(
                    f"Failed to find authors associated with post: {post_id}"
                )
            parsed_dump[post_id] = Question(
                text=text,
                id=post_id,
                authors=post_authors.get(post_id, {"Unknown"}),
                # Comments are sorted in chronological order.
                comments=comments.get(post_id, []),
                date=date,
                license=license,
                accepted_answer=accepted_id,
            )

        logger.info("Parsing Answers")
        # Reinitialize the iterator over the Posts as it was consumed when
        # looking for questions. We do this as a second pass so we know that
        # there will always be a question we can attach this answer to.
        post_xml = xml.iterate_xml(find_file(args.input, "Posts.xml"), "row")
        for question_id, answer_id, answer, date, score, license in pool.imap_unordered(
            process_answer, post_xml, chunksize=100
        ):
            if question_id is None:
                continue
            if answer_id not in post_authors:
                logger.warning(
                    f"Failed to find authors assocaited with answer: {answer_id}"
                )
            question = parsed_dump[question_id]
            question.answers.append(
                Answer(
                    text=answer,
                    authors=post_authors.get(answer_id, {"Unknown"}),
                    # Comments are sorted in chronological order.
                    comments=comments.get(answer_id, []),
                    date=date,
                    license=license,
                    score=score,
                    accepted=question.accepted_answer == answer_id,
                )
            )
            # Get and assign so that values are written back to the shelve.
            parsed_dump[question_id] = question

        # Sort answers to questions based on creation date, when when they are
        # added to the question text we know they will be in the correct order,
        # even if they are out of order in the dump/from multiprocessing.
        # Explicit loop instead of a compreshension because it might be a shelve :(
        for qid, q in parsed_dump.items():
            q.answers = sort_answers(q.answers)
            parsed_dump[qid] = q

        # Use iterators so we don't need to have the full dataset loaded at once.
        logger.info("Formatting Questions as Dolma Documents")
        # Even on rather large datasets, such as askubuntu.com, and shelves it
        # was faster to do the comment/answer sorting and run format dolma in
        # the main process. I assume the cost to serialize and decerialize the
        # question is large and especially when the main process is the only
        # writer.
        examples = map(
            functools.partial(
                format_dolma,
                site=site,
                extra_metadata={
                    "sort": args.sort,
                    "include_comments": args.include_comments,
                },
            ),
            parsed_dump.values(),
        )
        to_dolma(examples, os.path.join(args.output, "documents"), "se.jsonl.gz")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("stackexchange")
    main(args)
