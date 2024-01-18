"""Tests for stack exchange preprocessing.

Note: We don't test for the cases where there are multiple accepted answer as
it itsn't possible given how the accepted field if set based on a comparing the
answer id to an int field of the question. If there was ever a case where both
answers were accepted, the sort would be result in the being in the same order
they were in originally.
"""

import random

from preprocess import Answer, vote_sort


def test_vote_sort_low_accepted_is_first():
    # A should be first as it is accepted, even with a low score.
    a = Answer(
        authors=[], comments=[], score=100, accepted=True, date="", license="", text=""
    )
    b = Answer(
        authors=[],
        comments=[],
        score=1000,
        accepted=False,
        date="",
        license="",
        text="",
    )
    assert vote_sort([b, a]) == [a, b]


def test_vote_sort_high_accepted_is_first():
    # B should be first as it is accepted and it has high scores
    a = Answer(
        authors=[], comments=[], score=100, accepted=False, date="", license="", text=""
    )
    b = Answer(
        authors=[], comments=[], score=1000, accepted=True, date="", license="", text=""
    )
    assert vote_sort([b, a]) == [b, a]


def test_vote_sort_no_accepted_is_ordered():
    answers = [
        Answer(
            authors=[],
            comments=[],
            score=random.randint(10, 1000),
            accepted=False,
            date="",
            license="",
            text="",
        )
        for _ in range(random.randint(5, 10))
    ]
    prev_score = 100000
    for answer in vote_sort(answers):
        assert answer.score <= prev_score
        prev_score = answer.score
