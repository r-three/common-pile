from typing import Any, Dict, List, Optional, Tuple, Union

from dolma.core.data_types import Document, DocResult, Span
from dolma import add_tagger, BaseTagger
import numpy as np
from blingfire import text_to_words
from cached_path import cached_path


GOOGLE_1T_CORPUS = (
    "https://ai2-s2-research-public.s3-us-west-2.amazonaws.com/lucas/google-1T-unigram/unigram_freq.csv"
)


class UnigramPerplexityPredictor:
    """Predicts the perplexity of a passage based on the unigram distribution
    probability of the words in a large corpus."""

    UNK = "<unk>"

    def __init__(self, word_counts_path: str = GOOGLE_1T_CORPUS):
        local_word_counts_path = cached_path(word_counts_path)
        with open(local_word_counts_path) as f:
            word_counts = {
                word: int(count) for word, count in (line.strip().split(",", 1) for line in f) if count.isnumeric()
            }

        word_total = sum(word_counts.values())
        word_total_log = np.log2(word_total)
        self.words_logp = {word: np.log2(count) - word_total_log for word, count in word_counts.items()}

        # <unk> token has fictional count of √vocab_size + 1
        self.words_logp[self.UNK] = np.log2(np.sqrt(len(self.words_logp)) + 1) - word_total_log

    def log_p(self, word: str) -> float:
        return self.words_logp.get(word.lower(), self.words_logp[self.UNK])

    def predict(self, text: Union[str, List[str]]) -> float:
        if isinstance(text, str):
            text = text_to_words(text).split()

        log_prob = sum(self.log_p(word) / len(text) for word in text)
        return log_prob


@add_tagger("perplexity_tagger")
class PerplexityTagger(BaseTagger):
    def __init__(self):
        self.model = UnigramPerplexityPredictor()

    def predict(self, doc: Document) -> DocResult:
        ppl = self.model.predict(doc.text)
        span = Span(
            start=0,
            end=len(doc.text),
            type="perplexity",
            score=ppl
        )
        return DocResult(doc=doc, spans=[span])
