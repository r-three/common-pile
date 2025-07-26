import json
import re
from dolma.core.data_types import DocResult, Document, Span, TextSlice
from dolma.core.utils import split_paragraphs
from dolma import add_tagger, BaseTagger

class RegexDocumentTagger(BaseTagger):
    def __init__(self, patterns):
        self.patterns = patterns

    def predict(self, doc: Document) -> DocResult:
        spans = []
        for pattern in self.patterns:
            for m in re.finditer(pattern, doc.text):
                spans.append(Span(start=m.start(), end=m.end(), type="regex", score=1.0))
        return DocResult(doc=doc, spans=spans)
                    

class RegexTagger(BaseTagger):
    def __init__(self, patterns):
        self.patterns = patterns
    
    def predict_slice(self, text_slice: TextSlice) -> Span:
        if any([re.match(p, text_slice.text) for p in self.patterns]):
            return Span(start=text_slice.start, end=text_slice.end, type="regex", score=1.0)

    def predict(self, doc: Document) -> DocResult:
        spans = []
        units = split_paragraphs(doc.text)
        for unit in units:
            pred = self.predict_slice(unit)
            if pred:
                spans.append(pred)
        return DocResult(doc=doc, spans=spans)


@add_tagger("double_newline_tagger")
class DoubleNewLineTagger(RegexDocumentTagger):
    def __init__(self):
        super().__init__(["\n\n"])


@add_tagger("double_space_tagger")
class DoubleSpaceTagger(RegexDocumentTagger):
    def __init__(self):
        super().__init__(["  "])

 
@add_tagger("loc_books_dolma_regex_tagger")
class loc_books_dolmaRegexTagger(RegexTagger):
    def __init__(self):
        patterns = [r"^\s*\d+\s*$"]
        super().__init__(patterns)
    


@add_tagger("public_library_1929_dolma_regex_tagger")
class public_library_1929_dolmaRegexTagger(RegexTagger):
    def __init__(self):
        patterns = [r"^\s*\d+\s*$"]
        super().__init__(patterns)


@add_tagger("regulations_regex_tagger")
class regulations_RegexTagger(RegexTagger):
    def __init__(self):
        patterns = [r"^\s*\[\[Page [0-9]+\]\]\s*$"]
        super().__init__(patterns)


@add_tagger("usgpo_regex_tagger")
class usgpo_RegexTagger(RegexTagger):
    def __init__(self):
        patterns = [r"^\s*\[\[Page [0-9A-Za-z]+\]\]\s*$"]
        super().__init__(patterns)
