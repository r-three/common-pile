import json
from dolma.core.data_types import DocResult, Document, Span, TextSlice
from dolma.core.utils import split_paragraphs
from dolma import add_tagger, BaseTagger

class LineTagger(BaseTagger):
    def __init__(self, lines):
        self.lines = set([l.strip() for l in lines])
    
    def predict_slice(self, text_slice: TextSlice) -> Span:
        if text_slice.text.strip() in self.lines:
            return Span(start=text_slice.start, end=text_slice.end, type="line", score=1.0)

    def predict(self, doc: Document) -> DocResult:
        spans = []
        units = split_paragraphs(doc.text)
        for unit in units:
            pred = self.predict_slice(unit)
            if pred:
                spans.append(pred)
        return DocResult(doc=doc, spans=spans)


@add_tagger("usgpo_line_tagger")
class usgpoLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/usgpo.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("biodiversity-heritage-library_line_tagger")
class biodiversity_heritage_libraryLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/biodiversity-heritage-library.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)


@add_tagger("stackexchange-dolma_line_tagger")
class stackexchange_dolmaLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/stackexchange-dolma.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("public-domain-review_line_tagger")
class public_domain_reviewLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/public-domain-review.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("news-dolma_line_tagger")
class news_dolmaLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/news-dolma.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("licensed_pubmed_line_tagger")
class licensed_pubmedLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/licensed_pubmed.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("uk_hansard_line_tagger")
class uk_hansardLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/uk_hansard.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("ubuntu-chat-dolma_line_tagger")
class ubuntu_chat_dolmaLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/ubuntu-chat-dolma.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("USPTO_line_tagger")
class USPTOLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/USPTO.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("ca_hansard_line_tagger")
class ca_hansardLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/ca_hansard.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("wiki-dolma_line_tagger")
class wiki_dolmaLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/wiki-dolma.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("public_library_1929_dolma_line_tagger")
class public_library_1929_dolmaLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/public_library_1929_dolma.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("project_gutenberg-dolma_line_tagger")
class project_gutenberg_dolmaLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/project_gutenberg-dolma.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
    

@add_tagger("regulations_line_tagger")
class regulationsLineTagger(LineTagger):
    def __init__(self):
        with open("./line_tagger_configs/regulations.json", "r") as f:
            lines = json.load(f)
        super().__init__(lines)
