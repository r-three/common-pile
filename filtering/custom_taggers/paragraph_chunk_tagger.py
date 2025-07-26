import re

from dolma.core.data_types import DocResult, Document, Span, TextSlice
from dolma.core.utils import split_paragraphs
from dolma import add_tagger, BaseTagger
from rapidfuzz.fuzz import ratio


@add_tagger("paragraph_chunk_tagger")
class ParagraphChunkTagger(BaseTagger):
    def predict(self, doc: Document) -> DocResult:
        def get_text_blocks(lines, min_line_length=30, min_lines_in_block=3):
            mask = []

            valid_lines = [len(line.text.strip()) > min_line_length for line in lines]
            curr_index = 0
            while curr_index < len(lines):
                # Detect blocks of text that look like paragraphs
                if all(valid_lines[curr_index: curr_index + min_lines_in_block]):
                    # Get the whole block
                    while curr_index < len(lines) and valid_lines[curr_index]:
                        mask.append(True)
                        curr_index += 1
                    # Detect a short line ending a text block
                    if curr_index < len(lines) and len(lines[curr_index].text.strip()) > 0:
                        mask.append(True)
                        curr_index += 1
                else:
                    mask.append(False)
                    curr_index += 1
            return mask
        
        def get_headers(lines, min_header_length=5):
            mask = []
            for line in lines:
                if len(line.text.strip()) > min_header_length and re.match(r"[0-9A-Za-z\s\.,\:]+$", line.text.strip()):
                    mask.append(True)
                else:
                    mask.append(False)
            return mask

        
        def header_already_seen(header, previous_headers, threshold=70):
            for previous_header in previous_headers:
                if ratio(header.lower(), previous_header.lower()) >= threshold:
                    return True
            return False


        # N.b. this just splits on newline
        lines = split_paragraphs(doc.text, remove_empty=False)
        
        text_block_mask = get_text_blocks(lines)
        header_mask = get_headers(lines)
        
        headers = set()
        spans = []
        for i in range(len(lines)):
            # Check if this line is part of a text block
            if text_block_mask[i]:
                spans.append(Span(start=lines[i].start, end=lines[i].end, type="paragraph_chunk", score=1.0))
                # If this is a line internal to a text block, let's replace the terminal newline
                if i + 1 < len(lines) and text_block_mask[i+1]:
                    terminal_whitespace_start = len(lines[i].text.rstrip())
                    add_space = True
                    if lines[i].text.rstrip()[-1] == "-":
                        terminal_whitespace_start -= 1
                        add_space = False
                    if add_space:
                        spans.append(Span(start=lines[i].start + terminal_whitespace_start, end=lines[i].end, type="terminal_newline_with_space", score=1.0))
                    else:
                        spans.append(Span(start=lines[i].start + terminal_whitespace_start, end=lines[i].end, type="terminal_newline", score=1.0))
                # If this is a terminal line in the text block, tag it so we can make it a double newline
                elif i + 1 < len(lines) and not text_block_mask[i+1]:
                    spans.append(Span(start=lines[i].end - 1, end=lines[i].end, type="paragraph_terminal_newline", score=1.0))

            # Check if this line is a header and only keep one instance of each header to avoid keeping things like repeated chapter names at the top of each page
            elif header_mask[i] and not header_already_seen(lines[i].text.strip(), headers) and i + 1 < len(lines):
                j = i + 1
                while j < len(lines) and len(lines[j].text.strip()) == 0:
                    j += 1
                if j < len(lines) and text_block_mask[j]:
                    spans.append(Span(start=lines[i].start, end=lines[i].end, type="paragraph_chunk", score=1.0))
                    headers.add(lines[i].text.strip())
                else:
                    spans.append(Span(start=lines[i].start, end=lines[i].end, type="paragraph_chunk", score=0.0))
            else:
                spans.append(Span(start=lines[i].start, end=lines[i].end, type="paragraph_chunk", score=0.0))

        return DocResult(doc=doc, spans=spans)

'''
@add_tagger("paragraph_chunk_tagger")
class ParagraphChunkTagger(BaseTagger):
    def predict(self, doc: Document) -> DocResult:
        min_length = 20
        min_lines_in_chunk = 3
        
        # N.b. this just splits on newline
        lines = split_paragraphs(doc.text, remove_empty=False)
        
        # Create a mask indicating which lines are greater than min_length
        valid = [len(line.text.strip()) > min_length for line in lines]

        spans = []
        curr_index = 0
        while curr_index < len(lines):
            # Detect chunks of text that look like paragraphs
            if all(valid[curr_index: curr_index + min_lines_in_chunk]):
                # Get the whole paragraph chunk
                while curr_index < len(lines) and valid[curr_index]:
                    spans.append(Span(start=lines[curr_index].start, end=lines[curr_index].end, type="paragraph_chunk", score=1.0))
                    curr_index += 1
                # Detect a short line ending a paragraph
                if curr_index < len(lines) and len(lines[curr_index].text.strip()) > 0:
                    spans.append(Span(start=lines[curr_index].start, end=lines[curr_index].end, type="paragraph_chunk", score=1.0))
                    curr_index += 1
            
            # Detect headings
            elif len(lines[curr_index].text.strip()) > 10 and lines[curr_index].text.strip().isalnum():
                spans.append(Span(start=lines[curr_index].start, end=lines[curr_index].end, type="paragraph_chunk", score=1.0))
                curr_index += 1

            # Mark all other lines as junk not belonging to paragraphs
            else:
                spans.append(Span(start=lines[curr_index].start, end=lines[curr_index].end, type="paragraph_chunk", score=0.0))
                curr_index += 1

        return DocResult(doc=doc, spans=spans)
'''
