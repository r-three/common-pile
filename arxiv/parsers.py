import subprocess
import os
from glob import glob
import re
from contextlib import contextmanager
from typing import List, Optional

import html2text
from bs4 import BeautifulSoup
import pylatexenc.latex2text

from licensed_pile import logs
from arxiv_id import ArXivID


class LaTeXParser:
    """
    Base class for parsing LaTeX documents into plaintext.
    """
    def __init__(self):
        self.id = None

    def parse(self, paper_dir: str) -> str:
        arxiv_id = ArXivID.from_paper_dir(paper_dir)
        with self.with_id(arxiv_id):
            path = self.get_main_tex_file(paper_dir)
            if path is None:
                return None
            text = self.parse_latex(path)
            return None if text is None or len(text) == 0 else text
    
    def parse_latex(self, path: str) -> str:
        raise NotImplementedError

    def get_main_tex_file(self, paper_dir: str) -> str:
        logger = logs.get_logger("arxiv-papers")
        
        # Run `latexpand` and determine main .tex/.latex file to be the file with the longest expanded source code
        tex_files = glob(os.path.join(paper_dir, "**", "*.tex"), recursive=True) + glob(os.path.join(paper_dir, "**", "*.latex"), recursive=True)
        if len(tex_files) == 0:
            logger.error(f"{self.id}: Failed to find any .tex files")
            return None

        latexpand_lengths = [(f, len(subprocess.run(["latexpand", os.path.relpath(f, start=paper_dir)], cwd=paper_dir, capture_output=True).stdout)) for f in tex_files]
        main_tex_file = max(latexpand_lengths, key=lambda e: e[1])[0]
        logger.info(f"{self.id}: Identified {main_tex_file} as main .tex file")
        return main_tex_file
    
    @contextmanager
    def with_id(self, id: ArXivID):
        self.id = id
        try:
            yield
        finally:
            self.id = None


class LaTeXMLParser(LaTeXParser):
    """
    Parser for LaTeX documents that uses LaTeXML to transform into an intermediate HTML format.
    """
    def __init__(self, binding_paths: List[str] = [], keep_html=True):
        super().__init__()
        self.paths = binding_paths
        self.keep_html = keep_html
        self.html2text = html2text.HTML2Text()
        self.html2text.ignore_links = True
        self.html2text.ignore_images = True
    
    def cleanup_html_dir(self, dir: str):
        if not self.keep_html:
            os.rmdir(dir)

    def compile(self, input_file: str) -> Optional[str]:
        logger = logs.get_logger("arxiv-papers")

        logger.info(f"{self.id}: Compiling LaTeX to HTML using LaTeXML")

        latexml_dir = os.path.join(os.path.dirname(input_file), "latexml")
        latexml_file = os.path.join(latexml_dir, "paper.html")
        log_file = os.path.join(latexml_dir, "latexml.log")
        os.makedirs(latexml_dir, exist_ok=True)

        base_command = ["/u/nkandpa2/LaTeXML-0.8.8/blib/script/latexmlc", \
                        f"--source={input_file}", f"--dest={latexml_file}", \
                        "--preload=[nobibtex,ids,localrawstyles,nobreakuntex,magnify=1.8,zoomout=1.8,tokenlimit=249999999,iflimit=3599999,absorblimit=1299999,pushbacklimit=599999]latexml.sty", \
                        "--preload=ar5iv.sty", \
                        "--format=html5", \
                        "--mathtex", \
                        "--timeout=2700", \
                        "--noinvisibletimes", \
                        "--nodefaultresources", \
                        "--quiet", "--quiet", f"--log={log_file}"]
        binding_path_args = [f"--path={path}" for path in self.paths]
        result = subprocess.run(base_command + binding_path_args)
        
        if result.returncode or not os.path.exists(latexml_file):
            logger.error(f"{self.id}: LaTeXML compile failed")
            self.cleanup_html_dir(latexml_dir)
            return None
        
        with open(latexml_file, "r") as f:
            html = f.read()
            if len(html) == 0:
                logger.error(f"{self.id}: LaTeXML produced empty HTML file")
                html = None
            self.cleanup_html_dir(latexml_dir)
            return html

    def html_to_text(self, html: str) -> str:
        logger = logs.get_logger("arxiv-papers")

        logger.info(f"{self.id}: Converting HTML to plaintext")
        def remove_footer(soup: BeautifulSoup) -> BeautifulSoup:
            soup.find("footer").decompose()
            return soup
        
        def remove_parse_errors(soup: BeautifulSoup) -> BeautifulSoup:
            error_tags = soup.find_all(class_=re.compile(r"^ltx_ERROR"))
            for error_tag in error_tags:
                error_tag.decompose()
            return soup

        def replace_math(soup: BeautifulSoup) -> BeautifulSoup:
            math_tags = soup.find_all(class_="ltx_Math")
            for math_tag in math_tags:
                math_tag.string = f"${math_tag.string}$"
            
            unwrap_tags = ["ltx_eqn_table", "ltx_eqn_row", "ltx_eqn_cell"]
            for unwrap_tag in unwrap_tags:
                for tag in soup.find_all(class_=unwrap_tag):
                    tag.unwrap()

            return soup
        
        soup = BeautifulSoup(html, "html.parser")
        soup = remove_footer(replace_math(remove_parse_errors(soup)))
        return self.html2text.handle(str(soup))
    
    def parse_latex(self, path: str) -> str:
        html = self.compile(path)
        return None if html is None else self.html_to_text(html)


class PyLaTeXEncParser(LaTeXParser):
    """
    Parser for LaTeX documents that uses pylatexenc to convert to plaintext.
    """
    def __init__(self):
        super().__init__()

        # Initialize context database for parsing latex body
        def extract_macro_content(node, **kwargs):
            if node is None:
                return None
            arg_node = node.nodeargd.argnlist[0]
            if isinstance(arg_node, pylatexenc.latexwalker.LatexGroupNode):
                text = pylatexenc.latex2text.LatexNodes2Text().nodelist_to_text(arg_node.nodelist)
                return text
            return None

        self.l2t_db = pylatexenc.latex2text.get_default_latex_context_db()
        self.l2t_db.add_context_category(
            "overrides",
            prepend=True,
            macros=[
                pylatexenc.latex2text.MacroTextSpec("includegraphics"),
                pylatexenc.latex2text.MacroTextSpec("maketitle"),
                pylatexenc.latex2text.MacroTextSpec("title", simplify_repl=extract_macro_content),
                pylatexenc.latex2text.MacroTextSpec("author", simplify_repl=extract_macro_content),
            ],
            environments=[
                pylatexenc.latex2text.EnvironmentTextSpec("array"),
                pylatexenc.latex2text.EnvironmentTextSpec("pmatrix"),
                pylatexenc.latex2text.EnvironmentTextSpec("bmatrix"),
                pylatexenc.latex2text.EnvironmentTextSpec("smallmatrix"),
            ]
        )

    
    def process_bibliography(self, latex: str, path: str) -> str:
        logger = logs.get_logger("arxiv-papers")

        bbl_filename = os.path.splitext(path)[0] + ".bbl"
        if os.path.exists(bbl_filename):
            logger.info(f"{self.id}: Including bibliography from {bbl_filename}")
            with open(bbl_filename, "r") as f:
                bbl = f.read()
            bib_pattern = r"(\\bibliography\{[a-zA-Z0-9_-]+\}|\\printbibliography)"
            latex = re.sub(bib_pattern, lambda _: bbl, latex)
        return latex
    
    def replace_inputs(self, latex: str, root_directory: str) -> str:
        def replace_input(match):
            filename = match.group(1) + '.tex'
            filepath = os.path.join(root_directory, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r') as file:
                    return file.read()
            else:
                return match.group(0)

        input_pattern = r'\\input\{([\/a-zA-Z0-9_-]+)\}'
        return re.sub(input_pattern, replace_input, latex)
    
    def replace_citations(self, latex: str) -> str:
        bibitem_pattern = r'\\bibitem(?:\[([^\]]+)\])?\{([^}]+)\}'
        matches = re.findall(bibitem_pattern, latex)
        bibitems = {key: label if label else None for label, key in matches}

        for n, (key, label) in enumerate(bibitems.items()):
            # When the citation label is missing, make it a number
            # It'd be a bit odd if only a few of the labels were missing...
            # but that's not technically a problem, so we can ignore it.
            if label is None:
                bibitems[key] = str(n + 1)
            else:
                # Remove any newlines in the label
                label = " ".join([l.strip() for l in label.splitlines()])
                # Citation labels seem to often have curly braces; remove them
                label = label.strip("{}")
                label_matches = re.search(r"(.+)\((\d{4})\)?(.+)?", label)
                if label_matches is not None:
                    authors, year = label_matches.group(1, 2)
                    if authors:
                        # Use n.d. = no date if no year is given
                        year = year or "n.d."
                        bibitems[key] = f"{authors}, {year}"
                # Fall back on numerical citation....
                else:
                    bibitems[key] = str(n + 1)
        
        def replace_cite(match: re.Match) -> str:
            cite_type = match.group(1)
            keys = [key.strip() for key in match.group(2).split(',')]
            if not all([key in bibitems for key in keys]):
                return match.group(0)
            # Here are some in-text citation formats I know (no paranthesis/braket)
            if cite_type in ['citet', 'citealt', 'citealp']:
                return ', '.join([bibitems[key] for key in keys])
            # Otherwise add brackets
            else:
                return f"[{', '.join([bibitems[key] for key in keys])}]"

        cite_pattern = r'\\(cite[a-z]*)\{([^}]+)\}'
        latex = re.sub(cite_pattern, replace_cite, latex)
        return re.sub(bibitem_pattern, lambda match: f"[{bibitems[match.group(2)]}]", latex)
    
    def latex_to_text(self, latex: str, latex_context) -> str:
        text = pylatexenc.latex2text.LatexNodes2Text(math_mode="verbatim", latex_context=latex_context).latex_to_text(latex)
        text = "\n".join([l.strip() for l in text.splitlines()])
        text = "\n".join(
            [
                l for l in text.splitlines()
                if len(l.split()) > 1
                or l == ""
                or l.startswith("\\")
                or re.match(r"^\[[0-9]+\]$", l)
            ]
        )
        text = re.sub("\n\n+", "\n\n", text)
        return text
     
    def parse_latex(self, path: str) -> str:
        logger = logs.get_logger("arxiv-papers")

        try:
            with open(path) as f:
                latex = f.read()
        
            latex = self.process_bibliography(latex, path) 
        
            _, body = latex.split(r"\begin{document}", 1)
            body = self.replace_inputs(body, os.path.dirname(path))
            body = self.replace_citations(body)
            body_text = self.latex_to_text(body, self.l2t_db)
            return body_text
        except Exception as e:
            logger.error(f"{self.id}: Failed to parse with pylatexenc: {e}")
            return None
