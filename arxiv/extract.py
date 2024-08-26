import tarfile
import gzip
from tqdm.auto import tqdm
import os
import re

from licensed_pile import logs
from arxiv_id import ArXivID



def decompress_gz(f, output_dir):
    f.seek(0)
    with gzip.open(f, mode="r") as gz:
        contents = gz.read()

    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "main.tex"), "wb") as of:
        of.write(contents)


def decompress_tgz(f, output_dir):
    f.seek(0)
    with tarfile.open(fileobj=f, mode="r:gz") as tar:
        tar.extractall(output_dir)


def extract_papers(path, output_dir, filter_ids=set()):
    logger = logs.get_logger("arxiv-papers")
    extracted_paper_dirs = []

    with tarfile.open(path, mode="r:") as tarball:
        logger.info(f"Extracting papers in {path}")
        for member in tarball.getmembers():
            if not member.isfile():
                continue
            extension = os.path.splitext(os.path.basename(member.name))[1]
            arxiv_id = ArXivID.from_tarball_member(member)
            if extension != ".gz" or arxiv_id not in filter_ids:
                continue

            extracted_paper = tarball.extractfile(member=member)
            paper_dir = os.path.join(output_dir, arxiv_id.yymm, arxiv_id.id)
            try:
                decompress_tgz(extracted_paper, paper_dir)
            except tarfile.ReadError as e1:
                try:
                    decompress_gz(extracted_paper, paper_dir)
                except gzip.BadGzipFile as e2:
                    logger.error(f"Failed to read {arxiv_id} as .tgz file ({e1}) and .gz file ({e2})")
                    continue
            
            extracted_paper_dirs.append(paper_dir)

    os.remove(path)

    logger.info(f"Extracted {len(extracted_paper_dirs)} papers to {paper_dir}")
    return extracted_paper_dirs
