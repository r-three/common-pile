import re
import os

from licensed_pile import logs


class ArXivID:
    """
    Class for unified handling of the two types of arXiv IDs:
        - The new {yymm}.{id} format (e.g., 1202.2341)
        - The old {field}/{yymm}{id} format (e.g., math/0003117)

    This provides methods for parsing out the fields from ID strings as well as from tarball filenames in the arXiv data dumps
    """
    def __init__(self, field, yymm, id):
        self.field = field
        self.yymm = yymm
        self.id = id
    
    @classmethod
    def from_tarball_member(cls, member):
        logger = logs.get_logger("arxiv-papers")

        name = os.path.splitext(os.path.basename(member.name))[0]
        match = re.match(r"(?P<yymm>\d{4})\.(?P<id>\d+)", name)
        if match is not None:
            return cls(None, match.group("yymm"), match.group("id"))

        match = re.match(r"(?P<field>[a-z\-]+)(?P<yymm>\d{4})(?P<id>\d+)", name)
        if match is not None:
            return cls(match.group("field"), match.group("yymm"), match.group("id"))

        logger.error(f"Failed to parse arXiv ID from tarball member {member.name}")
        return None

    @classmethod
    def from_string(cls, id_string):
        logger = logs.get_logger("arxiv-papers")
        
        match = re.match(r"(?P<yymm>\d{4})\.(?P<id>\d+)", id_string)
        if match is not None:
            return cls(None, match.group("yymm"), match.group("id"))

        match = re.match(r"(?P<field>[a-z\-]+)/(?P<yymm>\d{4})(?P<id>\d+)", id_string)
        if match is not None:
            return cls(match.group("field"), match.group("yymm"), match.group("id"))
        
        logger.error(f"Failed to parse arXiv ID from string {id_string}")
        return None
    
    @classmethod
    def from_paper_dir(cls, path):
        logger = logs.get_logger("arxiv-papers")
        
        dirname = os.path.basename(path)
        match = re.match(r"(?P<yymm>\d{4})\.(?P<id>\d+)", dirname)
        if match is not None:
            return cls(None, match.group("yymm"), match.group("id"))

        match = re.match(r"(?P<field>[a-z\-]+)_(?P<yymm>\d{4})(?P<id>\d+)", dirname)
        if match is not None:
            return cls(match.group("field"), match.group("yymm"), match.group("id"))
 
        logger.error(f"Failed to parse arXiv ID from paper directory {path}")
        return None

    def __str__(self):
        if self.field is None:
            return f"{self.yymm}.{self.id}"
        return f"{self.field}_{self.yymm}{self.id}"
    
    def __hash__(self):
        return hash((self.field, self.yymm, self.id))

    def __eq__(self, other):
        if isinstance(other, ArXivID):
            return self.field == other.field and self.yymm == other.yymm and self.id == other.id
        return False
        
