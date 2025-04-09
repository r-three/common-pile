"""Tool to convert from HuggingFace BPE tokenizers to tiktoken.

Once you have converted to a tiktoken.Encoding, you can save it with the
tiktoken.load.dump_tiktoken_bpe function.
"""

import json
import os
import tempfile

import tiktoken
import tokenizers

from licensed_pile import logs


def extract_pattern_string(config):
    """Infer the regex pattern splitting string from the pre_tokenizers."""
    logger = logs.get_logger()
    pat_str = None
    pretok = config["pre_tokenizer"]
    if pretok["type"] == "Sequence":
        pretoks = pretok["pretokenizers"]
    else:
        pretoks = [pretok]
    for pretok in pretoks:
        if pretok["type"] == "Split":
            pat_str = pretok.get("pattern", {}).get("Regex")
            logger.info(
                f"Extracted {pat_str} from tokenizer as pattern splitting regex."
            )
        # Warn about incompatabilities.
        elif pretok["type"] == "Digits":
            logger.warning(
                "Found Digit Pretokenizer, this is not supported in TikToken, please roll it into your regex."
            )
        elif pretok["type"] != "ByteLevel":
            logger.warning(f"Found unsupported pretokenizer: {pretok}")
    return pat_str


def extract_special_tokens(config):
    """Extract the special tokens from the config."""
    logger = logs.get_logger()
    special_tokens = {}
    for token in config["added_tokens"]:
        logger.debug("Adding {token['content']} as token {token['id']}.")
        special_tokens[token["content"]] = token["id"]
    return special_tokens


def to_spaces(s: str, space_char: str = "Ġ") -> str:
    """Handle HuggingFace's use of Ġ to mean space."""
    return s.replace(space_char, " ")


def extract_merges(config):
    """Create the merges from the vocabulary."""
    merges = {}
    for v in config["model"]["vocab"]:
        merges[to_spaces(v).encode("utf-8")] = len(merges)
    # for t1, t2 in config["model"]["merges"]:
    #     merge_str = f"{to_spaces(t1)}{to_spaces(t2)}"
    #     merges[merge_str.encode('utf-8')] = len(merges) + 1
    return merges


def extract_hf_config(tokenizer: tokenizers.Tokenizer) -> dict:
    """Convert a HuggingFace tokenizer its config in json.

    It isn't clear if there is an easier way to get the json that writing and
    reading.

    This function handles both getting the config for both transformers
    tokenizers and tokenizers tokenizers.
    """
    with tempfile.TemporaryDirectory() as d:
        try:
            tokenizer.save_pretrained(d)
        except AttributeError:
            tokenizer.save(os.path.join(d, "tokenizer.json"))
        with open(os.path.join(d, "tokenizer.json")) as f:
            return json.load(f)


def convert_hf_to_tiktoken(
    name: str, tokenizer: tokenizers.Tokenizer
) -> tiktoken.Encoding:
    """Convert HF tokenizer to tiktoken Encoding."""
    config = extract_hf_config(tokenizer)
    return convert_hf_config_to_tiktoken(name, config)


def convert_hf_config_to_tiktoken(name: str, config) -> tiktoken.Encoding:
    """Convert HF tokenizer config to tiktoken Encoding."""
    logger = logs.get_logger()
    if config["normalizer"] is not None:
        logger.warning(
            "Normalizer found in HF Tokenizer, this is not support in TikToken."
        )

    # ToDo add warnings about unsupported tokenizer setttings.

    pat_str = extract_pat_str(config)
    special_tokens = extract_special_tokens(config)
    merges = extract_merges(config)

    return tiktoken.Encoding(
        name=name,
        pat_str=pat_str,
        mergeable_ranks=merges,
        special_tokens=special_tokens,
    )
