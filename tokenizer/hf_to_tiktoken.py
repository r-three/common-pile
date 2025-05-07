"""Tool to convert from HuggingFace BPE tokenizers to tiktoken.

Once you have converted to a tiktoken.Encoding, you can save it with the
tiktoken.load.dump_tiktoken_bpe function.
"""

import argparse
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


# Copied from transformers.models.gpt2.tokenization_gpt2.bytes_to_unicode
def bytes_to_unicode():
    """
    Returns list of utf-8 byte and a mapping to unicode strings. We specifically avoids mapping to whitespace/control
    characters the bpe code barfs on.

    The reversible bpe codes work on unicode strings. This means you need a large # of unicode characters in your vocab
    if you want to avoid UNKs. When you're at something like a 10B token dataset you end up needing around 5K for
    decent coverage. This is a significant percentage of your normal, say, 32K bpe vocab. To avoid that, we want lookup
    tables between utf-8 bytes and unicode strings.
    """
    bs = (
        list(range(ord("!"), ord("~") + 1))
        + list(range(ord("¡"), ord("¬") + 1))
        + list(range(ord("®"), ord("ÿ") + 1))
    )
    cs = bs[:]
    n = 0
    for b in range(2**8):
        if b not in bs:
            bs.append(b)
            cs.append(2**8 + n)
            n += 1
    cs = [chr(n) for n in cs]
    return dict(zip(bs, cs))


def extract_merges(config):
    """Create the merges from the vocabulary."""
    unicode_to_bytes = {v: k for k, v in bytes_to_unicode().items()}

    merges = {}
    for word in config["model"]["vocab"]:
        tt_word = []
        # Undo the monkeying they do that maps tiktoken byte level things into
        # proxy unicode values.
        for c in word:
            if c != " ":
                c = chr(unicode_to_bytes[c])
            tt_word.append(c.encode("latin-1"))
        merges[b"".join(tt_word)] = len(merges)
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

    pat_str = extract_pattern_string(config)
    special_tokens = extract_special_tokens(config)
    merges = extract_merges(config)

    return tiktoken.Encoding(
        name=name,
        pat_str=pat_str,
        mergeable_ranks=merges,
        special_tokens=special_tokens,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert HF Tokenizers into TikToken tokenizers."
    )
    parser.add_argument("--hf", help="Path to the HuggingFace tokenizer.")
    parser.add_argument("--tt", help="Where to save the TikToken tokenizer.")

    hf = tokenizers.Tokenizers.from_file(args.hf)
    tt = convert_hf_to_tiktoken("this-is-not-saved-so-it-doesn't-matter", hf)
    tiktoken.load.dump_tiktoken_bpe(tt._mergable_ranks, args.tt)
