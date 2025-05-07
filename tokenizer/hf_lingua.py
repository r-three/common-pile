"""Implementation of Lingua Tokenizer with HuggingFace tokenizers as the backend."""


from typing import List, Optional, Sequence, Tuple

import tokenizers
import transformers
from lingua import tokenizer as l_tokenizer

# These are common bos/eos sepcial tokens. When working with
# tokenizers.Tokenizer, we don't know what they used for bos/eos, we need to
# infer it from their vocabulary as we don't have an explicit .bos_id attr
DEFAULT_SPECIAL_TOKENS = {
    "bos": ["<bos>", "<|begin_of_text|>"],
    "eos": ["</s>", "<|end_of_text|>", "<|eot_id|>"],
    "pad": ["<pad>"],
}


def find_id(tokenizer, surfaces: Sequence[str]):
    """Look through surfaces to see if any are in the tokenizer's vocab."""
    token_id = None
    for surface in surfaces:
        token_id = tokenizer.token_to_id(surface)
        if token_id is not None:
            l_tokenizer.logger.info("Found id for special token: %s", surface)
            break
    else:
        l_tokenizer.logger.warning("No id found for speical token.")
    return token_id


class HFTokenizer(l_tokenizer.Tokenizer):
    """Lingua Tokenizer that uses HF tokenizers."""

    def __init__(self, model_path: str) -> None:
        try:
            # Try to load as a transformers.Tokenizer as it includes more
            # information about things like bos/eos
            transformers_tokenizer = transformers.AutoTokenizer.from_pretrained(
                model_path
            )
            l_tokenizer.logger.info("Loaded Transformers Tokenizer from %s", model_path)
            # Extract the underlying tokenizers.Tokenizer to get access to things
            # like the offests.
            self.hf_tokenizer = transformers_tokenizer._tokenizer
            l_tokenizer.logger.info(
                "Extracted Tokenizers Tokenizer from Transformers Tokenizer"
            )

            # Find special tokens based on the transformers.Tokenizer
            bos_token = transformers_tokenizer.bos_token
            l_tokenizer.logger.info(
                "Found bos_token: %s based on Transformers Tokenizer.", bos_token
            )
            self.bos_id = transformers_tokenizer.convert_tokens_to_ids(bos_token)

            eos_token = transformers_tokenizer.eos_token
            l_tokenizer.logger.info(
                "Found eos_token: %s based on Transformers Tokenizer.", eos_token
            )
            self.eos_id = transformers_tokenizer.convert_tokens_to_ids(eos_token)

            pad_token = transformers_tokenizer.pad_token
            l_tokenizer.logger.info(
                "Found pad_token: %s based on Transformers Tokenizer.", pad_token
            )
            if pad_token is not None:
                # It is ok for this not be set for models that don't have a pad
                # because it isn't set for some the other lingua implementations.
                self.pad_id = transformers_tokenizer.convert_tokens_to_ids(pad_token)

        except:
            # If we failed to load as a transformers.Tokenizer, load as a
            # tokenizers.Tokenizer
            self.hf_tokenizer = tokenizers.Tokenizer.from_file(model_path)
            l_tokenizer.logger.info("Loaded Tokenizers Tokenizer.")

            # We need to infer the special tokens. If you used a different
            # special token, it needs to be added tothe DEFAULT_SPECIAL_TOKENS
            # dict.
            l_tokenizer.logger.info("Infering bos id.")
            self.bos_id = find_id(self.hf_tokenizer, DEFAULT_SPECIAL_TOKENS["bos"])
            l_tokenizer.logger.info("Infering eos id.")
            self.eos_id = find_id(self.hf_tokenizer, DEFAULT_SPECIAL_TOKENS["eos"])
            l_tokenizer.logger.info("Infering pad id.")
            self.pad_id = find_id(self.hf_tokenizer, DEFAULT_SPECIAL_TOKENS["pad"])

        self.n_words = self.hf_tokenizer.get_vocab_size()

        l_tokenizer.logger.info(
            "#words: %d - BOS ID: %d - EOS ID: %d",
            self.n_words,
            self.bos_id,
            self.eos_id,
        )

    def encode(self, s: str, add_bos: bool, add_eos: bool):
        """Convert a string to a list of tokens."""
        # Never add bos/eos special tokens because we are using a
        # tokenizers.Tokenizer which doesn't auto add them.
        encoded = self.hf_tokenizer.encode(s, add_special_tokens=False).ids
        # Add bos/eos as needed, easy because we are not processing batches.
        if add_bos:
            encoded = [self.bos_id] + encoded
        if add_eos:
            encoded = encoded + [self.eos_id]
        return encoded

    def decode(self, tokens: List[int]):
        """Convert a list of tokens to a stirng."""
        return self.hf_tokenizer.decode(tokens)

    def get_token_offsets(
        self, text: str, tokens: Optional[List[int]] = None
    ) -> Tuple[List[str], List[int]]:
        """Get the offsets (and surface) for each token in the original string."""
        if tokens is not None:
            l_tokenizer.logger.warning(
                "`tokens` passed to `get_token_offsets`, but are ignored with the HFTokenizer."
            )

        # Don't add special tokens so we don't need to handle things like the
        # offset of the bos token.
        encoding = self.hf_tokenizer.encode(text, add_special_tokens=False)
        # Slice the original text instead of using encoding.tokens to avoid the
        # fact that tokenizers uses Ġ instead of space.
        substrs = [text[s:e] for s, e in encoding.offsets]
        return substrs, encoding.offsets
