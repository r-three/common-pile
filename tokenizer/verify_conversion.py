"""Code to test that our tokenizers all match.

Makes several assumptions about where things like llama models are available/saved
and where the llama library lives.
"""

import os
import sys

import datasets
import hf_to_tiktoken
import tiktoken
import transformers
from tqdm import tqdm

print("Loading Llama Tokenizer from HuggingFace.")
llama_hf = transformers.AutoTokenizer.from_pretrained("meta-llama/Llama-3.2-1B")


LLAMA_REPO = os.path.expanduser("~/dev/llama3/")
LLAMA_DIR = os.path.expanduser("~/.llama")
sys.path.append(LLAMA_REPO)
from llama import tokenizer

print("Loading Llama Tokenizer from llama/tiktoken")
llama_tt = tokenizer.Tokenizer(
    os.path.join(LLAMA_DIR, "checkpoints", "Llama3.2-1B", "tokenizer.model")
)

print("Converting HF Tokenizer.")
llama_convert = hf_to_tiktoken.convert_hf_to_tiktoken("converted", llama_hf)

assert (
    llama_convert._mergeable_ranks == llama_tt.model._mergeable_ranks
), "Mergeable Ranks don't match."
# The huggingface version of llama has some extra special tokens built in so
# we don't expect this to match.
# assert llama_convert._special_tokens == llama_tt.model._special_tokens, "Special Tokens don't match."
assert llama_convert._pat_str == llama_tt.model._pat_str, "Pattern Strings don't match."


# ds = datasets.load_dataset("allenai/c4", "en", split="validation", streaming=True)
ds = datasets.load_dataset("allenai/c4", "de", split="validation", streaming=True)
# ds = datasets.load_dataset("allenai/c4", "es", split="validation", streaming=True)
# ds = datasets.load_dataset("allenai/c4", "zh", split="validation", streaming=True)
# ds = datasets.load_dataset("omarkamali/emoji-map", split="train", streaming=True)

for i, example in tqdm(enumerate(ds)):
    text = example["text"]
    # Uncomment if using the emoji dataset.
    # text = f"{example['emoji']} {example['category']} {example['description_eng_Latn']}"
    hf_ts = llama_hf._tokenizer.encode(text, add_special_tokens=False).ids
    tt_ts = llama_tt.encode(text, bos=False, eos=False)
    ct_ts = llama_convert.encode(text)
    assert hf_ts == tt_ts, f"Example: {i} hf: {hf_ts} tt: {tt_ts}"
    assert ct_ts == tt_ts, f"Example: {i} tt: {tt_ts} ct: {ct_ts}"
