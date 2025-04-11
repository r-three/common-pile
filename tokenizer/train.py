"""Tools to train common-pile tokenizers."""

import argparse
import dataclasses
import glob
import json
from typing import Iterator, List

import datasets
import smart_open

from licensed_pile import logs, utils

parser = argparse.ArgumentParser(description="Train a common-pile tokenizer.")
parser.add_argument(
    "--algo",
    choices=["unigram", "bpe"],
    default="bpe",
    help="The type of tokenizer to train.",
)
parser.add_argument(
    "--dataset",
    help="A huggingface dataset to train on.",
)
parser.add_argument(
    "--streaming", action="store_true", help="Should we stream the hf dataset?"
)
parser.add_argument("--data_pattern", help="A glob of jsonl.gz files to train on.")
parser.add_argument(
    "--subset", default="default", help="The subset of the dataset to us."
)
parser.add_argument(
    "--vocab_size", default=64_000, type=int, help="The size of the vocab to use."
)
parser.add_argument(
    "--batch_size",
    default=100,
    type=int,
    help="The batch size for creating an iterator.",
)
parser.add_argument(
    "--output_path",
    default="common-pile-tokenizer",
    help="The name of the resulting tokenizer output.",
)
parser.add_argument(
    "--pattern_string",
    choices=["none", "tiktoken", "gpt2", "tiktoken+digits"],
    default="none",
    help="Should we use a regex to pre-split some text.",
)
parser.add_argument(
    "--nosplit_digits",
    action="store_true",
    help="Should we split numbers into individual digits, i.e., 1234 -> 1 2 3 4",
)
parser.add_argument(
    "--data_limit",
    type=float,
    default=-1,
    help="The size to limit the training dataset (in GB). Use -1 for whole dataset.",
)


BYTES_PER_GIGABYTE = 1000 * 1000 * 1000
PATTERN_STRINGS = {
    "tiktoken": r"""(?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+""",
    "tiktoken+digits": r"""(?i:'s|'t|'re|'ve|'m|'ll|'d)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}{1}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s*[\r\n]+|\s+(?!\S)|\s+""",
    "gpt2": r"""'s|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+""",
}


@dataclasses.dataclass
class SpecialTokens:
    """List of special tokens we want to include and their token id."""

    pad: str = "<pad>"
    pad_id: int = 0
    unk: str = "<unk>"
    unk_id: int = 1
    bos: str = "<|begin_of_text|>"  # Use the symbols from tiktoken for easier interopt
    bos_id: int = 2
    eos: str = "<|end_of_text|>"  # Use the symbols from tiktoken for easier interopt
    eos_id: int = 3


def load_hf_data(
    dataset: str, batch_size: int, subset: str = "default", streaming: bool = False
) -> Iterator[List[str]]:
    """Load training data from a huggingface dataset."""
    logger = logs.get_logger()
    subset_str = "" if subset == "default" else f"{subset},"
    logger.info(
        f"Loading dataset {dataset}[{subset_str}split=train] in batches of {batch_size}."
    )
    dataset = datasets.load_dataset(
        dataset, name=subset, split="train", streaming=streaming
    )
    # Batch as iterating because the for loop idiom doesn't work on stream data.
    batch = []
    for example in dataset:
        batch.append(example["text"])
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        logger = logs.get_logger()
        logger.warning("Yielding final ragged batch")
        yield batch


def load_jsonl_data(pattern: str, batch_size: int, **kwargs) -> Iterator[List[str]]:
    """Load training data from a collection of .jsonl.gz files."""
    logger = logs.get_logger()
    batch = []
    for file_path in glob.iglob(pattern):
        logger.info(f"Reading examples from {file_path}")
        with smart_open.open(file_path) as f:
            for line in f:
                if line:
                    batch.append(json.loads(line)["text"])
                if len(batch) == batch_size:
                    yield batch
                    batch = []
    if batch:
        logger.warning("Yielding final ragged batch")
        yield batch


def training_generator(data, batch_size: int, data_limit: float = -1):
    """Unify data format and add limits to training data size."""
    logger = logs.get_logger()
    data_size = 0

    for batch in data:
        # Lets us limit the size of data we want to train on.
        if data_limit > 0 and (data_size / BYTES_PER_GIGABYTE) > data_limit:
            logger.info("Stopping dataset loading as size limit was reached.")
            break
        # Underestimate of size, but faster and close enough for english.
        data_size += sum(len(b) for b in batch)
        # Don't return [${text}] if you have a batch size of 1.
        if batch_size == 1:
            yield batch[0]
        else:
            yield batch
    logger.info(f"Yielded {data_size / BYTES_PER_GIGABYTE}GB of text.")


def train_bpe(
    data_iter,
    output_path: str,
    vocab_size: int = 32_000,
    pattern_string: str | None = None,
    split_digits: bool = True,
):
    """Train a BPE model using HuggingFace Tokenizers."""
    from tokenizers import (
        Regex,
        Tokenizer,
        decoders,
        models,
        normalizers,
        pre_tokenizers,
        processors,
        trainers,
    )

    logger = logs.get_logger()
    logger.info(f"Training BPE Tokenizer with vocab_size={vocab_size}")
    tokenizer = Tokenizer(
        models.BPE(
            unk_token=SpecialTokens.unk,
            byte_fallback=True,
        )
    )
    tokenizer.normalizer = normalizers.NFKC()
    # We don't use the gpt2 regex built into the ByteLevel pretokenizer, if we
    # want that we use the explicit split pretokenzier with regex.
    pretokenizers = [
        pre_tokenizers.ByteLevel(add_prefix_space=False, use_regex=False),
    ]
    if pattern_string is not None:
        logger.info(f"Pre-split text based on regex: {pattern_string}")
        pretokenizers = [
            pre_tokenizers.Split(Regex(pattern_string), behavior="isolated")
        ] + pretokenizers

    if split_digits:
        logger.info("Spliting numbers into individual digits.")
        pretokenizers = [
            pre_tokenizers.Digits(individual_digits=True),
        ] + pretokenizers
    tokenizer.pre_tokenizer = pre_tokenizers.Sequence(pretokenizers)
    trainer = trainers.BpeTrainer(
        vocab_size=vocab_size,
        special_tokens=[
            SpecialTokens.pad,
            SpecialTokens.unk,
            SpecialTokens.bos,
            SpecialTokens.eos,
        ],
        min_frequency=2,
        max_token_length=30,
    )
    tokenizer.train_from_iterator(data_iter, trainer=trainer)
    tokenizer.post_processor = processors.ByteLevel(trim_offsets=False)
    tokenizer.decoder = decoders.ByteLevel()
    logger.info(f"Saving tokenizer to {output_path}")
    tokenizer.save(output_path)


def train_unigram(
    data_iter,
    output_path: str,
    vocab_size: int = 32_000,
    character_coverage: float = 0.99,
    max_sentence_length: int = 50_000,
    pattern_string: str | None = None,
    split_digits: bool = True,
):
    """Train a Unigram model with SentencePiece."""
    import sentencepiece as spm

    logger = logs.get_logger()
    logger.info(f"Training Unigram Tokenizer with vocab_size={vocab_size}")

    if pattern_string is not None:
        logger.warning(
            "Regex pattern string supplied but not supported for unigram yet. Ignoring."
        )

    if split_digits:
        logger.info("Spliting numbers into individual digits.")

    spm.SentencePieceTrainer.train(
        sentence_iterator=data_iter,
        model_prefix=output_path,
        vocab_size=vocab_size,
        character_coverage=character_coverage,
        model_type="unigram",
        split_digits=split_digits,
        # For formatting, allows newlines to appear.
        # Also, preallocating space symboles helps to
        # not have a bunch of spaces in a row.
        user_defined_symbols=["\n", "\r", "\r\n"] + [" " * 2**b for b in range(0, 4)],
        byte_fallback=True,
        normalization_rule_name="nfkc",
        # For Code.
        allow_whitespace_only_pieces=True,
        remove_extra_whitespaces=False,
        max_sentence_length=max_sentence_length,
        # Special Characters
        unk_id=SpecialTokens.unk_id,
        bos_id=SpecialTokens.bos_id,
        eos_id=SpecialTokens.eos_id,
        pad_id=SpecialTokens.pad_id,
        pad_piece=SpecialTokens.pad,
        unk_piece=SpecialTokens.unk,
        bos_piece=SpecialTokens.bos,
        eos_piece=SpecialTokens.eos,
        unk_surface=SpecialTokens.unk,
        train_extremely_large_corpus=True,
    )
    logger.info(f"Saving tokenizer to {output_path}.model")


def main():
    """Train a tokenizer."""
    args = parser.parse_args()
    logger = logs.configure_logging()

    # Adjust settings based on the type of tokenizer.
    if args.algo == "unigram":
        if args.batch_size != 1:
            logger.warning("Unigram selected as training algo, setting --batch_size=1")
            args.batch_size = 1
        train_tokenizer = train_unigram
    else:
        if not args.output_path.endswith(".bpe"):
            logger.warning('Adding ".bpe" suffix to --output_path')
            args.output_path = f"{args.output_path}.bpe"
        train_tokenizer = train_bpe

    # Load data from either huggingface or raw jsonl files.
    if (args.dataset is None) == (args.data_pattern is None):
        raise ValueError("Only one of --dataset or --data_pattern must be set.")
    if args.dataset is not None:
        data = load_hf_data(
            args.dataset, args.batch_size, subset=args.subset, streaming=args.streaming
        )
    if args.data_pattern is not None:
        data = load_jsonl_data(args.data_pattern, args.batch_size)

    if args.pattern_string == "tiktoken+digits":
        logger.warning(
            "tiktoken+digits regex used and splitting digits requested, this is redundant, skipping explicit digit splitting pretokenizer."
        )
        args.nosplit_digits = True
    # Convert nice names into ugly regex strings.
    pattern_string = PATTERN_STRINGS.get(args.pattern_string, None)

    # Setup the input data.
    data_iter = training_generator(data, args.batch_size, data_limit=args.data_limit)
    # Train the tokenizer.
    train_tokenizer(
        data_iter,
        args.output_path,
        args.vocab_size,
        pattern_string=pattern_string,
        split_digits=not args.nosplit_digits,
    )


if __name__ == "__main__":
    main()
