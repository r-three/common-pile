"""Tools to train common-pile tokenizers."""

import argparse
import dataclasses

import datasets

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Train a common-pile tokenizer.")
parser.add_argument(
    "--algo",
    choices=["unigram", "bpe"],
    default="bpe",
    help="The type of tokenizer to train.",
)
parser.add_argument(
    "--dataset",
    default="nkandpa2/common-pile-filtered",
    help="The dataset to train on.",
)
parser.add_argument(
    "--vocab_size", default=32_000, type=int, help="The size of the vocab to use."
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
    "--streaming", action="store_true", help="Should we stream the dataset?"
)


@dataclasses.dataclass
class SpecialTokens:
    pad: str = "<pad>"
    pad_id: int = 0
    unk: str = "<unk>"
    unk_id: int = 1
    bos: str = "<bos>"
    bos_id: int = 2
    eos: str = "</s>"
    eos_id: int = 3


def training_generator(dataset: str, batch_size: int, streaming: bool = False):
    dataset = datasets.load_dataset(dataset, split="train", streaming=streaming)
    # Don't return [${text}] if you have a batch size of 1.
    if batch_size == 1:
        yield from (d["text"] for d in dataset)
    else:
        for i in range(0, len(dataset), batch_size):
            yield dataset[i : i + batch_size]["text"]


def train_bpe(data_iter, output_path: str, vocab_size: int = 32_000):
    from tokenizers import (
        Tokenizer,
        decoders,
        models,
        normalizers,
        pre_tokenizers,
        processors,
        trainers,
    )

    tokenizer = Tokenizer(
        models.BPE(
            unk_token=SpecialTokens.unk,
            byte_fallback=True,
        )
    )
    tokenizer.normalizer = normalizers.NFKC()
    tokenizer.pre_tokenizer = pre_tokenizers.Sequence(
        [
            pre_tokenizers.Digits(individual_digits=True),
            pre_tokenizers.ByteLevel(add_prefix_space=False),
        ]
    )
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
    tokenizer.save(output_path)


def train_unigram(
    data_iter,
    output_path: str,
    vocab_size: int = 32_000,
    character_coverage: float = 0.99,
    max_sentence_length: int = 50_000,
):
    import sentencepiece as spm

    spm.SentencePieceTrainer.train(
        sentence_iterator=data_iter,
        model_prefix=output_path,
        vocab_size=vocab_size,
        character_coverage=character_coverage,
        model_type="unigram",
        split_digits=True,
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


def main():
    args = parser.parse_args()
    logger = logs.configure_logging()

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

    data_iter = training_generator(args.dataset, args.batch_size, args.streaming)
    train_tokenizer(data_iter, args.output_path, args.vocab_size)


if __name__ == "__main__":
    main()
