"""Demo the trained tokenizers."""

import argparse
import textwrap

parser = argparse.ArgumentParser(description="Train a common-pile tokenizer.")
parser.add_argument(
    "--algo",
    choices=["unigram", "bpe"],
    default="bpe",
    help="The type of tokenizer to train.",
)
parser.add_argument(
    "--tokenizer", required=True, help="The path to the tokenizer you want to run."
)


def load_unigram(path: str):
    import sentencepiece as spm

    return spm.SentencePieceProcessor(model_file=path)


def load_bpe(path: str):
    from tokenizers import Tokenizer

    return Tokenizer.from_file(path)


def main():
    args = parser.parse_args()

    if args.algo == "unigram":
        load_tokenizer = load_unigram
    else:
        load_tokenizer = load_bpe

    tokenizer = load_tokenizer(args.tokenizer)

    text = textwrap.dedent(
        """
        if __name__ == "__main__":
            args = parser.parse_args()
            main(args)
        """.strip(
            "\n"
        )
    )

    if args.algo == "unigram":
        print(tokenizer.encode_as_pieces(text))
        print(tokenizer.encode_as_ids(text))
        print(tokenizer.decode(tokenizer.encode(text)))
    else:
        encoded = tokenizer.encode(text)
        print(encoded.tokens)
        print(encoded.ids)
        print(tokenizer.decode(tokenizer.encode(text).ids))


if __name__ == "__main__":
    main()
