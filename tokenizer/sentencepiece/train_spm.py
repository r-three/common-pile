import json

import sentencepiece as spm
import smart_open


def sp_training_generator():
    with smart_open.open(
        "../../pep/data/peps-dolma/v0/documents/00000_peps.jsonl.gz"
    ) as f:
        for line in f:
            if line:
                yield json.loads(line)["text"]


print(next(sp_training_generator()))

spm.SentencePieceTrainer.train(
    sentence_iterator=sp_training_generator(),
    model_prefix="common-pile",
    vocab_size=20000,
    character_coverage=0.99,
    model_type="unigram",
    split_digits=True,
    user_defined_symbols=["\n", "\r"],
    byte_fallback=True,
    normalization_rule_name="nfkc",
    allow_whitespace_only_pieces=True,
    remove_extra_whitespaces=False,
    max_sentence_length=50000,
    unk_id=1,
    bos_id=2,
    eos_id=3,
    pad_id=0,
    pad_piece="<pad>",
    unk_piece="<unk>",
    bos_piece="<bos>",
    eos_piece="</s>",
    train_extremely_large_corpus=True,
)
