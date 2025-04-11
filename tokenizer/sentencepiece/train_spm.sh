#!/usr/bin/env sh


spm_train \
    --input=../../pep/data/peps-tok/documents/00000_peps.txt \
    --model_prefix=common_pile \
    --vocab_size=20000 \
    --character_coverage=0.99 \
    --model_type=unigram \
    --split_digits=true \
    --user_defined_symbols="<n>" \
    --byte_fallback=true \
    --normalization_rule_name=nfkc \
    --allow_whitespace_only_pieces=true \
    --remove_extra_whitespaces=false \
    --max_sentence_length=50000 \
    --unk_id=1 \
    --bos_id=2 \
    --eos_id=3 \
    --pad_id=0 \
    --train_extremely_large_corpus=true



# --vocab_size=32000 \
# --input_sentence_size
# --shuffle_input_sentence
