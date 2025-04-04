#!/usr/bin/env python3

import sentencepiece as spm

spm_model = spm.SentencePieceProcessor(model_file="common-pile.model")

text = """
if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
"""

print(spm_model.encode_as_pieces(text))

print(spm_model.decode(spm_model.encode(text)))
