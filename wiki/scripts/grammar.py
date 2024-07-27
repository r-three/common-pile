#!/usr/bin/env python3

import lark

grammar = r"""
?start: abs
      | PIPE
      | CHAR

CHAR_SEQ: CHAR+
CHAR: /[^"\\\{\}]/

PIPE: "{{!}}"

abs: "{{abs|" abs "}}"
   | "{{abs|" CHAR_SEQ "}}"
"""


grammar = r"""
math: "{{math|" template* "}}"
    | template*

template: phi
        | delta
        | ell
        | pi
        | equal
        | pipe
        | bra
        | ket
        | bra_ket
        | braket
        | norm
        | closed_open
        | closed_closed
        | open_open
        | open_closed
        | brace
        | overset
        | overline
        | sup
        | sub
        | sfrac
        | italic
        | bold
//         | su
        | WORD


phi: "{{phi}}"
pi:  "{{pi}}"
delta: "{{delta}}"
ell: "{{ell}}"
equal: "{{=}}"
pipe: "{{!}}"

bra: "{{bra|" template "}}"
   | "{{Dbra|" template "}}"
ket: "{{ket|" template "}}"
   | "{{Dket|" template "}}"
bra_ket: "{{bra-ket|" template "|" template "}}"
       | "{{Dbraket|" template "|" template "}}"
// By aliasing these cases to match the versions above, we can automatically
// reuse the transformer methods.
braket : "{{braket|bra|" template "}}" -> bra
       | "{{braket|ket|" template "}}" -> ket
       | "{{braket|bra-ket|" template "|" template "}}" -> bra_ket

closed_open: "{{closed-open|" template "}}"
closed_closed: "{{closed-closed|" template "}}"
open_open: "{{open-open|" template "}}"
open_closed: "{{open-closed|" template "}}"

brace: "{{" "brace" "|" template "}}"
norm: "{{norm|" template "}}"

sup: "<sup>" template "</sup>"
sub: "<sub>" template "</sub>"
sfrac: "{{sfrac|" template "|" template "}}"

// TODO: Revisit with priority compared to the terminal that picks up '
italic: "''" template "''"
bold: "'''" template "'''"

// TODO: Revisit
overset: "{{overset|" template "|" template "}}"
overline: "{{overline|" template "}}"


// su: "{{" "su" "|" "p=" template "|" "b=" template "|" "a=" template "}}"
//   | "{{" "su" "|" "p=" template "|" "a=" template "|" "b=" template "}}"
//   | "{{" "su" "|" "b=" template "|" "p=" template "|" "a=" template "}}"
//   | "{{" "su" "|" "b=" template "|" "a=" template "|" "p=" template "}}"
//   | "{{" "su" "|" "a=" template "|" "p=" template "|" "b=" template "}}"
//   | "{{" "su" "|" "a=" template "|" "b=" template "|" "p=" template "}}"

//%import common.WORD -> WORD
LETTER: /\w/
SYMBOL: /[^\w\{|\}<>]/
CHARACTER: LETTER | SYMBOL
WORD: CHARACTER+
"""

l = lark.Lark(grammar, start="math")


class TemplateToLaTex(lark.Transformer):
    def math(self, templates):
        return "$" + "".join(templates) + "$"

    def template(self, children):
        return children[0]

    def brace(self, param):
        param = param[0]
        return "\{" + param + "\}"

    def bra(self, param):
        param = param[0]
        return rf"\langle {param} |"

    def ket(self, param):
        param = param[0]
        return rf"| {param} \rangle"

    def bra_ket(self, params):
        bra, ket = params
        return rf"\langle {bra} | {ket} \rangle"

    def closed_closed(self, param):
        param = param[0]
        return f"[{param}]"

    def closed_open(self, param):
        param = param[0]
        return f"[{param})"

    def open_closed(self, param):
        param = param[0]
        return f"({param}]"

    def open_open(self, param):
        param = param[0]
        return f"({param})"

    def overset(self, param):
        over, under = param
        return rf"\overset{{{over}}}{{{under}}}"

    def overline(self, param):
        return rf"\overline{{{param[0]}}}"

    def sup(self, inner):
        return rf"^{{{inner[0]}}}"

    def sub(self, inner):
        return rf"_{{{inner[0]}}}"

    def sfrac(self, args):
        num, denom = args
        return f"{num}/{denom}"

    def norm(self, param):
        param = param[0]
        return rf"\| {param} \|"

    def italic(self, param):
        # Math mode is already italic.
        return param[0]

    def bold(self, param):
        return rf"\mathbf{{{param[0]}}}"

    def phi(self, _):
        return r"\phi"

    def pi(self, _):
        return r"\pi"

    def delta(self, _):
        return r"\delta"

    def equal(self, _):
        return "="

    def pipe(self, _):
        return "|"

    def ell(self, _):
        return r"\ell"

    def WORD(self, w):
        return w
