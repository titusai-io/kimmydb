#!/usr/bin/env python3
"""Write the Decimal128 arithmetic's golden corpus (ADR-252).

    python3 scripts/decimal_golden.py > crates/kimmy-core/tests/data/decimal_golden.tsv

libmpdec (Python's `decimal`) is the oracle: every expected value is what its
IEEE context answers, so the corpus holds kimmydb to a second implementation's
bytes on both release architectures (x86_64 and aarch64), in release builds.
It is generated, not hand-written, from a fixed seed, and checked in, so a
change in any result is a visible diff.

The context is the one the design fixes: 34 digits, emax 6144, emin -6143,
round half to even, clamp 1, traps off. libmpdec propagates NaN payloads and
does not refuse a zero divisor, so NaN results are written as the one NaN and no
row divides by a zero.

Row kinds (tab separated; a value is `[-]<coefficient>E<exponent>`, `NaN`,
`Infinity` or `-Infinity`):

    r1      <binary64 bits, hex>  <value>  <inexact 0|1>
    add|sub|mul|div  <value>  <value>  <value>
    sum|avg          <value> <value> ...  <value>        (the multiset, then the answer)
    prod             <value> <value> ...  <value>  <value|->   (a chain left to right, each step rounded,
                                                              then the exact product rounded once)
    todouble  <value>  <binary64 bits, hex>
    tolong    <value>  <integer | ERR>                     (truncated; ERR outside i64)
    tostr     <value>  <BSON text form>
    parse     <text>   <value | ERR>                       (the strict parser)
"""
import decimal
import random
import struct
import sys
from decimal import Decimal, Context, ROUND_HALF_EVEN

PREC, EMAX, EMIN = 34, 6144, -6143
ETINY, ETOP = EMIN - (PREC - 1), EMAX - (PREC - 1)

IEEE = Context(prec=PREC, rounding=ROUND_HALF_EVEN, Emax=EMAX, Emin=EMIN, clamp=1, traps=[])
# Exact arithmetic for sums and products: wide enough for any span of exponents.
EXACT = Context(prec=200_000, rounding=ROUND_HALF_EVEN, Emax=decimal.MAX_EMAX,
                Emin=decimal.MIN_EMIN, clamp=0, traps=[])

rng = random.Random(20261023)


def show(d):
    """A value in the corpus's own spelling."""
    if d.is_nan():
        return "NaN"
    if d.is_infinite():
        return "-Infinity" if d.is_signed() else "Infinity"
    sign, digits, exp = d.as_tuple()
    coefficient = int("".join(map(str, digits))) if digits else 0
    return f"{'-' if sign else ''}{coefficient}E{exp}"


def make(sign, coefficient, exponent):
    return Decimal((sign, tuple(int(c) for c in str(coefficient)), exponent))


def coefficient():
    kind = rng.random()
    if kind < 0.55:
        digits = rng.randint(1, 34)
        low = 0 if digits == 1 else 10 ** (digits - 1)
        return rng.randint(low, 10 ** digits - 1)
    if kind < 0.65:
        return 0
    if kind < 0.72:
        return 1
    if kind < 0.79:
        return 10 ** PREC - 1
    if kind < 0.86:
        return 10 ** rng.randint(1, 33)
    if kind < 0.93:
        return 10 ** rng.randint(1, 33) - 1
    return 5 * 10 ** rng.randint(0, 32)


def exponent():
    kind = rng.random()
    if kind < 0.35:
        return rng.randint(-40, 40)
    if kind < 0.65:
        return rng.randint(ETINY, ETOP)
    if kind < 0.75:
        return ETINY
    if kind < 0.85:
        return ETOP
    if kind < 0.93:
        return rng.randint(ETINY, ETINY + 80)
    return rng.randint(ETOP - 80, ETOP)


def finite():
    return make(rng.getrandbits(1), coefficient(), exponent())


def special_or_finite(p_special=0.06):
    r = rng.random()
    if r < p_special / 3:
        return Decimal("NaN")
    if r < p_special:
        return Decimal("-Infinity" if rng.getrandbits(1) else "Infinity")
    return finite()


def near(a):
    """A second operand whose exponent is close to the first's."""
    if not a.is_finite():
        return finite()
    e = a.as_tuple().exponent + rng.randint(-40, 40)
    return make(rng.getrandbits(1), coefficient(), max(ETINY, min(ETOP, e)))


def pair():
    a = special_or_finite()
    return a, (near(a) if rng.random() < 0.5 else special_or_finite())


def out(*fields):
    print("\t".join(str(f) for f in fields))


def binary_rows(n):
    ops = {
        "add": IEEE.add,
        "sub": IEEE.subtract,
        "mul": IEEE.multiply,
        "div": IEEE.divide,
    }
    for name, f in ops.items():
        done = 0
        while done < n:
            a, b = pair()
            if name == "div" and b.is_zero():
                continue  # a zero divisor is refused, not answered
            out(name, show(a), show(b), show(f(a, b)))
            done += 1


def exact_sum(terms):
    total = terms[0]
    for t in terms[1:]:
        total = EXACT.add(total, t)
    return total


def exact_product(terms):
    product = terms[0]
    for t in terms[1:]:
        product = EXACT.multiply(product, t)
    return product


def multiset_rows(n):
    for _ in range(n):
        size = rng.randint(1, 9)
        if rng.random() < 0.3:
            # Terms at nearby exponents, so the sum carries and cancels.
            base = finite()
            terms = [base] + [near(base) for _ in range(size - 1)]
        else:
            terms = [special_or_finite(0.03) for _ in range(size)]
        shown = " ".join(show(t) for t in terms)
        total = exact_sum(terms)
        # The sum: exact, rounded once. The mean: the exact sum over the count,
        # rounded once. The product: exact, rounded once.
        out("sum", shown, show(IEEE.create_decimal(total)))
        out("avg", shown, show(IEEE.divide(total, Decimal(size))))
    # The design's worked examples.
    for text in (
        ["1E+34", "0.5", "0.5", "-1E+34"],
        ["1E+6111", "1E-6176"],
        ["1.00", "2.00", "2.00"],
        ["2.693319332926108795000640676319904E+35", "7.447367703126639551991307315256628E+34"],
    ):
        terms = [Decimal(t) for t in text]
        shown = " ".join(show(t) for t in terms)
        total = exact_sum(terms)
        out("sum", shown, show(IEEE.create_decimal(total)))
        out("avg", shown, show(IEEE.divide(total, Decimal(len(terms)))))


def chain(terms):
    """A product left to right, each step rounded: what libmpdec gives for one
    multiplication after another, and what IEEE 754 defines a sequence of
    operations to be."""
    acc = terms[0]
    for t in terms[1:]:
        acc = IEEE.multiply(acc, t)
    if len(terms) == 1:
        acc = IEEE.multiply(Decimal(1), acc)
    return acc


def product_rows(n):
    """`prod <terms> <chain> <exact-once>`: the chain with each step rounded, and
    beside it the exact product rounded once (`-` when a term is not finite).
    The two differ where a step overflows, underflows or rounds, which is what
    the rows are for: the test asserts that many of them do."""
    def row(terms):
        shown = " ".join(show(t) for t in terms)
        stepwise = show(chain(terms))
        once = show(IEEE.create_decimal(exact_product(terms))) if all(t.is_finite() for t in terms) else "-"
        out("prod", shown, stepwise, once)

    for _ in range(n):  # random chains
        size = rng.randint(1, 8)
        row([special_or_finite(0.03) for _ in range(size)])
    for _ in range(n):  # 34-digit coefficients near 1: every step rounds
        size = rng.randint(2, 7)
        row([make(0, 10 ** 33 + rng.randint(0, 10 ** 9), rng.randint(-34, -30))
             if rng.random() < 0.5 else
             make(0, rng.randint(10 ** 34 - 10 ** 9, 10 ** 34 - 1), rng.randint(-35, -33))
             for _ in range(size)])
    for _ in range(n):  # extreme exponents: a step overflows or underflows
        size = rng.randint(3, 6)
        row([make(rng.getrandbits(1), coefficient() or 1, rng.choice([rng.randint(ETOP - 40, ETOP), rng.randint(ETINY, ETINY + 40), rng.randint(-3000, 3000)]))
             for _ in range(size)])
    for text in (
        ["1E+5000", "1E+5000", "1E-5000"],
        ["1E+5000", "1E-5000", "1E+5000"],
        ["1E-5000", "1E+5000", "1E+5000"],
        ["1.5", "1.0", "2"],
        ["1.000000000000000000000000000000001"] * 3,
        ["9.999999999999999999999999999999999"] * 4,
        ["2.30"],
        ["-0", "5", "Infinity"],
        ["5", "Infinity", "-0"],
    ):
        row([Decimal(t) for t in text])


def bits_to_double(bits):
    return struct.unpack(">d", struct.pack(">Q", bits))[0]


def double_to_bits(d):
    return struct.unpack(">Q", struct.pack(">d", d))[0]


def r1_rows(n):
    import math

    def emit(bits):
        d = bits_to_double(bits)
        if math.isnan(d):
            out("r1", f"{bits:016x}", "NaN", 0)
            return
        if math.isinf(d):
            out("r1", f"{bits:016x}", "Infinity" if d > 0 else "-Infinity", 0)
            return
        IEEE.clear_flags()
        r = IEEE.create_decimal_from_float(d)
        out("r1", f"{bits:016x}", show(r), int(bool(IEEE.flags[decimal.Inexact])))

    for _ in range(n):  # random bits
        emit(rng.getrandbits(64))
    for _ in range(n // 2):  # every binade, edge mantissas
        e = rng.randint(0, 2046)
        m = rng.choice([0, 1, (1 << 52) - 1, rng.getrandbits(52)])
        emit((rng.getrandbits(1) << 63) | (e << 52) | m)
    for _ in range(n):  # the tie doubles: 35-digit expansions, f in [-50, -26]
        f = rng.randint(-50, -26)
        m = rng.randint(1 << 52, (1 << 53) - 1)
        d = math.ldexp(m, f)
        emit(double_to_bits(-d if rng.getrandbits(1) else d))
    for _ in range(n // 4):  # two-place prices and tidy values
        d = rng.randint(0, 10_000_000) / 10 ** rng.randint(0, 3)
        emit(double_to_bits(d))
    for _ in range(n // 8):  # integral doubles
        emit(double_to_bits(float(rng.randint(-10 ** 6, 10 ** 6) * 10 ** rng.randint(0, 20))))
    for bits in (0, 1 << 63, 1, (1 << 52) - 1, 1 << 52, 0x7FEFFFFFFFFFFFFF, 0x7FF0000000000000,
                 0xFFF0000000000000, 0x7FF8000000000000):
        emit(bits)


def conversion_rows(n):
    for _ in range(n):
        d = special_or_finite(0.05)
        if d.is_nan():
            continue
        if d.is_infinite():
            out("todouble", show(d), f"{double_to_bits(float(d)):016x}")
            continue
        out("todouble", show(d), f"{double_to_bits(float(str(d))):016x}")
    for _ in range(n):
        d = finite()
        if rng.random() < 0.6:
            # Magnitudes that matter to integer conversion.
            d = make(rng.getrandbits(1), rng.randint(0, 10 ** rng.randint(1, 25)), rng.randint(-25, 3))
        v = int(d)  # truncates toward zero
        out("tolong", show(d), v if -(2 ** 63) <= v < 2 ** 63 else "ERR")
    for d in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        out("tolong", show(d), "ERR")
    for _ in range(n):
        d = finite()
        out("tostr", show(d), str(d))
    for d in ("Infinity", "-Infinity", "0", "-0", "0E+6111", "1E-6176"):
        out("tostr", show(Decimal(d)), str(Decimal(d)))


GRAMMAR = [
    "0", "-0", "+0", "1", "1.", ".1", "1.5", "-1.50", "+1.50", "1e5", "1E+5", "1E-5", "1.50E+3",
    "12345678901234567890123456789012345", "1234567890123456789012345678901234",
    "1.0000000000000000000000000000000000000", "0.0000000000000000000000000000000000001",
    "1E+6144", "1E+6145", "9.999999999999999999999999999999999E+6144", "1E-6176", "1E-6177",
    "1.0E-6176", "0E+9999", "0E-9999", "00012", "-00.100",
    "", ".", "++1", "-+1", "e100", "1e", "1e+", "0x10", " 1", "1 ", "1,5", "1_0", "--1", "1.2.3",
    "Infinity", "-Infinity", "+Inf", "inf", "INFINITY", "NaN", "nan", "-NaN", "Inf1", "In", "sNaN",
]


def parse_rows():
    import re
    syntax = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
    special = re.compile(r"^[+-]?(inf|infinity)$", re.I)
    nan = re.compile(r"^[+-]?s?nan$", re.I)
    for text in GRAMMAR:
        if special.match(text):
            out("parse", text, "-Infinity" if text.startswith("-") else "Infinity")
        elif nan.match(text):
            out("parse", text, "NaN")
        elif not syntax.match(text):
            out("parse", text, "ERR")
        else:
            IEEE.clear_flags()
            d = IEEE.create_decimal(text)
            lost = IEEE.flags[decimal.Inexact] or IEEE.flags[decimal.Overflow]
            # A value that would need rounding is refused: exact or an error.
            out("parse", text, "ERR" if lost else show(d))
    for _ in range(150):
        # Random well-formed texts of every length.
        digits = "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 40)))
        text = digits
        if rng.random() < 0.5:
            cut = rng.randint(0, len(digits))
            text = digits[:cut] + "." + digits[cut:]
            if text == ".":
                text = "0."
        if rng.random() < 0.5:
            text += rng.choice("eE") + rng.choice(["", "+", "-"]) + str(rng.randint(0, 6200))
        if rng.random() < 0.3:
            text = rng.choice(["", "-", "+"]) + text
        IEEE.clear_flags()
        if not syntax.match(text):
            out("parse", text, "ERR")
            continue
        d = IEEE.create_decimal(text)
        lost = IEEE.flags[decimal.Inexact] or IEEE.flags[decimal.Overflow]
        out("parse", text, "ERR" if lost else show(d))


def main():
    print(f"# Generated by scripts/decimal_golden.py with libmpdec {decimal.__libmpdec_version__}; "
          "seed 20261023. Do not edit: regenerate.")
    r1_rows(500)
    binary_rows(350)
    multiset_rows(250)
    product_rows(120)
    conversion_rows(200)
    parse_rows()


if __name__ == "__main__":
    sys.exit(main())
