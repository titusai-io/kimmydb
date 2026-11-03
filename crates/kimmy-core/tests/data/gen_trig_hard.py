#!/usr/bin/env python3
"""Generate `trig_hard.tsv` and `trig_kernels.tsv`: the hard cases of the decimal trigonometry,
for `src/decimal/arith/func/trig_hard_tests.rs` (the unit tests of `kimmy-core`, which a mutation
run of the crate executes).

    python3 gen_trig_hard.py                 (about a minute; writes both files beside itself)

`trig_hard.tsv` has the columns of `trig_oracle.tsv` (function, operand -- `y x` apart for
atan2 --, the 34-digit answer, the true value to 50 digits) and the same method: Python's
`decimal` at 250 digits through the functions of `gen_trig_oracle.py`, each checked by its
second route, and an operand whose true value is within 1e-40 of a rounding midpoint dropped.
The rows are the edges of the code and not a sample: either side of `LARGE` (50) and of the
operand at which `sinh` and `cosh` overflow, of `TINY` and `HUGE` (1e-40, 1e+40), the operands
that put `(1 + x) / (1 - x)`, `x + sqrt(x^2 - 1)` and `x + sqrt(x^2 + 1)` near one and at
their largest, and `atan2` with a ratio 79, 80 and 81 digits down or more, for a negative `x`
too. A few `atan2` rows are the exception to the midpoint rule, on purpose: a ratio that is
exactly a midpoint of the 34-digit grid (35 digits, the last a 5) has an arctangent a hair below
it (`r - r^3/3`), so the 34-digit answer is the lower neighbour whatever the evenness of its last
digit; those rows are rounded from the series to 500 digits, where the hair is in the digits.

`trig_kernels.tsv` has the private kernels, one tab-separated row each:

    ln_fixed   N        S                     ln(N / 10^114) at 114 places, truncated: sign, digits
    fixed_ratio limbs cy ey cx ex  N          floor(|y/x| * 10^(19 limbs))
    reduce     c e      quadrant R            c*10^e modulo pi/2: the quadrant, and the
                                              remainder at 133 places, truncated
"""
import os
import sys
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Context, Decimal as D, getcontext, localcontext
from fractions import Fraction

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_trig_oracle as g  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EMAX = 6144


def row(name, operand, value, rows):
    """Append (name, operand, 34-digit answer, 50-digit truth), `Infinity` past the format."""
    out = g.rounded(value)
    if out is None:
        sys.stderr.write(f"dropped (near a midpoint): {name} {operand}\n")
        return
    r34, true50 = out
    if r34.adjusted() > EMAX:
        rows.append((name, operand, "Infinity" if value > 0 else "-Infinity", "overflow"))
        return
    rows.append((name, operand, str(r34), str(true50)))


def forced_row(name, operand, value, rows, prec=600):
    """The same, for a value that IS within 1e-40 of a midpoint by construction: the caller gives
    `value` to enough digits (`prec`) that it is on its side of the midpoint, and the rounding is
    exact."""
    with localcontext() as c:
        c.prec = prec
        r34 = Context(prec=34, rounding=ROUND_HALF_EVEN).plus(value)
        true50 = Context(prec=50, rounding=ROUND_DOWN).plus(value)
    rows.append((name, operand, str(r34), str(true50)))


def small_ratio_series(r, prec):
    """atan r for 0 < r < 1e-79: r - r^3/3 + r^5/5 - r^7/7, in `prec` digits (enough to hold the
    hair r^3/3 below r: twice the digits of the ratio's smallness, and the digits kept)."""
    with localcontext() as c:
        c.prec = prec
        return r - r ** 3 / 3 + r ** 5 / 5 - r ** 7 / 7


FUNCS = {
    "sinh": g.sinh, "cosh": g.cosh, "tanh": g.tanh,
    "asinh": g.asinh, "acosh": g.acosh, "atanh": g.atanh,
}


def function_rows():
    rows = []

    def add(name, operands):
        for text in operands:
            x = D(text)
            v = FUNCS[name](x)
            g.checked(name, x, v)
            row(name, text, v, rows)

    # sinh, cosh, tanh either side of LARGE = 50 (e^x/2 from there on), of adjusted exponents
    # 0, 1, 2 and of the 4 past which e^x overflows; and the largest operand that does not.
    large = [
        "9.999999999999999999999999999999999", "10", "49.99999999999999999999999999999999",
        "50", "50.00000000000000000000000000000001", "60", "99.99999999999999999999999999999999",
        "100", "-50", "-49.99999999999999999999999999999999", "-60",
        "999.9999999999999999999999999999999", "9999.999999999999999999999999999999", "10000",
        "-10000", "12000", "-12000", "14000", "14149", "14149.3", "14149.4", "14150", "14150.06", "14150.07", "14150.08", "-14150",
        "14300", "14301", "100000",
    ]
    add("sinh", large)
    add("cosh", large)
    add("tanh", large)

    # atanh: the operand a hair from one (the ratio is 2 / 1e-34), from zero, either side of TINY.
    atanh = [
        "0.9999999999999999999999999999999999", "-0.9999999999999999999999999999999999",
        "0.999999999999999999999999999999999", "0.99999999999999999999",
        "0.5000000000000000000000000000000001", "0.1", "1E-20", "-1E-20",
        "1.234567890123456789012345678901234E-39", "-1.234567890123456789012345678901234E-39",
        "1E-39", "1.5E-39", "9.999999999999999999999999999999999E-40",
        "1.234567890123456789012345678901234E-40", "9.99999999999999999999999999999999E-41",
    ]
    add("atanh", atanh)

    # asinh: either side of HUGE (1e40, where it is ln(2x)) and of TINY, and the middle.
    asinh = [
        "9.999999999999999999999999999999999E+39", "1E+40", "-1E+40",
        "1.000000000000000000000000000000001E+40", "1E+41", "1E+100", "-1E+6000",
        "1E-39", "-1E-39", "1E-40", "9.9E-41", "0.5", "1", "2", "10", "1E+20",
        "123456789.1234567891234567891234567", "-3.000000000000000000000000000000001E+30",
    ]
    add("asinh", asinh)

    # acosh: a hair above one (the root is about 4.5e-17), the middle, either side of HUGE.
    acosh = [
        "1.000000000000000000000000000000001", "1.000000000000000000000000000000100",
        "1.00000000001", "1.5", "2", "10", "1E+20", "123456789.1234567891234567891234567",
        "9.999999999999999999999999999999999E+39", "1E+40", "1.000000000000000000000000000000001E+40",
        "1E+6000",
    ]
    add("acosh", acosh)

    # atan2: a ratio of 1e-79 down to 1e-81 and far below, for a positive and a negative x, with
    # a y and an x of few and many digits (the lift of the exact quotient follows the digits).
    atan2_pairs = []
    for gap in (79, 80, 81, 82, 100, 150, 250, 2000):
        for ytext, xtext in (
            ("7.3E{}", "9.1E-21"), ("1.234567890123456789012345678901234E{}", "1E-21"),
            ("5E{}", "9.876543210987654321098765432109876E-21"),
            ("1.111111111111111111111111111111111E{}", "3.333333333333333333333333333333333E-21"),
        ):
            # y exponent: |x| is about 1e-21 (adjusted -21 or -21), so |y| about 1e(-21 - gap)
            ey = -21 - gap
            for sign_y, sign_x in (("", ""), ("-", ""), ("", "-"), ("-", "-")):
                if sign_x == "-" and gap > 400:
                    continue
                atan2_pairs.append((sign_y + ytext.format(ey), sign_x + xtext))
    for ytext, xtext in atan2_pairs:
        y, x = D(ytext), D(xtext)
        gap = x.adjusted() - y.adjusted()
        v = g.atan2(y, x)
        # second route as in the oracle: the angle's sine and cosine are y and x over the hypotenuse
        with localcontext() as c:
            c.prec = g.P + 80 + 4 * gap
            closed = (g.pi_to(c.prec) * (1 if y >= 0 else -1) if x < 0 else 0) + y / x
            assert g.close(v, closed, 60), (ytext, xtext)
        row("atan2", f"{ytext} {xtext}", v, rows)

    # the ratio is a midpoint of the 34-digit grid: 35 digits ending in a 5 (an odd y over 2, 4
    # or 8 -- here 2 -- or an even y over 5...). Its arctangent is the ratio less r^3/3, so the
    # 34-digit answer is the neighbour below it (in magnitude), never half-even's choice.
    # The y below ends ...875: half to even of the exact ratio would round the 7 up to 8.
    for gap in (81, 82, 100, 300, 2000):
        ytext = f"9.876543210987654321098765432109875E{-18 - gap}"
        xtext = "2E-18"
        for sign in ("", "-"):
            y, x = D(sign + ytext), D(xtext)
            prec = 2 * gap + 700
            v = small_ratio_series(y / x, prec)
            forced_row("atan2", f"{sign + ytext} {xtext}", v, rows, prec)
        # an even digit before the 5, where half to even and the hair agree
        ytext = f"9.876543210987654321098765432109855E{-18 - gap}"
        y, x = D(ytext), D(xtext)
        forced_row("atan2", f"{ytext} {xtext}", small_ratio_series(y / x, 2 * gap + 700), rows, 2 * gap + 700)
    # an exact ratio of few digits: the answer is the ratio, a hair below
    for gap in (81, 90, 500):
        forced_row("atan2", f"3E{-18 - gap} 1E-18", small_ratio_series(D(f"3E-{gap}"), 2 * gap + 700), rows, 2 * gap + 700)
    return rows


# ---------------------------------------------------------------------------
# The kernels
# ---------------------------------------------------------------------------

S = 114


def ln_rows():
    values = [
        "1", "1.0000000000000000000000000000000001", "1.5", "2",
        # the mantissa boundary sqrt(10) = 3.16227766016837933199889354443271853...
        "3.162277660168379331998893544432718", "3.162277660168379331998893544432719",
        "3.162277660168379331998893544432720", "10", "9.99999999999999999999999999999999",
        "31.62277660168379331998893544432718", "100", "1E+20", "2E+34",
        "1.23456789012345678901234567890123456789E+39", "0.9999999999999999999", "0.5", "0.1",
        "0.316227766016837933199889354443271", "1E-10", "1E-50", "1E-80", "1E-81", "1E-90",
        "1.234567890123456789E-100", "7.000000000000000000000000000000000000000000000001E+30",
        # all 114 places in use
        "1." + "1234567890" * 12, "0." + "2345678901" * 12, "98765432109876543210." + "9876543210" * 12,
        "4." + "7" * 150,
    ]
    rows = []
    with localcontext() as c:
        c.prec = 400
        for text in values:
            v = D(text).quantize(D(1).scaleb(-S), rounding=ROUND_DOWN)
            n = int(v.scaleb(S))
            ln = v.ln()
            floor_ln = int(abs(ln).scaleb(S).to_integral_value(rounding=ROUND_DOWN))
            rows.append(("ln_fixed", str(n), ("-" if ln < 0 else "") + str(floor_ln)))
    return rows


def ratio_rows():
    rows = []
    cases = [
        # shift = ey - ex + places: positive, zero, negative (a numerator cut to few digits)
        (7, "1234567890123456789012345678901234", -133, 1, 0),
        (7, "1234567890123456789012345678901234", -220, 1, 0),
        (11, "1234567890123456789012345678901234", -220, 1, 0),
        (11, "1234567890123456789012345678901234", -230, 3, 0),
        (11, "1234567890123456789012345678901234", -209 - 33, 1, 0),
        (11, "1234567890123456789012345678901234", -209 - 34, 1, 0),
        (11, "7", 5, "9876543210987654321098765432109876", -30),
        (12, "1", -300, "9", 17),
        (12, "5555555555555555555555555555555555", 10, "5555555555555555555555555555555555", 10),
        (7, "42", 0, "1", 0),
        (7, "1", 0, "3", 0),
        (7, "1", -134, "3", 0),
    ]
    for limbs, cy, ey, cx, ex in cases:
        want = Fraction(int(cy)) * Fraction(10) ** ey / (Fraction(int(cx)) * Fraction(10) ** ex)
        n = (want * 10 ** (19 * limbs)).__floor__()
        rows.append(("fixed_ratio", str(limbs), cy, str(ey), str(cx), str(ex), str(n)))
    return rows


def reduce_rows():
    rows = []
    operands = [
        ("7", 0), ("1", 0), ("1570796326794896619231321691639751", -33),
        ("1", 18), ("1", 19), ("1", 20), ("1", 100), ("3", 300),
        ("1234567890123456789012345678901234", 1000), ("1", 6000),
        ("5555555555555555555555555555555555", 6111),
    ]
    for c, e in operands:
        x = D(c).scaleb(e)
        adj = x.adjusted()
        wide = 250 + max(adj, 0) + 200
        with localcontext() as ctx:
            ctx.prec = wide
            half = g.pi_to(wide + 30) / 2
            q = (x / half).to_integral_value(rounding="ROUND_FLOOR")
            r = x - q * half
            quadrant = int(q % 4)
            n = int(r.scaleb(133).to_integral_value(rounding=ROUND_DOWN))
        rows.append(("reduce", c, str(e), str(quadrant), str(n)))
    return rows


def main():
    getcontext().prec = g.P + 60
    g.pi_check()
    rows = function_rows()
    with open(os.path.join(HERE, "trig_hard.tsv"), "w") as f:
        f.write("\n".join("\t".join(r) for r in rows) + "\n")
    sys.stderr.write(f"{len(rows)} function rows\n")
    kernels = ln_rows() + ratio_rows() + reduce_rows()
    with open(os.path.join(HERE, "trig_kernels.tsv"), "w") as f:
        f.write("\n".join("\t".join(r) for r in kernels) + "\n")
    sys.stderr.write(f"{len(kernels)} kernel rows\n")


if __name__ == "__main__":
    main()
