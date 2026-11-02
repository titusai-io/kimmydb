#!/usr/bin/env python3
"""Generate `trig_oracle.tsv`: the correctly rounded 34-digit value of each decimal trigonometric
and hyperbolic function at a table of operands, for `tests/decimal_trig_oracle.rs`.

    python3 -I gen_trig_oracle.py > trig_oracle.tsv      (about 20 seconds)

The values come from Python's `decimal` at 250 digits (more for an operand of large exponent),
with `sin`/`cos` reduced modulo pi/2 against pi from Machin's formula, the arctangent by
argument halving and a series, and the hyperbolic functions from `decimal`'s own correctly
rounded `exp` and `ln`. Each is checked by a second route before it is written, and the routes
share no algorithm with the first:

- pi, to the full width any row uses (about 6,500 digits), by the Chudnovsky series with integer
  binary splitting, against Machin's formula;
- `sin`, `cos` and `tan` by reduction modulo 2 pi (not pi/2, so no quadrant table) with that
  independent pi, the series on r/3^25 and 25 triple-angle steps;
- `atan` by Newton's iteration on `tan`; the inverse functions by sending the answer back
  through their forward function (to 150 digits);
- `sinh` and `tanh` by the series for a small operand and `exp` otherwise, `cosh` by its own
  series, `asinh`, `acosh` and `atanh` by the round trip;
- `atan2` by the sine and cosine of the angle over the hypotenuse, or the first-order form for a
  ratio past 1e100.

An operand whose true value is within 1e-40 of a rounding midpoint is dropped, so the 34-digit
answer is not in doubt.

Columns: function, operand (for atan2, `y x` apart), the 34-digit answer, the true value to 50
digits (truncated). The random generator is seeded, so the file is the same on every run.
"""
import random
import sys
from decimal import ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_EVEN, Context, Decimal as D, getcontext, localcontext

P = 250            # digits of the working precision
CHECK = 200        # digits the cross-checks must agree to
_PI = {}


def pi_to(prec):
    """pi to `prec` digits, by Machin's formula, cached at the widest asked."""
    best = max([k for k in _PI if k >= prec], default=None)
    if best is not None:
        return _PI[best]
    with localcontext() as c:
        c.prec = prec + 25

        def atan_inverse(n):
            n2 = n * n
            term = D(1) / n
            total = term
            k, sign = 1, -1
            limit = D(10) ** -(prec + 25)
            while term > limit:
                term /= n2
                k += 2
                total += sign * term / k
                sign = -sign
            return total

        value = 16 * atan_inverse(5) - 4 * atan_inverse(239)
    _PI[prec] = value
    return _PI[prec]


def chudnovsky_pi(digits):
    """pi by the Chudnovsky series with integer binary splitting: nothing shared with Machin."""
    from math import isqrt

    def split(a, b):
        if b - a == 1:
            if a == 0:
                pab = qab = 1
            else:
                pab = (6 * a - 5) * (2 * a - 1) * (6 * a - 1)
                qab = a * a * a * 10939058860032000
            tab = pab * (545140134 * a + 13591409)
            return pab, qab, -tab if a % 2 else tab
        m = (a + b) // 2
        pam, qam, tam = split(a, m)
        pmb, qmb, tmb = split(m, b)
        return pam * pmb, qam * qmb, qmb * tam + pam * tmb

    sys.setrecursionlimit(10000)
    sys.set_int_max_str_digits(0)
    guard = 30
    terms = (digits + guard) // 14 + 2
    _, q, t = split(0, terms)
    scale = 10 ** (digits + guard)
    return D((426880 * isqrt(10005 * scale * scale) * q) // t).scaleb(-(digits + guard))


# The widest pi any row uses: the reduction of 5E+6143 at 250 digits and 40 guard digits.
PI_WIDTH = 6500


def pi_check():
    """Machin's pi against Chudnovsky's, over the whole width the rows use."""
    with localcontext() as c:
        c.prec = PI_WIDTH + 100
        machin = pi_to(PI_WIDTH + 40)
        chudnovsky = chudnovsky_pi(PI_WIDTH + 40)
        assert abs(machin - chudnovsky) < D(10) ** -(PI_WIDTH + 30), "pi disagrees"


def taylor_sincos(r, prec):
    """(sin r, cos r) for |r| < 2, by the Taylor series at `prec` digits."""
    with localcontext() as c:
        c.prec = prec + 10
        r2 = r * r
        limit = D(10) ** -(prec + 10)
        s, term, k = r, r, 1
        while abs(term) > limit:
            term = -term * r2 / ((2 * k) * (2 * k + 1))
            s += term
            k += 1
        cs, term, k = D(1), D(1), 1
        while abs(term) > limit:
            term = -term * r2 / ((2 * k - 1) * (2 * k))
            cs += term
            k += 1
        return s, cs


def sincos(x, prec=P):
    """(sin x, cos x) for any decimal x: reduced modulo pi/2, then the series."""
    if x == 0:
        return D(0), D(1)
    adj = max(0, x.adjusted())
    # a tiny operand below zero reduces to a hair under pi/2 (or 2 pi): its distance from the
    # multiple needs the digits of the smallness as well
    wide = prec + adj + 40 + max(0, -x.adjusted())
    with localcontext() as c:
        c.prec = wide
        half = pi_to(wide) / 2
        q = (x / half).to_integral_value(rounding=ROUND_FLOOR)
        r = x - q * half
        quadrant = int(q % 4)
    extra = max(0, -x.adjusted())          # an operand near zero keeps its digits
    s, cs = taylor_sincos(r, prec + 20 + extra)
    with localcontext() as c:
        c.prec = prec + 20 + extra
        return [(s, cs), (cs, -s), (-s, -cs), (-cs, s)][quadrant]


def sincos_by_tripling(x, prec=P):
    """The same by a route that shares neither the reduction nor the quadrants: reduced modulo
    2 pi with the Chudnovsky pi, the series on r / 3^25, and 25 triple-angle steps."""
    if x == 0:
        return D(0), D(1)
    adj = max(0, x.adjusted())
    extra = max(0, -x.adjusted())
    wide = prec + adj + 60 + extra
    with localcontext() as c:
        c.prec = wide
        two_pi = 2 * chudnovsky_pi(wide + 10)
        q = (x / two_pi).to_integral_value(rounding=ROUND_FLOOR)
        r = x - q * two_pi
    steps = 25
    s, cs = taylor_sincos(r / D(3) ** steps, prec + 40 + extra)
    with localcontext() as c:
        c.prec = prec + 60 + extra
        for _ in range(steps):
            s, cs = 3 * s - 4 * s ** 3, 4 * cs ** 3 - 3 * cs
        return s, cs


def atan(x, prec=P):
    """The arctangent by argument halving and the series."""
    if x == 0:
        return D(0)
    if x < 0:
        return -atan(-x, prec)
    with localcontext() as c:
        c.prec = prec + 30
        if x > 1:
            return pi_to(prec + 30) / 2 - atan(1 / x, prec)
        t = x
        for _ in range(6):
            t = t / (1 + (1 + t * t).sqrt())
        t2 = t * t
        total, power, k, sign = t, t, 1, -1
        limit = D(10) ** -(prec + 30)
        while power > limit:
            power *= t2
            k += 2
            total += sign * power / k
            sign = -sign
        return total * 64


def atan_by_newton(x, prec=P):
    """The arctangent by Newton's iteration on tan (the sine and cosine above), from a double."""
    import math
    if x < 0:
        return -atan_by_newton(-x, prec)
    with localcontext() as c:
        c.prec = prec + 30
        if x > 1:
            return pi_to(prec + 30) / 2 - atan_by_newton(1 / x, prec)
        y = D(math.atan(float(x))) if x > D("1e-300") else x
        for _ in range(9):
            s, cs = sincos(y, prec + 10)
            tan = s / cs
            y = y - (tan - x) * cs * cs
        return y


def asin(x, prec=P):
    with localcontext() as c:
        c.prec = prec + 30
        if abs(x) == 1:
            return D(1 if x > 0 else -1) * pi_to(prec + 30) / 2
        return atan(x / (1 - x * x).sqrt(), prec)


def acos(x, prec=P):
    with localcontext() as c:
        c.prec = prec + 30
        return pi_to(prec + 30) / 2 - asin(x, prec)


def wide_prec(x, prec=P):
    """Extra digits for an operand near zero, whose differences cancel."""
    return prec + 2 * max(0, -x.adjusted()) + 40


def sinh(x, prec=P):
    with localcontext() as c:
        c.prec = wide_prec(x, prec) + max(0, x.adjusted()) * 3
        e = x.exp()
        return (e - 1 / e) / 2


def cosh(x, prec=P):
    with localcontext() as c:
        c.prec = prec + 40 + max(0, x.adjusted()) * 3
        e = x.exp()
        return (e + 1 / e) / 2


def tanh(x, prec=P):
    with localcontext() as c:
        c.prec = wide_prec(x, prec) + max(0, x.adjusted()) * 3
        e = (2 * x).exp()
        return (e - 1) / (e + 1)


def asinh(x, prec=P):
    with localcontext() as c:
        c.prec = wide_prec(x, prec) + max(0, x.adjusted()) + 40
        a = abs(x)
        value = (a + (a * a + 1).sqrt()).ln()
        return value if x >= 0 else -value


def acosh(x, prec=P):
    with localcontext() as c:
        c.prec = prec + 40 + max(0, x.adjusted())
        return (x + (x * x - 1).sqrt()).ln()


def atanh(x, prec=P):
    with localcontext() as c:
        c.prec = wide_prec(x, prec) + 40
        return ((1 + x) / (1 - x)).ln() / 2


def atan2(y, x, prec=P):
    with localcontext() as c:
        # an angle of pi less a tiny ratio keeps the ratio's digits only at this width
        c.prec = prec + 40 + max(0, -(y / x).adjusted() if x != 0 else 0) + 40
        pi = pi_to(c.prec)
        if x > 0:
            return atan(y / x, prec)
        if x < 0:
            return (pi if y >= 0 else -pi) + atan(y / x, prec)
        return (pi / 2) * (1 if y > 0 else -1)


def close(a, b, digits=CHECK):
    with localcontext() as c:
        c.prec = P + 400
        scale = max(abs(a), abs(b))
        return abs(a - b) <= scale * D(10) ** -digits


def checked(name, x, value):
    """Cross-check `value` of `name` at `x` by a second route; raise if it disagrees."""
    with localcontext() as c:
        c.prec = P + 60
        if name == "sin":
            ok = close(value, sincos_by_tripling(x)[0])
        elif name == "cos":
            ok = close(value, sincos_by_tripling(x)[1])
        elif name == "tan":
            s, cs = sincos_by_tripling(x)
            ok = close(value, s / cs)
        elif name == "atan":
            ok = close(value, atan_by_newton(x))
        elif name == "asin":
            ok = close(sincos(value)[0], x, 150)
        elif name == "acos":
            ok = close(sincos(value)[1], x, 150)
        elif name == "sinh":
            e = x.exp()
            # direct series for a small operand, the exp form otherwise, to 150 digits
            if abs(x) < 1:
                total, term, k = x, x, 1
                while abs(term) > D(10) ** -(P + 60) * abs(x):
                    term = term * x * x / ((2 * k) * (2 * k + 1))
                    total += term
                    k += 1
                ok = close(value, total, 150)
            else:
                ok = close(value, (e - 1 / e) / 2, 150)
        elif name == "cosh":
            # its own series
            c.prec = max(c.prec, P + 60 + int(abs(x) / D("2.3")) + 40)
            total, term, k = D(1), D(1), 0
            limit = D(10) ** -(P + 60)
            while True:
                k += 1
                term = term * x * x / ((2 * k - 1) * (2 * k))
                total += term
                if abs(term) < limit * total and k > abs(x):
                    break
            ok = close(value, total, 150)
        elif name == "tanh":
            ok = close(value, sinh(x) / cosh(x), 150)
        elif name == "asinh":
            ok = close(sinh(value, P + 60), x, 150)
        elif name == "acosh":
            ok = close(cosh(value, P + 60), x, 150)
        elif name == "atanh":
            ok = close(tanh(value, P + 60), x, 150)
        else:
            ok = True
        assert ok, (name, x)


HALF_EVEN = Context(prec=34, rounding=ROUND_HALF_EVEN)
DOWN50 = Context(prec=50, rounding=ROUND_DOWN)


def rounded(value):
    """(34-digit answer, 50-digit truncation), or None when the value is near a midpoint."""
    with localcontext() as c:
        c.prec = P + 40
        r34 = HALF_EVEN.plus(value)
        down = Context(prec=34, rounding=ROUND_DOWN).plus(value)
        ulp = D(1).scaleb(down.adjusted() - 33)
        fraction = (value - down) / ulp if value >= 0 else (down - value) / ulp
        if abs(fraction - D("0.5")) < D("1e-40"):
            return None
        return r34, DOWN50.plus(value)


def mantissa(rng, digits=None):
    n = digits or rng.choice([34, 34, 34, 20, 12, 5, 1])
    return str(rng.randint(1, 9)) + "".join(str(rng.randint(0, 9)) for _ in range(n - 1))


def number(rng, exponent, sign=True, digits=None):
    m = mantissa(rng, digits)
    text = f"{m[0]}.{m[1:]}E{exponent}" if len(m) > 1 else f"{m}E{exponent}"
    if sign and rng.random() < 0.5:
        text = "-" + text
    return text


def main():
    getcontext().prec = P + 60
    pi_check()
    rng = random.Random(20261103)
    rows = []

    def emit(name, operand, value):
        if value.adjusted() > 6150:
            # past the largest Decimal128 by a margin: an overflow, which is an infinity
            rows.append((name, operand, "Infinity" if value > 0 else "-Infinity", "overflow"))
            return
        out = rounded(value)
        if out is None:
            return
        r34, true50 = out
        rows.append((name, operand, str(r34), str(true50)))

    trig_exps = [-100, -60, -40, -39, -30, -20, -10, -5, -2, -1, 0, 1, 2, 5, 10, 18, 19, 20, 30, 40, 100, 300, 1000, 3000, 6000]
    near_pi = ["1E+100", "3.141592653589793238462643383279503", "1.570796326794896619231321691639751", "6.283185307179586476925286766559006", "1E+6000", "5E+6143", "9.999999999999999999999999999999999E+100"]
    for name, fn in (("sin", lambda v: sincos(v)[0]), ("cos", lambda v: sincos(v)[1]), ("tan", lambda v: (lambda sc: sc[0] / sc[1])(sincos(v)))):
        operands = [number(rng, e) for e in trig_exps] + [number(rng, e, digits=34) for e in (0, 3, 50, 2000)] + near_pi
        for text in operands:
            x = D(text)
            v = fn(x)
            checked(name, x, v)
            emit(name, text, v)
    unit_exps = list(range(-100, 0, 6)) + [-1, -2, -3]
    near_one = [f"0.{'9' * k}" for k in (1, 2, 5, 15, 30, 33)] + ["0.5", "-0.5", "0.9999999999999999999999999999999999"]
    for name, fn in (("asin", asin), ("acos", acos)):
        operands = [number(rng, e) for e in unit_exps] + near_one
        for text in operands:
            x = D(text)
            v = fn(x)
            checked(name, x, v)
            emit(name, text, v)
    for text in [number(rng, e) for e in trig_exps + [4000, 6000]]:
        x = D(text)
        v = atan(x)
        checked("atan", x, v)
        emit("atan", text, v)
    for name, fn, exps in (("sinh", sinh, [-100, -60, -40, -30, -20, -10, -5, -2, -1, 0, 1, 2, 3, 4]), ("cosh", cosh, [-100, -40, -20, -10, -5, -2, -1, 0, 1, 2, 3, 4]), ("tanh", tanh, [-100, -60, -40, -30, -20, -10, -5, -2, -1, 0, 1, 2, 3])):
        for text in [number(rng, e) for e in exps] + [number(rng, e, digits=34) for e in (0, 1)]:
            x = D(text)
            v = fn(x)
            checked(name, x, v)
            emit(name, text, v)
    for text in [number(rng, e) for e in trig_exps + [4000, 6000]]:
        x = D(text)
        v = asinh(x)
        checked("asinh", x, v)
        emit("asinh", text, v)
    near_one_hi = [f"1.{'0' * (k - 1)}{rng.randint(1, 9)}" for k in (1, 2, 5, 10, 20, 30, 33)]
    for text in [number(rng, e, sign=False) for e in (0, 1, 2, 5, 10, 20, 39, 40, 100, 1000, 6000)] + near_one_hi + ["2", "1.5", "10"]:
        x = D(text)
        if x < 1:
            continue
        v = acosh(x)
        checked("acosh", x, v)
        emit("acosh", text, v)
    for text in [number(rng, e) for e in unit_exps] + near_one:
        x = D(text)
        v = atanh(x)
        checked("atanh", x, v)
        emit("atanh", text, v)
    pair_exps = [(0, 0), (0, 1), (1, 0), (-5, 5), (5, -5), (-30, 30), (0, 80), (0, 81), (0, 100), (0, 300), (0, 2000), (0, -90), (-100, 0), (-10, 90), (3, -3), (6000, 0), (0, 6000), (-100, 1000)]
    for ey, ex in pair_exps:
        for _ in range(2):
            y_text, x_text = number(rng, ey), number(rng, ex)
            y, x = D(y_text), D(x_text)
            v = atan2(y, x)
            if abs(ey - ex) > 100:
                # a second route for a ratio past 1e100 either way: the first-order form
                with localcontext() as c:
                    c.prec = P + 80 + 2 * abs(ey - ex)
                    pi = pi_to(c.prec)
                    ratio = y / x
                    if abs(y) > abs(x):
                        closed = (pi / 2 if y > 0 else -pi / 2) - x / y
                    elif x > 0:
                        closed = ratio
                    else:
                        closed = (pi if y >= 0 else -pi) + ratio
                    assert close(v, closed, 60), (y_text, x_text)
            else:
                # a second route: the angle's sine and cosine are y and x over the hypotenuse
                with localcontext() as c:
                    c.prec = P + 80 + 4 * abs(ey - ex)
                    h = (x * x + y * y).sqrt()
                    s, cs = sincos(v, P + 40 + 2 * abs(ey - ex))
                    assert close(s, y / h, 150) and close(cs, x / h, 150), (y_text, x_text)
            emit("atan2", f"{y_text} {x_text}", v)
    sys.stdout.write("\n".join("\t".join(r) for r in rows) + "\n")
    sys.stderr.write(f"{len(rows)} rows\n")


if __name__ == "__main__":
    main()
