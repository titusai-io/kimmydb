#!/usr/bin/env python3
"""Generate `pi.txt`: pi to 6,400 decimal places, for `trig.rs`.

    python3 -I gen_pi.py > pi.txt

By the Chudnovsky series with integer binary splitting, which shares nothing with the Machin
formula the tests rebuild it with (`trig_tests.rs`). The file is the digits `3` and the
6,400 after the point, with no point and no newline inside; one newline ends it.
"""
import sys
from math import isqrt

PLACES = 6400


def binary_split(a, b):
    if b - a == 1:
        if a == 0:
            pab = qab = 1
        else:
            pab = (6 * a - 5) * (2 * a - 1) * (6 * a - 1)
            qab = a * a * a * 10939058860032000
        tab = pab * (545140134 * a + 13591409)
        if a % 2 == 1:
            tab = -tab
        return pab, qab, tab
    m = (a + b) // 2
    pam, qam, tam = binary_split(a, m)
    pmb, qmb, tmb = binary_split(m, b)
    return pam * pmb, qam * qmb, qmb * tam + pam * tmb


def pi_digits(places):
    guard = 30
    terms = (places + guard) // 14 + 2
    _, q, t = binary_split(0, terms)
    scale = 10 ** (places + guard)
    value = (426880 * isqrt(10005 * scale * scale) * q) // t
    return str(value // 10 ** guard)


if __name__ == "__main__":
    sys.setrecursionlimit(10000)
    sys.set_int_max_str_digits(0)
    digits = pi_digits(PLACES)
    assert digits.startswith("31415926535897932384626433832795028841971693993751"), digits[:60]
    assert len(digits) == PLACES + 1, len(digits)
    sys.stdout.write(digits + "\n")
