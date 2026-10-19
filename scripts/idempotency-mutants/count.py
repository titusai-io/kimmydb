#!/usr/bin/env python3
"""The totals of docs/adr-240-mutation-rows.md, counted from its tables.

    count.py [--write] [<repo>]

Counts every row of every section's table by its Result: `killed` (and `killed (hung)`,
`killed (wider suites)`), `equivalent` (and `equivalent in safety`), `superseded`, `retired`,
`identical` (run once, as the row it repeats), and anything else as a survivor. Without
`--write` it checks that each section's summary line, the Totals table, the sentence under it
and the ADR-240 summary in docs/decisions.md say exactly that, and exits 1 if one does not;
with `--write` it rewrites them from the count. Nothing in those lines is typed by hand.
"""
import re
import sys

args = [a for a in sys.argv[1:] if a != "--write"]
WRITE = "--write" in sys.argv[1:]
REPO = args[0] if args else "."
ROWS_DOC = f"{REPO}/docs/adr-240-mutation-rows.md"
DECISIONS = f"{REPO}/docs/decisions.md"
KINDS = ("killed", "equivalent", "superseded", "retired", "identical", "survived")


def kind(result):
    result = result.strip()
    for k in ("killed", "equivalent", "superseded", "retired", "identical"):
        if result.startswith(k):
            return k
    return "survived"


def summary(t):
    return (f"**{t['rows']} rows: {t['killed']} killed, {t['equivalent']} equivalent, {t['superseded']} superseded, "
            f"{t['retired']} retired, {t['identical']} run once elsewhere, {t['survived']} survived.**")


text = open(ROWS_DOC).read()
head, _, rest = text.partition("\n## ")
rest = "## " + rest
parts = [p for p in re.split(r"(?m)^(?=## )", rest) if p.strip()]
sections = []  # (title, body, totals)
out = [head + "\n"]
grand = dict.fromkeys(("rows",) + KINDS, 0)
detail = dict.fromkeys(("hung", "wider"), 0)
bad = []
for part in parts:
    title = part.splitlines()[0][3:].strip()
    if title == "Totals":
        continue
    t = dict.fromkeys(("rows",) + KINDS, 0)
    for line in part.splitlines():
        cells = [c.strip() for c in re.split(r"(?<!\\)\|", line)[1:-1]]
        if len(cells) != 5 or cells[0] in ("Row", "---") or set(cells[0]) <= {"-"}:
            continue
        t["rows"] += 1
        t[kind(cells[2])] += 1
        if cells[2].startswith("killed") and "hung" in cells[2]:
            detail["hung"] += 1
        if cells[2].startswith("killed (wider"):
            detail["wider"] += 1
    if t["rows"] == 0:
        bad.append(f"section {title!r} has no table rows")
    line = summary(t)
    lines = re.split(r"(?m)^(\*\*\d+ rows: .*\*\*|<!--summary-->)$", part)
    if len(lines) != 3:
        bad.append(f"section {title!r}: no summary line (or more than one)")
        out.append(part)
    else:
        if lines[1] != line:
            bad.append(f"section {title!r}: says {lines[1]!r}, the table counts {line!r}")
        out.append(lines[0] + line + lines[2])
    sections.append((title, t))
    for k in grand:
        grand[k] += t[k]

table = ["## Totals", "", "| Piece | Rows | Killed | Equivalent | Superseded | Retired | Run once elsewhere | Survived |",
         "|---|---|---|---|---|---|---|---|"]
for title, t in sections:
    name = re.sub(r":.*$", "", title)
    table.append(f"| {name} | {t['rows']} | {t['killed']} | {t['equivalent']} | {t['superseded']} | {t['retired']} | "
                 f"{t['identical']} | {t['survived']} |")
table.append(f"| **All** | **{grand['rows']}** | **{grand['killed']}** | **{grand['equivalent']}** | "
             f"**{grand['superseded']}** | **{grand['retired']}** | **{grand['identical']}** | **{grand['survived']}** |")
own = grand["killed"] - detail["wider"]
tail = (f"Of the {grand['killed']} rows killed, {own} were killed by their own suites ({detail['hung']} of them by a "
        f"hang, with the tests that failed before it) and {detail['wider']} by the wider suites. "
        f"{grand['equivalent']} rows are equivalent, each with its argument in its Notes; {grand['superseded']} are "
        f"superseded and {grand['retired']} retired, each saying by what; {grand['identical']} repeat another row's "
        f"edit and test and ran once, as it; and {grand['survived']} survived.")
totals = "\n".join(table) + "\n\n" + tail + "\n"
body = "".join(out).rstrip("\n") + "\n\n"
old_totals = text[text.index("## Totals"):] if "## Totals" in text else ""
if old_totals != totals:
    bad.append("the Totals section does not match the tables")
new_text = body + totals

decisions = open(DECISIONS).read()
sentence = (f"(adr-240-mutation-rows.md): {grand['rows']} rows, {grand['killed']} killed, {grand['equivalent']} "
            f"equivalent, {grand['superseded']} superseded and {grand['retired']} retired, the {grand['identical']} "
            f"that G repeated from C run once, and {grand['survived']} survived.")
pattern = re.compile(r"\(adr-240-mutation-rows\.md\): \d+ rows, .*?(?:run once\.|survived\.)", re.S)
found = pattern.findall(decisions)
if len(found) != 1:
    bad.append(f"docs/decisions.md: the ADR-240 summary sentence occurs {len(found)} times")
elif found[0] != sentence:
    bad.append(f"docs/decisions.md says {found[0]!r}, the tables count {sentence!r}")

if WRITE:
    open(ROWS_DOC, "w").write(new_text)
    if len(found) == 1:
        open(DECISIONS, "w").write(pattern.sub(lambda _: sentence, decisions))
    print(f"written: {grand}")
    sys.exit(0)
for b in bad:
    print(b)
print(f"{grand['rows']} rows: " + ", ".join(f"{grand[k]} {k}" for k in KINDS))
sys.exit(1 if bad else 0)
