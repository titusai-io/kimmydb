# Conformance

One set of scenarios, run three ways, compared against one oracle.

```bash
cargo build --release --bin kimmyd
cargo build --release --example conformance -p kimmy-client
(cd clients/go && go build -o conformance-driver ./conformance)

./clients/conformance/run.py
./clients/conformance/run.py --client go --scenario paging_walks_everything
```

## Why it exists

Three clients passed matching scenarios only because they were **written** to
match. Nothing enforced it, and nothing would have noticed the day one of them
started paging differently — except a user.

## How it works

`scenarios.json` declares every scenario and the observations a correct client
must produce. Each client ships a small **driver** that runs a named scenario
and prints its observations as JSON:

```
driver list                                   → ["capabilities", ...]
driver run <scenario> <base-url> [dead-url]   → {"documents_seen": 250, ...}
```

The runner starts a fresh node per scenario, runs the scenario against every
driver, and compares. It checks two different things:

- **Coverage.** Every declared scenario must be implemented by every driver. A
  client that quietly stops covering one is a failure rather than a silence.
- **Behaviour.** The observations must match what is declared. This is the part
  a per-language test suite cannot do: three suites can each have a `failover`
  test and disagree about what failover means.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions. There is one oracle and three answers
to it.

## Adding a scenario

1. Add it to `scenarios.json` with its expected observations and *why* it
   matters.
2. Implement it in all three drivers — the runner fails until every one does.

## Adding a language

A line in `run.py`'s `DRIVERS` map and a driver that speaks those two commands.
Nothing else in the suite knows what language anything is written in.

## What it found on its first run

That the specification had claimed **collection creation is idempotent** since
M10 task 1, while the server has always answered `409`. Nothing had caught it
because every test in the repository created a collection exactly once. It is a
scenario now.
