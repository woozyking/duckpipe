"""The real deliverable (DESIGN.md sec 8): a genuine DuckPipe
pipeline run entirely inside a browser tab via Pyodide -- no server, no
upload, whatever file you pick never leaves the tab. Runs completely
unmodified: `index.html` calls `duckpipe.run()` on this exact file the
same way `duckpipe run pipeline.py` would from a shell.

The concrete niche this is built around: **checking a file for
sensitive-looking columns before you're allowed to send it anywhere** --
a real, everyday task for anyone handling an export under NDA, legal
hold, or a compliance policy that treats "upload it to find out if it's
sensitive" as itself the violation. A server-based checker can't
honestly offer "we never see your file"; a page that never makes a
network request for it can, and can prove it.

Deliberately pattern- and naming-heuristic-based, not a claim of
exhaustive PII detection (that's a genuinely hard, model-shaped problem)
-- honest about what it actually checks, the same way this project is
honest about DuckLake's/Pyodide's other limitations elsewhere.

Schema-agnostic on purpose -- the whole point is "drop in your own
file" -- so this scans whatever tabular file it's given instead of
assuming particular columns.

    profile -> scan_sensitive_columns -> triage_report
"""

import os

import duckdb

from duckpipe import task

# See examples/01_daily_batch_etl/duck.py's comment on this override --
# same convention, set here via `os.environ` from index.html instead of
# a shell env var (pyodide.setEnviron doesn't exist in this API).
SOURCE = os.environ.get("DUCKPIPE_EXAMPLE_DATA", "/data/input.csv")

# Value-pattern checks (RE2, DuckDB's regex dialect) -- a column is
# flagged when a majority of its own non-null values match, not on a
# single coincidental hit. Deliberately conservative, high-precision
# patterns over a best-effort PII classifier: a false negative here just
# means "look closer yourself," but a false positive would train people
# to stop trusting the tool.
_VALUE_PATTERNS = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "phone_number": r"^\+?1?[-. (]*\d{3}[-. )]*\d{3}[-. ]*\d{4}$",
    "ssn_like": r"^\d{3}-\d{2}-\d{4}$",
    "credit_card_like": r"^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$",
    "ip_address": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
}
# Column-name hints alone don't flag anything by themselves -- they only
# raise a low-confidence note when no value pattern already fired, since
# a column literally named "address" but full of empty strings shouldn't
# read as a confirmed finding.
_NAME_HINTS = (
    "email", "phone", "ssn", "social", "name", "address", "dob", "birth", "card", "account",
)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@task(cache=True)
def profile():
    """The one task that touches the whole file -- row count and column
    types, via `DESCRIBE`, not by reading the data into Python at all."""
    con = duckdb.connect()
    row_count = con.sql(f"SELECT count(*) FROM '{SOURCE}'").fetchone()[0]
    columns = con.sql(f"DESCRIBE SELECT * FROM '{SOURCE}'").fetchall()
    return {"row_count": row_count, "columns": [(name, dtype) for name, dtype, *_ in columns]}


@task
def scan_sensitive_columns(info=profile):
    """Per string column: which value patterns a majority of its non-null
    values match, plus whether its own name hints at something sensitive.
    Every check is a real DuckDB query over the actual file -- nothing
    here is guessed from column names alone.

    Deliberately not `cache=True`, unlike its siblings: this is the one
    step worth re-running every time regardless of whether the file
    changed, so a second run against the same file visibly shows both
    outcomes at once -- `profile`/`triage_report` reporting `skipped`
    (cached, unchanged) right next to this one reporting `success`
    (it actually ran), rather than everything skipping uniformly."""
    string_cols = [name for name, dtype in info["columns"] if dtype.split("(")[0] == "VARCHAR"]
    con = duckdb.connect()
    findings: dict[str, dict] = {}
    for col in string_cols:
        ident = _quote_ident(col)
        total = con.sql(
            f"SELECT count({ident}) FROM '{SOURCE}' WHERE {ident} IS NOT NULL"
        ).fetchone()[0]
        if total == 0:
            continue
        matches = {}
        for label, pattern in _VALUE_PATTERNS.items():
            n = con.sql(
                f"SELECT count(*) FROM '{SOURCE}' WHERE regexp_matches({ident}, '{pattern}')"
            ).fetchone()[0]
            if n / total >= 0.5:
                matches[label] = round(n / total, 2)
        name_hint = any(h in col.lower() for h in _NAME_HINTS)
        if matches or name_hint:
            findings[col] = {"pattern_matches": matches, "name_suggests_sensitive": name_hint}
    return findings


@task(cache=True)
def triage_report(info=profile, findings=scan_sensitive_columns):
    confirmed = [c for c, f in findings.items() if f["pattern_matches"]]
    worth_a_look = [c for c, f in findings.items() if not f["pattern_matches"]]
    if confirmed:
        verdict = (
            f"{len(confirmed)} column(s) match a sensitive-data pattern -- "
            "treat this file as restricted before sharing it anywhere"
        )
    elif worth_a_look:
        verdict = (
            f"no confirmed pattern matches, but {len(worth_a_look)} column name(s) "
            "hint at something sensitive -- worth a manual look"
        )
    else:
        verdict = "no sensitive-looking columns found by these checks (not a guarantee)"
    return {
        "rows": info["row_count"],
        "columns_scanned": len(info["columns"]),
        "confirmed": confirmed,
        "worth_a_look": worth_a_look,
        "detail": findings,
        "verdict": verdict,
    }


if __name__ == "__main__":
    from duckpipe import run

    run(__file__)
