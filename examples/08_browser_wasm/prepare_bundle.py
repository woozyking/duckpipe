"""Copies DuckPipe's real, unmodified source into this example's own
directory so `index.html` can `fetch()` it into Pyodide's virtual
filesystem and `import duckpipe` for real (DESIGN.md sec 8) --
the same role `Dockerfile`'s `COPY src/ src/` plays in
`../07_serverless_executor`, just for a browser instead of a container.
Nothing here rewrites a single line; this is packaging, not porting.

Also generates a small, entirely synthetic "support ticket export"
sample CSV -- fake names, `@example.com` addresses (IANA-reserved for
documentation), 555-555-01xx phone numbers and 000-xx-xxxx SSNs (both
ranges North American authorities never issue to a real person/number,
so nothing here is or resembles anyone real), 192.0.2.x IPs
(RFC 5737 TEST-NET-1) -- so the demo has something worth flagging the
moment it loads, without asking anyone to hand over a real file just to
see the tool work.

Run this once before serving the example (see README.md); re-run it any
time `src/duckpipe/` changes to keep the copy in sync -- both generated
directories are gitignored, not committed.
"""

import csv
import json
import shutil
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE.parent.parent / "src" / "duckpipe"

_FIRST_NAMES = [
    "Alex", "Bianca", "Carlos", "Dana", "Ekene", "Fiona", "Grace", "Hiro",
    "Ines", "Jamal", "Kira", "Liam", "Maya", "Noah", "Priya", "Quinn",
    "Rosa", "Sam", "Tara", "Uzo",
]
_LAST_NAMES = [
    "Nguyen", "Okafor", "Silva", "Kim", "Patel", "Rossi", "Novak", "Diaz",
    "Haddad", "Ito", "Kowalski", "Reyes", "Sato", "Weber", "Yilmaz",
    "Zhang", "Brown", "Adeyemi", "Larsen", "Costa",
]
_PLANS = ["free", "pro", "team", "enterprise"]
_NOTES = [
    "asked about export formats",
    "billing question, resolved",
    "reported a slow dashboard load",
    "requested a feature: dark mode",
    "trial extension request",
]


def _support_tickets_csv() -> list[list[str]]:
    header = [
        "ticket_id", "customer_name", "email", "phone", "ssn", "account_id",
        "plan", "monthly_spend", "signup_ip", "notes",
    ]
    rows = [header]
    for i in range(20):
        first, last = _FIRST_NAMES[i], _LAST_NAMES[i]
        rows.append([
            f"T-{1000 + i}",
            f"{first} {last}",
            f"{first.lower()}.{last.lower()}@example.com",
            f"555-555-01{i:02d}",
            f"000-{10 + i:02d}-{1000 + i:04d}",
            f"acct_{2000 + i}",
            _PLANS[i % len(_PLANS)],
            f"{19.99 + i * 5:.2f}",
            f"192.0.2.{i + 1}",
            _NOTES[i % len(_NOTES)],
        ])
    return rows


def main() -> None:
    dest_pkg = HERE / "duckpipe_src" / "duckpipe"
    shutil.rmtree(dest_pkg.parent, ignore_errors=True)
    dest_pkg.mkdir(parents=True)
    py_files = sorted(SRC.glob("*.py"))
    for f in py_files:
        shutil.copyfile(f, dest_pkg / f.name)
    # index.html fetches this list rather than hardcoding filenames, so it
    # never drifts from whatever src/duckpipe/ actually contains.
    (dest_pkg.parent / "manifest.json").write_text(json.dumps([f.name for f in py_files]))
    print(f"copied {len(py_files)} files from {SRC} -> {dest_pkg}")

    dest_data = HERE / "sample_data"
    dest_data.mkdir(exist_ok=True)
    csv_path = dest_data / "input.csv"
    with csv_path.open("w", newline="") as f:
        csv.writer(f).writerows(_support_tickets_csv())
    print(f"generated synthetic sample data -> {csv_path}")


if __name__ == "__main__":
    main()
