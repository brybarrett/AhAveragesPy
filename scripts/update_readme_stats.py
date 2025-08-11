"""Update README.md auction statistics line automatically.

Finds the markdown heading line containing 'unique BIN auctions' and replaces
it with an updated count derived from the latest compressed database snapshot.

Preference order:
  1. database2.sql.gz (detailed schema with pricesV2)
  2. database.sql.gz   (legacy schema with prices)

If neither exists, the script exits without error (so CI doesn't fail).
"""
from __future__ import annotations
import gzip, re, sqlite3, sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
README_PATH = ROOT / 'README.md'
DUMPS = [ROOT / 'database2.sql.gz', ROOT / 'database.sql.gz']

def load_dump(dump: Path) -> sqlite3.Connection:
    sql_bytes = gzip.decompress(dump.read_bytes())
    text = sql_bytes.decode('utf-8', errors='replace')
    con = sqlite3.connect(':memory:')
    con.executescript(text)
    return con

def obtain_count(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    for table in ('pricesV2','prices'):
        try:
            cur.execute(f'SELECT COUNT(*) FROM {table}')
            return int(cur.fetchone()[0])
        except Exception:
            continue
    return 0

def build_line(count: int) -> str:
    now = datetime.now(timezone.utc)
    ts = now.strftime('%H:%M %d/%m/%Y')
    return f"### {count:,} unique BIN auctions that contain a buyer as of {ts} UTC since *roughly* beginning of March 2025"

def update_readme(line: str) -> bool:
    text = README_PATH.read_text(encoding='utf-8')
    pattern = re.compile(r'^### .*unique BIN auctions.*$', re.MULTILINE)
    if pattern.search(text):
        new_text = pattern.sub(line, text, count=1)
    else:
        new_text = line + '\n\n' + text
    if new_text != text:
        README_PATH.write_text(new_text, encoding='utf-8')
        return True
    return False

def main() -> int:
    dump = next((d for d in DUMPS if d.exists()), None)
    if not dump:
        print('No dump found; skipping README stat update.')
        return 0
    try:
        con = load_dump(dump)
    except Exception as e:
        print(f'Failed to load dump {dump.name}: {e}', file=sys.stderr)
        return 1
    try:
        count = obtain_count(con)
    finally:
        con.close()
    line = build_line(count)
    changed = update_readme(line)
    if changed:
        print('README updated:', line)
    else:
        print('README already up to date.')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
