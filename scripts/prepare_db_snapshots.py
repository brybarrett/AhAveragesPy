"""Utility to shrink and snapshot SQLite databases as compressed SQL dumps.

Creates .sql.gz dumps for database.db (legacy) and database2.db (v2) if they
exist in the repository root. Intended to run inside CI before committing so
large binary SQLite files are not stored directly in git history (avoids the
100 MB hard limit and keeps diffs small).

Restoration:
  gzip -dc database2.sql.gz | sqlite3 database2.db

We do a LIGHT optimization (optional pruning hook, PRAGMA optimize, VACUUM).
Add any domain‑specific row pruning inside prune_db() if desired.
"""
from __future__ import annotations
import gzip, os, sqlite3, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_FILES = ["database.db", "database2.db"]

def human(size: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    n = float(size)
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{n:.2f}{u}"
        n /= 1024
    return f"{size}B"

def prune_db(db_path: Path) -> None:
    """Hook to optionally prune old / unnecessary rows to cap growth.
    Currently a no‑op. Example (keep last 30 days of pricesV2):
        cur.execute("DELETE FROM pricesV2 WHERE timestamp < strftime('%s','now') - 30*86400")
    """
    pass

def optimize_and_dump(db_path: Path):
    if not db_path.exists():
        return None
    dump_path = db_path.with_suffix(".sql.gz")
    try:
        con = sqlite3.connect(str(db_path))
        cur = con.cursor()
        prune_db(db_path)
        try:
            cur.execute("PRAGMA optimize;")
        except Exception:
            pass
        con.commit()
        try:
            temp_file = db_path.with_suffix(".vacuuming")
            cur.execute(f"VACUUM INTO '{temp_file.name}'")
            con.close()
            temp_file.replace(db_path)
            con = sqlite3.connect(str(db_path))
            cur = con.cursor()
        except Exception:
            try:
                cur.execute("VACUUM;")
                con.commit()
            except Exception:
                pass
        con.close()
        dump_bytes = subprocess.check_output(["sqlite3", str(db_path), ".dump"], text=False)
        with gzip.open(dump_path, "wb", compresslevel=9, mtime=0) as gz:
            gz.write(dump_bytes)
        orig = db_path.stat().st_size
        comp = dump_path.stat().st_size
        ratio = (1 - comp / orig) * 100 if orig else 0
        print(f"Snapshot {db_path.name}: {human(orig)} -> {human(comp)} ({ratio:.1f}% smaller) -> {dump_path.name}")
        return dump_path
    except subprocess.CalledProcessError as e:
        print(f"Error dumping {db_path}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error processing {db_path}: {e}", file=sys.stderr)
    return None

def main() -> int:
    produced = []
    for name in DB_FILES:
        p = ROOT / name
        out = optimize_and_dump(p)
        if out:
            produced.append(out)
    if not produced:
        print("No databases found to snapshot.")
        return 0
    for name in DB_FILES:
        p = ROOT / name
        if p.exists():
            try:
                os.remove(p)
                print(f"Removed raw DB {p.name} (kept compressed dump).")
            except Exception as e:
                print(f"Warning: could not remove {p}: {e}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
