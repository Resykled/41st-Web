#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch latest credits.db (remote URL or local path) and build data/snapshot.json
to feed the 41st-web.html dashboard.

Usage examples:
  python3 fetch_build_snapshot.py \
    --db-url https://raw.githubusercontent.com/Resykled/41st_web-bot-/update-bot/credits.db \
    --out /var/www/41st/data/snapshot.json

  # or, if the DB is already local:
  python3 fetch_build_snapshot.py --db /path/to/credits.db --out ./data/snapshot.json
"""
import argparse, json, os, sqlite3, sys, tempfile, time, urllib.request, urllib.error

def info(msg):  print(f"[INFO] {msg}")
def warn(msg):  print(f"[WARN] {msg}", file=sys.stderr)
def die(msg, rc=1): print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(rc)

def download_to_tmp(url: str) -> str:
    """Download URL to a temporary file; return local path."""
    info(f"Downloading DB from: {url}")
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            suffix = ".db"
            fd, tmp = tempfile.mkstemp(suffix=suffix)
            with os.fdopen(fd, "wb") as f:
                f.write(r.read())
        info(f"Downloaded to {tmp}")
        return tmp
    except urllib.error.HTTPError as e:
        die(f"HTTP error {e.code} while fetching DB: {e.reason}")
    except Exception as e:
        die(f"Failed to download DB: {e}")

def open_sqlite_ro(path: str) -> sqlite3.Connection:
    """Open sqlite in read-only mode if possible."""
    if not os.path.exists(path):
        die(f"DB file not found: {path}")
    uri = f"file:{os.path.abspath(path)}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError:
        # Fallback (some builds may not support uri=True)
        return sqlite3.connect(path)

def int_or_0(x):
    try:
        return int(x)
    except Exception:
        return 0

def build_snapshot(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()

    # Collect all user_ids appearing in any relevant table
    user_ids = set()
    for t in ("register_status", "user_credits", "user_daily", "user_purchases"):
        try:
            for (uid,) in cur.execute(f"SELECT user_id FROM {t}"):
                user_ids.add(uid)
        except Exception:
            pass

    # user_credits
    credits = {uid: {"current_credits":0, "max_credits":0, "removed_credits":0} for uid in user_ids}
    try:
        for uid, curr, maxc, rem in cur.execute(
            "SELECT user_id, current_credits, max_credits, removed_credits FROM user_credits"
        ):
            credits[uid] = {
                "current_credits": int_or_0(curr),
                "max_credits": int_or_0(maxc),
                "removed_credits": int_or_0(rem),
            }
    except Exception:
        pass

    # user_daily
    daily = {uid: {"last_claim": None, "streak": 0} for uid in user_ids}
    try:
        for uid, last, streak in cur.execute(
            "SELECT user_id, last_claim, streak FROM user_daily"
        ):
            daily[uid] = {"last_claim": last, "streak": int_or_0(streak)}
    except Exception:
        pass

    # user_purchases
    purchases = {}
    try:
        for uid, item in cur.execute(
            "SELECT user_id, item_name FROM user_purchases ORDER BY id"
        ):
            purchases.setdefault(uid, []).append(item)
    except Exception:
        pass

    # register_status
    registered = set()
    try:
        registered = {uid for (uid,) in cur.execute("SELECT user_id FROM register_status")}
    except Exception:
        pass

    # Assemble per-user records
    users = []
    for uid in sorted(user_ids, key=lambda x: str(x)):
        u = {
            "user_id": str(uid),
            "registered": uid in registered,
            "credits": credits.get(uid, {"current_credits":0, "max_credits":0, "removed_credits":0}),
            "daily":   daily.get(uid,   {"last_claim": None, "streak": 0}),
            "purchases": purchases.get(uid, []),
        }
        u["net_credits"] = (u["credits"]["current_credits"] or 0) - (u["credits"]["removed_credits"] or 0)
        users.append(u)

    # roles: non_stacking_roles + role_credits
    def read_roles(table):
        out = []
        try:
            for name, amount in cur.execute(f"SELECT role_name, credit_amount FROM {table}"):
                out.append({"role_name": name, "credit_amount": int_or_0(amount)})
        except Exception:
            pass
        return out

    roles = {
        "non_stacking_roles": read_roles("non_stacking_roles"),
        "role_credits":       read_roles("role_credits"),
    }

    snapshot = {
        "generated_at": int(time.time()),
        "data": users,
        "roles": roles,
    }
    return snapshot

def atomic_write_json(obj: dict, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".json", dir=os.path.dirname(out_path))
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, out_path)
    os.chmod(out_path, 0o644)

def main():
    ap = argparse.ArgumentParser(description="Export credits.db to snapshot.json for 41st dashboard")
    ap.add_argument("--db", help="Path to local SQLite DB (credits.db)")
    ap.add_argument("--db-url", help="Remote RAW URL to credits.db (e.g. https://raw.githubusercontent.com/.../credits.db)")
    ap.add_argument("--out", required=True, help="Output JSON path (e.g. /var/www/41st/data/snapshot.json)")
    args = ap.parse_args()

    if not args.db and not args.db_url:
        die("Provide either --db or --db-url")

    tmp_db = None
    db_path = None
    try:
        if args.db_url:
            db_path = tmp_db = download_to_tmp(args.db_url)
        else:
            db_path = args.db
        conn = open_sqlite_ro(db_path)
        with conn:
            snapshot = build_snapshot(conn)
        atomic_write_json(snapshot, args.out)
        info(f"Wrote {args.out}: users={len(snapshot['data'])}, roles={len(snapshot['roles']['non_stacking_roles'])+len(snapshot['roles']['role_credits'])}")
    finally:
        if tmp_db and os.path.exists(tmp_db):
            try: os.remove(tmp_db)
            except Exception: pass

if __name__ == "__main__":
    main()
