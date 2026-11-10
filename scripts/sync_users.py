#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SHB 人员名单 → 本地库 users 表（全量 upsert，覆盖刷新）。

用法:
  python3 sync_users.py            # 拉云端人员名单并入库
  python3 sync_users.py --dry-run  # 只看结果，不写库
说明:
  - 取数：shb-cli part personal users --page-size 200（只读）
  - 只写本地 users 表 / shb_meta，不碰其他表
  - 部门（dept）SHB 接口无此字段：保留库中已有值，不被覆盖为空
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime

sys.path.insert(0, "/root/.openclaw/workspace/shb-lib")
import dictsync  # noqa: E402  同步批次日志

DB_PATH = "/root/.openclaw/workspace/shb-tasks/data/shb_tasks.db"
META_KEY = "users_last_save_at"


def cli(argv, timeout=120):
    env = dict(os.environ)
    env["NO_COLOR"] = "1"
    env.setdefault("SHB_WEB_GUARD", "1")
    p = subprocess.run(["shb-cli"] + list(argv), capture_output=True, text=True,
                       env=env, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "shb-cli failed")[:300])
    out = p.stdout.strip()
    # 先整段解析（多行 pretty JSON），再退化到逐行找 JSON
    try:
        return json.loads(out)
    except Exception:
        pass
    for line in out.splitlines():
        line = line.strip()
        if line.startswith(("[", "{")):
            try:
                return json.loads(line)
            except Exception:
                pass
    return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    _t0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    users = cli(["part", "personal", "users", "--page-size", "200", "-o", "raw"])
    if isinstance(users, dict):
        users = users.get("data") or users.get("list") or []
    rows = [(u.get("userId"), u.get("displayName"), u.get("staffId"), u.get("head"))
            for u in users if u.get("userId")]

    if a.dry_run:
        for r in rows:
            print(" | ".join(str(x) for x in r))
        print("共 %d 人（dry-run，未写库）" % len(rows))
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c = sqlite3.connect(DB_PATH)
    for uid, name, staff, head in rows:
        c.execute(
            "INSERT INTO users(user_id,name,staff_id,head,saved_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET name=excluded.name, staff_id=excluded.staff_id, "
            "head=excluded.head, saved_at=excluded.saved_at",
            (uid, name, staff, head, now))
    c.execute("INSERT INTO shb_meta(key,value,updated_at) VALUES(?,?,?) "
              "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
              (META_KEY, now, now))
    c.commit()
    c.close()
    dictsync.log_sync_run("users", list_rows=len(rows), upserted=len(rows), started=_t0)
    print("users upsert %d 人" % len(rows))


if __name__ == "__main__":
    main()
