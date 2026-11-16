#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SHB 人员名单 → 本地库 users 表（全量 upsert，覆盖刷新）。

用法:
  python3 sync_users.py            # 拉云端人员名单并入库
  python3 sync_users.py --dry-run  # 只看结果，不写库

说明:
  - 取数（2026-11-16 换源）：POST /api/user/outside/user/getUserList，按 page/pageSize 翻页拉全量。
    · 原 shb-cli part personal users 不支持翻页（--page-size 默认 50，200 即硬上限），
      超过 200 人会静默漏人且日志仍记成功，故弃用。
  - 字段：姓名 displayName / 片区 tagList（排除公司名标签）/ 权限角色 roles / 状态 state
  - 只写本地 users 表 / shb_meta，不碰其他表
  - 健壮性：空结果即判失败（防刷空库/掩盖故障）；成功与失败都写同步批次日志
"""
import argparse
import json
import os
import sqlite3
import sys
import urllib.request
from datetime import datetime

sys.path.insert(0, "/root/.openclaw/workspace/shb-lib")
import dictsync  # noqa: E402  同步批次日志

DB_PATH = "/root/.openclaw/workspace/shb-tasks/data/shb_tasks.db"
META_KEY = "users_last_save_at"
CFG = "/root/.config/shb-cli/config.json"
CRED = "/root/.config/shb-cli/credentials.json"
PAGE_SIZE = 100
MAX_PAGES = 100


def auth():
    """取 shb-cli 当前身份的 domain + 完整令牌"""
    cfg = json.load(open(CFG, encoding="utf-8"))
    prof = (cfg.get("profiles") or {}).get(cfg.get("current_profile") or "default") or {}
    cred = json.load(open(CRED, encoding="utf-8"))
    tok = ((cred.get("entries") or {}).get(prof.get("token_ref")) or {}).get("token")
    if not prof.get("domain") or not tok:
        raise RuntimeError("读不到售后宝 domain/token，请先在「身份管理」授权登录")
    return prof["domain"], tok


def fetch_page(domain, token, page):
    body = json.dumps({"page": page, "pageSize": PAGE_SIZE}).encode("utf-8")
    req = urllib.request.Request(
        "https://%s/api/user/outside/user/getUserList" % domain,
        data=body, method="POST",
        headers={"token": token, "shbversion": "3", "shb-language": "zh",
                 "timezone": "Asia/Shanghai", "content-type": "application/json",
                 "accept": "application/json, text/plain, */*",
                 "user-agent": "shb-web-sync/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_all():
    """翻页拉全量用户；返回原始用户 dict 列表"""
    domain, token = auth()
    out, page = [], 1
    while page <= MAX_PAGES:
        d = fetch_page(domain, token, page)
        dd = d.get("data") or d.get("result") or {}
        lst = dd.get("list") or []
        out.extend(lst)
        pages = dd.get("pages") or 0
        if not lst or page >= pages:
            break
        page += 1
    return out


def _names(items, keys):
    out = []
    for it in (items or []):
        if isinstance(it, dict):
            nm = next((it.get(k) for k in keys if it.get(k)), "")
        else:
            nm = str(it or "")
        nm = (nm or "").strip()
        if nm and nm not in out:
            out.append(nm)
    return out


def area_of(u):
    """片区：人员标签，排除公司名（含「有限公司」）"""
    tags = _names(u.get("tagList"), ("tagName", "name"))
    return ",".join([t for t in tags if "有限公司" not in t])


def roles_of(u):
    """权限角色"""
    return ",".join(_names(u.get("roles"), ("name", "roleName", "displayName")))


def build_rows(users):
    rows = []
    for u in users:
        uid = u.get("userId")
        if not uid:
            continue
        rows.append((uid, u.get("displayName") or "", u.get("staffId") or "",
                     u.get("head") or "", area_of(u), roles_of(u), u.get("state") or ""))
    return rows


def ensure_columns(c):
    """幂等补列（2026-11-16 新增片区/角色/状态）"""
    have = {r[1] for r in c.execute("PRAGMA table_info(users)")}
    for col in ("area", "roles", "state"):
        if col not in have:
            c.execute("ALTER TABLE users ADD COLUMN %s TEXT" % col)


def save(rows):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c = sqlite3.connect(DB_PATH)
    try:
        ensure_columns(c)
        c.executemany(
            "INSERT INTO users(user_id,name,staff_id,head,area,roles,state,saved_at) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET name=excluded.name, staff_id=excluded.staff_id, "
            "head=excluded.head, area=excluded.area, roles=excluded.roles, "
            "state=excluded.state, saved_at=excluded.saved_at",
            [r + (now,) for r in rows])
        c.execute("INSERT INTO shb_meta(key,value,updated_at) VALUES(?,?,?) "
                  "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                  (META_KEY, now, now))
        c.commit()
        return len(rows)
    finally:
        c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    _t0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        users = fetch_all()
        rows = build_rows(users)
        if not rows:
            # 空结果一律判失败：接口异常时若放过去，会把「成功」写进日志并掩盖故障
            raise RuntimeError("云端返回 0 人，判定为异常，未写库")

        if a.dry_run:
            for r in rows:
                print(" | ".join(str(x) for x in r))
            print("共 %d 人（dry-run，未写库）" % len(rows))
            return

        n = save(rows)
        dictsync.log_sync_run("users", list_rows=len(rows), upserted=n, started=_t0)
        print("users upsert %d 人" % n)
    except Exception as e:  # noqa: BLE001
        try:
            dictsync.log_sync_run("users", list_rows=0, upserted=0, started=_t0)
        except Exception:
            pass
        print("users 同步失败：%s" % str(e)[:200], file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
