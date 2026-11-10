#!/usr/bin/env python3
"""SHB 站点信息 → 钉钉 AI表格 同步脚本（一次性快照，手动触发）

决策（2026-08-29 grill-me 确认）：
  1. 同步模式：手动触发快照
  2. 表格：新建「SHB站点信息」base + 「站点」表
  3. 字段：7 列全部 text（编号/项目编码/站点客户/站点名称/站点编码/创建人/创建时间）
  4. 去重键：站点编码 + 站点名称（均 strip 前后空格），存在跳过、不存在新增
  5. 分享：建表后不自动分享，由管理员手动处理
"""
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import TABS, MAX_PAGE_SIZE, build_search_payload, call_bridge, build_rows

BASE_NAME = "SHB站点信息"
TABLE_NAME = "站点"
DISPLAY_ORDER = ["编号", "项目编码", "站点客户", "站点名称", "站点编码", "创建人", "创建时间"]
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "aitable_state.json")


def load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def dws(*args):
    """调用 dws CLI，返回解析后的 JSON；失败返回 {"_raw":..., "_err":...}"""
    try:
        r = subprocess.run(["dws"] + list(args), capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        return {"_raw": "", "_err": "dws 调用超时"}
    try:
        return json.loads(r.stdout) if r.stdout.strip() else {"_raw": "", "_err": r.stderr[:300]}
    except Exception:
        return {"_raw": r.stdout[:500], "_err": r.stderr[:300]}


def fetch_station_rows():
    """全量拉取站点数据（7 列展示行），复用 shb-web 查询逻辑"""
    tab_cfg = TABS["station"]
    all_rows = []
    page = 1
    while True:
        payload = build_search_payload(tab_cfg, "", "", "", "", page, MAX_PAGE_SIZE)
        result = call_bridge("paas", "form", "search",
                             "--data", json.dumps(payload, ensure_ascii=False),
                             "--fields", ",".join(tab_cfg["fields"]), "-o", "raw")
        if "error" in result:
            raise RuntimeError(f"站点查询失败: {result['error']}")
        data = result.get("output") if "output" in result else result
        if isinstance(data, str):
            data = json.loads(data)
        rows_raw = data.get("rows", []) if isinstance(data, dict) else []
        if not rows_raw:
            break
        all_rows.extend(build_rows(tab_cfg, rows_raw))
        total = data.get("page", {}).get("totalElements", 0) if isinstance(data, dict) else 0
        if len(all_rows) >= total:
            break
        page += 1
    return all_rows


def ensure_base_table():
    """找到或创建 base + table，返回 (base_id, table_id, field_map{列名: fieldId})。
    优先用状态文件持久化的 ID（base search 搜不到刚创建未打开的 base）。"""
    state = load_state()
    base_id = state.get("base_id")
    # ── base：状态文件 → search → create ──
    if not base_id:
        r = dws("aitable", "base", "search", "--query", BASE_NAME)
        for b in (r.get("data", {}).get("bases") or []):
            if b.get("baseName") == BASE_NAME:
                base_id = b.get("baseId")
                break
    if not base_id:
        r = dws("aitable", "base", "create", "--name", BASE_NAME)
        base_id = (r.get("data", {}) or {}).get("baseId")
    if not base_id:
        raise RuntimeError(f"base 创建/查找失败: {r}")
    # ── table：状态文件 → get → create ──
    table_id = state.get("table_id")
    if not table_id:
        r = dws("aitable", "table", "get", "--base-id", base_id)
        for t in (r.get("data", {}).get("tables") or []):
            if t.get("tableName") == TABLE_NAME:
                table_id = t.get("tableId")
                break
    if not table_id:
        fields = [{"fieldName": n, "type": "text"} for n in DISPLAY_ORDER]
        r = dws("aitable", "table", "create", "--base-id", base_id, "--name", TABLE_NAME,
                "--fields", json.dumps(fields, ensure_ascii=False))
        table_id = (r.get("data", {}) or {}).get("tableId")
    if not table_id:
        raise RuntimeError(f"table 创建/查找失败: {r}")
    # ── fieldId 映射 ──
    r = dws("aitable", "table", "get", "--base-id", base_id, "--table-ids", table_id)
    field_map = {}
    tables = r.get("data", {}).get("tables") or []
    if tables:
        for f in (tables[0].get("fields") or []):
            field_map[f.get("fieldName")] = f.get("fieldId")
    missing = [n for n in DISPLAY_ORDER if n not in field_map]
    if missing:
        raise RuntimeError(f"表格字段缺失: {missing}（返回: {r}）")
    save_state({"base_id": base_id, "table_id": table_id, "field_map": field_map})
    return base_id, table_id, field_map


def fetch_existing_keys(base_id, table_id, field_map):
    """拉取表格现有记录的去重键集合（站点编码|站点名称）。
    record query 失败时返回 None 并警告（调用方按空集处理）。"""
    id_to_name = {v: k for k, v in field_map.items()}
    keys = set()
    cursor = None
    while True:
        args = ["aitable", "record", "query", "--base-id", base_id,
                "--table-id", table_id, "--limit", "100"]
        if cursor:
            args += ["--cursor", cursor]
        r = dws(*args)
        data = r.get("data", {}) or {}
        records = data.get("records") or []
        if not records:
            if r.get("status") == "error" or r.get("error"):
                print(f"   ⚠️ record query 失败: {r.get('error') or r.get('_err')}（按空集处理）")
                return None
            break
        for rec in records:
            cells = rec.get("cells") or {}
            code = ""
            name = ""
            for k, v in cells.items():
                col = id_to_name.get(k, k)
                if col == "站点编码":
                    code = str(v or "").strip()
                elif col == "站点名称":
                    name = str(v or "").strip()
            keys.add(f"{code}|{name}")
        cursor = data.get("nextCursor") or data.get("cursor")
        if not cursor:
            break
        if len(keys) > 100000:  # 防死循环
            break
    return keys


def main():
    print("① 拉取站点数据（shb 查询）...")
    rows = fetch_station_rows()
    print(f"   站点 {len(rows)} 条")
    if not rows:
        print("   无数据，退出")
        return

    print("② 准备钉钉 AI表格...")
    base_id, table_id, field_map = ensure_base_table()
    print(f"   base={base_id}")
    print(f"   table={table_id}（{TABLE_NAME}）")

    print("③ 拉取现有记录去重键...")
    existing = fetch_existing_keys(base_id, table_id, field_map)
    if existing is None:
        existing = set()
        print("   ⚠️ 无法确认现有记录，按空集处理（可能产生重复）")
    else:
        print(f"   现有去重键 {len(existing)} 个")

    print("④ 比对去重...")
    to_create = []
    skipped = 0
    for row in rows:
        code = str(row.get("站点编码", "") or "").strip()
        name = str(row.get("站点名称", "") or "").strip()
        key = f"{code}|{name}"
        if key in existing:
            skipped += 1
            continue
        cells = {field_map[n]: (row.get(n, "") or "") for n in DISPLAY_ORDER}
        to_create.append({"cells": cells})
    print(f"   新增 {len(to_create)} 条，跳过 {skipped} 条")

    if to_create:
        for i in range(0, len(to_create), 100):
            batch = to_create[i:i + 100]
            r = dws("aitable", "record", "create", "--base-id", base_id,
                    "--table-id", table_id,
                    "--records", json.dumps(batch, ensure_ascii=False))
            n = r.get("data", {}) or {}
            print(f"   批次 {i // 100 + 1}: {n}")
            if r.get("status") == "error" or r.get("error"):
                print(f"   ⚠️ 写入异常: {r.get('error') or r.get('_err')}")

    print("✅ 完成")
    print(f"📎 表格地址: https://alidocs.dingtalk.com/i/nodes/{base_id}")


if __name__ == "__main__":
    main()
