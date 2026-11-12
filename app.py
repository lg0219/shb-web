"""SHB Web 查询服务 v2 - 通用查询层（2026-08-26 改造）

设计决策（grill 盘问确认）：
  1. 给同事用，共享身份（李刚 token），纯只读
  2. 去掉 CREATE_USER 过滤，全部可见
  3. 独立密码文件 .shb-passwd（nginx 层，见部署）
  4. 当前 1 Tab（站点信息），未来多 Tab 只加配置
  5. 权限：permissions.json（照搬 u8-web 模式）
  6. 缓存：TTL 5 分钟
  7. 日志：查询日志（用户 + 条件 + 耗时）
  8. 导出：仅 admin
  9. 前端路径：绝对路径 /shb/api/... 修复 404 bug
"""
import csv
import fcntl
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from flask import Flask, jsonify, make_response, redirect, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRIDGE = os.path.join(BASE_DIR, "bridge.py")
PERMISSIONS_FILE = os.path.join(BASE_DIR, "permissions.json")
HTPASSWD_FILE = "/etc/nginx/.shb-passwd"          # nginx Basic Auth 密码文件
HTPASSWD_OWNER = "root:www-data"                  # 保持 640，nginx(www-data) 可读
UA_ADMIN = "admin"                                # 内置管理员，权限固定全量
UA_USERNAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,31}$")
UA_PASSWORD_MIN = 6
LOG_FILE = os.path.join(BASE_DIR, "shb-web.log")

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200
CACHE_TTL = 300  # 5 分钟

# ── 日志（查询日志 + Flask access log 都进 shb-web.log） ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("shb-web")

# 在线报表取数层（云端实时查询，与本地库完全隔离；2026-11-10 新增）
sys.path.insert(0, BASE_DIR)
import online as shb_online  # noqa: E402
import weekly_core  # noqa: E402  周计划公共算法层（本地/在线共用）
import workload_core  # noqa: E402  工作量公共算法层（本地/在线共用）
import debug_core  # noqa: E402  调试进度公共算法层（本地/在线共用）

# ── Tab 定义（未来加 Tab 只改这里） ──
# display_fields: (字段key, 显示名)
# search_fields: 关键字搜索覆盖的字段
# select_field: 下拉筛选字段（可选）
TABS = {
    "station": {
        "title": "站点信息",
        "template_biz_id": "93c2a55f-f06f-41b6-8ab0-4733fe16d775",
        # 项目编码/站点编码来自客户管理（customer），用站点客户名二次查询填充：
        #   __customer_serial   → customer.serialNumber（项目编码）
        #   __customer_sitecode → customer.attribute.field_57HbU2ZNIfWB0aRB（站点编码）
        "customer_name_field": "fieldSmAZvfCuXhYft5JCk6fIYYlTcTzEhywB",
        "display_fields": [
            ("serialNumber", "编号"),
            ("__customer_serial", "项目编码"),
            ("fieldSmAZvfCuXhYft5JCk6fIYYlTcTzEhywB", "站点客户"),
            ("fieldGBOFoieAxcsNOZ09UUMiIQ3eyztY5tRf", "站点名称"),
            ("__customer_sitecode", "站点编码"),
            ("CREATE_USER", "创建人"),
            ("CREATE_TIME", "创建时间"),
        ],
        # 传给 paas form search 的 --fields 列表（后端按此返回，含系统字段大写 key）
        "fields": [
            "serialNumber",
            "fieldSmAZvfCuXhYft5JCk6fIYYlTcTzEhywB",
            "fieldGBOFoieAxcsNOZ09UUMiIQ3eyztY5tRf",
            "fieldgYaydJFRdaiWDzBc3Z6Ej62UWercV2qk",
            "CREATE_USER",
            "CREATE_TIME",
        ],
        "search_fields": [
            "fieldGBOFoieAxcsNOZ09UUMiIQ3eyztY5tRf",
            "fieldSmAZvfCuXhYft5JCk6fIYYlTcTzEhywB",
        ],
        "select_field": "fieldgYaydJFRdaiWDzBc3Z6Ej62UWercV2qk",
    },
}
ALL_TABS = list(TABS.keys())
ONLINE_TITLES = {"station": "站点信息"}

# ══ 本地备份库（shb-tasks SQLite，只读） ══════════════════════════════
# 用途：验证 SHB 工单备份落库结果（按工单类型分页，字段全显示）
BACKUP_DB = os.path.abspath(os.path.join(BASE_DIR, "..", "shb-tasks", "data", "shb_tasks.db"))
OVERVIEW_TAB = "bk_overview"

# 通用实体 Tab：entity_type -> 显示名（PAAS 表单标题动态取自库）
ENTITY_TITLES = {
    "customer": "客户档案",
    "customer_linkman": "客户联系人",
    "customer_address": "客户地址",
    "product": "设备信息（产品）",
}

# 站点信息：改为读本地备份（不再实时调 shb-cli）；与本地客户库 join 补项目编码/站点编码
STATION_TAB = "station"
STATION_ENTITY = "paas:93c2a55f-f06f-41b6-8ab0-4733fe16d775"
STATION_CUST_FIELD = "fieldSmAZvfCuXhYft5JCk6fIYYlTcTzEhywB"  # 站点客户（客户名）
STATION_CUST_LABEL = "站点客户"
# 站点信息 Tab 固定展示列（顺序即表头顺序；其余系统字段/自定义字段一律不展示）
STATION_VIEW_COLS = [
    ("__cust_serial", "项目编码"),
    ("__cust_sitecode", "站点编码"),
    ("fieldGBOFoieAxcsNOZ09UUMiIQ3eyztY5tRf", "站点名称"),
    (STATION_CUST_FIELD, "客户名称"),
    ("createUser", "创建人"),
    ("createTime", "创建时间"),
]
# 已单独成 Tab 的 PAAS 表单（不再重复生成 bkent: Tab）
PAAS_TAB_SKIP = {STATION_ENTITY}

# ══ 本地业务结果 Tab（2026-09-23 新增）═══════════════════════════════════
# 行程单 / 项目进度：“查询”只读本地结果表；“保存”仅 admin（实时抓数→入库，后台任务）；“导出”= Excel
# 2026-11-12 hunter 定：本地去掉旧「项目进度」页签（原 progress），「项目进度2」改名为「项目进度」
RESULT_TITLES = {"trip": "行程单", "weekly-plan": "周计划", "progress2": "项目进度",
                  "workload": "服务人员工作量", "debug-progress": "调试进度",
                 "records": "工单动态", "users": "SHB用户"}

# ══ 在线报表（2026-11-10 新增，hunter 批准）═════════════════════════════
# 独立分组「在线报表」：点查询直接调售后宝云端，与本地备份库完全无关。
# 取数实现见 online.py；失败只报错，不降级本地。结果内存缓存 CACHE_TTL。
ONLINE_TABS = {
    "ol:trip": "行程单（在线）",
    "ol:weekly-plan": "周计划（在线）",
    "ol:progress2": "项目进度（在线）",
    "ol:workload": "服务人员工作量（在线）",
    "ol:debug-progress": "调试进度（在线）",
    "ol:maintain-progress": "保运进度（在线）",
    "ol:accept-progress": "验收进度（在线）",
    "ol:audit-check": "稽核检查（在线）",
}
ONLINE_CACHE_TTL = 1800  # 在线报表结果缓存 30 分钟（2026-11-10 hunter 定）
# 不缓存的在线报表：当前为空（2026-11-11 hunter 定）。
# 变更：保运进度原因「含暂存数据、担心刚暂存完查不到」被排除缓存，
# 现改为与其他在线报表一致的 30 分钟缓存（缓存键含 keyword 与日期参数，不同查询互不覆盖）。
# 若将来某张表需要实时取数，把 tab 名加进这个集合即可。
ONLINE_NO_CACHE = set()

# ══ 系统 Tab（对照表等）2026-10-08 ══
SYSTEM_TITLES = {"fieldmap": "数据字典", "sync": "同步中心", "synclog": "同步日志",
                  "identity": "身份管理", "users-account": "账号管理"}
RESULT_SCRIPT_PY = "/usr/bin/python3"
RESULT_SCRIPTS = {
    "trip": os.path.abspath(os.path.join(BASE_DIR, "..", "skills", "shb-trip-export", "scripts", "export_trip.py")),
    "progress": os.path.abspath(os.path.join(BASE_DIR, "..", "skills", "shb-project-progress-query", "scripts", "save_project_progress.py")),
    "users": os.path.join(BASE_DIR, "scripts", "sync_users.py"),
    "workload": os.path.abspath(os.path.join(BASE_DIR, "..", "shb-tasks", "sync_records.py")),
}
RESULT_JOBS: dict = {}

# 进度表宽表列（与技能固定表一致）
PROGRESS_COLUMNS = ["客户编号", "客户名称", "站点名称",
                    "发货时间", "开工日期", "探伤完成", "约检日期", "保温完成",
                    "内部验收", "验收人",
                    "预调试完成", "调试开始", "调试完成",
                    "消防验收", "计量验收", "充装验收", "保运完成"]


def is_result_tab(tab):
    return isinstance(tab, str) and tab in RESULT_TITLES


def _meta_value(key):
    try:
        conn = backup_conn()
        r = conn.execute("select value from shb_meta where key=?", (key,)).fetchone()
        conn.close()
        return r["value"] if r else ""
    except Exception:
        return ""


def query_trip(person, date_begin, date_end, page, page_size):
    """行程单（本地结果表 trip_records）：按姓名 + 日期区间"""
    where, args = [], []
    if person:
        where.append("person like ?")
        args.append(f"%{person}%")
    if date_begin:
        where.append("day >= ?")
        args.append(date_begin)
    if date_end:
        where.append("day <= ?")
        args.append(date_end)
    wsql = (" where " + " and ".join(where)) if where else ""
    conn = backup_conn()
    try:
        total = conn.execute(f"select count(*) c from trip_records{wsql}", args).fetchone()["c"]
        rs = conn.execute(
            f"select person, day, in_loc, out_loc, task_nos, task_types, work from trip_records{wsql} "
            f"order by day asc, person limit ? offset ?", args + [page_size, (page - 1) * page_size]).fetchall()
        persons = [r["person"] for r in conn.execute(
            "select distinct person from trip_records where coalesce(person,'')<>'' order by person")
            if r["person"]]
    finally:
        conn.close()
    base = (page - 1) * page_size
    rows = [{"序号": base + i, "姓名": r["person"], "日期": r["day"], "到场地点": r["in_loc"],
             "离场地点": r["out_loc"], "工单编号": r["task_nos"], "工单类型": r["task_types"],
             "工作内容": r["work"]} for i, r in enumerate(rs, 1)]
    return {"ok": True, "tab": "trip", "title": RESULT_TITLES["trip"], "rows": rows,
            "columns": ["序号", "姓名", "日期", "到场地点", "离场地点", "工单编号", "工单类型", "工作内容"],
            "total": total, "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "saved_at": _meta_value("trip_last_save_at"),
            "persons": persons}


def query_users(name, dept, page, page_size):
    """用户（本地 users 表）：按姓名 / 部门；列 = 用户ID / 姓名 / 部门"""
    where, args = [], []
    if name:
        where.append("name like ?")
        args.append(f"%{name}%")
    if dept:
        where.append("coalesce(dept,'') like ?")
        args.append(f"%{dept}%")
    wsql = (" where " + " and ".join(where)) if where else ""
    conn = backup_conn()
    try:
        total = conn.execute(f"select count(*) c from users{wsql}", args).fetchone()["c"]
        rs = conn.execute(
            f"select user_id, name, coalesce(dept,'') dept from users{wsql} "
            f"order by name limit ? offset ?", args + [page_size, (page - 1) * page_size]).fetchall()
    finally:
        conn.close()
    rows = [{"用户ID": r["user_id"], "姓名": r["name"] or "", "部门": r["dept"]} for r in rs]
    return {"ok": True, "tab": "users", "title": RESULT_TITLES["users"], "rows": rows,
            "columns": ["用户ID", "姓名", "部门"], "total": total, "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "saved_at": _meta_value("users_last_save_at")}


def query_progress(serial, name, site, page, page_size):
    """项目进度（本地结果表 project_progress）：一客户一行 × 14 个进度列

    2026-10-22 改版：行粒度从「客户×工单类型」改为「一客户一行」，
    旧的 progress_records / PROGRESS_COL_MAP 已废弃。
    """
    where, args = [], []
    if serial:
        where.append("customer_serial like ?")
        args.append(f"%{serial}%")
    if name:
        where.append("customer_name like ?")
        args.append(f"%{name}%")
    if site:
        where.append("site_name like ?")
        args.append(f"%{site}%")
    wsql = (" where " + " and ".join(where)) if where else ""
    conn = backup_conn()
    try:
        total = conn.execute("select count(*) c from project_progress" + wsql, args).fetchone()["c"]
        recs = conn.execute("select * from project_progress" + wsql +
                            " order by customer_serial limit ? offset ?",
                            args + [page_size, (page - 1) * page_size]).fetchall()
        keys = set(recs[0].keys()) if recs else set()
    finally:
        conn.close()
    col_map = {"客户编号": "customer_serial", "客户名称": "customer_name", "站点名称": "site_name"}
    rows = []
    for r in recs:
        row = {}
        for col in PROGRESS_COLUMNS:
            src = col_map.get(col, col)
            v = r[src] if src in keys else ""
            row[col] = "" if v is None else str(v)
        rows.append(row)
    return {"ok": True, "tab": "progress", "title": "项目进度（旧版已下线）", "rows": rows,
            "columns": list(PROGRESS_COLUMNS), "total": total, "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "saved_at": _meta_value("progress_last_save_at")}
# ══ 工单动态报表（2026-10-21 新增）════════════════════════════════════
RECORD_COLUMNS = ["时间", "工单编号", "工单类型", "客户", "操作", "操作人", "地址", "内容"]


def _rec_summary(action, content):
    """把动态 content（JSON 串）翻成人话"""
    if not content:
        return ""
    try:
        d = json.loads(content) if isinstance(content, str) else (content or {})
    except Exception:
        return str(content)[:120]
    if not isinstance(d, dict):
        return str(d)[:120]
    if d.get("cardName"):
        return f"{d.get('cardName')} · {d.get('action') or ''}".strip(" ·")
    if d.get("buttonName"):
        return f"提交节点：{d['buttonName']}"
    if d.get("updateFields"):
        return f"修改字段：{d['updateFields']}"
    if d.get("customExceptionReason"):
        return f"原因：{d['customExceptionReason']}"
    if d.get("reason"):
        return f"原因：{d['reason']}"
    if d.get("synergy"):
        return f"协同：{d['synergy']}"
    if d.get("updateContent"):
        return str(d["updateContent"])[:120]
    if d.get("action"):
        return str(d["action"])[:120]
    return ""


def query_records(month, keyword, action, page, page_size):
    """工单动态（本地 task_records）：按月份 + 关键字 + 操作类型"""
    month = month or datetime.now().strftime("%Y-%m")
    where, args = ["r.create_time like ?"], [month + "%"]
    if keyword:
        where.append("(t.task_no like ? or coalesce(t.customer_name,'') like ? "
                     "or coalesce(r.user_name,'') like ? or coalesce(r.action,'') like ? "
                     "or coalesce(t.template_name,'') like ?)")
        args += [f"%{keyword}%"] * 5
    if action:
        where.append("r.action = ?")
        args.append(action)
    wsql = " where " + " and ".join(where)
    conn = backup_conn()
    try:
        total = conn.execute(
            "select count(*) c from task_records r left join tasks t on t.id = r.task_id" + wsql,
            args).fetchone()["c"]
        rs = conn.execute(
            "select r.create_time, r.task_no, r.action, r.user_name, r.content, r.address, "
            "       coalesce(t.template_name,'') tt, coalesce(t.customer_name,'') cust "
            "  from task_records r left join tasks t on t.id = r.task_id" + wsql +
            " order by r.create_time desc, r.task_no limit ? offset ?",
            args + [page_size, (page - 1) * page_size]).fetchall()
        actions = [r["action"] for r in conn.execute(
            "select distinct action from task_records where coalesce(action,'')<>'' order by action")
            if r["action"]]
        last = conn.execute("select max(finished_at) f from sync_runs where mode like 'records:%'").fetchone()["f"]
    finally:
        conn.close()
    rows = [{"时间": r["create_time"] or "", "工单编号": r["task_no"] or "",
             "工单类型": r["tt"], "客户": r["cust"], "操作": r["action"] or "",
             "操作人": r["user_name"] or "", "地址": r["address"] or "",
             "内容": _rec_summary(r["action"], r["content"])} for r in rs]
    return {"ok": True, "tab": "records", "title": RESULT_TITLES["records"],
            "rows": rows, "columns": list(RECORD_COLUMNS), "total": total,
            "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "month": month, "actions": actions, "saved_at": last or ""}


# ══ 服务人员工作量（2026-10-21 新增）═══════════════════════════════════
# 口径：出发=动态「出发」最早 / 到场=动态「到达」最晚 / 途中时长=差值
#      到场签到、离场打卡取附加组件卡片；工单时长=离场签到−到场签到（小时，1 位小数）
WORKLOAD_COLUMNS = ["服务人员", "角色", "工单类型", "客户编号", "客户", "服务部门", "省份",
                    "出发打卡", "到场打卡", "途中时长",
                    "到场签到时间", "离场签到时间", "工单时长", "提示"]
_WL_DT_FMTS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


def _wl_dt(s):
    if not s:
        return None
    s = str(s).strip().replace("/", "-")
    for f in _WL_DT_FMTS:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    return None


def _wl_cards(extra):
    """cardId -> 卡片名 / 字段显示名->字段key / 卡片行数据"""
    names, fmap, rows = {}, {}, {}
    for c in extra.get("cardInfo") or []:
        cid = c.get("cardId")
        names[cid] = c.get("cardName") or ""
        fmap[cid] = {f.get("displayName"): f.get("fieldName") for f in (c.get("fields") or [])}
    rt = extra.get("receiptTaskForUpdate") or {}
    for c in rt.get("cardInfo") or []:
        rows[c.get("cardId")] = c.get("attribute") or []
    return names, fmap, rows


def _wl_card_time(extra, card_name, field_names, mode):
    names, fmap, rows = _wl_cards(extra)
    vals = []
    for cid, cname in names.items():
        if cname != card_name:
            continue
        fm = fmap.get(cid) or {}
        for r in rows.get(cid) or []:
            for fname in field_names:
                key = fm.get(fname)
                if key and r.get(key):
                    dt = _wl_dt(r.get(key))
                    if dt:
                        vals.append(dt)
    if not vals:
        return None
    return min(vals) if mode == "min" else max(vals)


def _wl_province(extra, addr):
    names, fmap, rows = _wl_cards(extra)
    for cid in rows:
        fm = fmap.get(cid) or {}
        for r in rows.get(cid) or []:
            for _disp, key in fm.items():
                v = r.get(key)
                if isinstance(v, dict) and v.get("province"):
                    return v["province"]
    if addr:
        head = str(addr).split(",")[0].strip()
        if head.endswith(("省", "市")) or "自治区" in head:
            return head
    return ""


def _wl_area(detail):
    for tag in ((detail.get("customer") or {}).get("tags") or []):
        name = tag.get("tagName") or ""
        if "片区" in name:
            return name
    return ""


def _wl_hours(a, b):
    if not a or not b:
        return ""
    return round((b - a).total_seconds() / 3600.0, 1)


def query_workload(date_begin="", date_end="", keyword="", page=1, page_size=50):
    """服务人员工作量（只读本地库）

    2026-11-12：与在线版共用 workload_core.build_workload()，只换数据源；
    筛选条件同在线：建单日期区间（默认当周）+ 关键字（客户编号/客户/服务人员/工单类型）
    """
    # 默认当周（周一~周日），与在线 rng(params,"week") 一致
    today = datetime.now()
    try:
        d0 = datetime.strptime((date_begin or "").strip(), "%Y-%m-%d")
    except Exception:
        d0 = today
    mon = (d0 - timedelta(days=d0.weekday()))
    b = (date_begin or "").strip() or mon.strftime("%Y-%m-%d")
    e = (date_end or "").strip() or (mon + timedelta(days=6)).strftime("%Y-%m-%d")

    conn = backup_conn()
    try:
        rowsdb = conn.execute(
            "select t.id, t.task_no, t.template_name, t.executor_name, t.customer_name, "
            "       r.detail_json, r.extra_json "
            "  from tasks t left join task_raw r on r.id = t.id "
            " where coalesce(t.deleted,0)=0 "
            "   and coalesce(t.create_time,'') >= ? and coalesce(t.create_time,'') <= ? "
            " order by t.create_time",
            (b, e + " 23:59:59")).fetchall()
        recs = {}
        for r in conn.execute("select task_id, action, create_time from task_records"):
            recs.setdefault(r["task_id"], []).append(
                {"action": r["action"], "createTime": r["create_time"]})
    finally:
        conn.close()

    tasks, details = [], {}
    for r in rowsdb:
        dj, ej = {}, {}
        try:
            dj = json.loads(r["detail_json"]) if r["detail_json"] else {}
            ej = json.loads(r["extra_json"]) if r["extra_json"] else {}
        except Exception:
            pass
        tid = r["id"]
        tasks.append({
            "id": tid, "taskNo": r["task_no"], "templateName": r["template_name"],
            "executorName": r["executor_name"], "customerEntity": r["customer_name"],
        })
        details[tid] = {"task": dj,
                        "cardInfo": ej.get("cardInfo"),
                        "receiptTaskForUpdate": ej.get("receiptTaskForUpdate")}

    out, persons = workload_core.build_workload(
        tasks, details, recs, keyword,
        ts=shb_online.ts_any, cards=shb_online.card_sign_records,
        hours=shb_online.person_work_hours, province=shb_online._wl_province)

    total = len(out)
    start = (page - 1) * page_size
    return {"ok": True, "tab": "workload", "title": RESULT_TITLES["workload"],
            "rows": out[start:start + page_size], "columns": list(WORKLOAD_COLUMNS),
            "total": total, "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "saved_at": _meta_value("workload_last_save_at"),
            "persons": persons}


def query_progress2(keyword, cust_name, page, page_size, show_done=False,
                    date_begin="", date_end=""):
    """项目进度2：客户档案 17 列（只读本地库）

    show_done：是否包含已完成项目（运行日期 = 保运完毕日期 非空即已完成）
    日期筛选（2026-11-12，与在线版同口径）：
      · 有填关键字 → 关键字优先，不限时间
      · 否则按「客户档案最后更新日期」(entities.updated_at) 筛；留空 = 不限
    """
    date_begin = (date_begin or "").strip()
    date_end = (date_end or "").strip()
    use_date = bool(date_begin or date_end) and not keyword
    conn = backup_conn()
    try:
        want = ["serialNumber", "name"] + [f for f, _ in P2_DATE_FIELDS] + [P2_BAOYUN_PERSON]
        ph = ",".join("?" * len(want))
        vals, upd = {}, {}
        for a in conn.execute(
                "select ea.entity_id, ea.field_name, ea.value_text, e.updated_at "
                "from entity_attrs ea left join entities e "
                "on e.entity_type='customer' and e.id=ea.entity_id "
                "where ea.entity_type='customer' and ea.field_name in (" + ph + ")",
                want).fetchall():
            vals.setdefault(a["entity_id"], {})[a["field_name"]] = (a["value_text"] or "").strip()
            upd[a["entity_id"]] = a["updated_at"] or ""

        # 调试工单 → 客户名：负责人 + 协同人（去重、保序）
        persons = {}
        for t in conn.execute(
                "select t.customer_name, t.executor_name, r.detail_json "
                "from tasks t left join task_raw r on r.id = t.id "
                "where t.template_id=? and coalesce(t.deleted,0)=0",
                (DEBUG_TASK_TID,)).fetchall():
            cname = (t["customer_name"] or "").strip()
            if not cname:
                continue
            names = persons.setdefault(cname, [])
            ex = (t["executor_name"] or "").strip()
            if ex and ex not in names:
                names.append(ex)
            if t["detail_json"]:
                try:
                    dd = json.loads(t["detail_json"])
                except Exception:
                    dd = {}
                for s in (dd.get("synergies") or []):
                    nm = ((s or {}).get("displayName") or "").strip()
                    if nm and nm not in names:
                        names.append(nm)
                # 2026-11-12：补转派协同人（与在线版口径一致）
                for s in (dd.get("reAllotUserList")
                          or (dd.get("attribute") or {}).get("reAllotUserList") or []):
                    nm = ((s or {}).get("userName") or (s or {}).get("displayName") or "").strip()
                    if nm and nm not in names:
                        names.append(nm)

        rows, seen = [], set()
        for _eid, m in vals.items():
            serial = m.get("serialNumber", "")
            cname = m.get("name", "")
            # 日期筛选：按客户档案最后更新日期（有关键字时不生效）
            if use_date:
                _ut = (upd.get(_eid) or "")[:10]
                if _ut:
                    if date_begin and _ut < date_begin:
                        continue
                    if date_end and _ut > date_end:
                        continue
            if keyword and keyword not in serial and keyword not in cname:
                continue
            if cust_name and cust_name not in cname:
                continue
            # 12 个日期字段全空 → 不显示
            if not any(m.get(f, "") for f, _ in P2_DATE_FIELDS):
                continue
            # 未勾选「显示已完成」→ 隐藏运行日期（= 保运完毕日期）非空的项目
            _run_date = m.get("field_Brf7x05dyDVU9HOL", "")
            if _run_date and not show_done:
                continue
            if serial:
                if serial in seen:
                    continue
                seen.add(serial)
            row = {"客户编号": serial, "客户名称": cname}
            for f, label in P2_DATE_FIELDS:
                row[label] = m.get(f, "")
            row["调试人员"] = ",".join(persons.get(cname, []))
            row["保运人员"] = m.get(P2_BAOYUN_PERSON, "")
            row["运行日期"] = row.get("保运完毕日期", "")
            rows.append(row)

        rows.sort(key=lambda r: (r["客户编号"] or ""))
        total = len(rows)
        start = (page - 1) * page_size
        return {"ok": True, "tab": "progress2", "title": RESULT_TITLES["progress2"],
                "rows": rows[start:start + page_size], "columns": list(PROGRESS2_COLUMNS),
                "total": total, "page": page, "page_size": page_size,
                "has_next": page * page_size < total, "has_prev": page > 1,
                "total_pages": max(1, (total + page_size - 1) // page_size),
                "saved_at": ""}
    finally:
        conn.close()


# ══ 周计划（2026-10-27 新增）══════════════════════════════════════════
# 行 = 周一~周日；列 = 本周有工单的负责人（按工单量降序）
# 单元格 = 该负责人当天命中的工单，每单两行：`序号.工单号` / `短词 | 客户全称`
WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def _wp_escape(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _wp_short_type(tname):
    """工单类型 → 短词（去掉「工单」二字）"""
    t = (tname or "").strip()
    return t[:-2] if t.endswith("工单") and len(t) > 2 else t


def _wp_customer_norm(conn, raw_names):
    """客户名规范化：按「去空白后相同」匹配客户档案，用档案全称；匹配不到保留原值"""
    keys = {re.sub(r"\s+", "", n) for n in raw_names if n}
    mapping = {}
    if keys:
        try:
            for r in conn.execute(
                    "select a.value_text v from entity_attrs a "
                    "join entities e on e.entity_type = a.entity_type and e.id = a.entity_id "
                    "where a.entity_type='customer' and a.field_name='name' "
                    "and coalesce(e.deleted,0)=0").fetchall():
                v = (r["v"] or "").strip()
                if not v:
                    continue
                k = re.sub(r"\s+", "", v)
                if k in keys and k not in mapping:
                    mapping[k] = v
        except Exception:
            pass
    return lambda x: mapping.get(re.sub(r"\s+", "", x or ""), (x or "").strip())


def _wp_parse_ts(v):
    """毫秒时间戳 / 字符串时间 → datetime；解析失败返回 None"""
    if v in (None, "", 0, "0"):
        return None
    try:
        n = int(v)
        if n > 1000000000000:
            n = n / 1000.0
        return datetime.fromtimestamp(n)
    except (TypeError, ValueError):
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(v).strip(), fmt)
        except ValueError:
            continue
    return None


def query_weekly_plan(week_start="", mode="plan", keyword=""):
    """周计划 / 周总结（只读本地库，不写库）

    2026-11-12：与在线版共用 weekly_core.build_weekly()，只换数据源（本地 SQLite 备份库）
    mode=plan    ：计划开始 / 计划结束
    mode=summary ：实际开始 / 完成时间；未完成视为持续进行
    """
    conn = backup_conn()
    try:
        rowsdb = conn.execute(
            "select t.id, t.task_no, t.template_name, t.executor_name, t.customer_name, "
            "       t.state_cn, t.plan_start_time, t.plan_end_time, r.detail_json "
            "  from tasks t left join task_raw r on r.id = t.id "
            " where coalesce(t.deleted,0)=0").fetchall()
    finally:
        conn.close()

    tasks, details = [], {}
    for r in rowsdb:
        dj = {}
        if r["detail_json"]:
            try:
                dj = json.loads(r["detail_json"])
            except Exception:
                dj = {}
        tid = r["id"]
        tasks.append({
            "id": tid, "taskNo": r["task_no"], "templateName": r["template_name"],
            "executorName": r["executor_name"], "customer": r["customer_name"],
            "state": r["state_cn"],
            "planStartTime": r["plan_start_time"], "planEndTime": r["plan_end_time"],
            "startTime": dj.get("startTime"), "completeTime": dj.get("completeTime"),
        })
        details[tid] = {"task": {"executor": dj.get("executor"),
                                 "synergies": dj.get("synergies") or []}}

    monday, days = weekly_core.week_days(week_start)
    out = weekly_core.build_weekly(tasks, details, monday, days, mode=mode, keyword=keyword)

    return {"ok": True, "tab": "weekly-plan", "title": RESULT_TITLES["weekly-plan"],
            "mode": mode,
            "rows": out["rows"], "columns": out["columns"],
            "total": len(out["rows"]), "page": 1, "page_size": max(1, len(out["rows"])),
            "has_next": False, "has_prev": False, "total_pages": 1,
            "html_cells": True,
            "week_start": out["week_start"], "week_end": out["week_end"],
            "persons": out["persons"]}


def backup_conn():
    """只读打开备份库（页缓存提至 64MB，避免大库首次冷读慢）"""
    conn = sqlite3.connect(f"file:{BACKUP_DB}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("pragma cache_size = -65536")
    except Exception:
        pass
    return conn


# ══ 调试进度表（2026-10-23 新增，只读混合方案：tasks 静态 + task_records 动态）═══
DEBUG_PROGRESS_COLUMNS = ["项目编号", "项目名称", "调试人", "建单时间", "接受时间",
                         "出发时间", "达到时间", "调试内容/时间/调试人", "完成数/总数", "完成日期"]
DEBUG_TASK_TID = "f6690742-8462-42c9-b7dd-fe36ea405eb6"


def _dp_ts(s):
    """task_records.create_time（字符串时间戳）→ 'YYYY-MM-DD HH:MM:SS'"""
    if not s:
        return ""
    s = str(s).strip()
    if s.isdigit():
        try:
            return datetime.fromtimestamp(int(s) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return s
    return s


def query_debug_progress(keyword, date_begin="", date_end="", page=1, page_size=200):
    """调试进度（明细，只读本地库）

    2026-11-12：与在线共用 debug_core.build_debug()，只换数据源。
      · 有填关键字 → 不限时间、结果不按调试日期筛（与在线同口径）
      · 无关键字   → 只查最近 30 天建单的工单，并按调试日期筛所选区间（默认当周）
      · 本地无「节点暂存」数据 → 只显示已提交的调试项（在线的暂存项本地看不到）
    """
    keyword = (keyword or "").strip()
    today = datetime.now()
    try:
        d0 = datetime.strptime((date_begin or "").strip(), "%Y-%m-%d")
    except Exception:
        d0 = today
    mon = d0 - timedelta(days=d0.weekday())
    b_day = (date_begin or "").strip() or mon.strftime("%Y-%m-%d")
    e_day = (date_end or "").strip() or (mon + timedelta(days=6)).strftime("%Y-%m-%d")
    date_filter = not keyword

    conn = backup_conn()
    try:
        sql = ("select t.id, t.task_no, t.customer_name, t.create_time, r.detail_json "
               "  from tasks t left join task_raw r on r.id = t.id "
               " where t.template_id=? and coalesce(t.deleted,0)=0")
        args = [DEBUG_TASK_TID]
        if keyword:
            sql += " and (t.task_no like ? or t.customer_name like ?)"
            args += ["%" + keyword + "%", "%" + keyword + "%"]
        else:
            win_b = (datetime.strptime(b_day, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
            sql += " and coalesce(t.create_time,'') >= ? and coalesce(t.create_time,'') <= ?"
            args += [win_b, e_day + " 23:59:59"]
        rowsdb = conn.execute(sql, args).fetchall()
    finally:
        conn.close()

    tasks, details = [], {}
    for r in rowsdb:
        dj = {}
        if r["detail_json"]:
            try:
                dj = json.loads(r["detail_json"])
            except Exception:
                dj = {}
        tasks.append({"id": r["id"], "taskNo": r["task_no"],
                      "customerEntity": r["customer_name"]})
        details[r["id"]] = {"task": dj}

    out = debug_core.build_debug(tasks, details, {}, keyword,
                                 date_filter=date_filter, b_day=b_day, e_day=e_day,
                                 ts=shb_online._dp_ts)

    total = len(out)
    start = (page - 1) * page_size
    return {"ok": True, "tab": "debug-progress", "title": RESULT_TITLES["debug-progress"],
            "rows": out[start:start + page_size], "columns": list(debug_core.DEBUG_COLUMNS),
            "total": total, "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size)}


def list_backup_types():
    """全部工单类型：[{'id','name','n'}]，工单数降序 + 名称"""
    try:
        conn = backup_conn()
    except Exception as e:
        logger.error("BACKUP_DB open failed: %s", e)
        return []
    try:
        rows = conn.execute(
            "select t.id, t.name, "
            " (select count(*) from tasks k where k.template_id = t.id and coalesce(k.deleted,0)=0) n "
            "from task_types t where coalesce(t.enabled,0)=1 order by n desc, t.name"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("list_backup_types failed: %s", e)
        return []
    finally:
        conn.close()


def backup_tab_titles():
    """备份相关 Tab：key -> 显示名（概览 + 工单类型 + 客户/联系人/地址/设备/PAAS）"""
    titles = {OVERVIEW_TAB: "备份概览"}
    for t in list_backup_types():
        titles["bk:" + t["id"]] = t["name"] or t["id"]
    for et, title in ENTITY_TITLES.items():
        titles["bkent:" + et] = title
    try:
        conn = backup_conn()
        for r in conn.execute(
                "select entity_type, max(case when field_name='templateBizName' then value_text end) v "
                "from entity_attrs where entity_type like 'paas:%' group by entity_type").fetchall():
            if r["entity_type"] in PAAS_TAB_SKIP:
                continue
            titles["bkent:" + r["entity_type"]] = r["v"] or r["entity_type"]
        conn.close()
    except Exception as e:
        logger.error("entity tab titles failed: %s", e)
    titles[STATION_TAB] = "站点信息"
    return titles


def is_backup_tab(tab):
    return (tab in (OVERVIEW_TAB, STATION_TAB) or
            (isinstance(tab, str) and tab.startswith(("bk:", "bkent:"))))


def entity_title(etype):
    if etype in ENTITY_TITLES:
        return ENTITY_TITLES[etype]
    try:
        conn = backup_conn()
        r = conn.execute(
            "select value_text from entity_attrs where entity_type=? and field_name='templateBizName' limit 1",
            (etype,)).fetchone()
        conn.close()
        if r and r["value_text"]:
            return r["value_text"]
    except Exception:
        pass
    return etype


def _label(field_name, display_name):
    dn = (display_name or "").strip()
    return dn if dn and dn != field_name else field_name


def backup_type_columns(conn, template_id):
    """该类型全部列：主表列 + 自定义字段（类型定义在前，实际出现的补齐）"""
    cols = [(fn, label) for fn, label in BACKUP_MAIN_COLUMNS]
    cseen = set()
    custom = []
    for r in conn.execute(
        "select field_name, display_name from task_fields where template_id = ? "
        "order by coalesce(is_system,0) desc, field_name",
        (template_id,),
    ).fetchall():
        fn = r["field_name"]
        if fn in cseen:
            continue
        cseen.add(fn)
        custom.append((fn, _label(fn, r["display_name"])))
    for r in conn.execute(
        "select field_name, max(display_name) dn from task_attributes "
        "where task_id in (select id from tasks where template_id = ?) "
        "group by field_name order by field_name",
        (template_id,),
    ).fetchall():
        fn = r["field_name"]
        if fn in cseen:
            continue
        cseen.add(fn)
        custom.append((fn, _label(fn, r["dn"])))
    # 中文名重复时补 field 名，保证前端表头 key 唯一
    used = {}
    out = []
    for fn, label in cols + custom:
        if label in used:
            used[label] += 1
            label = f"{label} ({fn})"
        else:
            used[label] = 1
        out.append((fn, label))
    return out


def query_backup(tab, keyword, date_begin, date_end, page, page_size):
    """按工单类型查本地备份库（所有字段全显示）"""
    template_id = tab[3:]
    conn = backup_conn()
    try:
        cols = backup_type_columns(conn, template_id)
        tr = conn.execute("select name from task_types where id = ?", (template_id,)).fetchone()
        tname = (tr["name"] if tr else "") or template_id
        where = ["template_id = ?", "coalesce(deleted,0) = 0"]
        args = [template_id]
        if date_begin:
            where.append("create_time >= ?")
            args.append(date_begin + " 00:00:00")
        if date_end:
            where.append("create_time <= ?")
            args.append(date_end + " 23:59:59")
        if keyword:
            kw = f"%{keyword}%"
            where.append("(task_no like ? or customer_name like ? or id in "
                         "(select task_id from task_attributes where value_text like ?))")
            args += [kw, kw, kw]
        wsql = " and ".join(where)
        total = conn.execute(f"select count(*) c from tasks where {wsql}", args).fetchone()["c"]
        trows = conn.execute(
            f"select * from tasks where {wsql} order by create_time desc, task_no desc limit ? offset ?",
            args + [page_size, (page - 1) * page_size],
        ).fetchall()
        ids = [r["id"] for r in trows]
        attrs = {}
        if ids:
            ph = ",".join("?" * len(ids))
            for a in conn.execute(
                f"select task_id, field_name, value_text from task_attributes where task_id in ({ph})",
                ids,
            ).fetchall():
                attrs.setdefault(a["task_id"], {})[a["field_name"]] = a["value_text"] or ""
        rows = []
        for tk in trows:
            amap = attrs.get(tk["id"], {})
            out = {}
            for fn, label in cols:
                if fn in amap:
                    out[label] = amap[fn]
                elif fn in tk.keys():
                    v = tk[fn]
                    out[label] = "" if v is None else str(v)
                else:
                    out[label] = ""
            rows.append(out)
        return {
            "ok": True, "tab": tab, "title": tname, "rows": rows, "total": total,
            "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "columns": [label for _, label in cols],
        }
    finally:
        conn.close()


def _customer_map(conn):
    """本地客户库 → {客户名: (项目编码, 站点编码)}，供站点页本地 join"""
    try:
        return {r["lbl"]: (r["sn"] or "", r["sc"] or "") for r in conn.execute(
            "select e.label lbl, "
            "max(case when a.field_name='serialNumber' then a.value_text end) sn, "
            "max(case when a.field_name=? then a.value_text end) sc "
            "from entities e join entity_attrs a on a.entity_type=e.entity_type and a.entity_id=e.id "
            "where e.entity_type='customer' and coalesce(e.deleted,0)=0 group by e.label",
            (CUSTOMER_SITE_CODE_FIELD,)).fetchall() if r["lbl"]}
    except Exception:
        return {}


def query_entity(etype, keyword, date_begin, date_end, page, page_size):
    """按实体类型查备份库（客户/联系人/地址/产品/PAAS 表单；字段全显示）"""
    conn = backup_conn()
    try:
        cols, seen = [], set()
        for r in conn.execute(
                "select field_name, display_name from entity_fields where entity_type=? order by seq",
                (etype,)).fetchall():
            fn = r["field_name"]
            if fn in seen:
                continue
            seen.add(fn)
            cols.append((fn, _label(fn, r["display_name"])))
        for r in conn.execute(
                "select distinct field_name from entity_attrs where entity_type=? order by field_name",
                (etype,)).fetchall():
            fn = r["field_name"]
            if fn in seen:
                continue
            seen.add(fn)
            cols.append((fn, fn))
        used, oc = {}, []
        for fn, lab in cols:
            if lab in used:
                used[lab] += 1
                lab = f"{lab} ({fn})"
            else:
                used[lab] = 1
            oc.append((fn, lab))
        # 站点信息：只展示固定 6 列（隐藏系统字段与其他自定义字段），顺序固定
        extra_cols = []
        if etype == STATION_ENTITY:
            extra_cols = [("__cust_serial", "项目编码"), ("__cust_sitecode", "站点编码")]
            oc = list(STATION_VIEW_COLS)
        where = ["entity_type = ?", "coalesce(deleted,0)=0"]
        args = [etype]
        if date_begin:
            where.append("created_at >= ?")
            args.append(date_begin + " 00:00:00")
        if date_end:
            where.append("created_at <= ?")
            args.append(date_end + " 23:59:59")
        if keyword:
            kw = f"%{keyword}%"
            where.append("(label like ? or serial like ? or id in "
                         "(select entity_id from entity_attrs where entity_type=? and value_text like ?))")
            args += [kw, kw, etype, kw]
        wsql = " and ".join(where)
        total = conn.execute(f"select count(*) c from entities where {wsql}", args).fetchone()["c"]
        erows = conn.execute(
            f"select id from entities where {wsql} order by coalesce(serial,label), label limit ? offset ?",
            args + [page_size, (page - 1) * page_size]).fetchall()
        ids = [r["id"] for r in erows]
        vals = {}
        if ids:
            ph = ",".join("?" * len(ids))
            for a in conn.execute(
                    f"select entity_id, field_name, value_text from entity_attrs "
                    f"where entity_type=? and entity_id in ({ph})", [etype] + ids).fetchall():
                vals.setdefault(a["entity_id"], {})[a["field_name"]] = a["value_text"] or ""
        cmap = _customer_map(conn) if etype == STATION_ENTITY else {}
        rows = []
        for e in erows:
            m = vals.get(e["id"], {})
            row = {lab: m.get(fn, "") for fn, lab in oc if not fn.startswith("__")}
            if extra_cols:
                sn, sc = cmap.get(m.get(STATION_CUST_FIELD, ""), ("", ""))
                row["项目编码"] = sn
                row["站点编码"] = sc
                row = {lab: row.get(lab, "") for _f, lab in oc}
            rows.append(row)
        return {
            "ok": True, "tab": "bkent:" + etype, "title": entity_title(etype),
            "rows": rows, "total": total, "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "columns": [lab for _, lab in oc],
        }
    finally:
        conn.close()


def query_fieldmap(keyword, page, page_size):
    """数据字典（系统 Tab）：列出全部字段定义 —— 工单各类型字段（task_fields）
    + 实体字段（entity_fields：客户/联系人/地址/产品/PAAS 表单），支持搜索。
    数据源为本地备份库对照表（定义表），不读 schema.json。
    """
    conn = backup_conn()
    try:
        kw = (keyword or "").strip().lower()
        ent_label = dict(ENTITY_TITLES)
        ent_label[STATION_ENTITY] = "站点信息"
        ent_label["paas:d8c34f70-4f20-4c31-a1da-825d63582cd5"] = "问题工单"

        rows = []
        # 工单字段（含主表 + 各环节）
        for r in conn.execute(
                "select coalesce(tt.name,'?') scope, tf.field_name fn, "
                "tf.display_name dn, tf.is_system sys "
                "from task_fields tf left join task_types tt on tt.id = tf.template_id"):
            rows.append((r["scope"], r["fn"], r["dn"], r["sys"]))
        # 实体字段
        for r in conn.execute(
                "select entity_type et, field_name fn, display_name dn, is_system sys "
                "from entity_fields"):
            et = r["et"] or ""
            scope = ent_label.get(et)
            if not scope:
                scope = "PAAS表单" if et.startswith("paas:") else (et or "?")
            rows.append((scope, r["fn"], r["dn"], r["sys"]))

        if kw:
            rows = [x for x in rows if kw in (str(x[0]) + str(x[1]) + str(x[2])).lower()]
        rows.sort(key=lambda x: (str(x[0] or ""), str(x[1] or "")))
        total = len(rows)

        page_size = max(1, int(page_size or 20))
        page = max(1, int(page or 1))
        start = (page - 1) * page_size
        cols = ["范围", "字段名", "中文名", "类别", "系统字段"]
        out = []
        for scope, fn, dn, sys in rows[start:start + page_size]:
            fn = fn or ""
            out.append({cols[0]: scope, cols[1]: fn, cols[2]: (dn or ""),
                        cols[3]: "字段" if fn.startswith("field") else "系统键",
                        cols[4]: "是" if sys else "否"})
        return {
            "ok": True, "tab": "fieldmap", "title": SYSTEM_TITLES["fieldmap"],
            "columns": cols, "rows": out, "total": total,
            "page": page, "page_size": page_size,
            "has_next": page * page_size < total, "has_prev": page > 1,
            "total_pages": max(1, (total + page_size - 1) // page_size),
        }
    finally:
        conn.close()


def build_backup_overview():
    """备份概览：总览 / 说明 / 工单备份 / 档案备份 / 报表备份"""
    conn = backup_conn()
    sections = []
    ISSUE_ENTITY = "paas:d8c34f70-4f20-4c31-a1da-825d63582cd5"  # 问题工单
    try:
        one = lambda sql, a=(): conn.execute(sql, a).fetchone()[0]
        try:
            _sz = os.path.getsize(BACKUP_DB)
            _sz_s = f"{_sz/1048576:.1f} MB"
            _mt = datetime.fromtimestamp(os.path.getmtime(BACKUP_DB)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            _sz_s, _mt = "-", "-"

        # ── 1. 总览（原有内容，保留）──
        _ent_short = {"customer": "客户", "customer_linkman": "联系人",
                      "customer_address": "地址", "product": "设备"}
        _ec = {}
        for _r in conn.execute("select entity_type et, count(*) n from entities "
                               "where coalesce(deleted,0)=0 group by entity_type"):
            _et = _r["et"]
            _lab = _ent_short.get(_et) or ("PAAS" if str(_et).startswith("paas:") else _et)
            _ec[_lab] = _ec.get(_lab, 0) + _r["n"]
        _ent_line = " / ".join(f"{k} {v}" for k, v in _ec.items()) or "-"
        _alive = one("select count(*) from tasks where coalesce(deleted,0)=0")
        _del = one("select count(*) from tasks where coalesce(deleted,0)=1")
        _en = one("select count(*) from task_types where coalesce(enabled,0)=1")
        _dis = one("select count(*) from task_types where coalesce(enabled,0)=0")
        _runs = one("select count(*) from sync_runs")
        _last = one("select coalesce(max(finished_at),'-') from sync_runs")
        sections.append({
            "title": "总览",
            "columns": ["项目", "值"],
            "rows": [
                ["工单：未删除 / 已消失", f"{_alive} / {_del}"],
                ["工单类型：启用 / 未启用", f"{_en} / {_dis}"],
                ["工单字段值条数（task_attributes）", one("select count(*) from task_attributes")],
                ["工单原始 JSON 留存（task_raw）", one("select count(*) from task_raw")],
                ["实体备份合计（未删除）", one("select count(*) from entities where coalesce(deleted,0)=0")],
                ["实体分类型", _ent_line],
                ["结果表：行程单 / 项目进度 / 用户",
                 f"{one('select count(*) from trip_records')} / "
                 f"{one('select count(*) from project_progress')} / "
                 f"{one('select count(*) from users')}"],
                ["字典：工单字段 / 实体字段",
                 f"{one('select count(*) from task_fields')} / "
                 f"{one('select count(*) from entity_fields')}"],
                ["同步批次：次数 / 最后完成", f"{_runs} / {_last}"],
                ["库文件", BACKUP_DB],
                ["库文件大小 / 最后修改", f"{_sz_s} / {_mt}"],
            ],
        })

        # ── 2. 说明（备份对象 + 频率）──
        sections.append({
            "title": "说明",
            "columns": ["项目", "内容", "备份频率"],
            "rows": [
                ["工单备份",
                 "工程工单、预调试工单、调试工单、验收工单、保运工单、售后工单、日常工单、改造工单",
                 "周增量 + 季度全量(自动)"],
                ["档案备份",
                 "客户档案、客户联系人、客户地址、设备信息（产品）、站点信息、问题工单及其他 PAAS 表单",
                 "周增量 + 季度全量(自动)"],
                ["报表备份",
                 "行程单、项目进度",
                 "手动触发"],
                ["同步中心",
                 "系统 → 同步中心，可随时手动抓取并备份。"
                 "增量：拉全量列表、仅对变更过的数据取详情（快）；"
                 "全量：强制重拉全部详情（慢，用于防漏）",
                 "手动触发"],
                ["数据字典",
                 "系统 → 同步中心 → 字段字典：刷新售后宝全部表单的字段定义"
                 "（工单字段 + 客户/联系人/地址/设备/PAAS 表单字段），"
                 "供「数据字典」页查中英文对照",
                 "工单备份、档案备份启动时都会按时间检查：未满 7 天跳过（不比对差异）；满 7 天由先执行的备份任务自动全量重拉"],
            ],
        })

        # ── 最后备份时间（按同步任务分类，取最近一条成功记录）──
        def _last_ok(cond):
            r = conn.execute(
                "select finished_at from sync_runs where status='ok' and " + cond +
                " order by run_id desc limit 1").fetchone()
            return (r["finished_at"] or "-") if r else "-"

        t_last = _last_ok("mode not like 'extra:%' and mode not like 'trip:%' "
                          "and mode not in ('progress','users','dict')")
        e_last = _last_ok("mode like 'extra:%'")
        paas_last = _last_ok("mode like 'extra:%paas%'")
        trip_last = _last_ok("mode like 'trip:%'")
        prog_last = _last_ok("mode = 'progress'")
        FREQ_AUTO = "周增量 + 季度全量(自动)"

        # ── 3. 工单备份（类型顺序＝售后宝显示顺序；问题工单置底）──
        TYPE_ORDER = ["工程工单", "预调试工单", "调试工单", "验收工单",
                      "保运工单", "售后工单", "日常工单", "改造工单"]
        task_types = conn.execute(
            "select t.id, t.name, "
            "(select count(*) from tasks k where k.template_id=t.id and coalesce(k.deleted,0)=0) n "
            "from task_types t where coalesce(t.enabled,0)=1"
        ).fetchall()
        _ord = {n: i for i, n in enumerate(TYPE_ORDER)}
        task_types = sorted(task_types,
                            key=lambda t: (_ord.get(t["name"], len(TYPE_ORDER)), t["name"] or ""))
        tk_rows = [[t["name"] or t["id"], t["n"], FREQ_AUTO, t_last] for t in task_types]
        _ie = conn.execute("select count(*) n from entities "
                           "where entity_type=? and coalesce(deleted,0)=0", (ISSUE_ENTITY,)).fetchone()
        tk_rows.append(["问题工单", (_ie["n"] if _ie else 0), FREQ_AUTO, paas_last])
        sections.append({
            "title": "工单备份",
            "columns": ["工单类型", "工单数", "备份频率", "最后备份时间"],
            "rows": tk_rows,
        })

        # ── 4. 档案备份（不含问题工单）──
        nm = {r["entity_type"]: r["v"] for r in conn.execute(
            "select entity_type, max(case when field_name='templateBizName' then value_text end) v "
            "from entity_attrs where entity_type like 'paas:%' group by entity_type").fetchall()}
        ent_rows = []
        for r in conn.execute(
                "select entity_type, count(*) n from entities "
                "where coalesce(deleted,0)=0 and entity_type<>? "
                "group by entity_type order by n desc", (ISSUE_ENTITY,)).fetchall():
            et = r["entity_type"]
            ent_rows.append([ENTITY_TITLES.get(et) or nm.get(et) or et, r["n"], FREQ_AUTO, e_last])
        sections.append({
            "title": "档案备份",
            "columns": ["实体", "条数", "备份频率", "最后备份时间"],
            "rows": ent_rows,
        })

        # ── 5. 报表备份（行程单 / 项目进度）──
        sections.append({
            "title": "报表备份",
            "columns": ["名称", "条数", "备份频率", "最后备份时间"],
            "rows": [
                ["行程单", one("select count(*) from trip_records"), "手动触发", trip_last],
                ["项目进度", one("select count(*) from project_progress"), "手动触发", prog_last],
            ],
        })

        return {"ok": True, "tab": OVERVIEW_TAB, "sections": sections}
    finally:
        conn.close()


app = Flask(__name__)
# 保持 JSON 键插入顺序（display_fields 顺序 = 前端表头顺序），否则 jsonify 默认按字母序重排
app.json.sort_keys = False

# ── 权限 ──
def load_permissions() -> dict:
    try:
        with open(PERMISSIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return {}


def _locked_write_json(filepath, data):
    """独占锁写入 JSON，防止并发覆盖（多管理员同时改权限）"""
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    os.replace(tmp, filepath)


def get_user_permissions():
    user = request.headers.get("X-Remote-User", "") or "admin"
    bk = backup_tab_titles()

    def dedup(seq):
        seen, out = set(), []
        for t in seq:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    if user == "admin":
        return user, dedup(ALL_TABS + list(RESULT_TITLES.keys()) + list(bk.keys())
                          + list(SYSTEM_TITLES.keys()) + list(ONLINE_TABS.keys()))
    perms = load_permissions().get(user, {})
    allowed = perms.get("tabs", []) if isinstance(perms, dict) else []
    tabs = []
    for t in allowed:
        if t == "*":
            tabs.extend(_all_tab_keys())
        elif t == "bk:*":
            tabs.extend(backup_tab_titles().keys())
        elif (t in TABS or t in bk or t in RESULT_TITLES or t in SYSTEM_TITLES
              or t in ONLINE_TABS):
            tabs.append(t)
    return user, dedup(tabs)


# ══ 账号管理（shb-web 登录用户 = nginx Basic Auth 账号 + Tab 权限）══
# 两处数据源必须同步维护：
#   /etc/nginx/.shb-passwd   → 能登录（用户名 + apr1 哈希）
#   permissions.json         → 登录后能看哪些 Tab
# admin 为内置管理员：固定拥有全部 Tab，不可删除。


def _all_tab_keys():
    """全部可分配的 Tab key（含 admin 专属项；用户要求不做排除）"""
    seen, out = set(), []
    for t in (list(TABS.keys()) + list(backup_tab_titles().keys())
              + list(RESULT_TITLES.keys()) + list(SYSTEM_TITLES.keys())
              + list(ONLINE_TABS.keys())):
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _tab_titles():
    """Tab key → 中文标题"""
    titles = dict(ONLINE_TITLES)
    titles.update(RESULT_TITLES)
    titles.update(SYSTEM_TITLES)
    titles.update(ONLINE_TABS)
    titles.update(backup_tab_titles())
    return titles


def _ua_audit(action, target, extra=None):
    """账号变更审计（写 shb-mcp/audit.log，与写操作同一条流水）"""
    entry = {"action": "users_account:" + action, "operator": request.headers.get(
        "X-Remote-User", "") or UA_ADMIN, "target": target,
        "status": "done", "ts": datetime.now().astimezone().isoformat()}
    if extra:
        entry.update(extra)
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error("UA_AUDIT_FAIL %s", e)


def _read_htpasswd():
    """读取 nginx 密码文件 → {user: hash}（忽略空行/注释）"""
    out = {}
    try:
        with open(HTPASSWD_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or ":" not in line:
                    continue
                name, _, h = line.partition(":")
                if name:
                    out[name] = h
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.error("HTPASSWD_READ_FAIL %s", e)
    return out


def _write_htpasswd(accounts):
    """原子写 nginx 密码文件：临时文件 + rename，保持 640 root:www-data，写前留备份"""
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    if os.path.exists(HTPASSWD_FILE):
        try:
            shutil.copy2(HTPASSWD_FILE, HTPASSWD_FILE + ".bak-" + ts)
        except Exception as e:
            logger.error("HTPASSWD_BACKUP_FAIL %s", e)
    tmp = HTPASSWD_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for name in sorted(accounts):
            f.write(f"{name}:{accounts[name]}\n")
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, 0o640)
    try:
        shutil.chown(tmp, "root", "www-data")
    except Exception as e:
        logger.error("HTPASSWD_CHOWN_FAIL %s", e)
    os.replace(tmp, HTPASSWD_FILE)


def _hash_password(password):
    """生成 apr1 哈希（与现有文件同格式），走 stdin 避免密码出现在进程参数里"""
    p = subprocess.run(["openssl", "passwd", "-apr1", "-stdin"], input=password,
                       capture_output=True, text=True, timeout=10)
    h = (p.stdout or "").strip()
    if p.returncode != 0 or not h.startswith("$apr1$"):
        raise RuntimeError((p.stderr or "openssl 生成哈希失败").strip()[:200])
    return h


def _ua_admin_only():
    return (request.headers.get("X-Remote-User", "") or UA_ADMIN) == UA_ADMIN


def _ua_validate_username(name):
    if not name:
        return "用户名不能为空"
    if not UA_USERNAME_RE.match(name):
        return "用户名只允许字母/数字/下划线/点/连字符，长度 1-32"
    return ""


def _ua_validate_password(pw, required=False):
    if not pw:
        return "密码不能为空" if required else ""
    if len(pw) < UA_PASSWORD_MIN:
        return f"密码至少 {UA_PASSWORD_MIN} 位"
    if any(c in pw for c in "\r\n:"):
        return "密码不能包含换行或冒号"
    return ""


@app.route("/api/users-account", methods=["GET"])
def api_users_account():
    """列出全部用户：登录账号（htpasswd）+ Tab 权限（permissions.json）"""
    if not _ua_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    perms = load_permissions()
    accounts = _read_htpasswd()
    titles = _tab_titles()
    rows = []
    for name in sorted(set(perms) | set(accounts), key=lambda x: (x != UA_ADMIN, x)):
        p = perms.get(name)
        tabs = p.get("tabs", []) if isinstance(p, dict) else (p if isinstance(p, list) else [])
        is_admin = (name == UA_ADMIN)
        if is_admin:
            tabs = ["*"]
        rows.append({
            "user": name,
            "tabs": list(tabs),
            "is_admin": is_admin,
            "has_password": name in accounts,
        })
    return jsonify({"ok": True, "rows": rows,
                    "tabs": [{"key": k, "title": titles.get(k, k)} for k in _all_tab_keys()]})


@app.route("/api/users-account", methods=["POST"])
def api_users_account_save():
    """新增用户 / 修改 Tab 权限 / 重置密码。密码留空表示不改。"""
    if not _ua_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    body = request.get_json(silent=True) or {}
    name = (body.get("user") or "").strip()
    tabs = body.get("tabs")
    password = body.get("password") or ""
    if isinstance(tabs, str):
        tabs = [t for t in (x.strip() for x in tabs.split(",")) if t]

    err = _ua_validate_username(name)
    if err:
        return jsonify({"ok": False, "error": err})

    perms = load_permissions()
    accounts = _read_htpasswd()
    exists = (name in perms) or (name in accounts)
    is_admin = (name == UA_ADMIN)

    # 密码：新用户必填；admin 已有密码时留空=不改
    need_pw = (not exists) or (not is_admin and name not in accounts)
    err = _ua_validate_password(password, required=need_pw)
    if err:
        return jsonify({"ok": False, "error": err})

    # Tab 权限：admin 固定全量；其他用户必须是已知 key（未知一律拒绝，不静默丢弃）
    if is_admin:
        tabs = ["*"]
    else:
        if tabs is None:
            tabs = []
        if not isinstance(tabs, list):
            return jsonify({"ok": False, "error": "tabs 必须是数组"})
        valid = set(_all_tab_keys())
        unknown = [t for t in tabs if t not in valid]
        if unknown:
            return jsonify({"ok": False, "error": f"未知 Tab: {', '.join(unknown)}"})
        tabs = [t for t in tabs if t != "*"]        # 非 admin 不允许通配全部

    # 1) 写密码文件
    if password:
        try:
            accounts[name] = _hash_password(password)
            _write_htpasswd(accounts)
        except Exception as e:
            return jsonify({"ok": False, "error": f"写入登录密码失败: {e}"})

    # 2) 写权限文件（admin 恒为 *，不做实体列表，避免误导）
    perms[name] = {"tabs": tabs}
    perms[UA_ADMIN] = {"tabs": ["*"]}
    try:
        _locked_write_json(PERMISSIONS_FILE, perms)
    except Exception as e:
        return jsonify({"ok": False, "error": f"写入 Tab 权限失败: {e}"})

    # 3) 回读校验（密码只看存在性，权限逐项比对）
    back_acc = _read_htpasswd()
    back_perm = load_permissions().get(name, {})
    if name not in back_acc:
        return jsonify({"ok": False, "error": "校验失败：登录账号未落盘"})
    if back_perm.get("tabs") != tabs:
        return jsonify({"ok": False, "error": "校验失败：Tab 权限未落盘"})

    _ua_audit("save", name, {"tabs": tabs, "password_changed": bool(password),
                             "created": not exists})
    logger.info("UA_SAVE user=%s created=%s pw=%s tabs=%d",
                name, not exists, bool(password), len(tabs))
    return jsonify({"ok": True, "user": name, "tabs": tabs,
                    "created": not exists, "password_changed": bool(password)})


@app.route("/api/users-account/<path:name>", methods=["DELETE"])
def api_users_account_delete(name):
    """删除用户：同时移除登录账号与权限"""
    if not _ua_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    name = (name or "").strip()
    if name == UA_ADMIN:
        return jsonify({"ok": False, "error": "admin 是内置管理员，不可删除"})
    perms = load_permissions()
    accounts = _read_htpasswd()
    if name not in perms and name not in accounts:
        return jsonify({"ok": False, "error": f"用户不存在: {name}"})

    if name in accounts:
        accounts.pop(name, None)
        try:
            _write_htpasswd(accounts)
        except Exception as e:
            return jsonify({"ok": False, "error": f"移除登录账号失败: {e}"})
    if name in perms:
        perms.pop(name, None)
        try:
            _locked_write_json(PERMISSIONS_FILE, perms)
        except Exception as e:
            return jsonify({"ok": False, "error": f"移除 Tab 权限失败: {e}"})

    if name in _read_htpasswd() or name in load_permissions():
        return jsonify({"ok": False, "error": "校验失败：用户仍存在"})

    _ua_audit("delete", name)
    logger.info("UA_DELETE user=%s", name)
    return jsonify({"ok": True})


# ── TTL 缓存 ──
_cache: dict = {}


def _cache_key(prefix, **kwargs):
    parts = [prefix]
    for k in sorted(kwargs):
        parts.append(f"{k}={kwargs[k]}")
    parts.append(f"_u={request.headers.get('X-Remote-User', '') or 'admin'}")
    return "|".join(parts)


def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() < entry["expires_at"]:
        return entry["result"]
    if entry:
        del _cache[key]
    return None


def _cache_set(key, value, ttl=CACHE_TTL):
    _cache[key] = {"result": value, "expires_at": time.time() + ttl}


# ── bridge 调用 ──
def call_bridge(*args, timeout=60):
    import subprocess
    try:
        r = subprocess.run(
            [sys.executable, BRIDGE] + list(args),
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            return {"error": (r.stderr or r.stdout or "bridge error")[:300]}
        out = r.stdout.strip()
        if out.startswith("{"):
            return json.loads(out)
        return {"output": out}
    except subprocess.TimeoutExpired:
        return {"error": "查询超时"}
    except Exception as e:
        return {"error": str(e)[:300]}


def fmt(val):
    if not val:
        return ""
    if isinstance(val, dict):
        return val.get("name", "") or val.get("label", "") or str(val)[:50]
    if isinstance(val, list) and val and isinstance(val[0], dict):
        return val[0].get("name", "") or val[0].get("label", "") or ""
    return str(val)[:50]


# ── 客户信息补充（项目编码/站点编码来自客户管理） ──
CUSTOMER_SITE_CODE_FIELD = "field_57HbU2ZNIfWB0aRB"  # 客户管理中的站点编码字段


def fetch_customer_info(customer_name: str) -> dict:
    """按客户名称查客户管理，返回 {serial: 客户编号, sitecode: 站点编码}。
    结果与查询用户无关，缓存 key 不带用户维度。"""
    if not customer_name:
        return {}
    key = "cust|" + customer_name
    cached = _cache_get(key)
    if cached is not None:
        return cached
    info = {}
    result = call_bridge("customer", "list", "--keyword", customer_name, "-o", "raw")
    if "error" not in result:
        data = result.get("output") if "output" in result else result
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                data = None
        if isinstance(data, dict):
            for cust in data.get("list", []) or []:
                if cust.get("name") == customer_name:
                    attr = cust.get("attribute", {}) or {}
                    info = {
                        "serial": cust.get("serialNumber", "") or "",
                        "sitecode": attr.get(CUSTOMER_SITE_CODE_FIELD, "") or "",
                    }
                    break
    _cache_set(key, info)
    return info


def build_rows(tab_cfg, rows_raw):
    """按 display_fields 构造展示行；__customer_* 特殊 key 走客户二次查询填充。
    客户查询并发执行（每页内去重后最多 4 并发），结果带 TTL 缓存。"""
    display_fields = tab_cfg["display_fields"]
    cust_name_field = tab_cfg.get("customer_name_field", "")
    names = []
    if cust_name_field:
        seen = set()
        for r in rows_raw:
            n = fmt(r.get(cust_name_field, ""))
            if n and n not in seen:
                seen.add(n)
                names.append(n)
    infos = {}
    if names:
        with ThreadPoolExecutor(max_workers=4) as ex:
            for n, info in zip(names, ex.map(fetch_customer_info, names)):
                infos[n] = info
    rows = []
    for row in rows_raw:
        out = {dn: "" for _, dn in display_fields}
        for fn, dn in display_fields:
            if not fn.startswith("__"):
                out[dn] = fmt(row.get(fn, ""))
        if cust_name_field:
            cname = fmt(row.get(cust_name_field, ""))
            info = infos.get(cname, {})
            for fn, dn in display_fields:
                if fn == "__customer_serial":
                    out[dn] = info.get("serial", "")
                elif fn == "__customer_sitecode":
                    out[dn] = info.get("sitecode", "")
        rows.append(out)
    return rows


# ── 构造 paas form search payload ──
def build_search_payload(tab_cfg, keyword, date_begin, date_end, select_val, page, page_size):
    payload = {
        "templateBizId": tab_cfg["template_biz_id"],
        "pageNum": page,
        "pageSize": page_size,
    }
    if keyword:
        payload["keyword"] = keyword
        payload["searchCondition"] = tab_cfg["search_fields"]
    conditions = []
    if select_val:
        conditions.append({
            "property": tab_cfg["select_field"],
            "operator": "eq",
            "value": select_val,
        })
    if conditions:
        payload["conditions"] = conditions
    if date_begin and date_end:
        # 创建时间范围（epoch 毫秒）
        try:
            b1 = int(datetime.strptime(date_begin, "%Y-%m-%d").timestamp() * 1000)
            b2 = int(datetime.strptime(date_end, "%Y-%m-%d").timestamp() * 1000) + 86399999
        except ValueError:
            b1 = b2 = None
        if b1 and b2:
            payload["systemConditions"] = [{
                "property": "createTime",
                "operator": "between",
                "betweenValue1": b1,
                "betweenValue2": b2,
            }]
    return payload


# ── 页面 ──
@app.route("/")
def index():
    user, tabs = get_user_permissions()
    titles = dict(ONLINE_TITLES)
    titles.update(RESULT_TITLES)
    titles.update(SYSTEM_TITLES)
    titles.update(ONLINE_TABS)
    titles.update(backup_tab_titles())
    return render_template("index.html", current_user=user, tabs=tabs,
                           all_tabs=ALL_TABS, tab_titles=titles)


# ── 查询 API ──
@app.route("/api/search")
def api_search():
    tab = request.args.get("tab", "station")
    keyword = (request.args.get("keyword") or "").strip()
    date_begin = (request.args.get("date_begin") or "").strip()
    date_end = (request.args.get("date_end") or "").strip()
    select_val = (request.args.get("select") or "").strip()
    page = max(1, int(request.args.get("page", "1") or "1"))
    page_size = min(MAX_PAGE_SIZE, max(1, int(request.args.get("page_size", DEFAULT_PAGE_SIZE))))

    user, user_tabs = get_user_permissions()
    if tab not in user_tabs:
        return make_response(jsonify({"ok": False, "error": "无权限访问此查询"}), 403)

    # ── 在线报表（ol:*）必须走 /api/online-query（后台任务 + 轮询） ──
    if tab in ONLINE_TABS:
        return jsonify({"ok": False,
                        "error": "在线报表请用「查询」按钮（POST /api/online-query）"})

    # ── 本地备份库分支（只读 SQLite） ──
    if is_backup_tab(tab):
        start_bk = time.time()
        try:
            if tab == OVERVIEW_TAB:
                _ov_key = _cache_key("overview")
                _ov_hit = _cache_get(_ov_key)
                if _ov_hit is not None:
                    resp = _ov_hit
                else:
                    resp = build_backup_overview()
                    _cache_set(_ov_key, resp)
            elif tab == STATION_TAB:
                resp = query_entity(STATION_ENTITY, keyword, date_begin, date_end, page, page_size)
            elif tab.startswith("bkent:"):
                resp = query_entity(tab[len("bkent:"):], keyword, date_begin, date_end, page, page_size)
            else:
                resp = query_backup(tab, keyword, date_begin, date_end, page, page_size)
        except Exception as e:
            logger.error("BACKUP_QUERY_FAIL user=%s tab=%s err=%s", user, tab, e)
            return jsonify({"ok": False, "error": f"备份库查询失败: {e}"})
        n = len(resp.get("rows", [])) if "rows" in resp else f"sections={len(resp.get('sections', []))}"
        logger.info("QUERY user=%s tab=%s kw=%s rows=%s %.0fms", user, tab, keyword, n,
                    (time.time() - start_bk) * 1000)
        return jsonify(resp)

    # ── 本地业务结果分支（行程单 / 项目进度，只读本地结果表） ──
    if is_result_tab(tab):
        start_r = time.time()
        try:
            if tab == "trip":
                resp = query_trip(keyword, date_begin, date_end, page, page_size)
            elif tab == "users":
                resp = query_users(keyword, (request.args.get("kw2") or "").strip(), page, page_size)
            elif tab == "workload":
                resp = query_workload(date_begin, date_end, keyword, page, page_size)
            elif tab == "records":
                resp = query_records((request.args.get("kw2") or "").strip(), keyword,
                                     (request.args.get("kw3") or "").strip(), page, page_size)
            elif tab == "progress2":
                resp = query_progress2(keyword, (request.args.get("kw2") or "").strip(),
                                       page, page_size,
                                       show_done=(request.args.get("show_done") == "1"),
                                       date_begin=(request.args.get("date_begin") or "").strip(),
                                       date_end=(request.args.get("date_end") or "").strip())
            elif tab == "weekly-plan":
                resp = query_weekly_plan((request.args.get("week_start") or "").strip(),
                                         (request.args.get("mode") or "plan").strip(),
                                         (request.args.get("keyword") or "").strip())
            elif tab == "debug-progress":
                resp = query_debug_progress((request.args.get("keyword") or "").strip(),
                                            (request.args.get("date_begin") or "").strip(),
                                            (request.args.get("date_end") or "").strip(),
                                            page, page_size)
            else:
                resp = query_progress(keyword, (request.args.get("kw2") or "").strip(),
                                      (request.args.get("kw3") or "").strip(), page, page_size)
        except Exception as e:
            logger.error("RESULT_QUERY_FAIL user=%s tab=%s err=%s", user, tab, e)
            return jsonify({"ok": False, "error": f"本地结果查询失败: {e}"})
        logger.info("QUERY user=%s tab=%s kw=%s rows=%d total=%s %.0fms", user, tab, keyword,
                    len(resp.get("rows", [])), resp.get("total"), (time.time() - start_r) * 1000)
        return jsonify(resp)

    # ── 系统 Tab（字段对照表 / 同步中心 / 同步日志） ──
    if tab in SYSTEM_TITLES:
        if tab == "sync":
            return jsonify({"ok": False, "error": "同步中心请用 /api/sync/status"})
        if tab == "synclog":
            return jsonify({"ok": False, "error": "同步日志请用 /api/synclog"})
        if tab == "identity":
            return jsonify({"ok": False, "error": "身份管理请用 /api/identity/list"})
        return jsonify(query_fieldmap(keyword, page, page_size))

    if tab not in TABS:
        return jsonify({"ok": False, "error": f"未知查询: {tab}"})
    tab_cfg = TABS[tab]

    start = time.time()
    cache_key = _cache_key("search", tab=tab, kw=keyword, b=date_begin, e=date_end, s=select_val,
                           p=page, ps=page_size)
    cached = _cache_get(cache_key)
    if cached is not None:
        logger.info("QUERY user=%s tab=%s kw=%s date=%s~%s select=%s page=%s CACHE_HIT %.0fms",
                    user, tab, keyword, date_begin, date_end, select_val, page, (time.time()-start)*1000)
        return jsonify(cached)

    payload = build_search_payload(tab_cfg, keyword, date_begin, date_end, select_val, page, page_size)
    result = call_bridge("paas", "form", "search",
                         "--data", json.dumps(payload, ensure_ascii=False),
                         "--fields", ",".join(tab_cfg["fields"]), "-o", "raw")
    if "error" in result:
        logger.error("QUERY_FAIL user=%s tab=%s err=%s", user, tab, result["error"])
        return jsonify({"ok": False, "error": result["error"]})

    data = result.get("output") if "output" in result else result
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return jsonify({"ok": False, "error": "返回数据解析失败"})

    rows_raw = data.get("rows", []) if isinstance(data, dict) else []
    total = 0
    if isinstance(data, dict):
        pg = data.get("page", {})
        total = pg.get("totalElements", len(rows_raw))

    display_fields = tab_cfg["display_fields"]
    rows = build_rows(tab_cfg, rows_raw)
    total_pages = max(1, (total + page_size - 1) // page_size) if page_size else 1

    resp = {
        "ok": True,
        "tab": tab,
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": (page * page_size) < total,
        "has_prev": page > 1,
        "total_pages": total_pages,
    }
    _cache_set(cache_key, resp)
    logger.info("QUERY user=%s tab=%s kw=%s date=%s~%s select=%s page=%s rows=%d total=%d %.0fms",
                user, tab, keyword, date_begin, date_end, select_val, page, len(rows), total,
                (time.time()-start)*1000)
    return jsonify(resp)


# ── 导出 CSV（有该 Tab 权限即可导出） ──
@app.route("/api/export")
def api_export():
    user = request.headers.get("X-Remote-User", "") or "admin"
    _, user_tabs = get_user_permissions()
    tab = request.args.get("tab", "station")
    if tab not in user_tabs:
        return make_response(jsonify({"ok": False, "error": "无权限导出该 Tab"}), 403)

    if tab == "records":
        return jsonify({"ok": False, "error": "该报表不支持导出"})
    keyword = (request.args.get("keyword") or "").strip()
    date_begin = (request.args.get("date_begin") or "").strip()
    date_end = (request.args.get("date_end") or "").strip()
    select_val = (request.args.get("select") or "").strip()
    # ── 本地备份库导出（仅 admin） ──
    if is_backup_tab(tab):
        if tab == OVERVIEW_TAB:
            return jsonify({"ok": False, "error": "概览页不支持导出"})
        out_rows, page = [], 1
        etype = STATION_ENTITY if tab == STATION_TAB else (
            tab[len("bkent:"):] if tab.startswith("bkent:") else None)
        while True:
            if etype:
                r = query_entity(etype, keyword, date_begin, date_end, page, MAX_PAGE_SIZE)
            else:
                r = query_backup(tab, keyword, date_begin, date_end, page, MAX_PAGE_SIZE)
            if not r.get("ok"):
                return jsonify({"ok": False, "error": r.get("error", "导出失败")})
            out_rows.extend(r["rows"])
            if page >= r["total_pages"]:
                break
            page += 1
        out = io.StringIO()
        if out_rows:
            w = csv.DictWriter(out, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)
        resp = make_response(out.getvalue())
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = (
            f"attachment; filename=backup_{tab[3:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
        logger.info("EXPORT user=%s tab=%s rows=%d (backup)", user, tab, len(out_rows))
        return resp

    # ── 系统 Tab（字段对照表）CSV 导出 ──
    if tab in SYSTEM_TITLES:
        if tab in ("sync", "synclog"):
            return jsonify({"ok": False, "error": "该页不支持导出"})
        r = query_fieldmap(keyword, 1, 10 ** 9)
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=r["columns"])
        w.writeheader()
        w.writerows(r["rows"])
        resp = make_response(out.getvalue())
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = (
            f"attachment; filename=fieldmap_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
        logger.info("EXPORT user=%s tab=%s rows=%d (fieldmap)", user, tab, len(r["rows"]))
        return resp

    if tab not in TABS:
        return jsonify({"ok": False, "error": f"未知查询: {tab}"})
    tab_cfg = TABS[tab]

    all_rows = []
    page = 1
    while True:
        payload = build_search_payload(tab_cfg, keyword, date_begin, date_end, select_val, page, MAX_PAGE_SIZE)
        result = call_bridge("paas", "form", "search",
                             "--data", json.dumps(payload, ensure_ascii=False),
                             "--fields", ",".join(tab_cfg["fields"]), "-o", "raw")
        if "error" in result:
            return jsonify({"ok": False, "error": result["error"]})
        data = result.get("output") if "output" in result else result
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                break
        rows_raw = data.get("rows", []) if isinstance(data, dict) else []
        if not rows_raw:
            break
        all_rows.extend(build_rows(tab_cfg, rows_raw))
        total = 0
        if isinstance(data, dict):
            total = data.get("page", {}).get("totalElements", 0)
        if len(all_rows) >= total:
            break
        page += 1

    out = io.StringIO()
    if all_rows:
        w = csv.DictWriter(out, fieldnames=list(all_rows[0].keys()))
        w.writeheader()
        w.writerows(all_rows)
    resp = make_response(out.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f"attachment; filename={tab}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    logger.info("EXPORT user=%s tab=%s rows=%d", user, tab, len(all_rows))
    return resp


# ── 本地业务结果：Excel 导出改在前端生成（SheetJS，与 u8-web 同方案，服务端零依赖） ──


# ══ 在线报表查询（2026-11-10 新增）═════════════════════════════════════
# 后台任务 + /api/job 轮询（进度：已取 N/M 单）；结果内存缓存 CACHE_TTL，失败不缓存。
# 铁律：只走 online.py（云端），绝不读写本地备份库。


def _online_week_monday(s):
    """周计划：同周内任一天都归一为周一日期（缓存键与取数参数都用它）"""
    try:
        d = datetime.strptime((s or "").strip(), "%Y-%m-%d")
    except Exception:
        d = datetime.now()
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


# 关键字筛在结果行上的字段（这些报表的行粒度=人/客户，可以在缓存的全员结果上过）
_ONLINE_KEYWORD_COL = {"ol:weekly-plan": "负责人", "ol:trip": "姓名"}


def _online_filter(tab, resp, keyword, types=""):
    """在全员结果上按关键字 / 错误类型过滤行（只对能行过滤的报表生效）

    2026-11-11 hunter 定：稽核检查的「错误类型」（types）**不参与取数、不进缓存键**，
    改在这里做行过滤——切换错误类型直接命中缓存，不再重查云端。
    """
    if not resp:
        return resp
    if not keyword and not (tab == "ol:audit-check" and types):
        return resp
    col = _ONLINE_KEYWORD_COL.get(tab)
    if col:
        rows = [r for r in (resp.get("rows") or []) if keyword in str(r.get(col) or "")]
    elif tab == "ol:progress2":
        rows = [r for r in (resp.get("rows") or [])
                if keyword in str(r.get("客户编号") or "")
                or keyword in str(r.get("客户名称") or "")]
    elif tab == "ol:workload":
        # 关键字可匹配客户编号 / 客户 / 服务人员 / 工单类型（2026-11-11）
        rows = [r for r in (resp.get("rows") or [])
                if keyword in str(r.get("客户编号") or "")
                or keyword in str(r.get("客户") or "")
                or keyword in str(r.get("服务人员") or "")
                or keyword in str(r.get("工单类型") or "")]
    elif tab == "ol:audit-check":
        rows = resp.get("rows") or []
        if types:
            _ts = [x.strip() for x in types.split(",") if x.strip()]
            rows = [r for r in rows if r.get("错误类型") in _ts]
        if keyword:
            rows = [r for r in rows if keyword in str(r.get("涉及人员") or "")]
    elif tab == "ol:debug-progress":
        keys = ("项目编码", "客户名称", "调试人员", "调试内容")
        rows = [r for r in (resp.get("rows") or [])
                if any(keyword in str(r.get(k) or "") for k in keys)]
    else:
        return resp
    out = dict(resp)
    out["rows"] = rows
    out["total"] = len(rows)
    # 2026-11-12 hunter 定：行程单（ol:trip）的人名标签栏始终显示全员。
    # 原来这里把 persons 也按 keyword 过滤，导致点某个姓名后其他名字从标签栏消失；
    # 结果行仍按 keyword 过滤（上面 rows），只是标签栏不收窄。
    if resp.get("persons") and tab != "ol:trip":
        out["persons"] = [p for p in resp["persons"] if keyword in p]
    return out


def _online_worker(job_id, tab, params, cache_key, keyword="", types=""):
    j = RESULT_JOBS.get(job_id)
    if not j:
        return
    j["status"] = "running"
    j["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        def _prog(msg):
            j["progress"] = msg
        resp = shb_online.query(tab, params, progress=_prog)
        if cache_key:
            _cache_set(cache_key, resp, ONLINE_CACHE_TTL)   # 缓存全量（不带姓名过滤）
        j["result"] = _online_filter(tab, resp, keyword, types)
        j["status"] = "ok"
        j["progress"] = "完成（%s 秒，%d 行）" % (resp.get("elapsed", "?"), len(resp.get("rows") or []))
    except Exception as e:
        j["status"] = "failed"
        j["error"] = str(e)[:500]
        logger.error("ONLINE_FAIL job=%s tab=%s err=%s", job_id, tab, e)
    j["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("ONLINE_DONE job=%s tab=%s status=%s progress=%s",
                job_id, tab, j.get("status"), (j.get("progress") or "")[:80])


@app.route("/api/online-query", methods=["POST"])
def api_online_query():
    """在线报表查询：命中缓存直接返回结果，否则起后台任务，前端轮询 /api/job"""
    user, user_tabs = get_user_permissions()
    tab = (request.args.get("tab") or "").strip()
    if tab not in ONLINE_TABS:
        return jsonify({"ok": False, "error": "未知在线报表：%s" % tab})
    if tab not in user_tabs:
        return make_response(jsonify({"ok": False, "error": "无权限访问此报表"}), 403)
    params = request.get_json(silent=True) or {}
    if not isinstance(params, dict):
        params = {}
    keyword = (params.get("keyword") or "").strip()
    types = (params.get("types") or "").strip()   # 稽核检查：错误类型（只做结果行过滤）
    base = dict(params)
    base.pop("keyword", None)
    if tab in ("ol:progress2", "ol:debug-progress", "ol:maintain-progress", "ol:accept-progress"):
        # 项目进度 / 调试进度 / 保运进度 / 验收进度（2026-11-11）：关键字参与取数（决定是否按时间筛），
        # 因此 keyword 必须随 base 进缓存键，避免「按时间筛的结果」被关键字查询复用。
        base["keyword"] = keyword
    if tab == "ol:weekly-plan":
        # 只要落在同一周，选周内哪天都走同一份缓存（2026-11-10 hunter 定）
        base["week_start"] = _online_week_monday(base.get("week_start"))
    if tab == "ol:debug-progress":
        pass  # 关键字已进 base（2026-11-11 改：参与取数，不再只做结果行过滤）
    if tab == "ol:audit-check":
        # 2026-11-11 hunter 定：错误类型不参与取数、不进缓存键（后端本来就先取全量再按类型过滤），
        # 改由 _online_filter 做行过滤 → 切换错误类型直接命中缓存，秒出。
        base.pop("types", None)
    ck = None if tab in ONLINE_NO_CACHE else _cache_key(
        "online", tab=tab, p=json.dumps(base, sort_keys=True, ensure_ascii=False))
    hit = _cache_get(ck) if ck else None
    if hit is not None:
        logger.info("ONLINE_CACHE_HIT user=%s tab=%s kw=%s", user, tab, keyword)
        return jsonify({"ok": True, "cached": True, "job_id": "",
                        "result": _online_filter(tab, hit, keyword, types)})

    # 清理该类型已结束的任务，避免内存累积
    for sid, sj in list(RESULT_JOBS.items()):
        if sj.get("tab") == "online:" + tab and sj.get("status") in ("ok", "failed"):
            RESULT_JOBS.pop(sid, None)

    job_id = uuid.uuid4().hex[:12]
    RESULT_JOBS[job_id] = {"id": job_id, "tab": "online:" + tab, "key": tab,
                           "user": user, "status": "queued", "progress": "排队中",
                           "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    threading.Thread(target=_online_worker,
                     args=(job_id, tab, base, ck, keyword, types), daemon=True).start()
    logger.info("ONLINE_QUERY user=%s tab=%s job=%s params=%s",
                user, tab, job_id, json.dumps(params, ensure_ascii=False))
    return jsonify({"ok": True, "cached": False, "job_id": job_id, "tab": tab})


# ── 本地业务结果：保存（实时抓数入库，仅 admin；后台任务） ──
def _job_worker(job_id, argv):
    j = RESULT_JOBS.get(job_id)
    if not j:
        return
    j["status"] = "running"
    j["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=3600)
        j["code"] = p.returncode
        j["output"] = (p.stdout or "").strip()[-1500:]
        j["error"] = (p.stderr or "").strip()[-1500:]
        j["status"] = "ok" if p.returncode == 0 else "failed"
    except Exception as e:
        j["status"] = "failed"
        j["error"] = str(e)[:500]
    j["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("SAVE_DONE job=%s tab=%s status=%s out=%s err=%s", job_id, j.get("tab"), j["status"],
                (j.get("output") or "")[:200], (j.get("error") or "")[:200])


@app.route("/api/save", methods=["POST"])
def api_save():
    user = request.headers.get("X-Remote-User", "") or "admin"
    if user != "admin":
        return make_response(jsonify({"ok": False, "error": "仅管理员可保存"}), 403)
    tab = request.args.get("tab", "")
    if tab not in RESULT_SCRIPTS:
        return jsonify({"ok": False, "error": f"不支持的保存类型: {tab}"})
    if tab == "trip":
        month = (request.args.get("month") or datetime.now().strftime("%Y-%m")).strip()
        argv = [RESULT_SCRIPT_PY, RESULT_SCRIPTS["trip"], "--month", month, "--all-users",
                "--save-db", "--src", "web-save", "--out", "/tmp"]
    elif tab == "users":
        month = ""
        argv = [RESULT_SCRIPT_PY, RESULT_SCRIPTS["users"]]
    elif tab == "workload":
        month = (request.args.get("month") or datetime.now().strftime("%Y-%m")).strip()
        argv = [RESULT_SCRIPT_PY, RESULT_SCRIPTS["workload"], "--month", month]
    else:
        month = ""
        argv = [RESULT_SCRIPT_PY, RESULT_SCRIPTS["progress"]]
    for sid, sj in list(RESULT_JOBS.items()):
        if sj.get("status") in ("ok", "failed"):
            RESULT_JOBS.pop(sid, None)
    job_id = uuid.uuid4().hex[:12]
    RESULT_JOBS[job_id] = {"id": job_id, "tab": tab, "month": month, "user": user, "status": "queued",
                           "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    threading.Thread(target=_job_worker, args=(job_id, argv), daemon=True).start()
    logger.info("SAVE user=%s tab=%s month=%s job=%s", user, tab, month, job_id)
    return jsonify({"ok": True, "job_id": job_id, "tab": tab, "month": month})


@app.route("/api/job")
def api_job():
    user = request.headers.get("X-Remote-User", "") or "admin"
    j = RESULT_JOBS.get(request.args.get("id", ""))
    if not j:
        return jsonify({"ok": False, "error": "任务不存在或已清理"})
    if user != "admin" and j.get("user") != user:
        return make_response(jsonify({"ok": False, "error": "无权限"}), 403)
    return jsonify({"ok": True, **j})


# ══ 同步中心（系统 Tab，仅 admin）2026-10-09 ═════════════════════════════
# 把「从云端拉数→写本地库」的动作统一成白名单任务，手动触发，复用异步 job 机制。
SYNC_SCRIPT_PY = "/usr/bin/python3"
SYNC_TASKS = {
    "backup-task": {"title": "工单备份", "desc": "工单列表+详情（增量 / 全量）",
                    "script": os.path.join(BASE_DIR, "..", "shb-tasks", "sync.py"),
                    "writes": "tasks / task_attributes / task_raw", "param": "mode"},
    "backup-extra": {"title": "实体备份", "desc": "客户/联系人/地址/设备/PAAS",
                     "script": os.path.join(BASE_DIR, "..", "shb-tasks", "sync_extra.py"),
                     "writes": "entities / entity_attrs", "param": "mode"},
    "trip": {"title": "行程单入库", "desc": "按月并入 trip_records",
             "script": os.path.join(BASE_DIR, "..", "skills", "shb-trip-export", "scripts", "export_trip.py"),
             "writes": "trip_records", "param": "month"},
    "progress": {"title": "项目进度入库", "desc": "全量并入 project_progress（一客户一行 × 14 列）",
                 "script": os.path.join(BASE_DIR, "..", "skills", "shb-project-progress-query", "scripts", "save_project_progress.py"),
                 "writes": "project_progress", "param": ""},
    "users": {"title": "用户入库", "desc": "拉人员名单并入 users",
              "script": os.path.join(BASE_DIR, "scripts", "sync_users.py"),
              "writes": "users", "param": ""},
    "dict": {"title": "字段字典", "desc": "刷新 schema.json + 定义表",
             "script": os.path.join(BASE_DIR, "..", "shb-mcp", "shb-schema-sync.py"),
             "writes": "task_fields / entity_fields", "param": ""},
    "records": {"title": "工单动态入库", "desc": "每单拉进度记录（出发/到达/指派…）并入 task_records",
                "script": os.path.join(BASE_DIR, "..", "shb-tasks", "sync_records.py"),
                "writes": "task_records", "param": "month"},
}


def _sync_argv(key, mode, month):
    """按任务+参数拼 argv（服务端固定，不接受任意命令）"""
    script = SYNC_TASKS[key]["script"]
    if key == "backup-task":
        a = [script, "--full"] + (["--force-detail"] if mode == "full" else [])
    elif key == "backup-extra":
        a = [script, "--full"] + (["--force-full"] if mode == "full" else [])
    elif key == "trip":
        m = month or datetime.now().strftime("%Y-%m")
        a = [script, "--month", m, "--all-users", "--save-db", "--src", "web-sync", "--out", "/tmp"]
    elif key == "records":
        m = month or datetime.now().strftime("%Y-%m")
        a = [script, "--month", m]
    else:
        a = [script]
    return [SYNC_SCRIPT_PY] + a


def _sync_last(key):
    """读该任务在 sync_runs 里的最近一条（时间/状态）；读不到返回 None"""
    conds = {
        "backup-task": "mode not like 'extra:%' and mode not like 'trip:%' "
                       "and mode not like 'records:%' "
                       "and mode not in ('progress','users','dict')",
        "backup-extra": "mode like 'extra:%'",
        "trip": "mode like 'trip:%'",
        "records": "mode like 'records:%'",
        "progress": "mode = 'progress'",
        "users": "mode = 'users'",
        "dict": "mode = 'dict'",
    }
    c = conds.get(key)
    if not c:
        return None
    try:
        conn = backup_conn()
    except Exception:
        return None
    try:
        r = conn.execute("select finished_at,status from sync_runs where " + c +
                         " order by run_id desc limit 1").fetchone()
        return {"time": (r["finished_at"] or "")[:19], "status": r["status"]} if r else None
    except Exception:
        return None
    finally:
        conn.close()


_SYNC_SCHED_CACHE = {"ts": 0.0, "data": None}


def sync_schedule_info():
    """定时备份排期（2026-11-12 hunter 定：同步中心页底部展示）

    读 `openclaw cron list --json`，取周增量 / 季度全量两个任务的表达式与下次执行时间；
    结果缓存 10 分钟，失败返回 ok=False（前端显示固定文案）。
    """
    now = time.time()
    if _SYNC_SCHED_CACHE["data"] is not None and now - _SYNC_SCHED_CACHE["ts"] < 600:
        return _SYNC_SCHED_CACHE["data"]
    out = {"ok": False, "weekly": None, "quarterly": None}
    try:
        p = subprocess.run(["/usr/bin/openclaw", "cron", "list", "--json"],
                           capture_output=True, text=True, timeout=20)
        d = json.loads(p.stdout or "{}")
        for j in (d.get("jobs") or []):
            nm = j.get("name") or ""
            if nm not in ("shb-task-sync-weekly", "shb-task-sync-quarterly"):
                continue
            nx = (j.get("state") or {}).get("nextRunAtMs")
            out["weekly" if nm.endswith("weekly") else "quarterly"] = {
                "expr": (j.get("schedule") or {}).get("expr") or "",
                "next": datetime.fromtimestamp(nx / 1000).strftime("%Y-%m-%d %H:%M") if nx else "",
            }
        out["ok"] = bool(out["weekly"] or out["quarterly"])
    except Exception as e:
        logger.warning("SYNC_SCHED_FAIL %s", e)
    _SYNC_SCHED_CACHE.update({"ts": now, "data": out})
    return out


@app.route("/api/sync/status")
def api_sync_status():
    user = request.headers.get("X-Remote-User", "") or "admin"
    if user != "admin":
        return make_response(jsonify({"ok": False, "error": "无权限"}), 403)
    tasks = []
    for key, cfg in SYNC_TASKS.items():
        running = None
        for sj in RESULT_JOBS.values():
            if sj.get("tab") == "sync:" + key and sj.get("status") in ("queued", "running"):
                running = sj
                break
        tasks.append({"key": key, "title": cfg["title"], "desc": cfg["desc"],
                      "writes": cfg["writes"], "param": cfg["param"],
                      "last": _sync_last(key), "running": bool(running),
                      "running_status": (running or {}).get("status")})
    return jsonify({"ok": True, "tasks": tasks, "schedule": sync_schedule_info()})


@app.route("/api/sync", methods=["POST"])
def api_sync():
    user = request.headers.get("X-Remote-User", "") or "admin"
    if user != "admin":
        return make_response(jsonify({"ok": False, "error": "仅管理员可同步"}), 403)
    key = request.args.get("task", "")
    if key not in SYNC_TASKS:
        return jsonify({"ok": False, "error": f"未知同步任务: {key}"})
    for sj in RESULT_JOBS.values():
        if sj.get("tab") == "sync:" + key and sj.get("status") in ("queued", "running"):
            return jsonify({"ok": False, "error": "该任务正在运行中，请稍候"})
    mode = (request.args.get("mode") or "incr").strip()
    month = (request.args.get("month") or "").strip()
    argv = _sync_argv(key, mode, month)
    for sid, sj in list(RESULT_JOBS.items()):
        if sj.get("status") in ("ok", "failed"):
            RESULT_JOBS.pop(sid, None)
    job_id = uuid.uuid4().hex[:12]
    RESULT_JOBS[job_id] = {"id": job_id, "tab": "sync:" + key, "key": key, "mode": mode,
                           "month": month, "user": user, "status": "queued",
                           "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    threading.Thread(target=_job_worker, args=(job_id, argv), daemon=True).start()
    logger.info("SYNC user=%s task=%s mode=%s month=%s job=%s", user, key, mode, month, job_id)
    return jsonify({"ok": True, "job_id": job_id, "task": key})


@app.route("/api/synclog")
def api_synclog():
    """同步日志（系统 Tab，仅 admin）：读 sync_runs，全部同步任务的批次记录"""
    user = request.headers.get("X-Remote-User", "") or "admin"
    if user != "admin":
        return make_response(jsonify({"ok": False, "error": "无权限"}), 403)
    page = max(1, int(request.args.get("page", "1") or "1"))
    page_size = min(200, max(1, int(request.args.get("page_size", "50") or "50")))
    status = (request.args.get("status") or "").strip()
    conn = backup_conn()
    try:
        where = " where status=?" if status else ""
        args = (status,) if status else ()
        total = conn.execute("select count(*) from sync_runs" + where, args).fetchone()[0]
        rws = conn.execute(
            "select run_id,started_at,finished_at,mode,list_rows,tasks_upserted,"
            "detail_calls,errors,status from sync_runs" + where +
            " order by run_id desc limit ? offset ?",
            args + (page_size, (page - 1) * page_size)).fetchall()
    finally:
        conn.close()
    cols = ["批次", "开始", "结束", "模式", "列表条数", "落库条数", "取详情次数", "错误", "状态"]
    rows = [{"批次": r["run_id"], "开始": (r["started_at"] or ""), "结束": (r["finished_at"] or ""),
             "模式": r["mode"], "列表条数": r["list_rows"], "落库条数": r["tasks_upserted"],
             "取详情次数": r["detail_calls"], "错误": r["errors"], "状态": r["status"]} for r in rws]
    return jsonify({"ok": True, "columns": cols, "rows": rows, "total": total,
                    "current_page": page, "total_pages": max(1, (total + page_size - 1) // page_size)})


# ── 退出登录（修复：返回 401 清 Basic Auth 缓存） ──
@app.route("/logout")
def logout():
    # Basic Auth 清凭据只能靠 401 + WWW-Authenticate。但浏览器弹框后会用同一 URL 重试，
    # 若停在 /shb/logout 就会无限 401。故用一次性 cookie 区分两段：
    #   ① 首次点退出（无标记）→ 401，浏览器丢弃当前凭据并弹登录框
    #   ② 弹框输入后重试（有标记）→ 302 回 /shb，落在正常首页，不再循环
    if request.headers.get("Authorization") and request.cookies.get("shb_logging_out") == "1":
        resp = redirect("/shb/")
        resp.delete_cookie("shb_logging_out", path="/")
        return resp
    html = """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"><title>已退出</title>
<style>body{font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;background:#f5f6fa;color:#2d3436;margin:0}
.card{text-align:center;background:#fff;padding:40px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.06)}
h1{font-size:24px;margin-bottom:8px;color:#0984e3}p{color:#636e72;font-size:14px}
.btn{padding:10px 28px;background:#0984e3;color:#fff;border:none;border-radius:6px;font-size:14px;cursor:pointer;text-decoration:none;display:inline-block;margin-top:20px}
.btn:hover{background:#0773c5}</style></head>
<body><div class="card"><h1>✅ 已退出</h1><p>登录凭证已清除，请关闭浏览器标签页后重新打开。</p>
<a class="btn" href="/shb/">重新登录</a></div></body></html>"""
    resp = make_response(html, 401)
    # werkzeug 的响应头只接受 latin-1；中文 realm 需按 UTF-8 字节透传，否则会抛
    # UnicodeEncodeError 导致 401 无法返回（nginx 侧表现为 502）
    _realm = "SHB 站点信息".encode("utf-8").decode("latin-1")
    resp.headers["WWW-Authenticate"] = 'Basic realm="' + _realm + '"'
    resp.set_cookie("shb_logging_out", "1", max_age=120, path="/")
    return resp


# ── 操作日志（仅 admin）─────────────────────────────────────────
AUDIT_LOG_PATH = os.path.join(BASE_DIR, "..", "shb-mcp", "audit.log")


def _read_audit_entries(filter_status):
    """读取 audit.log + 轮转文件 .1~.10，返回按时间倒序的 entries（最新在前）"""
    files = [AUDIT_LOG_PATH] + [f"{AUDIT_LOG_PATH}.{i}" for i in range(1, 11)]
    entries = []
    for fp in files:
        if not os.path.exists(fp):
            continue
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if filter_status and entry.get("status") != filter_status:
                    continue
                entries.append(entry)
    entries.sort(key=lambda e: e.get("ts", ""), reverse=True)
    return entries


@app.route("/api/audit")
def api_audit():
    user = request.headers.get("X-Remote-User", "") or "admin"
    if user != "admin":
        return make_response(jsonify({"ok": False, "error": "无权限访问此接口"}), 403)

    page = max(1, int(request.args.get("page", "1")))
    page_size = min(200, max(1, int(request.args.get("page_size", "50"))))
    filter_status = request.args.get("status", "").strip()

    entries = _read_audit_entries(filter_status)
    total = len(entries)
    start = (page - 1) * page_size
    rows = entries[start:start + page_size]

    return jsonify({
        "ok": True,
        "rows": rows,
        "total": total,
        "current_page": page,
        "total_pages": max(1, (total + page_size - 1) // page_size),
    })


# ── 身份管理（仅 admin，2026-10-23）─────────────────────────────
ID_CFG_PATH = os.path.expanduser("~/.config/shb-cli/config.json")
ID_CRED_PATH = os.path.expanduser("~/.config/shb-cli/credentials.json")
ID_LOCK_PATH = "/tmp/shb_identity_switch.lock"
ID_AUTH_JOBS: dict = {}
ID_LOCK_TTL = 35 * 60  # 授权等待锁最长存活秒数


def _id_admin_only():
    user = request.headers.get("X-Remote-User", "") or "admin"
    return user == "admin"


def _id_backup():
    """按 SOP 先备份 config.json + credentials.json"""
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    made = []
    for p in (ID_CFG_PATH, ID_CRED_PATH):
        if os.path.exists(p):
            dst = f"{p}.bak-{ts}"
            shutil.copy2(p, dst)
            made.append(dst)
    return made


def _id_read_cfg():
    with open(ID_CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _id_probe(profile):
    """探测某身份令牌是否有效（只读）"""
    env = {**os.environ, "NO_COLOR": "1", "SHB_WEB_GUARD": "1"}
    try:
        p = subprocess.run(["shb-cli", "--profile", profile, "config"],
                           capture_output=True, text=True, env=env, timeout=60)
    except Exception as e:
        return {"valid": False, "error": str(e)}
    out = ((p.stdout or "") + (p.stderr or "")).strip()
    low = out.lower()
    valid = ("valid: true" in low) or ('"valid": true' in low)
    return {"valid": valid, "rc": p.returncode, "raw": out[-500:]}


def _id_set_current(profile):
    """原子写 current_profile：临时文件 + rename"""
    cfg = _id_read_cfg()
    cfg["current_profile"] = profile
    tmp = ID_CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, ID_CFG_PATH)


def _id_audit(entry):
    entry.setdefault("action_source", "shb-web:identity")
    entry.setdefault("ts", datetime.now().astimezone().isoformat())
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error("ID_AUDIT_FAIL %s", e)


def _id_active_job():
    """返回当前仍在等待的身份（若有且未超时）"""
    for name, job in list(ID_AUTH_JOBS.items()):
        proc = job.get("proc")
        alive = proc is not None and proc.poll() is None
        if alive and (time.time() - job.get("start", 0)) < ID_LOCK_TTL:
            return name, job
        if not alive:
            ID_AUTH_JOBS.pop(name, None)
            if os.path.exists(ID_LOCK_PATH):
                try:
                    os.remove(ID_LOCK_PATH)
                except Exception:
                    pass
    return None, None


@app.route("/api/identity/list")
def api_identity_list():
    if not _id_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    try:
        cfg = _id_read_cfg()
    except Exception as e:
        return jsonify({"ok": False, "error": f"读取配置失败: {e}"})
    cur = cfg.get("current_profile") or "default"
    profs = cfg.get("profiles") or {}
    rows = []
    for name, p in profs.items():
        p = p or {}
        rows.append({
            "profile": name,
            "显示名": p.get("display_name") or name,
            "最后登录": (p.get("last_login_at") or "")[:19].replace("T", " "),
            "当前": "★" if name == cur else "",
        })
    rows.sort(key=lambda r: (r["当前"] == "", r["显示名"]))
    busy, _job = _id_active_job()
    return jsonify({"ok": True, "current": cur, "rows": rows, "waiting": busy or ""})


@app.route("/api/identity/switch", methods=["POST"])
def api_identity_switch():
    if not _id_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    data = request.get_json(silent=True) or {}
    target = (data.get("profile") or "").strip()
    try:
        cfg = _id_read_cfg()
    except Exception as e:
        return jsonify({"ok": False, "error": f"读取配置失败: {e}"})
    if target not in (cfg.get("profiles") or {}):
        return jsonify({"ok": False, "error": f"身份不存在: {target}"})
    old = cfg.get("current_profile") or "default"
    if target == old:
        return jsonify({"ok": True, "status": "already", "current": old,
                        "message": "已是当前身份"})

    probe = _id_probe(target)
    if not probe.get("valid"):
        _id_audit({"action": "identity_probe", "operator": "admin", "target": target,
                   "status": "invalid"})
        return jsonify({"ok": True, "status": "need_auth", "profile": target,
                        "message": "该身份令牌无效或已过期，需先生成授权链接"})

    made = _id_backup()
    try:
        _id_set_current(target)
        re_probe = _id_probe(target)
        ok = bool(re_probe.get("valid"))
        err = "" if ok else (re_probe.get("raw") or "校验未通过")
    except Exception as e:
        ok, err = False, str(e)

    if not ok:
        try:
            _id_set_current(old)
        except Exception as e:
            logger.error("ID_ROLLBACK_FAIL %s", e)
        _id_audit({"action": "identity_switch", "operator": "admin", "from": old,
                   "to": target, "status": "failed", "error": err})
        return jsonify({"ok": False, "error": "切换后校验失败，已回滚到原身份",
                        "current": old})

    _id_audit({"action": "identity_switch", "operator": "admin", "from": old,
               "to": target, "status": "success", "backup": made})
    logger.info("IDENTITY_SWITCH %s -> %s", old, target)
    return jsonify({"ok": True, "status": "switched", "current": target})


@app.route("/api/identity/authlink", methods=["POST"])
def api_identity_authlink():
    if not _id_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    data = request.get_json(silent=True) or {}
    target = (data.get("profile") or "").strip()
    try:
        cfg = _id_read_cfg()
    except Exception as e:
        return jsonify({"ok": False, "error": f"读取配置失败: {e}"})
    if target not in (cfg.get("profiles") or {}):
        return jsonify({"ok": False, "error": f"身份不存在: {target}"})

    busy, _job = _id_active_job()
    if busy:
        return jsonify({"ok": False, "error": f"已有待授权任务（{busy}），请先取消"})

    _id_backup()
    env = {**os.environ, "NO_COLOR": "1", "SHB_MCP_GUARD": "1"}
    cmd = ["shb-cli", "auth", "login", "--profile", target, "--env", "dingtalk",
           "--no-browser", "--timeout", "30m"]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, env=env, bufsize=1)
    except Exception as e:
        return jsonify({"ok": False, "error": f"启动授权失败: {e}"})

    url = ""
    deadline = time.time() + 12
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        for tok in line.split():
            if tok.startswith("https://") and "authorize" in tok:
                url = tok.strip().rstrip(".,;)")
                break
        if url:
            break

    ID_AUTH_JOBS[target] = {"proc": proc, "url": url, "status": "waiting" if url else "starting",
                            "start": time.time()}
    try:
        with open(ID_LOCK_PATH, "w", encoding="utf-8") as f:
            f.write(target)
    except Exception:
        pass
    _id_audit({"action": "identity_authlink", "operator": "admin", "target": target,
               "status": "issued" if url else "no_url"})
    return jsonify({"ok": True, "profile": target, "url": url,
                    "status": "waiting" if url else "starting"})


@app.route("/api/identity/authstatus")
def api_identity_authstatus():
    if not _id_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    target = (request.args.get("profile") or "").strip()
    job = ID_AUTH_JOBS.get(target)
    if not job:
        return jsonify({"ok": True, "status": "none"})
    proc = job.get("proc")
    if proc is not None and proc.poll() is None:
        if (time.time() - job.get("start", 0)) > ID_LOCK_TTL:
            try:
                proc.terminate()
            except Exception:
                pass
            return jsonify({"ok": True, "status": "timeout", "url": job.get("url", "")})
        return jsonify({"ok": True, "status": "waiting", "url": job.get("url", ""),
                        "elapsed": int(time.time() - job.get("start", 0))})

    probe = _id_probe(target)
    if probe.get("valid"):
        job["status"] = "success"
        _id_audit({"action": "identity_authlink", "operator": "admin", "target": target,
                   "status": "authorized"})
        return jsonify({"ok": True, "status": "success", "valid": True})
    job["status"] = "timeout"
    return jsonify({"ok": True, "status": "timeout", "valid": False,
                    "rc": proc.poll() if proc is not None else None})


@app.route("/api/identity/cancel", methods=["POST"])
def api_identity_cancel():
    if not _id_admin_only():
        return make_response(jsonify({"ok": False, "error": "仅管理员可操作"}), 403)
    data = request.get_json(silent=True) or {}
    target = (data.get("profile") or "").strip()
    job = ID_AUTH_JOBS.pop(target, None)
    if job and job.get("proc") is not None and job["proc"].poll() is None:
        try:
            job["proc"].terminate()
        except Exception:
            pass
    if os.path.exists(ID_LOCK_PATH):
        try:
            os.remove(ID_LOCK_PATH)
        except Exception:
            pass
    _id_audit({"action": "identity_authlink", "operator": "admin", "target": target,
               "status": "cancelled"})
    return jsonify({"ok": True})


@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "shb-web", "time": datetime.now().isoformat()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5782, debug=False)
