#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SHB 在线报表取数层（云端实时查询，与本地备份库完全隔离）

建立：2026-11-10（hunter 批准，方案见 memory/SHB.md §7 未决项）
铁律（不可破坏）：
  1. 只读云端；**绝不打开 shb_tasks.db**（本文件不出现 backup_conn / sqlite3）
  2. shb-cli 子进程必须带 SHB_WEB_GUARD=1（/usr/bin/shb-cli 双通道守卫之一）
  3. 工单动态（出发/到达/卡片/完成）走直连 HTTP，端点和请求头沿用
     shb-tasks/sync_records.py；令牌复用 shb-cli 的 credentials.json
  4. 单次查询总预算 QUERY_BUDGET 秒，超时抛 TimeoutError（前端报错，不降级本地）

对外接口：
  query(tab, params, progress=None) -> dict
    tab     : ol:trip / ol:weekly-plan / ol:progress2 / ol:workload / ol:debug-progress
    params  : {keyword, date_begin, date_end, week_start, mode, show_done}
    progress: 可选回调，参数为一句中文进度（如「已取 3/12 单」）
  返回结构与 /api/search 一致（ok/rows/columns/total/...）
"""
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ── 常量 ──────────────────────────────────────────────────────────────
CFG = "/root/.config/shb-cli/config.json"
CRED = "/root/.config/shb-cli/credentials.json"
PAGE_SIZE = 200
WORKERS = 16              # 并发路数（2026-11-10 8→12→16，实测无报错）
CLI_TIMEOUT = 30
HTTP_TIMEOUT = 30
QUERY_BUDGET = 180         # 秒：单次在线查询总预算（行程单默认整月数据量大，预留充足）

DEBUG_TASK_TID = "f6690742-8462-42c9-b7dd-fe36ea405eb6"   # 调试工单类型

STATE_CN = {"created": "待指派", "allocated": "已指派", "accepted": "已接受",
            "processing": "进行中", "finished": "已完成", "refused": "已拒绝",
            "costed": "已结算", "closed": "已关闭", "offed": "已取消",
            "taskPool": "工单池"}

# 行程单「工作内容」取值字段（与 skills/shb-trip-export/scripts/export_trip.py 一致）
GEN_FIELD = {
    "售后工单": ("field_wDK5XbrVjGZHNTHC", 100),
    "日常工单": ("field_kPdz7igWScK4oqdl", 100),
    "验收工单": ("field_gZKddPgQ15cYoPHu", None),
    "改造工单": ("field_2ILYNVM6ncPnjss4", None),
}
CODE_FIELD = {
    "调试工单": ("field_MYEWB8m7RicrrL6i", "现场调试"),
    "保运工单": ("field_1PC56nTwEJw7HMaE", "现场保运"),
}

# 项目进度2：客户档案自定义日期字段（与 app.py 本地版 P2_DATE_FIELDS 一致）
P2_DATE_FIELDS = [
    ("field_4Du9tW3FdX5ofNd7", "设备发货日期"),
    ("field_pWtw8xgz7KTQVXaV", "安装开始日期"),
    ("field_XIbL8TbFkaQO7yCq", "安装完毕日期"),
    ("field_DM2DAoBb203rhrVf", "工程验收日期"),
    ("field_uBS7HOVJAndqk3On", "调试物品发货日期"),
    ("field_d2P4mgHXg1Cmrl9N", "调试开始日期"),
    ("field_aizN4CQl2gOZpJea", "调试完毕日期"),
    ("field_LJtmwhb97rF9YfZm", "消防验收日期"),
    ("field_iW3hzpnLhqiOCkPa", "计量验收日期"),
    ("field_buJ9ddsuAZn8nvDp", "充装验收日期"),
    ("field_2gfjf8Zl09aWwuab", "保运开始日期"),
    ("field_Brf7x05dyDVU9HOL", "保运完毕日期"),
]
P2_BAOYUN_PERSON = "field_Yc3YHI7w2qowAOZP"   # 保运人员

TRIP_COLUMNS = ["序号", "姓名", "日期", "到场地点", "离场地点", "工单编号", "工单类型", "工作内容"]
WORKLOAD_COLUMNS = ["工单类型", "客户编号", "客户", "服务部门", "服务人员", "省份",
                    "出发打卡", "到场打卡", "途中时长",
                    "工单到场签到日期", "工单离场签到日期", "工单时长"]
# 调试进度改用明细行：一条调试内容一行（2026-11-10 hunter 指定格式）
DEBUG_COLUMNS = ["序号", "调试人员", "项目编码", "客户名称", "调试内容", "调试日期"]
# 调试组件（调试图上的「调试项选择」字段）列 ID，2026-11-10 核实
DBG_ITEMS_FIELD = "field_FwVetv884eAgXTfa"        # 调试项选择（多行调试组件）
DBG_ITEM_CONTENT = "field45wlp82DxDQKzlmhYjFsnVXPFDeKKeBc"   # 调试内容
DBG_ITEM_TIME = "field_cJ6uku6AtKpo1XLw"          # 调试时间
DBG_ITEM_PERSON = "field_UDTamlMA7djGJC9M"        # 调试人
DBG_ITEM_STATUS = "field_W76Oo6QsGbbQzE88"        # 状态（已经完成/…）
PROGRESS2_COLUMNS = [
    "客户编号", "客户名称",
    "设备发货日期", "安装开始日期", "安装完毕日期", "工程验收日期",
    "调试物品发货日期", "调试开始日期", "调试完毕日期", "调试人员",
    "消防验收日期", "计量验收日期", "充装验收日期",
    "保运开始日期", "保运完毕日期", "保运人员", "运行日期",
]
WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

TAB_TITLES = {
    "ol:trip": "行程单（在线）",
    "ol:weekly-plan": "周计划（在线）",
    "ol:progress2": "项目进度（在线）",
    "ol:workload": "服务人员工作量（在线）",
    "ol:debug-progress": "调试进度（在线）",
}


# ── 基础工具 ──────────────────────────────────────────────────────────
def cli(argv, timeout=CLI_TIMEOUT):
    """调 shb-cli（web 守卫通道）并解析 JSON；失败抛 RuntimeError"""
    env = {**os.environ, "NO_COLOR": "1", "SHB_WEB_GUARD": "1"}
    p = subprocess.run(["shb-cli"] + list(argv), capture_output=True, text=True,
                       env=env, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "shb-cli 调用失败").strip()[:400])
    for line in (p.stdout or "").splitlines():
        line = line.strip()
        if line.startswith(("{", "[")):
            try:
                return json.loads(line)
            except Exception:
                continue
    return {}


def load_auth():
    """取 shb-cli 当前身份的 domain + token（与 sync_records.py 同法）"""
    cfg = json.load(open(CFG, encoding="utf-8"))
    prof = (cfg.get("profiles") or {}).get(cfg.get("current_profile") or "default") or {}
    domain = prof.get("domain")
    ref = prof.get("token_ref")
    cred = json.load(open(CRED, encoding="utf-8"))
    token = ((cred.get("entries") or {}).get(ref) or {}).get("token")
    if not domain or not token:
        raise RuntimeError("读不到售后宝 domain/token，请先在「身份管理」授权登录")
    return domain, token


def http_get(path, params):
    """直连售后宝 HTTP 接口（token 请求头，同 sync_records.py）"""
    domain, token = load_auth()
    url = "https://%s%s?%s" % (domain, path, urllib.parse.urlencode(params))
    req = urllib.request.Request(url, headers={
        "token": token,
        "shbversion": "3",
        "shb-language": "zh",
        "timezone": "Asia/Shanghai",
        "accept": "application/json, text/plain, */*",
        "user-agent": "shb-web-online/1.0",
    })
    last = None
    for i in range(2):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1)
    raise RuntimeError("售后宝接口请求失败：%s" % last)


def ts_ms(ms):
    """毫秒时间戳 → 'YYYY-MM-DD HH:MM:SS'（本机 Asia/Shanghai）"""
    if not ms:
        return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ms)


def ts_any(v):
    """毫秒/秒时间戳 / 带时区 ISO / 'YYYY-MM-DD HH:MM:SS' → datetime（本地时区）或 None

    🔴 2026-11-10 修：售后宝返回的是 UTC（如 2026-11-08T16:00:00.000+0000），
    必须先按 %z 解析再 astimezone() 转本地，否则跨周边界会差 8 小时。
    """
    if v in (None, "", 0, "0"):
        return None
    if isinstance(v, (int, float)):
        try:
            n = v / 1000.0 if v > 1e11 else float(v)
            return datetime.fromtimestamp(n)
        except Exception:
            return None
    s = str(v).strip()
    # 带时区（+0000 / +00:00 / Z）→ 解析后转本地
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M%z"):
        try:
            return datetime.strptime(s, fmt).astimezone().replace(tzinfo=None)
        except ValueError:
            continue
    if s.endswith("Z"):
        try:
            return datetime.strptime(s[:-1], "%Y-%m-%dT%H:%M:%S.%f").replace(
                tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
        except ValueError:
            try:
                return datetime.strptime(s[:-1], "%Y-%m-%dT%H:%M:%S").replace(
                    tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
            except ValueError:
                pass
    # 无时区的字符串视为本地时间（与本地库口径一致）
    s2 = s.replace("T", " ")[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s2, fmt)
        except ValueError:
            continue
    return None


def sval(v):
    """字段值 → 纯文本（行程单工作内容用；与 export_trip.py 一致）"""
    if v is None:
        return ""
    if isinstance(v, list):
        return "、".join(x for x in (sval(i) for i in v) if x)
    if isinstance(v, dict):
        return " ".join(x for x in (sval(i) for i in v.values() if isinstance(i, str)) if x)
    if isinstance(v, bool):
        return "是" if v else "否"
    return str(v)


def cell(v):
    """云端字段值 → 展示文本（dict 取名称/地址；list 顿号连接；毫秒转时间）"""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "是" if v else "否"
    if isinstance(v, dict):
        for k in ("adAddress", "allAddress", "name", "displayName", "label",
                  "catalogName", "pathName", "address"):
            if v.get(k):
                return str(v[k])
        return ""
    if isinstance(v, list):
        parts = []
        for x in v:
            if isinstance(x, dict):
                t = x.get("tagName") or x.get("name") or x.get("displayName") or x.get("label")
                if t:
                    parts.append(str(t))
            elif x not in (None, ""):
                parts.append(str(x))
        return "、".join(parts)
    if isinstance(v, (int, float)) and v > 1e11:
        return ts_ms(v)
    return str(v)


def esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def d_range(date_begin, date_end):
    """'YYYY-MM-DD' → ('YYYY-MM-DDT00:00:00', 'YYYY-MM-DDT23:59:59')；空则默认最近 7 天"""
    today = datetime.now()
    b = (date_begin or "").strip() or (today - timedelta(days=6)).strftime("%Y-%m-%d")
    e = (date_end or "").strip() or today.strftime("%Y-%m-%d")
    return b + "T00:00:00", e + "T23:59:59"


def month_range_now():
    """当月 1 日 ~ 当月最后一天（行程单默认）"""
    today = datetime.now()
    first = today.replace(day=1)
    last = (first + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    return first.strftime("%Y-%m-%dT00:00:00"), last.strftime("%Y-%m-%dT23:59:59")


def week_range_now():
    """本自然周：周一 00:00 ~ 周日 23:59（工作量/调试进度默认）"""
    today = datetime.now()
    mon = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    sun = mon + timedelta(days=6)
    return mon.strftime("%Y-%m-%dT00:00:00"), sun.strftime("%Y-%m-%dT23:59:59")


def rng(params, kind="week"):
    """按类型取查询区间：显式传日期优先；否则 trip=当月，workload/debug=当周，其他=近 7 天"""
    db = (params.get("date_begin") or "").strip()
    de = (params.get("date_end") or "").strip()
    if db or de:
        return d_range(db, de)
    if kind == "month":
        return month_range_now()
    if kind == "week":
        return week_range_now()
    return d_range("", "")


def _check(deadline, stage):
    if deadline and time.time() > deadline:
        raise TimeoutError("云端查询超时（%s，预算 %ds）" % (stage, QUERY_BUDGET))


# ── 云端取数 ──────────────────────────────────────────────────────────
def search_tasks(create_begin, create_end, template_id=None, deadline=None, progress=None):
    """分页拉工单列表（task search）。create_*: 'YYYY-MM-DDTHH:MM:SS'"""
    rows, page = [], 1
    while True:
        body = {"page": page, "pageSize": PAGE_SIZE}
        if template_id:
            body["templateId"] = template_id
        if create_begin:
            body["createTimeStart"] = create_begin
        if create_end:
            body["createTimeEnd"] = create_end
        d = cli(["task", "search", "--data", json.dumps(body, ensure_ascii=False), "-o", "raw"])
        content = d.get("content") or []
        rows.extend(content)
        if progress:
            progress("已拉工单列表 %d 条" % len(rows))
        tp = d.get("totalPages") or 0
        if not content or page >= tp:
            break
        page += 1
        _check(deadline, "拉取工单列表")
    return rows


def task_detail(task_id):
    """工单详情（完整 initJson：task 主体 + 卡片定义 + 回执/附加组件）"""
    d = cli(["task", "detail", "--id", task_id, "-o", "raw"])
    return d.get("initJson") or {}


def map_details(tasks, deadline=None, progress=None):
    """8 路并发拉工单详情 → {task_id: initJson}"""
    out = {}
    total = len(tasks)
    if not total:
        return out

    def _one(t):
        return t.get("id"), task_detail(t.get("id"))

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_one, t) for t in tasks]
        for fut in as_completed(futs):
            _check(deadline, "取工单详情")
            tid, init = fut.result()
            out[tid] = init
            done += 1
            if progress:
                progress("已取 %d/%d 单" % (done, total))
    return out


def node_temp_storage(task_id, node_id):
    """当前节点「暂存」内容（2026-11-10 按前端抓包核实）

    GET /api/task/outside/pc/task/getCurrentNodeTemporaryStorage
        ?taskId=<id>&currentNodeId=<nodeId>
    返回 result.formData，与已提交节点字段同结构（含 field_FwVetv884eAgXTfa 调试项）
    """
    d = http_get("/api/task/outside/pc/task/getCurrentNodeTemporaryStorage",
                 {"taskId": task_id, "currentNodeId": node_id})
    return ((d.get("result") or {}).get("formData") or {})


def fetch_records(task_id):
    """拉一张工单的全部动态（自动翻页；端点同 sync_records.py）"""
    out, page = [], 1
    while True:
        d = http_get("/api/task/outside/pc/taskrecord/list",
                     {"taskId": task_id, "pageNum": page, "pageSize": 50})
        res = d.get("result") or {}
        rows = res.get("list") or []
        out.extend(rows)
        if len(rows) < 50 or len(out) >= (res.get("total") or 0):
            break
        page += 1
        if page > 20:
            break
    return out


def map_records(tasks, deadline=None, progress=None):
    """8 路并发拉工单动态 → {task_id: [rows]}"""
    out = {}
    total = len(tasks)
    if not total:
        return out

    def _one(t):
        return t.get("id"), fetch_records(t.get("id"))

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_one, t) for t in tasks]
        for fut in as_completed(futs):
            _check(deadline, "取工单动态")
            tid, rows = fut.result()
            out[tid] = rows
            done += 1
            if progress:
                progress("已取动态 %d/%d 单" % (done, total))
    return out


def _card_rows(init):
    """initJson → {(userName, cardName): [{'t':..., 'loc':...}]}
    仅保留「到场签到 / 离场打卡」两种附加组件卡片（行程单/工作量共用）"""
    defs = {}
    for c in (init.get("cardInfo") or []):
        fmap = {f.get("fieldName"): f.get("displayName") for f in (c.get("fields") or [])}
        defs[c.get("cardId")] = (c.get("cardName"), fmap)
    cards = {}
    for srcname in ("receiptTaskForRead", "receiptTaskForUpdate"):
        rt = init.get(srcname) or {}
        for card in (rt.get("cardInfo") or []):
            nm, fmap = defs.get(card.get("cardId"), ("?", {}))
            if nm not in ("到场签到", "离场打卡"):
                continue
            for r in (card.get("attribute") or []):
                vals = {fmap.get(k, k): v for k, v in r.items() if str(k).startswith("field_")}
                loc = vals.get("位置") if isinstance(vals.get("位置"), dict) else {}
                tstamp = vals.get("签到日期") or vals.get("时间") or loc.get("time")
                dt = ts_any(tstamp)
                cards.setdefault((r.get("userName") or "", nm), []).append(
                    {"t": dt, "loc": loc.get("address") or ""})
    return cards


# ── 报表 1：行程单（在线）────────────────────────────────────────────
def work_of(ty, task, state_cn):
    """工单类型 → 工作内容（同 export_trip.py work_of）"""
    attr = task.get("attribute") or {}
    if ty == "工程工单":
        return "工程" + (state_cn or "")
    if ty == "预调试工单":
        return "预调试"
    if ty in CODE_FIELD:
        fn, fix = CODE_FIELD[ty]
        code = sval(attr.get(fn)).strip()
        return ("%s，%s" % (code, fix)) if code else fix
    if ty in GEN_FIELD:
        fn, limit = GEN_FIELD[ty]
        val = sval(attr.get(fn)).strip()
        if val and limit and len(val) > limit:
            val = val[:limit]
        return val
    return ""


def q_trip(params, progress, deadline):
    person = (params.get("keyword") or "").strip()
    b, e = rng(params, "month")          # 默认取当月（hunter 2026-11-10 定）
    tasks = search_tasks(b, e, deadline=deadline, progress=progress)
    # 待指派/已指派/工单池的单不可能有签到记录 → 不取详情（2026-11-10 优化）
    tasks = [t for t in tasks
             if t.get("state") not in ("created", "allocated", "taskPool")]
    if person:
        tasks = [t for t in tasks if person in (t.get("executorName") or "")]
    details = map_details(tasks, deadline=deadline, progress=progress)

    recs = []
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        state_cn = STATE_CN.get(t.get("state"), t.get("state") or "")
        wc = work_of(t.get("templateName") or task.get("templateName"), task, state_cn)
        cards = _card_rows(init)
        users = {k[0] for k in cards}
        for u in users:
            ins = cards.get((u, "到场签到"), [])
            outs = cards.get((u, "离场打卡"), [])
            for i in range(max(len(ins), len(outs))):
                a = ins[i] if i < len(ins) else {}
                l = outs[i] if i < len(outs) else {}
                recs.append({"name": u, "in": a.get("t"), "in_loc": a.get("loc") or "",
                             "out": l.get("t"), "out_loc": l.get("loc") or "",
                             "taskNo": t.get("taskNo"), "type": t.get("templateName"), "work": wc})

    # 按（人, 天）聚合：到场取最早、离场取最晚，单号/类型「，」、工作内容「；」去重连接
    groups = {}
    for r in recs:
        d = r["in"] or r["out"]
        if not d:
            continue
        key = (r["name"], d.strftime("%Y-%m-%d"))
        g = groups.setdefault(key, {"in": None, "in_loc": "", "out": None, "out_loc": "",
                                    "taskNo": [], "type": [], "work": []})
        di, do = r["in"], r["out"]
        if di and (g["in"] is None or di < g["in"]):
            g["in"], g["in_loc"] = di, r["in_loc"]
        if do and (g["out"] is None or do > g["out"]):
            g["out"], g["out_loc"] = do, r["out_loc"]
        for k, src in (("taskNo", r["taskNo"]), ("type", r["type"]), ("work", r["work"])):
            if src and src not in g[k]:
                g[k].append(src)

    rows = []
    for i, ((name, date), g) in enumerate(
            sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1])), 1):
        rows.append({"序号": i, "姓名": name, "日期": date,
                     "到场地点": g["in_loc"], "离场地点": g["out_loc"],
                     "工单编号": "，".join(g["taskNo"]), "工单类型": "，".join(g["type"]),
                     "工作内容": "；".join(g["work"])})
    persons = sorted({r["姓名"] for r in rows if r["姓名"]})
    return _resp("ol:trip", rows, TRIP_COLUMNS, persons=persons,
                 extra={"range": [b[:10].replace("-", "/"), e[:10].replace("-", "/")]})


# ── 报表 2：周计划 / 周总结（在线）────────────────────────────────────
def _task_persons(t, init):
    """工单的归属人 = 负责人 + 协同人（hunter 2026-11-10 定）

    负责人：列表 executorName / executorUser，详情 task.executor
    协同人：详情 task.synergies[].displayName
    """
    out = []

    def add(n):
        n = (n or "").strip()
        if n and n not in out:
            out.append(n)

    add(t.get("executorName"))
    add((t.get("executorUser") or {}).get("displayName"))
    task = (init or {}).get("task") or {}
    add((task.get("executor") or {}).get("displayName"))
    for u in (task.get("synergies") or []):
        add((u or {}).get("displayName"))
    return out


def q_weekly(params, progress, deadline):
    mode = (params.get("mode") or "plan").strip()
    keyword = (params.get("keyword") or "").strip()      # 姓名筛选（2026-11-10 新增）
    try:
        d = datetime.strptime((params.get("week_start") or "").strip(), "%Y-%m-%d")
    except Exception:
        d = datetime.now()
    monday = (d - timedelta(days=d.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    days = [monday + timedelta(days=i) for i in range(7)]

    # 窗口：往前 365 天 ~ 本周末（保证「早已创建、本周才计划/执行」的单也在范围内）
    win_b = (monday - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00")
    win_e = days[-1].strftime("%Y-%m-%dT23:59:59")
    tasks = search_tasks(win_b, win_e, deadline=deadline, progress=progress)
    week_end_ts = days[-1].replace(hour=23, minute=59, second=59)

    # ── 第一遍：筛出与本周有交集的工单（三条业务规则等价于区间重叠）──
    #   ① 开始/结束都在本周  ② 任一端在本周  ③ 跨过本周（开始早于周一、结束晚于周日）
    pre = []
    for t in tasks:
        if t.get("state") == "offed":           # 排除已取消（2026-11-02 修复口径）
            continue
        if mode == "summary":
            st, en = ts_any(t.get("startTime")), ts_any(t.get("completeTime"))
        else:
            # 周计划口径：计划开始为空 → 不取；有开始无结束 → 视为进行中，覆盖整周
            st, en = ts_any(t.get("planStartTime")), ts_any(t.get("planEndTime"))
        if not st:
            continue
        if not (st <= week_end_ts and (en is None or en >= monday)):
            continue
        pre.append((t, st, en))

    # ── 第二遍：取这些工单的详情，解析全部参与人（一单多人同时计入）──
    details = map_details([t for t, _s, _e in pre],
                          deadline=deadline, progress=progress) if pre else {}

    items = []
    for t, st, en in pre:
        cust = ((t.get("customerEntity") or {}).get("name")
                or (t.get("customer") or {}).get("name") or "")
        for person in _task_persons(t, details.get(t.get("id"))):
            if keyword and keyword not in person:
                continue
            items.append((t.get("taskNo"), t.get("templateName"), person, cust, st, en))

    cells, person_tasks = {}, {}
    for no, tname, person, cust, st, en in items:
        for i, day in enumerate(days):
            if st <= day.replace(hour=23, minute=59, second=59) and (en is None or en >= day):
                cells.setdefault((i, person), []).append((no, tname, cust))
                person_tasks.setdefault(person, set()).add(no)

    counts = {p: len(v) for p, v in person_tasks.items()}
    persons = sorted(counts, key=lambda p: (-counts[p], p))

    head_cols = ["负责人"]
    for i, day in enumerate(days):
        head_cols.append("%s<br>%s" % (WEEKDAY_CN[i], day.strftime("%m-%d")))

    out_rows = []
    for p in persons:
        row = {"负责人": esc(p) + "<br>" + str(counts[p])}
        for i, day in enumerate(days):
            parts = []
            day_items = sorted(cells.get((i, p)) or [], key=lambda x: x[0] or "")
            for n, (no, tname, cust) in enumerate(day_items, 1):
                short = tname or ""
                if short.endswith("工单") and len(short) > 2:
                    short = short[:-2]
                parts.append("%d. %s | %s" % (n, esc(short), esc(cust)))
            row[head_cols[i + 1]] = "<br>".join(parts)
        out_rows.append(row)

    return {"ok": True, "tab": "ol:weekly-plan", "title": TAB_TITLES["ol:weekly-plan"],
            "mode": mode, "rows": out_rows, "columns": head_cols,
            "total": len(out_rows), "page": 1, "page_size": max(1, len(out_rows)),
            "has_next": False, "has_prev": False, "total_pages": 1,
            "html_cells": True, "persons": persons, "online": True,
            "week_start": monday.strftime("%Y-%m-%d"),
            "week_end": days[-1].strftime("%Y-%m-%d")}


# ── 报表 3：项目进度2（在线）────────────────────────────────────────
def page_customers(update_start, deadline=None, progress=None):
    """客户档案分页（customer list --data，按更新时间过滤）；返回 list"""
    rows, page = [], 1
    while True:
        body = {"pageNum": page, "pageSize": 100}
        if update_start:
            body["updateTimeStart"] = update_start
        d = cli(["customer", "list", "--data", json.dumps(body, ensure_ascii=False), "-o", "raw"])
        part = d.get("list") or []
        rows.extend(part)
        if progress:
            progress("已拉客户档案 %d 条" % len(rows))
        tp = d.get("pages") or 0
        if not part or (tp and page >= tp):
            break
        page += 1
        _check(deadline, "拉取客户档案")
    return rows


def q_progress2(params, progress, deadline):
    keyword = (params.get("keyword") or "").strip()
    show_done = str(params.get("show_done") or "") == "1"
    b, e = d_range(params.get("date_begin"), params.get("date_end"))
    b_dt, e_dt = ts_any(b.replace("T", " ")), ts_any(e.replace("T", " "))

    customers = page_customers(b.replace("T", " "), deadline=deadline, progress=progress)

    # 时间窗过滤（客户更新时间落在区间内；取不到时间的不丢，交给字段判断）
    picked = []
    for c in customers:
        ut = ts_any(c.get("updateTime") or c.get("updatedTime") or c.get("createTime"))
        if ut and b_dt and e_dt and not (b_dt <= ut <= e_dt):
            continue
        picked.append(c)

    # 调试工单 → 客户名 → 调试人员（负责人 + 协同人）；只为命中的客户取详情
    names = {(c.get("name") or "").strip() for c in picked if c.get("name")}
    dbg_tasks = search_tasks(None, None, template_id=DEBUG_TASK_TID,
                             deadline=deadline, progress=progress)
    dbg_tasks = [t for t in dbg_tasks
                 if ((t.get("customerEntity") or {}).get("name") or "") in names]
    dbg_details = map_details(dbg_tasks, deadline=deadline, progress=progress)
    persons = {}
    for t in dbg_tasks:
        cname = ((t.get("customerEntity") or {}).get("name") or "").strip()
        if not cname:
            continue
        lst = persons.setdefault(cname, [])
        init = dbg_details.get(t.get("id")) or {}
        task = init.get("task") or {}
        ex = (t.get("executorName") or (t.get("executorUser") or {}).get("displayName")
              or (task.get("executor") or {}).get("displayName") or "").strip()
        if ex and ex not in lst:
            lst.append(ex)
        for u in (task.get("reAllotUserList")
                  or (task.get("attribute") or {}).get("reAllotUserList") or []):
            nm = (u or {}).get("userName") or (u or {}).get("displayName") or ""
            if nm and nm not in lst:
                lst.append(nm)

    rows = []
    for c in picked:
        attr = c.get("attribute") or {}
        serial = (c.get("serialNumber") or "").strip()
        cname = (c.get("name") or "").strip()
        if keyword and keyword not in serial and keyword not in cname:
            continue
        vals = {f: _date_only(cell(attr.get(f))) for f, _ in P2_DATE_FIELDS}
        if not any(vals.values()):
            continue                                  # 12 个日期全空 → 不显示
        run_date = vals.get("field_Brf7x05dyDVU9HOL", "")   # 运行日期 = 保运完毕日期
        if run_date and not show_done:
            continue
        row = {"客户编号": serial, "客户名称": cname}
        for f, label in P2_DATE_FIELDS:
            row[label] = vals.get(f, "")
        row["调试人员"] = ",".join(persons.get(cname, []))
        row["保运人员"] = cell(attr.get(P2_BAOYUN_PERSON))
        row["运行日期"] = row.get("保运完毕日期", "")
        rows.append(row)

    rows.sort(key=lambda r: (r.get("客户编号") or ""))
    seen, uniq = set(), []
    for r in rows:
        k = r.get("客户编号") or ""
        if k and k in seen:
            continue
        if k:
            seen.add(k)
        uniq.append(r)
    return _resp("ol:progress2", uniq, PROGRESS2_COLUMNS)


# ── 报表 4：服务人员工作量（在线）────────────────────────────────────
_WL_FMTS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


def _wl_cards(extra):
    """cardId → 卡片名 / 字段显示名→字段key / 卡片行数据（本地版同结构）"""
    names, fmap, rows = {}, {}, {}
    for c in (extra.get("cardInfo") or []):
        cid = c.get("cardId")
        names[cid] = c.get("cardName") or ""
        fmap[cid] = {f.get("displayName"): f.get("fieldName") for f in (c.get("fields") or [])}
    rt = extra.get("receiptTaskForUpdate") or {}
    for c in (rt.get("cardInfo") or []):
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
                    dt = ts_any(r.get(key))
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


def _wl_hours(a, b):
    if not a or not b:
        return ""
    return round((b - a).total_seconds() / 3600.0, 1)


def q_workload(params, progress, deadline):
    person = (params.get("keyword") or "").strip()
    b, e = rng(params, "week")            # 默认当周（周一~周日）
    tasks = search_tasks(b, e, deadline=deadline, progress=progress)
    if person:
        tasks = [t for t in tasks if person in (t.get("executorName") or "")]

    details = map_details(tasks, deadline=deadline, progress=progress)
    records = map_records(tasks, deadline=deadline, progress=progress)

    out = []
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        recs = records.get(t.get("id")) or []
        dep_ts = sorted({r.get("createTime") for r in recs
                         if r.get("action") == "出发" and r.get("createTime")})
        arr_ts = sorted({r.get("createTime") for r in recs
                         if r.get("action") == "到达" and r.get("createTime")})
        dep = ts_any(dep_ts[0]) if dep_ts else None
        arr = ts_any(arr_ts[-1]) if arr_ts else None
        extra = {"cardInfo": init.get("cardInfo"),
                 "receiptTaskForUpdate": init.get("receiptTaskForUpdate")}
        sign_in = _wl_card_time(extra, "到场签到", ("签到日期", "时间"), "min")
        sign_out = _wl_card_time(extra, "离场打卡", ("时间", "签到日期"), "max")
        cust = task.get("customer") or {}
        area = ""
        for tag in (cust.get("tags") or []):
            nm = (tag or {}).get("tagName") or ""
            if "片区" in nm:
                area = nm
                break
        addr = ((task.get("attribute") or {}).get("taskArriveAddress")
                or task.get("taddressStr") or "")
        out.append({
            "工单类型": t.get("templateName") or task.get("templateName") or "",
            "客户编号": cust.get("serialNumber") or "",
            "客户": ((t.get("customerEntity") or {}).get("name") or cust.get("name") or ""),
            "服务部门": area,
            "服务人员": (t.get("executorName")
                       or (t.get("executorUser") or {}).get("displayName") or ""),
            "省份": _wl_province(extra, addr),
            "出发打卡": dep.strftime("%Y-%m-%d %H:%M:%S") if dep else "",
            "到场打卡": arr.strftime("%Y-%m-%d %H:%M:%S") if arr else "",
            "途中时长": _wl_hours(dep, arr),
            "工单到场签到日期": sign_in.strftime("%Y-%m-%d %H:%M:%S") if sign_in else "",
            "工单离场签到日期": sign_out.strftime("%Y-%m-%d %H:%M:%S") if sign_out else "",
            "工单时长": _wl_hours(sign_in, sign_out),
        })
    persons = sorted({r["服务人员"] for r in out if r["服务人员"]})
    return _resp("ol:workload", out, WORKLOAD_COLUMNS, persons=persons)


# ── 报表 5：调试进度（在线）──────────────────────────────────────────
def _dp_ts(v):
    """动态 createTime（毫秒）→ 'YYYY-MM-DD HH:MM:SS'"""
    if not v:
        return ""
    dt = ts_any(v)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else str(v)


def q_debug(params, progress, deadline):
    """调试进度（明细）：一条调试内容一行

    列：序号 / 调试人员 / 项目编码 / 客户名称 / 调试内容 / 调试日期
    数据源：调试组件 —— 当前节点暂存 + 已提交字段，按 bizId 合并去重（暂存优先）
    日期筛：按调试内容自身的日期
    关键字：在结果行上过滤（项目编码/客户名称/调试人员/调试内容），命中缓存

    性能（2026-11-10 优化）：
      · 每个工单一个线程，同时取 task detail + 节点暂存（原来是两轮串行）
      · 不再拉工单动态（新版明细已无出发/到达列）→ 请求数减半
    """
    keyword = (params.get("keyword") or "").strip()
    b, e = rng(params, "week")                 # 日期区间（默认当周）
    b_day, e_day = b[:10], e[:10]
    _anchor = ts_any(b_day) or datetime.now()
    # 默认只查最近 30 天（2026-11-10 hunter 定）；带关键字时放宽到 730 天以覆盖历史
    back = 730 if keyword else 30
    win_b = (_anchor - timedelta(days=back)).strftime("%Y-%m-%dT00:00:00")
    tasks = search_tasks(win_b, e, template_id=DEBUG_TASK_TID,
                         deadline=deadline, progress=progress)

    if keyword:
        # 关键字可能是客户编号（BY…）或客户名：先用客户列表解析成客户名，再预筛工单
        names = set()
        try:
            cd = cli(["customer", "list", "--keyword", keyword, "-o", "raw"])
            for c in (cd.get("list") or []):
                nm = (c.get("name") or "").strip()
                if nm:
                    names.add(nm)
        except Exception:
            pass

        def _hit(t):
            if keyword in (t.get("taskNo") or ""):
                return True
            nm = ((t.get("customerEntity") or {}).get("name") or "")
            return keyword in nm or nm in names

        tasks = [t for t in tasks if _hit(t)]

    dmap, tmap = {}, {}

    def _one(t):
        tid = t.get("id")
        init = {}
        try:
            init = task_detail(tid)
        except Exception:
            init = {}
        _task = init.get("task") or {}
        nid = ((_task.get("attribute") or {}).get("currentNodeId")
               or (init.get("receiptTaskForUpdate") or {}).get("currentNodeId"))
        tmp = {}
        # 已完结/取消的工单基本不会有草稿 → 跳过暂存调用（省约 0.4s/单）
        if nid and (t.get("state") not in ("finished", "closed", "costed", "offed")):
            try:
                tmp = node_temp_storage(tid, nid)
            except Exception:
                tmp = {}
        return tid, init, tmp

    if tasks:
        total = len(tasks)
        done = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(_one, t) for t in tasks]
            for fut in as_completed(futs):
                _check(deadline, "取工单详情/暂存")
                tid, init, tmp = fut.result()
                dmap[tid] = init
                tmap[tid] = tmp
                done += 1
                if progress:
                    progress("已取 %d/%d 单" % (done, total))

    rows = []
    for t in tasks:
        init = dmap.get(t.get("id")) or {}
        task = init.get("task") or {}
        cust = task.get("customer") or {}
        serial = (cust.get("serialNumber") or t.get("customerId") or "")
        cname = ((t.get("customerEntity") or {}).get("name") or cust.get("name") or "")
        _sub_items = ((task.get("attribute") or {}).get(DBG_ITEMS_FIELD) or [])
        _tmp_items = ((tmap.get(t.get("id")) or {}).get(DBG_ITEMS_FIELD) or [])
        # 两边合并去重：暂存在前（同一 bizId 以暂存为准），再补已提交
        items = list(_tmp_items) + list(_sub_items)
        _seen, _uniq = set(), []
        for it in items:
            bid = (it or {}).get("bizId")
            if not bid:
                _persons = it.get(DBG_ITEM_PERSON) if isinstance(it.get(DBG_ITEM_PERSON), list) else []
                bid = "%s|%s|%s" % (it.get(DBG_ITEM_CONTENT), it.get(DBG_ITEM_TIME),
                                    (_persons[0] or {}).get("displayName") if _persons else "")
            if bid in _seen:
                continue
            _seen.add(bid)
            _uniq.append(it)
        for it in _uniq:
            content = str(it.get(DBG_ITEM_CONTENT) or "").strip()
            names = "、".join([(u or {}).get("displayName") or ""
                              for u in (it.get(DBG_ITEM_PERSON) or [])]).strip("、")
            day = _dp_ts(it.get(DBG_ITEM_TIME))[:10]
            if not content and not names:
                continue
            if day and (day < b_day or day > e_day):
                continue                      # 调试日期不在区间内
            if keyword and not any(keyword in str(x) for x in (serial, cname, names, content)):
                continue
            rows.append({"调试人员": names, "项目编码": serial, "客户名称": cname,
                         "调试内容": content, "调试日期": day})

    rows.sort(key=lambda r: (r["项目编码"] or "", r["调试日期"] or "", r["调试内容"] or ""))
    out = []
    for i, r in enumerate(rows, 1):
        out.append({"序号": i, "调试人员": r["调试人员"], "项目编码": r["项目编码"],
                    "客户名称": r["客户名称"], "调试内容": r["调试内容"], "调试日期": r["调试日期"]})
    return _resp("ol:debug-progress", out, DEBUG_COLUMNS)


# ── 统一响应 + 分发 ──────────────────────────────────────────────────
def _date_only(s):
    """日期字段统一成 YYYY-MM-DD（去掉多余的 00:00:00 时间部分）"""
    s = (s or "").strip()
    if not s:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    return "%s-%s-%s" % (m.group(1), m.group(2), m.group(3)) if m else s


def _resp(tab, rows, columns, persons=None, extra=None):
    data = {"ok": True, "tab": tab, "title": TAB_TITLES.get(tab, tab),
            "rows": rows, "columns": list(columns), "total": len(rows),
            "page": 1, "page_size": max(1, len(rows)),
            "has_next": False, "has_prev": False, "total_pages": 1,
            "online": True}
    if persons is not None:
        data["persons"] = persons
    if extra:
        data.update(extra)
    return data


HANDLERS = {
    "ol:trip": q_trip,
    "ol:weekly-plan": q_weekly,
    "ol:progress2": q_progress2,
    "ol:workload": q_workload,
    "ol:debug-progress": q_debug,
}


def query(tab, params, progress=None):
    """统一入口：tab → 云端取数 → 响应 dict；异常向上抛（由调用方转成失败任务）"""
    h = HANDLERS.get(tab)
    if not h:
        raise RuntimeError("未知在线报表：%s" % tab)
    deadline = time.time() + QUERY_BUDGET
    cb = progress or (lambda _msg: None)
    t0 = time.time()
    data = h(params or {}, cb, deadline)
    data["elapsed"] = round(time.time() - t0, 1)
    return data
