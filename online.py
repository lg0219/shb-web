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
MAINT_TASK_TID = "1d0a8205-e753-43b2-b811-9c970c12dab5"  # 保运工单类型
# 保运「服务内容」11 项（第 11 项为手填文本），列名与售后宝字段显示名一致（2026-11-11 核实）
MAINT_ITEMS = [
    ("field_Fdlx52zIjIrKAeLs", "1、检查控制系统是否显示正常"),
    ("field_dsSkKVBWER6TrzeN", "2、检查各阀门开关、控制是否正常"),
    ("field_7mCCgFC0OzxT5r2U", "3、排除储罐内液氮"),
    ("field_WJSDwBqVK3v2XAm6", "4、储罐进LNG"),
    ("field_hqI0u84UGX3uqeER", "5、管路漏点处理、冷紧"),
    ("field_KkHmn7g8H5bz7LPg", "6、培训卸车操作"),
    ("field_imFTbYT1HoJhW53J", "7、培训加液操作"),
    ("field_S0NlIfaLX4pQ1PHa", "8、培训站控系统"),
    ("field_U88OKT4q1ZMaNYHO", "9、培训收费系统"),
    ("field_54aaJjrmq6acfKlM", "10、安全培训"),
    ("field_jTtLugqBo0ndoXAR", "11、其他"),
]
MAINT_COLUMNS = ["项目编号", "项目名称", "开始时间", "完成时间"] + [d for _f, d in MAINT_ITEMS]

# 验收工单（2026-11-11 新增「验收进度」报表）
ACCEPT_TASK_TID = "8024da3e-6881-48d5-ac58-9d0d4ee1da0a"
ACCEPT_COLUMNS = ["负责人", "项目编号", "项目名称", "工单状态", "开始时间", "完成时间",
                  "验收类别", "是否合格", "整改内容"]

# 稽核检查（2026-11-11 新增）
AUDIT_TYPES = ("工程工单", "预调试工单", "调试工单", "验收工单", "保运工单")
AUDIT_COLUMNS = ["错误类型", "错误描述", "涉及人员", "工单类型", "工单号"]

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
WORKLOAD_COLUMNS = ["服务人员", "角色", "工单类型", "客户编号", "客户", "服务部门", "省份",
                    "出发打卡", "到场打卡", "途中时长",
                    "到场签到时间", "离场签到时间", "工单时长", "提示"]
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
    "ol:maintain-progress": "保运进度（在线）",
    "ol:accept-progress": "验收进度（在线）",
    "ol:audit-check": "稽核检查（在线）",
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
    failed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_one, t) for t in tasks]
        for fut in as_completed(futs):
            _check(deadline, "取工单详情")
            try:
                tid, init = fut.result()
            except Exception:            # 单条超时/失败不拖坡整次查询（2026-11-11）
                failed += 1
                continue
            out[tid] = init
            done += 1
            if progress:
                progress("已取 %d/%d 单" % (done, total))
    if failed and not out:
        raise RuntimeError("工单详情全部获取失败（%d 单）" % failed)
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
    failed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_one, t) for t in tasks]
        for fut in as_completed(futs):
            _check(deadline, "取工单动态")
            try:
                tid, rows = fut.result()
            except Exception:            # 单条失败不影响整体
                failed += 1
                continue
            out[tid] = rows
            done += 1
            if progress:
                progress("已取动态 %d/%d 单" % (done, total))
    if failed and not out:
        raise RuntimeError("工单动态全部获取失败（%d 单）" % failed)
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
    """客户档案分页（customer list --data，按更新时间过滤）；返回 list

    update_start 为空/None → 拉全量（2026-11-11：项目进度日期留空 = 不限时间）。
    pageSize 100→200（2026-11-11）：全量 1829 个客户只需 10 次分页调用。
    """
    rows, page = [], 1
    while True:
        body = {"pageNum": page, "pageSize": 200}
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
    # 日期口径（2026-11-11 hunter 确认）：
    #   · 有填关键字 → 关键字优先，不限时间（在全部客户里按编号/名称匹配）
    #   · 没填关键字且填了日期 → 按「客户档案最后更新日期」筛
    #   · 都没填 → 不限时间（拉全量，约 1829 个客户）
    _db = (params.get("date_begin") or "").strip()
    _de = (params.get("date_end") or "").strip()
    if keyword or not (_db or _de):
        b = e = ""
    else:
        b, e = d_range(_db, _de)          # 单边为空时 d_range 会补另一端
    b_dt = ts_any(b.replace("T", " ")) if b else None
    e_dt = ts_any(e.replace("T", " ")) if e else None

    customers = page_customers(b.replace("T", " ") if b else None,
                               deadline=deadline, progress=progress)

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


def card_sign_records(extra):
    """卡片签到记录 → {人名: {"in": [datetime], "out": [datetime]}}

    2026-11-11：到场/离场按「签到人」分开取（不再跨人取极值）。
    人名取卡片行的 userName，缺失时回退「签到人员」字段。
    """
    names, fmap, rows = _wl_cards(extra)
    out = {}
    for cid, cname in names.items():
        if cname not in ("到场签到", "离场打卡"):
            continue
        fm = fmap.get(cid) or {}
        for r in rows.get(cid) or []:
            u = (r.get("userName") or "").strip()
            if not u:
                for disp, key in fm.items():
                    if disp == "签到人员" and r.get(key):
                        u = cell(r.get(key)).strip()
                        break
            if not u:
                continue
            rec = out.setdefault(u, {"in": [], "out": []})
            for disp, key in fm.items():
                if not r.get(key):
                    continue
                if cname == "到场签到" and disp in ("签到日期", "时间"):
                    dt = ts_any(r.get(key))
                    if dt:
                        rec["in"].append(dt)
                elif cname == "离场打卡" and disp in ("时间", "签到日期"):
                    dt = ts_any(r.get(key))
                    if dt:
                        rec["out"].append(dt)
    return out


def person_work_hours(ins, outs, arrive_dt=None):
    """按人算工时（2026-11-11 hunter 定的业务口径）

    1) 同一天内：到场取最早、离场取最晚（同天误点重复签到）
    2) 跨天/跨夜：按「到场 → 下一次离场」配对，不按自然日拆
       （中间整天没签到的跳过，不影响配对）
    3) 只缺到场：用到达时间（同一天且早于离场时），否则当天 9:00
    4) 只缺离场：当天 17:00；若到场晚于 17:00 → 次日 17:00
    5) 到、离都没签：不计算

    返回 (hours|None, 实际签到到场列表, 实际签到离场列表, 提示列表)
    兑底值只参与计算，不返回列表（不在“签到时间”列里显示）
    """
    ins = sorted(x for x in (ins or []) if x)
    outs = sorted(x for x in (outs or []) if x)
    day_in, day_out = {}, {}
    for x in ins:
        d = x.date()
        if d not in day_in or x < day_in[d]:
            day_in[d] = x
    for x in outs:
        d = x.date()
        if d not in day_out or x > day_out[d]:
            day_out[d] = x

    def _fallback_out(t):
        """缺离场 → 当天 17:00；已过 17:00 则次日 17:00"""
        b = t.replace(hour=17, minute=0, second=0, microsecond=0)
        return b + timedelta(days=1) if b <= t else b

    def _fallback_in(t):
        """缺到场 → 当天 9:00；9:00 晚于离场则前一天 9:00"""
        b = t.replace(hour=9, minute=0, second=0, microsecond=0)
        return b - timedelta(days=1) if b > t else b

    total = 0.0
    n = 0
    tips = []
    used_in, used_out = [], []
    pending = None          # 待配对的到场

    for d in sorted(set(day_in) | set(day_out)):
        di, do = day_in.get(d), day_out.get(d)
        if di:
            if pending:                     # 上一个到场跨天失败 → 缺离场
                fb = _fallback_out(pending)
                total += (fb - pending).total_seconds() / 3600
                n += 1
                tips.append("%s 缺离场签到，按 17:00 计算" % pending.strftime("%m-%d"))
                used_in.append(pending)
            pending = di
        if do:
            if pending:
                total += (do - pending).total_seconds() / 3600
                n += 1
                used_in.append(pending)
                used_out.append(do)
                pending = None
            else:                           # 孤离场 → 缺到场
                base, why = None, ""
                if arrive_dt and arrive_dt <= do and arrive_dt.date() == d:
                    base, why = arrive_dt, "按到达时间计算"
                else:
                    base, why = _fallback_in(do), "按 9:00 计算"
                total += (do - base).total_seconds() / 3600
                n += 1
                tips.append("%s 缺到场签到，%s" % (do.strftime("%m-%d"), why))
                used_out.append(do)

    if pending:
        fb = _fallback_out(pending)
        total += (fb - pending).total_seconds() / 3600
        n += 1
        tips.append("%s 缺离场签到，按 17:00 计算" % pending.strftime("%m-%d"))
        used_in.append(pending)

    if n == 0:
        return None, [], [], []
    return round(total, 1), sorted(used_in), sorted(used_out), tips


def q_workload(params, progress, deadline):
    kw = (params.get("keyword") or "").strip()
    b, e = rng(params, "week")            # 默认当周（周一~周日）
    tasks = search_tasks(b, e, deadline=deadline, progress=progress)

    details = map_details(tasks, deadline=deadline, progress=progress)
    records = map_records(tasks, deadline=deadline, progress=progress)

    out = []
    persons = set()
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
        cust = task.get("customer") or {}
        area = ""
        for tag in (cust.get("tags") or []):
            nm = (tag or {}).get("tagName") or ""
            if "片区" in nm:
                area = nm
                break
        addr = ((task.get("attribute") or {}).get("taskArriveAddress")
                or task.get("taddressStr") or "")
        base = {
            "工单类型": t.get("templateName") or task.get("templateName") or "",
            "客户编号": cust.get("serialNumber") or "",
            "客户": ((t.get("customerEntity") or {}).get("name") or cust.get("name") or ""),
            "服务部门": area,
            "省份": _wl_province(extra, addr),
            "出发打卡": dep.strftime("%Y-%m-%d %H:%M:%S") if dep else "",
            "到场打卡": arr.strftime("%Y-%m-%d %H:%M:%S") if arr else "",
            "途中时长": _wl_hours(dep, arr),
        }
        boss = (t.get("executorName")
                or (t.get("executorUser") or {}).get("displayName")
                or (task.get("executor") or {}).get("displayName") or "").strip()
        sig = card_sign_records(extra)
        if not sig:
            row = dict(base)
            row.update({"服务人员": boss, "角色": "负责人" if boss else "",
                        "到场签到时间": "", "离场签到时间": "",
                        "工单时长": "", "提示": "无签到记录", "__warn": 0, "__tips": ""})
            out.append(row)
            if boss:
                persons.add(boss)
            continue
        # 一单多人 → 一人一行（负责人与协同人分开统计）
        for user, rec in sig.items():
            hours, used_in, used_out, tips = person_work_hours(rec["in"], rec["out"], arr)
            row = dict(base)
            row["服务人员"] = user
            row["角色"] = "负责人" if user == boss else "协同人"
            row["到场签到时间"] = "、".join(x.strftime("%m-%d %H:%M") for x in used_in)
            row["离场签到时间"] = "、".join(x.strftime("%m-%d %H:%M") for x in used_out)
            row["工单时长"] = hours if hours is not None else ""
            row["提示"] = "；".join(tips)
            row["__warn"] = 1 if tips else 0
            row["__tips"] = "；".join(tips)
            out.append(row)
            persons.add(user)
    # 关键字过滤（2026-11-11）：客户编号 / 客户 / 服务人员 / 工单类型
    if kw:
        out = [r for r in out
               if kw in (r.get("客户编号") or "") or kw in (r.get("客户") or "")
               or kw in (r.get("服务人员") or "") or kw in (r.get("工单类型") or "")]
    return _resp("ol:workload", out, WORKLOAD_COLUMNS,
                 persons=sorted({r["服务人员"] for r in out if r.get("服务人员")}))


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
    # 时间口径（2026-11-11 hunter 定）：
    #   · 有填关键字 → 忽略时间条件：工单列表不限起始/截止时间，结果也不按调试日期过滤
    #   · 无关键字   → 只查最近 30 天
    if keyword:
        win_b, win_e, date_filter = None, None, False
    else:
        win_b = (_anchor - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00")
        win_e, date_filter = e, True
    tasks = search_tasks(win_b, win_e, template_id=DEBUG_TASK_TID,
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
            if date_filter and day and (day < b_day or day > e_day):
                continue                      # 无关键字时按调试日期筛区间
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


# ── 报表 6：保运进度（在线）──────────────────────────────────
def q_maint(params, progress, deadline):
    """保运进度（在线）：一个项目（客户）一行

    口径（2026-11-11 hunter 确认）：
      · 筛选时间 = 工单开始时间，默认当月
      · 同一客户多单 → 取创建日期最近的一单
      · 项目编号/项目名称 = 客户编号/客户名称
      · 服务内容 11 项 = 已提交值 + 暂存值合并（暂存优先）
    """
    keyword = (params.get("keyword") or "").strip()
    b, e = rng(params, "month")
    # 关键字优先（2026-11-11 hunter 定）：有填关键字 → 不限时间，
    # 在项目编号 / 项目名称 / 工单号上做包含匹配
    if keyword:
        b_dt = e_dt = None
    else:
        b_dt = ts_any(b.replace("T", " ")) if b else None
        e_dt = ts_any(e.replace("T", " ")) if e else None

    tasks = search_tasks(None, None, template_id=MAINT_TASK_TID,
                         deadline=deadline, progress=progress)
    tasks = [t for t in tasks if (t.get("templateName") or "") == "保运工单"]
    details = map_details(tasks, deadline=deadline, progress=progress) if tasks else {}

    # 先按关键字/「开始时间」筛，再按客户取创建时间最近的一单
    best = {}
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        cust = task.get("customer") or {}
        serial = (cust.get("serialNumber") or "").strip()
        cname = (cust.get("name") or (t.get("customerEntity") or {}).get("name") or "").strip()
        if keyword and not (keyword in (t.get("taskNo") or "")
                            or keyword in serial or keyword in cname):
            continue
        st = ts_any(task.get("startTime"))
        if not st:
            if not keyword:          # 无关键字时必须有开始时间才能判月份
                continue
        elif b_dt and e_dt and not (b_dt <= st <= e_dt):
            continue
        key = serial or cname
        if not key:
            continue
        ct = ts_any(t.get("createTime") or task.get("createTime")) or datetime.min
        cur = best.get(key)
        if cur is None or ct > cur[0]:
            best[key] = (ct, t, task)

    rows = []
    for _key, (_ct, t, task) in best.items():
        attr = task.get("attribute") or {}
        tmp = {}
        if t.get("state") not in ("finished", "closed", "costed", "offed"):
            nid = attr.get("currentNodeId")
            if nid:
                try:
                    tmp = node_temp_storage(t.get("id"), nid)
                except Exception:
                    tmp = {}
        cust = task.get("customer") or {}
        serial = (cust.get("serialNumber") or "").strip()
        cname = (cust.get("name") or (t.get("customerEntity") or {}).get("name") or "").strip()
        st = ts_any(task.get("startTime"))
        ft = ts_any(task.get("completeTime"))
        row = {"项目编号": serial, "项目名称": cname,
               "开始时间": st.strftime("%Y-%m-%d %H:%M:%S") if st else "",
               "完成时间": ft.strftime("%Y-%m-%d %H:%M:%S") if ft else ""}
        for f, label in MAINT_ITEMS:
            v = tmp.get(f)
            if v is None or v == "" or v == [] or v == {}:
                v = attr.get(f)
            row[label] = cell(v)
        rows.append(row)

    rows.sort(key=lambda r: (r.get("项目编号") or "", r.get("项目名称") or ""))
    return _resp("ol:maintain-progress", rows, MAINT_COLUMNS)


# ── 报表 7：验收进度（在线）──────────────────────────────────
def _subform_text(v):
    """子表单（list[dict]）→ 一行文字：逐行拼接非空值，多行用「；」连接"""
    if not isinstance(v, list):
        return cell(v)
    out = []
    for row in v:
        if not isinstance(row, dict):
            continue
        vals = [cell(x).strip() for k, x in row.items()
                if k != "subFormContentId" and cell(x).strip()]
        if vals:
            out.append("；".join(vals))
    return "；".join(out)


def q_accept(params, progress, deadline):
    """验收进度（在线）：一单一行

    口径（2026-11-11 hunter 确认）：
      · 筛选时间 = 工单开始时间，默认当月；有关键字时不限时间
      · 关键字 = 项目编号 / 项目名称 / 工单号
      · 验收类别以验收节点值为准，为空回退工单级值
      · 整改内容 = 「整改项」子表单
      · 已提交值 + 当前节点暂存值合并（暂存优先）；结果走缓存（30 分钟）
    """
    keyword = (params.get("keyword") or "").strip()
    b, e = rng(params, "month")
    if keyword:
        b_dt = e_dt = None
    else:
        b_dt = ts_any(b.replace("T", " ")) if b else None
        e_dt = ts_any(e.replace("T", " ")) if e else None

    tasks = search_tasks(None, None, template_id=ACCEPT_TASK_TID,
                         deadline=deadline, progress=progress)
    tasks = [t for t in tasks if (t.get("templateName") or "") == "验收工单"]
    details = map_details(tasks, deadline=deadline, progress=progress) if tasks else {}

    rows = []
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        cust = task.get("customer") or {}
        serial = (cust.get("serialNumber") or "").strip()
        cname = (cust.get("name") or (t.get("customerEntity") or {}).get("name") or "").strip()
        if keyword and not (keyword in (t.get("taskNo") or "")
                            or keyword in serial or keyword in cname):
            continue
        st = ts_any(task.get("startTime"))
        if not st:
            if not keyword:
                continue
        elif b_dt and e_dt and not (b_dt <= st <= e_dt):
            continue

        attr = task.get("attribute") or {}
        tmp = {}
        if t.get("state") not in ("finished", "closed", "costed", "offed"):
            nid = attr.get("currentNodeId")
            if nid:
                try:
                    tmp = node_temp_storage(t.get("id"), nid)
                except Exception:
                    tmp = {}

        def pick(f):
            v = tmp.get(f)
            if v is None or v == "" or v == [] or v == {}:
                v = attr.get(f)
            return v

        ex = ((task.get("executor") or {}).get("displayName")
              or (t.get("executorUser") or {}).get("displayName")
              or t.get("executorName") or "")
        ft = ts_any(task.get("completeTime"))
        cat = cell(pick("field_xF3USPMV2qusrPcd")) or cell(pick("field_gZKddPgQ15cYoPHu"))
        rows.append({
            "负责人": ex,
            "项目编号": serial,
            "项目名称": cname,
            "工单状态": STATE_CN.get(t.get("state"), t.get("state") or ""),
            "开始时间": st.strftime("%Y-%m-%d %H:%M:%S") if st else "",
            "完成时间": ft.strftime("%Y-%m-%d %H:%M:%S") if ft else "",
            "验收类别": cat,
            "是否合格": cell(pick("field_Kj4IeaWFxaNj4FiJ")),
            "整改内容": _subform_text(pick("field_mOho8BGmdv9B1FGu")),
        })

    rows.sort(key=lambda r: (r.get("开始时间") or "", r.get("项目编号") or ""), reverse=True)
    return _resp("ol:accept-progress", rows, ACCEPT_COLUMNS)


# ── 报表 8：稽核检查（在线）──────────────────────────────────
def q_audit_check(params, progress, deadline):
    """稽核检查（在线）：一工单一行，多种错误多行

    口径（2026-11-11 hunter 确认）：
      1) 重复工单：同客户编号 + 同工单类型 >1 单即报
         范围 = 工程/预调试/调试/验收/保运；涉及人员取建单人；工单号多个换行
      2) 未签到：只看已开始（有开始时间）的工单，**按人**判
         缺到场→「缺到场签到」；缺离场→「缺离场打卡」；都缺→「未打卡」
      3) 筛选：按工单**创建时间**，默认当月；关键字按姓名（涉及人员）
    """
    keyword = (params.get("keyword") or "").strip()
    types = [x.strip() for x in (params.get("types") or "").split(",") if x.strip()]
    b, e = rng(params, "month")
    tasks = search_tasks(b, e, deadline=deadline, progress=progress)
    tasks = [t for t in tasks if (t.get("templateName") or "") in AUDIT_TYPES]
    details = map_details(tasks, deadline=deadline, progress=progress) if tasks else {}

    rows = []

    # ── 1) 重复工单 ──
    groups = {}
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        serial = ((task.get("customer") or {}).get("serialNumber") or "").strip()
        typ = t.get("templateName") or ""
        if not serial or not typ:
            continue
        groups.setdefault((serial, typ), []).append((t, task))
    for (serial, typ), lst in groups.items():
        if len(lst) < 2:
            continue
        makers, nos = [], []
        for t, task in lst:
            no = (t.get("taskNo") or "").strip()
            if no:
                nos.append(no)
            mk = ((task.get("createUser") or {}).get("displayName")
                  or (t.get("createUserName") or "") or "").strip()
            if mk and mk not in makers:
                makers.append(mk)
        rows.append({
            "错误类型": "重复工单",
            "错误描述": "保运工单之前的所有工单，一个项目只能一次工单，%s这个项目有%d个工单。"
                        % (serial, len(lst)),
            "涉及人员": "、".join(makers),
            "工单类型": typ,
            "工单号": "\n".join(nos),
        })

    # ── 2) 未签到（按人）──
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        if not ts_any(task.get("startTime")):
            continue                      # 没开始的不查打卡
        extra = {"cardInfo": init.get("cardInfo"),
                 "receiptTaskForUpdate": init.get("receiptTaskForUpdate")}
        sig = card_sign_records(extra)
        # 参与人 = 协同人（含转派协同名单）+ 实际签到人；**不强制包含负责人**
        # （2026-11-11 hunter 定：参与人就是协同人，负责人不一定去现场）
        people = []
        _syn = [(x or {}).get("displayName") or (x or {}).get("userName")
                for x in (task.get("synergies") or [])]
        _syn += [(x or {}).get("displayName") or (x or {}).get("userName")
                 for x in (task.get("reAllotUserList")
                           or (task.get("attribute") or {}).get("reAllotUserList") or [])]
        for u in _syn + list(sig):
            u = (u or "").strip()
            if u and u not in people:
                people.append(u)
        for u in people:
            rec = sig.get(u) or {"in": [], "out": []}
            has_in, has_out = bool(rec["in"]), bool(rec["out"])
            if has_in and has_out:
                continue
            if not has_in and not has_out:
                desc = "未打卡"
            elif not has_in:
                desc = "缺到场签到"
            else:
                desc = "缺离场打卡"
            rows.append({
                "错误类型": "未签到",
                "错误描述": desc,
                "涉及人员": u,
                "工单类型": t.get("templateName") or "",
                "工单号": (t.get("taskNo") or ""),
            })

    # ── 3) 计划日期未填 ──
    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        ps = (task.get("planStartTime") or "").strip()
        pe = (task.get("planEndTime") or "").strip()
        if ps and pe:
            continue
        if not ps and not pe:
            desc = "计划时间未填"
        elif not ps:
            desc = "计划开始时间未填"
        else:
            desc = "计划结束时间未填"
        boss = ((task.get("executor") or {}).get("displayName")
                or (t.get("executorUser") or {}).get("displayName")
                or t.get("executorName") or "").strip()
        rows.append({
            "错误类型": "计划日期未填",
            "错误描述": desc,
            "涉及人员": boss,
            "工单类型": t.get("templateName") or "",
            "工单号": (t.get("taskNo") or ""),
        })

    if types:
        rows = [r for r in rows if r.get("错误类型") in types]
    if keyword:
        rows = [r for r in rows if keyword in (r.get("涉及人员") or "")]
    rows.sort(key=lambda r: (r.get("错误类型") or "", r.get("工单号") or ""))
    return _resp("ol:audit-check", rows, AUDIT_COLUMNS)


# ── 统一响应 + 分发 ─────────────────────────────────────────────────────
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
    "ol:maintain-progress": q_maint,
    "ol:accept-progress": q_accept,
    "ol:audit-check": q_audit_check,
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
