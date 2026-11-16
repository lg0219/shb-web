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

import weekly_core  # noqa: E402  周计划公共算法层（本地/在线共用）
import workload_core  # noqa: E402  工作量公共算法层（本地/在线共用）
import debug_core  # noqa: E402  调试进度公共算法层（本地/在线共用）

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

# 在线工单查询（2026-11-13 hunter 定：先做工程工单）
ENGINEER_TASK_TID = "d5f0af73-6c27-4c8a-8c01-fca8d2ca0a16"
TASK_QUERY_COLUMNS = ["工单编号", "工单类型", "客户编号", "客户", "状态", "负责人",
                      "创建时间", "更新日期", "计划开始", "计划结束"]
# 问题工单（PAAS 表单，与本地 bkent:paas 同一个模板 biz id）
ISSUE_TEMPLATE_BIZ_ID = "d8c34f70-4f20-4c31-a1da-825d63582cd5"
# 8 个工单类型（2026-11-13：先放在线查询里，类型条可点选）
TASK_TYPES = {
    "工程工单": "d5f0af73-6c27-4c8a-8c01-fca8d2ca0a16",
    "预调试工单": "aa7f2af4-8b83-4bd4-ac48-b92fce5cca6e",
    "调试工单": "f6690742-8462-42c9-b7dd-fe36ea405eb6",
    "验收工单": "8024da3e-6881-48d5-ac58-9d0d4ee1da0a",
    "保运工单": "1d0a8205-e753-43b2-b811-9c970c12dab5",
    "售后工单": "4993cd38-3bff-4c79-8178-1089ee3ce95e",
    "日常工单": "c33632ed-7bcb-4285-a17e-bc6ea8470e9b",
    "改造工单": "25daa54e-9cc7-4be1-a22f-b235af3fc40d",
}

# 稽核检查（2026-11-11 新增）
AUDIT_TYPES = ("工程工单", "预调试工单", "调试工单", "验收工单", "保运工单")
# 2026-11-12 hunter 定：工单号后新增「时间」列 —— 重复工单=建单时间（多单用、连接）；
# 漏打卡=缺卡日期（哪天漏的）；计划日期未填 / 验收整改 不涉及 → 留空
AUDIT_COLUMNS = ["错误类型", "错误描述", "涉及人员", "工单类型", "工单号", "时间"]

STATE_CN = {"created": "待指派", "allocated": "已指派", "accepted": "已接受",
            "processing": "进行中", "finished": "已完成", "refused": "已拒绝",
            "costed": "已结算", "closed": "已关闭", "offed": "已取消",
            "taskPool": "工单池"}
# 状态中文名 → 代码（状态多选筛选用）
STATE_REV = {v: k for k, v in STATE_CN.items()}

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
    "ol:task-query": "工单查询",
    "ol:issue-tasks": "问题工单",
    "ol:work-report": "工单管理",
    "ol:task-map": "工单地图",
    "ol:customer-map": "客户地图",
    "ol:staff-map": "人员地图",
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
    url = "https://%s%s" % (domain, path)
    if params:
        url += "?" + urllib.parse.urlencode(params)
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
    if kind == "day":                      # 2026-11-16：当天 00:00:00 ~ 23:59:59
        today = datetime.now().strftime("%Y-%m-%d")
        return d_range(today, today)
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


def search_tasks_by_conditions(conds, template_id=None, deadline=None, progress=None, label="条件筛选"):
    """按高级搜索条件（systemConditions）分页拉工单列表

    ⚠️ systemConditions 的时间必须 'YYYY-MM-DD HH:mm:ss'，不能用 ISO 的 T 格式。
    """
    rows, page = [], 1
    while True:
        body = {"page": page, "pageSize": PAGE_SIZE, "systemConditions": conds}
        if template_id:
            body["templateId"] = template_id
        d = cli(["task", "search", "--data", json.dumps(body, ensure_ascii=False), "-o", "raw"])
        content = d.get("content") or []
        rows.extend(content)
        if progress:
            progress("已拉工单列表（%s）%d 条" % (label, len(rows)))
        tp = d.get("totalPages") or 0
        if not content or page >= tp:
            break
        page += 1
        _check(deadline, "拉取工单列表")
    return rows


def search_tasks_by_update(update_begin, update_end, template_id=None, deadline=None, progress=None):
    """按「更新时间」区间拉工单列表（2026-11-12 hunter 定：行程单改用此口径）

    依据：打卡会更新工单，updateTime 落在区间内的单就包含当月有活动的单（含跨月单，
    解决「按建单时间取数漏掉 10 月建单、11 月还在打卡」的问题）。

    ⚠️ 但搜索索引的 updateTime 可能滞后（打卡不刷新它，如 TS2026000144：列表 10-31、详情 11-03），
    所以 q_trip 里还会并上「已接受~完成」状态区间的工单。
    """
    def _fmt(s):
        return (s or "").replace("T", " ")[:19]
    conds = [{"property": "updateTime", "operator": "between",
              "betweenValue1": _fmt(update_begin), "betweenValue2": _fmt(update_end)}]
    return search_tasks_by_conditions(conds, template_id=template_id, deadline=deadline,
                                     progress=progress, label="按更新日期")


def task_detail(task_id):
    """工单详情（完整 initJson：task 主体 + 卡片定义 + 回执/附加组件）"""
    d = cli(["task", "detail", "--id", task_id, "-o", "raw"])
    return d.get("initJson") or {}


def task_card_list(task_id):
    """附加组件实例列表（真实打卡记录）

    2026-11-12：task detail 的 receiptTaskForUpdate.cardInfo 对部分单为空（如 YTS2026000039，
    明明有打卡但卡片行为空），真实数据要用 task card list 读。返回卡片列表（含 cardId / attribute 行）。
    """
    d = cli(["task", "card", "list", "--task-id", task_id, "-o", "raw"])
    return d if isinstance(d, list) else [d]


def _has_card_rows(init):
    for src in ("receiptTaskForRead", "receiptTaskForUpdate"):
        rt = init.get(src) or {}
        for c in (rt.get("cardInfo") or []):
            if c.get("attribute"):
                return True
    return False


def _fill_cards_fallback(task_id, init):
    """回执里读不到卡片行时，用 task card list 兜底补齐到 receiptTaskForUpdate.cardInfo

    只改内存结构，下游 _wl_cards / card_sign_records / _card_rows 无需改动。
    """
    try:
        lst = task_card_list(task_id)
    except Exception:
        return
    rows = []
    for c in (lst or []):
        if not isinstance(c, dict) or not c.get("cardId"):
            continue
        if not (c.get("attribute") or []):
            continue
        rows.append({"cardId": c.get("cardId"),
                     "cardName": c.get("cardName") or "",
                     "inputType": c.get("inputType") or "multiple",
                     "attribute": c.get("attribute") or []})
    if not rows:
        return
    rt = init.get("receiptTaskForUpdate") or {}
    rt["cardInfo"] = (rt.get("cardInfo") or []) + rows
    init["receiptTaskForUpdate"] = rt


def map_details(tasks, deadline=None, progress=None):
    """8 路并发拉工单详情 → {task_id: initJson}

    2026-11-12：详情回执里没有卡片行时，自动用 task card list 兜底（只对空卡片的单多一次请求）
    """
    out = {}
    total = len(tasks)
    if not total:
        return out

    def _one(t):
        tid = t.get("id")
        init = task_detail(tid)
        if not _has_card_rows(init):
            _fill_cards_fallback(tid, init)
        return tid, init

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
    # 2026-11-12 hunter 定：候选工单 = 【更新日期在区间】∪【已接受~完成区间内、时间与区间有交集的单】
    #   原因：搜索索引的 updateTime 可能滞后（打卡不刷新它），单靠更新日期会漏（如 TS2026000144）
    def _fmt(s):
        return (s or "").replace("T", " ")[:19]
    _b, _e = _fmt(b), _fmt(e)
    _cand = {}

    def _merge(lst):
        for x in lst:
            if x.get("id"):
                _cand.setdefault(x["id"], x)

    _merge(search_tasks_by_update(b, e, deadline=deadline, progress=progress))
    for _conds, _label in (
        ([{"property": "state", "operator": "in", "inValue": ["accepted", "processing", "finished"]},
          {"property": "startTime", "operator": "le", "value": _e},
          {"property": "completeTime", "operator": "ge", "value": _b}], "已接受~完成·区间内"),
        ([{"property": "state", "operator": "in", "inValue": ["accepted", "processing"]},
          {"property": "startTime", "operator": "le", "value": _e}], "未完成·跨月"),
    ):
        try:
            _merge(search_tasks_by_conditions(_conds, deadline=deadline, progress=progress, label=_label))
        except Exception:
            pass                      # 补集查询失败不影响主口径
    tasks = list(_cand.values())
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
            # 2026-11-12 hunter 定：到场/离场按「自然日」配对
            #   原为数组下标配对（第1个到场配第1个离场），跨天会错位并丢掉「只有离场」的那天
            byday = {}
            for r in (cards.get((u, "到场签到")) or []):
                if not r.get("t"):
                    continue
                d = r["t"].strftime("%Y-%m-%d")
                cur = byday.setdefault(d, {"in": None, "in_loc": "", "out": None, "out_loc": ""})
                if cur["in"] is None or r["t"] < cur["in"]:      # 同天多条：到场取最早
                    cur["in"], cur["in_loc"] = r["t"], r.get("loc") or ""
            for r in (cards.get((u, "离场打卡")) or []):
                if not r.get("t"):
                    continue
                d = r["t"].strftime("%Y-%m-%d")
                cur = byday.setdefault(d, {"in": None, "in_loc": "", "out": None, "out_loc": ""})
                if cur["out"] is None or r["t"] > cur["out"]:    # 同天多条：离场取最晚
                    cur["out"], cur["out_loc"] = r["t"], r.get("loc") or ""
            for _d, cur in byday.items():
                recs.append({"name": u, "in": cur["in"], "in_loc": cur["in_loc"],
                             "out": cur["out"], "out_loc": cur["out_loc"],
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
    # 结果只保留打卡日期落在所选区间内的行（2026-11-12：候选按更新时间取，可能带入区间外的打卡）
    _bd, _ed = b[:10], e[:10]
    for (name, date), g in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        if not (_bd <= date <= _ed):
            continue
        rows.append({"姓名": name, "日期": date,
                     "到场地点": g["in_loc"], "离场地点": g["out_loc"],
                     "工单编号": "\n".join(g["taskNo"]), "工单类型": "\n".join(g["type"]),
                     "工作内容": "；".join(g["work"])})
    for i, r in enumerate(rows, 1):
        r["序号"] = i
    rows = [{k: r[k] for k in TRIP_COLUMNS} for r in rows]
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
    """周计划 / 周总结（在线）

    2026-11-12：计算逻辑抽到 weekly_core.build_weekly()，与本地报表共用同一套算法，只换数据源。
    """
    mode = (params.get("mode") or "plan").strip()
    keyword = (params.get("keyword") or "").strip()      # 姓名筛选（2026-11-10 新增）
    monday, days = weekly_core.week_days(params.get("week_start"))

    # 窗口：往前 365 天 ~ 本周末（保证「早已创建、本周才计划/执行」的单也在范围内）
    win_b = (monday - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00")
    win_e = days[-1].strftime("%Y-%m-%dT23:59:59")
    tasks = search_tasks(win_b, win_e, deadline=deadline, progress=progress)

    # 只为与本周有交集的单取详情（与算法层同一判定，避免多余请求）
    week_end_ts = days[-1].replace(hour=23, minute=59, second=59)
    pre = []
    for t in tasks:
        if (t.get("state") or "") in ("offed",):
            continue
        if mode == "summary":
            st, en = ts_any(t.get("startTime")), ts_any(t.get("completeTime"))
        else:
            st, en = ts_any(t.get("planStartTime")), ts_any(t.get("planEndTime"))
        if not st:
            continue
        if not (st <= week_end_ts and (en is None or en >= monday)):
            continue
        pre.append(t)
    details = map_details(pre, deadline=deadline, progress=progress) if pre else {}

    out = weekly_core.build_weekly(tasks, details, monday, days, mode=mode, keyword=keyword)

    return {"ok": True, "tab": "ol:weekly-plan", "title": TAB_TITLES["ol:weekly-plan"],
            "mode": mode, "rows": out["rows"], "columns": out["columns"],
            "total": len(out["rows"]), "page": 1, "page_size": max(1, len(out["rows"])),
            "has_next": False, "has_prev": False, "total_pages": 1,
            "html_cells": True, "persons": out["persons"], "online": True,
            "week_start": out["week_start"], "week_end": out["week_end"]}


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
    """服务人员工作量（在线）

    2026-11-12：计算抽到 workload_core.build_workload()，与本地报表共用同一套算法，只换数据源。
    """
    kw = (params.get("keyword") or "").strip()
    b, e = rng(params, "week")            # 默认当周（周一~周日）
    tasks = search_tasks(b, e, deadline=deadline, progress=progress)
    details = map_details(tasks, deadline=deadline, progress=progress)
    records = map_records(tasks, deadline=deadline, progress=progress)

    out, persons = workload_core.build_workload(
        tasks, details, records, kw,
        ts=ts_any, cards=card_sign_records, hours=person_work_hours, province=_wl_province)
    return _resp("ol:workload", out, WORKLOAD_COLUMNS, persons=persons)


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

    # 2026-11-12：行构建抽到 debug_core.build_debug()，与本地报表共用同一套算法
    out = debug_core.build_debug(tasks, dmap, tmap, keyword,
                                 date_filter=date_filter, b_day=b_day, e_day=e_day,
                                 ts=_dp_ts)
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


def _subform_has_content(v):
    """子表单是否有实际内容（2026-11-11 hunter 定：不存在 / 空数组 / 各行字段全空 → 都算空）"""
    if v in (None, "", [], {}):
        return False
    if not isinstance(v, list):
        if isinstance(v, dict):
            return bool(v)
        return bool(cell(v).strip())
    for row in v:
        if not isinstance(row, dict):
            if cell(row).strip():
                return True
            continue
        for k, x in row.items():
            if k == "subFormContentId":      # 子表单行 ID，不算内容
                continue
            if cell(x).strip():
                return True
    return False


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
      2) 漏打卡（原「未签到」，2026-11-11 hunter 改名；2026-11-12 改为**全部工单类型**）：只看已开始（有开始时间）的工单，**按人**判
         缺到场→「缺到场签到」；缺离场→「缺离场打卡」；都缺→「未打卡」
      3) 筛选：按工单**创建时间**，默认当月；关键字按姓名（涉及人员）
    """
    keyword = (params.get("keyword") or "").strip()
    types = [x.strip() for x in (params.get("types") or "").split(",") if x.strip()]
    # 2026-11-16 hunter 定：默认筛当天（原为当月）
    b, e = rng(params, "day")
    tasks = search_tasks(b, e, deadline=deadline, progress=progress)
    # 2026-11-12 hunter 定：**漏打卡查全部工单类型**；重复工单只查这 5 类；
    #   计划日期未填 = 除工程工单外全部类型（工程工单有计划日期要求另行处理，不查此项）
    audit_tasks = [t for t in tasks if (t.get("templateName") or "") in AUDIT_TYPES]
    # 详情：漏打卡查全部 + 计划日期未填查（除工程外）全部 → 等于全量拉
    details = map_details(tasks, deadline=deadline, progress=progress) if tasks else {}

    rows = []

    # ── 1) 重复工单（仅 5 类：工程/预调试/调试/验收/保运）──
    groups = {}
    for t in audit_tasks:
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
        makers, nos, times = [], [], []
        for t, task in lst:
            no = (t.get("taskNo") or "").strip()
            if no:
                nos.append(no)
            mk = ((task.get("createUser") or {}).get("displayName")
                  or (t.get("createUserName") or "") or "").strip()
            if mk and mk not in makers:
                makers.append(mk)
            # 「时间」= 各工单建单时间（2026-11-12 hunter 定）
            ct = ts_any(t.get("createTime") or task.get("createTime"))
            if ct:
                times.append(ct.strftime("%Y-%m-%d"))
        rows.append({
            "错误类型": "重复工单",
            "错误描述": "保运工单之前的所有工单，一个项目只能一次工单，%s这个项目有%d个工单。"
                        % (serial, len(lst)),
            "涉及人员": "、".join(makers),
            "工单类型": typ,
            "工单号": "\n".join(nos),
            "时间": "\n".join(times),
        })

    # ── 2) 漏打卡（全部工单类型，按人；原「未签到」，2026-11-11 hunter 改名）──
    for t in tasks:
        init = details.get(t.get("id"))
        if init is None:
            continue                      # 详情没拿到（失败/未拉）则不判，避免误报「未打卡」
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
            # 「时间」= 缺卡日期（2026-11-12 hunter 定：就是「哪天漏卡了」）
            #   缺离场 → 用当天到场签到的日期；缺到场 → 用离场打卡的日期；
            #   到场离场都缺 → 用该工单的实际开始日期
            if not has_out and rec["in"]:
                miss_day = min(rec["in"]).strftime("%Y-%m-%d")
            elif not has_in and rec["out"]:
                miss_day = min(rec["out"]).strftime("%Y-%m-%d")
            else:
                _st = ts_any(task.get("startTime") or t.get("startTime"))
                miss_day = _st.strftime("%Y-%m-%d") if _st else ""
            rows.append({
                "错误类型": "漏打卡",
                "错误描述": desc,
                "涉及人员": u,
                "工单类型": t.get("templateName") or "",
                "工单号": (t.get("taskNo") or ""),
                "时间": miss_day,
            })

    # ── 3) 计划日期未填（2026-11-12 hunter 定：除工程工单外全部类型）──
    for t in tasks:
        if (t.get("templateName") or "") == "工程工单":
            continue                      # 工程工单不查计划日期
        init = details.get(t.get("id"))
        if init is None:
            continue                      # 详情没拿到则不判，避免误报
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
            "时间": "",          # 计划日期未填：不涉及时间（hunter 2026-11-12 定）
        })

    # ── 4) 验收整改（仅验收工单）──
    # 2026-11-11 hunter 定：是否合格 = 「不合格」且整改内容子表单为空 → 报此错误
    #   口径：① 只查验收工单，其他类型不涉及；② 进行中的也查，但只读正式值（不读暂存）；
    #        ③ 子表单不存在/空数组/各行全空都算空；④ 负责人为空（工单未分配）则跳过。
    for t in audit_tasks:
        if (t.get("templateName") or "") != "验收工单":
            continue
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        attr = task.get("attribute") or {}
        if cell(attr.get("field_Kj4IeaWFxaNj4FiJ")).strip() != "不合格":
            continue
        if _subform_has_content(attr.get("field_mOho8BGmdv9B1FGu")):
            continue
        boss = ((task.get("executor") or {}).get("displayName")
                or (t.get("executorUser") or {}).get("displayName")
                or t.get("executorName") or "").strip()
        if not boss:
            continue
        rows.append({
            "错误类型": "验收整改",
            "错误描述": "验收未通过，缺整改内容子表单",
            "涉及人员": boss,
            "工单类型": t.get("templateName") or "",
            "工单号": (t.get("taskNo") or ""),
            "时间": "",          # 验收整改：不涉及时间（hunter 2026-11-12 定）
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


def _fmt_dt(v, fmt="%Y-%m-%d %H:%M:%S"):
    d = ts_any(v)
    return d.strftime(fmt) if d else ""


_CUST_ALL_CACHE = {"ts": 0.0, "map": {}}


def _customer_serial_map_all():
    """全量客户：名称 → 客户编号（1 小时缓存）

    部分工单模板（如日常工单）的 attribute 里没有客户编号，customerEntity 也不带 serialNumber，
    只能靠客户名称回查客户档案（2026-11-13 修：非工程工单客户编号显示不出来）。
    """
    now = time.time()
    if _CUST_ALL_CACHE["map"] and now - _CUST_ALL_CACHE["ts"] < 3600:
        return _CUST_ALL_CACHE["map"]
    m, page = {}, 1
    while page <= 15:
        try:
            d = cli(["customer", "list", "--page", str(page), "--page-size", "200", "-o", "raw"],
                    timeout=60)
        except Exception:
            break
        rows = d.get("list") or []
        for r in rows:
            nm = (r.get("name") or "").strip()
            sn = (r.get("serialNumber") or "").strip()
            if nm and sn:
                m[nm] = sn
        if not rows or not d.get("hasNextPage"):
            break
        page += 1
    if m:
        _CUST_ALL_CACHE.update(ts=now, map=m)
    return m


_TASK_CUSTFIELD_CACHE = {}


def _task_cust_field(tid):
    """工单类型 → 「客户编号」字段 ID（按字段显示名探测，缓存 1 小时）

    2026-11-13：工程/预调试/调试/日常有客户编号字段，验收/保运/售后/改造没有。
    优先读字段值；字段不存在或值为空时再走 BY 值扫描 / 客户名回查。
    """
    if not tid:
        return ""
    hit = _TASK_CUSTFIELD_CACHE.get(tid)
    if hit is not None:
        return hit
    fld = ""
    try:
        d = cli(["task", "field", "list", "--type-id", tid, "-o", "raw"], timeout=30)
        rows = d if isinstance(d, list) else (d.get("content") or d.get("rows") or [])
        for f in rows:
            nm = str(f.get("displayName") or "")
            if "客户编号" in nm or "客户编码" in nm:
                fld = f.get("fieldName") or ""
                break
    except Exception:
        fld = ""
    _TASK_CUSTFIELD_CACHE[tid] = fld
    return fld


def q_task_query(params, progress, deadline):
    """工单查询（在线）：按类型查工单主表

    2026-11-13 hunter 定：
      · 默认只查当天（创建日期），无日期参数时后端也按当天兜底
      · 日期口径可选：create=创建日期 / plan=计划开始日期
      · 状态可多选（stateList）
      · 关键字匹配：工单号 / 客户编号 / 客户名称 / 负责人 / 描述（接口 keyword 不认负责人，故本地过滤）
      · 单类型最多拉 10 页，避免大结果集卡页面
    """
    keyword = (params.get("keyword") or "").strip()
    b = (params.get("date_begin") or "").strip()
    e = (params.get("date_end") or "").strip()
    if not b and not e:
        b = e = datetime.now().strftime("%Y-%m-%d")
    # 2026-11-13 hunter 定：不再分创建/计划口径，统一按「工单更新日期」
    # 注意：systemConditions 的时间必须是 "YYYY-MM-DD HH:mm:ss"，不能用 ISO 的 T 格式
    conds = []
    if b or e:
        conds.append({"property": "updateTime", "operator": "between",
                      "betweenValue1": (b or "1970-01-01") + " 00:00:00",
                      "betweenValue2": (e or "2099-12-31") + " 23:59:59"})

    raw_types = (params.get("types") or "").strip()
    tids = []
    if raw_types == "__all__":          # 2026-11-13 hunter 定：全部 = 8 类都查
        tids = list(TASK_TYPES.values())
    elif raw_types:
        for nm in raw_types.split(","):
            nm = nm.strip()
            tid = TASK_TYPES.get(nm)
            if tid and tid not in tids:
                tids.append(tid)
    if not tids:
        tids = [ENGINEER_TASK_TID]

    states = params.get("states") or []
    if isinstance(states, str):
        states = [x.strip() for x in states.split(",") if x.strip()]
    # 2026-11-13 修：勾满所有状态 = 不限状态。否则「全选」会把未列出的新状态（如工单池）挡在外面
    if states and set(states) >= set(STATE_CN.values()):
        states = []
    state_codes = [STATE_REV[x] for x in states if x in STATE_REV] if states else []

    # 2026-11-16 优化：8 个工单类型串行→并发（实测提速 78%，8.88s→1.95s）
    rows, MAX_PAGES = [], 10

    def _fetch_one_type(tid):
        """单个工单类型翻页拉取（并发单元）"""
        out, page = [], 1
        while page <= MAX_PAGES:
            body = {"page": page, "pageSize": PAGE_SIZE, "templateId": tid}
            if conds:
                body["systemConditions"] = conds
            if state_codes:
                body["stateList"] = state_codes
            d = cli(["task", "search", "--data", json.dumps(body, ensure_ascii=False), "-o", "raw"])
            batch = d.get("content") or []
            for _t in batch:
                _t["__tid"] = tid
            out.extend(batch)
            tp = d.get("totalPages") or 0
            if not batch or page >= tp:
                break
            page += 1
            _check(deadline, "工单查询")
        return out

    with ThreadPoolExecutor(max_workers=min(8, len(tids))) as ex:
        for _part in ex.map(_fetch_one_type, tids):
            rows.extend(_part)
            if progress:
                progress("已拉工单 %d 条" % len(rows))

    out, seen = [], set()
    for t in rows:
        no = t.get("taskNo") or ""
        if no and no in seen:
            continue
        seen.add(no)
        cust = (t.get("customerEntity") or {})
        cname = cust.get("name") or (t.get("customer") or {}).get("name") or ""
        attr = t.get("attribute") or {}
        # 客户编号：不同工单模板存放的字段 ID 不同（工程 field_bQ4PDx…、调试 field_MYEWB…、
        # 预调试 field_ZkNhM… 等），customerEntity 里又没有 serialNumber。
        # 2026-11-13 修：先按已知字段取，取不到再扫 attribute 里符合 BY+数字 的值。
        _cf = _task_cust_field(t.get("templateId") or t.get("__tid"))
        serial = cust.get("serialNumber") or (attr.get(_cf) if _cf else "") or \
                 attr.get("field_bQ4PDxGD1TqvSbFU") or ""
        if isinstance(serial, dict):
            serial = serial.get("name") or serial.get("serialNumber") or ""
        serial = str(serial or "").strip()
        if not serial:
            for _k, _v in attr.items():
                if isinstance(_v, str) and re.match(r"^BY\d{4,}(-\d+)?$", _v.strip()):
                    serial = _v.strip()
                    break
        # 列表行里负责人常在 executorUser.displayName（executorName 为空），2026-11-13 修
        ex = (t.get("executorName")
              or (t.get("executorUser") or {}).get("displayName")
              or (t.get("executor") or {}).get("displayName") or "")
        desc = t.get("description") or ""
        if keyword:
            hay = " ".join([no, str(serial), cname, ex, desc])
            if keyword not in hay:
                continue
        out.append({
            "工单编号": no,
            "工单类型": t.get("templateName") or "",
            "客户编号": str(serial or ""),
            "客户": cname,
            "状态": STATE_CN.get(t.get("state"), t.get("state") or ""),
            "负责人": ex,
            "创建时间": _fmt_dt(t.get("createTime")),
            "更新日期": _fmt_dt(t.get("updateTime")),
            "计划开始": _fmt_dt(t.get("planStartTime")),
            "计划结束": _fmt_dt(t.get("planEndTime")),
            "__id": t.get("id") or "",
        })
    # 客户编号兜底：attribute 里没有的（日常工单等）按客户名称回查客户档案
    if any(not x.get("客户编号") for x in out):
        try:
            _cmap = _customer_serial_map_all()
            for x in out:
                if not x.get("客户编号") and x.get("客户"):
                    x["客户编号"] = _cmap.get(str(x["客户"]).strip(), "") or ""
        except Exception:
            pass

    # 排序：先按时间倒序，再按类型顺序稳定排序 → 工程工单排最前，其他按预调试/调试/…顺序
    out.sort(key=lambda x: (x.get("创建时间") or "", x.get("工单编号") or ""), reverse=True)
    _order = {nm: i for i, nm in enumerate(TASK_TYPES.keys())}
    out.sort(key=lambda x: _order.get(x.get("工单类型"), 99))
    return _resp("ol:task-query", out, TASK_QUERY_COLUMNS, persons=list(TASK_TYPES.keys()))


# ── 问题工单（PAAS 表单）：列表口径 + 详情（2026-11-13 hunter 定）───────────
# 列表只保留这 11 列；内容ID / 表单名称 / 无中文名的遗留字段一律不展示
ISSUE_LIST_COLUMNS = ["编号", "项目编码", "数据状态", "当前节点", "创建时间", "问题描述", "紧急程度",
                      "主责任人员", "处理方案", "完成日期", "处理结果", "客户"]
# 客户字段（含关联客户的编号 relationDate.serialNumber）
ISSUE_CUSTOMER_FIELD = "fieldYHr9khcoPm8KM4rL6Mcxi7xWAqUJY3CV"
# 模板里已删除/弃用的遗留字段：老数据仍有值，接口不返回中文名，这里补上
ISSUE_LEGACY_LABELS = {
    "fieldzWP3auuCq1DGz4jcru9vQl2MRZfLKjHG": "处理结果",
    # 系统字段（模板字段接口不返回，详情页要显示中文）
    "serialNumber": "编号",
    "flowStatus": "数据状态",
    "currentNode": "当前节点",
    "createTime": "创建时间",
    "createUser": "创建人",
}
# 详情页分区（按表单顺序）
ISSUE_DETAIL_SECTIONS = [
    ("基本信息", ["serialNumber", "flowStatus", "currentNode", "createTime", "createUser"]),
    ("问题情况", ["fieldCkGHayYCFkZpxyIpcZi0EDejC3i6BzrO", "fielduMxDanXfhdNxbSdSDmXammD1eqr1h6d6",
                  "field7ROtDQTjE2qnPIY3YrCdIH71k5KnR1by", "fieldYHr9khcoPm8KM4rL6Mcxi7xWAqUJY3CV",
                  "field0BIWi5f4ckld4Lv3CqkoZOczGXyWIGpc", "fieldsTrkLHlaGRKdNT0oQpRvybFEnUjqc31n",
                  "fieldrpOOWx4w3NLUvD5HUU0kp4x6qF7UBMRO", "fieldg9qP0sgokhHWZfD8o46T9gOgUKaXTCKT"]),
    ("责任分配", ["fieldKzbzxQYBONa5lP1xsONPyPVOU6PituNi", "field1q38DI7gJ2FpKJQH1EvfCnL1Xp1sH0sg",
                  "fielda74LteL9SssW7qxrbl1KLZintPQUQwws", "field6YsDIgv1XOj8rXWTxAjUZsvxo8G9kGs4",
                  "fieldA1Ap4QWvX5S8hMjR8ZtXZstjcJZE70sq"]),
    ("处理与完成", ["fieldmApTY9ARF2juOYN8IfVFOjX4s4WTotB1", "fieldC5mFYVuPIYD1uSjw6wDUiSoAjhSzCCDN",
                    "fieldqa8AAh1Oyast1mfcXXcgjSjew8dnexfX", "fieldKR8SlIbZYkP0U0FZSVHjwwKFcrMv5iVR",
                    "field2fgZeaOf96zKeBHggr4MfWxejvebtF5U", "fieldL97jUmNOU7deyzlvkj9b2FT1iJDg7Dij",
                    "field73exUHuTAu0xVTeyxlvMqXnkNuUvdueC", "fieldzWP3auuCq1DGz4jcru9vQl2MRZfLKjHG"]),
]
ISSUE_FIELD_CACHE = {"ts": 0.0, "labels": {}, "types": {}}


def _issue_labels(force=False):
    """模板字段 → 中文名 / 表单类型（含子表单），缓存 10 分钟"""
    now = time.time()
    if not force and ISSUE_FIELD_CACHE["labels"] and now - ISSUE_FIELD_CACHE["ts"] < 600:
        return ISSUE_FIELD_CACHE["labels"], ISSUE_FIELD_CACHE["types"]
    labels, types = {}, {}

    def walk(fs):
        for f in fs or []:
            fn = f.get("fieldName")
            if not fn:
                continue
            labels[fn] = f.get("displayName") or fn
            types[fn] = f.get("formType") or ""
            walk(f.get("subFormFieldList"))

    try:
        d = cli(["paas", "template-fields", "--template-biz-id", ISSUE_TEMPLATE_BIZ_ID, "-o", "raw"])
        walk(d.get("paasFormFieldVOList"))
    except Exception:
        pass
    labels.update(ISSUE_LEGACY_LABELS)
    ISSUE_FIELD_CACHE.update(ts=now, labels=labels, types=types)
    return labels, types


def _issue_cell(v):
    """列表单元格：标量/数组/对象 → 可读文本"""
    if v in (None, "", [], {}):
        return ""
    if isinstance(v, dict):
        return str(v.get("displayName") or v.get("name") or v.get("label") or "")
    if isinstance(v, list):
        parts = []
        for x in v:
            if isinstance(x, dict):
                t = x.get("displayName") or x.get("name") or x.get("label") or ""
            else:
                t = str(x)
            if t:
                parts.append(t)
        return "；".join(parts)
    return str(v)


_ISSUE_CUST_CACHE = {"ts": 0.0, "map": {}}
_ISSUE_NAME_CACHE = {"ts": 0.0, "map": {}}


def _customer_serial_by_name(name):
    """客户名称 → 客户编号（云端客户列表），结果缓存 10 分钟

    老工单的客户关联被重建过，relationDate.serialNumber 是空的，
    只能按客户名称回查（2026-11-13 hunter 要求项目编码必须显示客户编号）。
    """
    name = (name or "").strip()
    if not name:
        return ""
    now = time.time()
    cache = _ISSUE_NAME_CACHE
    if now - cache["ts"] > 600:
        cache["map"] = {}
        cache["ts"] = now
    m = cache["map"]
    if name in m:
        return m[name]
    sn = ""
    try:
        d = cli(["customer", "list", "--keyword", name, "--page-size", "5", "-o", "raw"])
        rows = d.get("list") or d.get("content") or []
        for r in rows:
            if (r.get("name") or "").strip() == name:
                sn = r.get("serialNumber") or ""
                break
        if not sn and rows:
            sn = rows[0].get("serialNumber") or ""
    except Exception:
        sn = ""
    m[name] = sn
    return sn


def _issue_customer_map(rows):
    """bizId → 客户编号：先批量取表单关联编号，空值再按客户名称回查"""
    now = time.time()
    cache = _ISSUE_CUST_CACHE
    if now - cache["ts"] > 120:
        cache["map"] = {}
        cache["ts"] = now
    m = cache["map"]
    todo = [r for r in rows if r.get("bizId") and r.get("bizId") not in m]
    ids = [r.get("bizId") for r in todo]
    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        try:
            d = cli(["paas", "form", "batch-get",
                     "--data", json.dumps({"bizIds": chunk}, ensure_ascii=False),
                     "-o", "raw"], timeout=60)
        except Exception:
            break
        for bid, rec in (d or {}).items():
            sn = ""
            for it in (rec.get("paasFormValueList") or []):
                if it.get("fieldName") == ISSUE_CUSTOMER_FIELD:
                    v = _issue_json(it.get("value"))
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        sn = ((v[0].get("relationDate") or {}).get("serialNumber")) or ""
                    break
            m[bid] = sn

    # 表单里没有编号的老工单 → 按客户名称回查（并发，结果有 10 分钟缓存）
    lack = [r for r in todo if not m.get(r.get("bizId"))]
    if lack:
        names = list({(r.get(ISSUE_CUSTOMER_FIELD) or "").strip() for r in lack} - {""})
        got = {}
        if names:
            with ThreadPoolExecutor(max_workers=min(WORKERS, len(names))) as ex:
                futs = {ex.submit(_customer_serial_by_name, n): n for n in names}
                for fu in as_completed(futs):
                    got[futs[fu]] = fu.result() or ""
        for r in lack:
            nm = (r.get(ISSUE_CUSTOMER_FIELD) or "").strip()
            if got.get(nm):
                m[r.get("bizId")] = got[nm]
    return m


def q_issue_tasks(params, progress, deadline):
    """问题工单（在线）：PAAS 表单

    2026-11-13 hunter 定：
      · 列表 12 列（去掉内容ID/表单名称/是否完成流程/无中文名遗留字段），新增项目编码与客户列，创建时间只显示日期（悬停看全量）
      · 按创建时间筛选，无参数时默认本周（周一→今天）
      · 状态多选按「数据状态」过滤
    """
    keyword = (params.get("keyword") or "").strip()
    b = (params.get("date_begin") or "").strip()
    e = (params.get("date_end") or "").strip()
    if not b and not e:
        now = datetime.now()
        b = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
        e = now.strftime("%Y-%m-%d")
    states = params.get("states") or []
    if isinstance(states, str):
        states = [x.strip() for x in states.split(",") if x.strip()]
    # 2026-11-13 hunter 定：问题工单默认不显示「已完成」，需要时在状态里勾上
    exclude_finished = not states

    rows, page, MAX_PAGES = [], 1, 10
    while page <= MAX_PAGES:
        d = cli(["paas", "form", "search",
                 "--template-biz-id", ISSUE_TEMPLATE_BIZ_ID,
                 "--page-num", str(page), "--page-size", str(PAGE_SIZE), "-o", "raw"])
        batch = d.get("rows") or []
        rows.extend(batch)
        if progress:
            progress("已拉表单 %d 条" % len(rows))
        tp = (d.get("page") or {}).get("totalPages") or 0
        if not batch or page >= tp:
            break
        page += 1
        _check(deadline, "问题工单")

    cust_map = _issue_customer_map(rows)
    out = []
    for r in rows:
        ct = str(r.get("createTime") or "")
        day = ct[:10]
        if b and day and day < b:
            continue
        if e and day and day > e:
            continue
        st = str(r.get("flowStatus") or "")
        if states and st not in states:
            continue
        if exclude_finished and st == "已完成":
            continue
        item = {
            "编号": str(r.get("serialNumber") or ""),
            "项目编码": cust_map.get(r.get("bizId"), "") or "",
            "数据状态": st,
            "当前节点": str(r.get("currentNode") or ""),
            "创建时间": ct,
            "问题描述": _issue_cell(r.get("field0BIWi5f4ckld4Lv3CqkoZOczGXyWIGpc")),
            "紧急程度": _issue_cell(r.get("fieldrpOOWx4w3NLUvD5HUU0kp4x6qF7UBMRO")),
            "主责任人员": _issue_cell(r.get("field1q38DI7gJ2FpKJQH1EvfCnL1Xp1sH0sg")),
            "处理方案": _issue_cell(r.get("fieldC5mFYVuPIYD1uSjw6wDUiSoAjhSzCCDN")),
            "完成日期": _issue_cell(r.get("field2fgZeaOf96zKeBHggr4MfWxejvebtF5U")),
            "是否完成流程": _issue_cell(r.get("field73exUHuTAu0xVTeyxlvMqXnkNuUvdueC")),
            "处理结果": _issue_cell(r.get("fieldzWP3auuCq1DGz4jcru9vQl2MRZfLKjHG")),
            "客户": _issue_cell(r.get(ISSUE_CUSTOMER_FIELD)),
            "__id": r.get("bizId") or "",
        }
        if keyword:
            hay = " ".join(str(item.get(c) or "") for c in ISSUE_LIST_COLUMNS)
            if keyword not in hay:
                continue
        out.append(item)
    out.sort(key=lambda x: x.get("创建时间") or "", reverse=True)
    return _resp("ol:issue-tasks", out, ISSUE_LIST_COLUMNS)


_ISSUE_ROWS_CACHE = {"ts": 0.0, "rows": {}}


def _issue_rows_by_id():
    """问题工单全量行（详情页取状态/节点用），缓存 2 分钟"""
    now = time.time()
    if _ISSUE_ROWS_CACHE["rows"] and now - _ISSUE_ROWS_CACHE["ts"] < 120:
        return _ISSUE_ROWS_CACHE["rows"]
    rows, page = [], 1
    while page <= 10:
        d = cli(["paas", "form", "search", "--template-biz-id", ISSUE_TEMPLATE_BIZ_ID,
                 "--page-num", str(page), "--page-size", str(PAGE_SIZE), "-o", "raw"])
        batch = d.get("rows") or []
        rows.extend(batch)
        tp = (d.get("page") or {}).get("totalPages") or 0
        if not batch or page >= tp:
            break
        page += 1
    m = {r.get("bizId"): r for r in rows if r.get("bizId")}
    _ISSUE_ROWS_CACHE.update(ts=now, rows=m)
    return m


def _issue_json(v):
    if isinstance(v, str):
        s = v.strip()
        if s[:1] in ("[", "{", '"'):
            try:
                return json.loads(s)
            except Exception:
                return v
    return v


def _issue_value(raw, ftype):
    """字段原值 → (显示文本, 附件链接列表)"""
    v = _issue_json(raw)
    if v in (None, "", [], {}):
        return "", []
    urls = []
    if ftype == "user":
        items = v if isinstance(v, list) else [v]
        names = [_issue_cell(x) for x in items]
        return "、".join([n for n in names if n]), urls
    if ftype == "customer":
        parts = []
        for x in (v if isinstance(v, list) else [v]):
            if not isinstance(x, dict):
                parts.append(str(x))
                continue
            nm = x.get("name") or x.get("displayName") or ""
            sn = ((x.get("relationDate") or {}).get("serialNumber")) or ""
            parts.append(("%s（%s）" % (nm, sn)) if sn else nm)
        return "；".join([p for p in parts if p]), urls
    if ftype == "attachment":
        for x in (v if isinstance(v, list) else [v]):
            if not isinstance(x, dict):
                continue
            nm = x.get("filename") or x.get("originFileName") or "附件"
            u = x.get("url") or ""
            if u:
                urls.append({"name": nm, "url": u})
            else:
                urls.append({"name": nm, "url": ""})
        return "、".join(u["name"] for u in urls), urls
    if isinstance(v, list):
        vals = []
        for x in v:
            if isinstance(x, dict):
                vals.append(x.get("label") or x.get("name") or x.get("displayName") or "")
            else:
                vals.append(str(x))
        return "；".join([x for x in vals if x]), urls
    if isinstance(v, dict):
        return str(v.get("label") or v.get("name") or v.get("displayName") or ""), urls
    return str(v), urls


def issue_fields_batch(biz_ids):
    """批量取问题工单全部字段（导出用）→ {bizId: [{label,value,urls}]}

    2026-11-13 hunter 定：问题工单导出要「全部字段」（详情页除客户外），含图片附件链接。
    系统字段走 search 缓存，表单字段走 batch-get（50/批），字段名用模板中文名。
    """
    labels, types = _issue_labels()
    rows_map = _issue_rows_by_id()
    ids = [b for b in (biz_ids or []) if b]
    out = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        try:
            d = cli(["paas", "form", "batch-get",
                     "--data", json.dumps({"bizIds": chunk}, ensure_ascii=False),
                     "-o", "raw"], timeout=90)
        except Exception:
            d = {}
        for bid in chunk:
            rec = (d or {}).get(bid) or {}
            sr = rows_map.get(bid) or {}
            items, seen = [], set()

            def add(label, value, urls=None):
                if value in (None, "", [], {}):
                    return
                if label in seen:
                    return
                seen.add(label)
                items.append({"label": label, "value": str(value), "urls": urls or []})

            # 系统字段（与详情页一致）
            add("编号", sr.get("serialNumber"))
            add("数据状态", sr.get("flowStatus"))
            add("当前节点", sr.get("currentNode"))
            add("创建时间", sr.get("createTime"))
            add("创建人", sr.get("createUser"))

            vals = {}
            for it in rec.get("paasFormValueList") or []:
                fn = it.get("fieldName")
                if fn:
                    vals[fn] = it
            order = [f for _sec, fs in ISSUE_DETAIL_SECTIONS for f in fs]
            order += [f for f in vals if f not in order]
            for f in order:
                if f == ISSUE_CUSTOMER_FIELD:      # 导出不要客户（hunter 定）
                    continue
                if f not in labels:                # 无中文名的遗留字段跳过
                    continue
                it = vals.get(f)
                if not it:
                    continue
                lab = labels[f]
                ftype = types.get(f) or it.get("formType") or ""
                if ftype == "subForm" or it.get("isSubForm") == 1:
                    rows_out = []
                    for sub in it.get("subFormContentVOList") or []:
                        parts = []
                        for vo in (sub or {}).get("subFormValueVOList") or []:
                            k = vo.get("fieldName")
                            txt, _ = _issue_value(vo.get("value"), types.get(k) or "")
                            if txt:
                                parts.append("%s：%s" % (labels.get(k, k), txt))
                        if parts:
                            rows_out.append("；".join(parts))
                    add(lab, "\n".join(rows_out))
                else:
                    txt, urls = _issue_value(it.get("value"), ftype)
                    add(lab, txt, urls)
            out[bid] = items
    return out


def issue_detail(biz_id):
    """问题工单详情：paas form get → 按中文名分区的完整信息"""
    labels, types = _issue_labels()
    row = _issue_rows_by_id().get(biz_id) or {}
    d = cli(["paas", "form", "get", "--content-biz-id", biz_id, "-o", "raw"])

    raw_vals = {}
    for it in d.get("paasFormValueList") or []:
        fn = it.get("fieldName")
        if fn:
            raw_vals[fn] = it

    # 系统字段取列表行（get 接口只有 flowStatus 数字，没有当前节点）
    sys_vals = {
        "serialNumber": row.get("serialNumber") or "",
        "flowStatus": row.get("flowStatus") or "",
        "currentNode": row.get("currentNode") or "",
        "createTime": row.get("createTime") or "",
        "createUser": row.get("createUser") or "",
    }

    def build(field, it):
        ftype = types.get(field) or (it or {}).get("formType") or ""
        if field in sys_vals:
            return str(sys_vals[field]), []
        if not it:
            return "", []
        if ftype == "subForm" or it.get("isSubForm") == 1:
            rows_out = []
            for sub in it.get("subFormContentVOList") or []:
                parts = []
                for vo in (sub or {}).get("subFormValueVOList") or []:
                    k = vo.get("fieldName")
                    txt, _ = _issue_value(vo.get("value"), types.get(k) or "")
                    if txt:
                        parts.append("%s：%s" % (labels.get(k, k), txt))
                if parts:
                    rows_out.append("；".join(parts))
            return "\n".join(rows_out), []
        return _issue_value(it.get("value"), ftype)

    sections, used = [], set()
    for name, fields in ISSUE_DETAIL_SECTIONS:
        items = []
        for f in fields:
            it = raw_vals.get(f)
            txt, urls = build(f, it)
            used.add(f)
            if txt:
                items.append({"label": labels.get(f, f), "value": txt, "urls": urls})
        if items:
            sections.append({"name": name, "items": items})

    # 其余未列出但有值的字段 → 「其他字段」
    others = []
    for f, it in raw_vals.items():
        if f in used:
            continue
        # 内部系统键（CREATE_USER / WFLOW_STATUS 等全大写）不展示（2026-11-13 hunter 定）
        if re.match(r"^[A-Z][A-Z0-9_]+$", f):
            continue
        txt, urls = build(f, it)
        if txt:
            others.append({"label": labels.get(f, f), "value": txt, "urls": urls})
    if others:
        sections.append({"name": "其他字段", "items": others})

    return {"title": row.get("serialNumber") or labels.get("serialNumber") or "问题工单详情",
            "sections": sections}


# ── 报表 9：工单管理（dataware 平台报表直读，2026-11-16 hunter 批准）──
# 数据源 = 售后宝「管理报表 → 人效考核 → 工单管理」的图表配置；
# 每块直接 POST 到 queryCustomChartData，平台已聚合好，不需自己遍历工单算。
DW_WORK_REPORT_ID = "2000000000000073092"
DW_CFG_TTL = 3600
_DW_CFG = {"ts": 0.0, "data": None}


def http_post(path, body):
    """直连售后宝 POST 接口（token 头，同 http_get）"""
    domain, token = load_auth()
    req = urllib.request.Request(
        "https://%s%s" % (domain, path),
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"token": token, "shbversion": "3", "shb-language": "zh",
                 "timezone": "Asia/Shanghai", "content-type": "application/json",
                 "accept": "application/json, text/plain, */*",
                 "user-agent": "shb-web-online/1.0"})
    last = None
    for _ in range(2):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1)
    raise RuntimeError("售后宝接口请求失败：%s" % last)


def dw_config(report_id=None):
    """工单管理报表配置（1 小时缓存，约 113KB）"""
    rid = report_id or DW_WORK_REPORT_ID
    now = time.time()
    if _DW_CFG["data"] and now - _DW_CFG["ts"] < DW_CFG_TTL:
        return _DW_CFG["data"]
    d = http_get("/api/dataware/outside/custom/report/V2/getConfigV2/%s" % rid, {})
    data = d.get("data") or {}
    if data:
        _DW_CFG.update(ts=now, data=data)
    return data


def _dw_ms(day, end=False):
    """'YYYY-MM-DD' → 毫秒时间戳（本地时区，end=True 取当日 23:59:59）"""
    dt = datetime.strptime(day, "%Y-%m-%d")
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp() * 1000)


def q_work_report(params, progress, deadline):
    """工单管理（在线）：直读平台数据报表的图表结果

    2026-11-16 hunter 定：
      · 页面 = 管理报表/人效考核/工单管理（报表 id 见 DW_WORK_REPORT_ID）
      · 20 个块中 3 个 splitBar 是分组标题（isSection），17 个是真图表
      · 过滤：把每块 dataRanges 里「统计时间(vDate/date_value)」的 dynamicValue 清空，
        换成用户所选区间的起止毫秒 → 支持自定义时间
    """
    db = (params.get("date_begin") or "").strip()
    de = (params.get("date_end") or "").strip()
    if db or de:
        b, e = d_range(db, de)
    else:
        b, e = week_range_now()          # 默认本周（与平台报表一致）
    ms_b, ms_e = _dw_ms(b[:10]), _dw_ms(e[:10], end=True)

    cfg = dw_config()
    blocks = cfg.get("pageChartConfigList") or []
    if not blocks:
        raise RuntimeError("取不到「工单管理」报表配置，请先在身份管理里重新授权")

    # 改写时间条件（只改「统计时间」维度）
    for blk in blocks:
        for r in (blk.get("dataRanges") or []):
            dim = r.get("dimension") or {}
            if dim.get("dataOrigin") == "vDate" or dim.get("dataField") == "date_value":
                r["dynamicValue"] = None
                r["values"] = [str(ms_b), str(ms_e)]

    def _is_section(blk):
        return (blk.get("chartType") == "splitBar"
                and not (blk.get("indexs") or blk.get("dimensions")))

    def _one(blk):
        d = http_post("/api/dataware/outside/custom/report/V2/queryCustomChartData", blk)
        return d.get("data") or []

    todo = [x for x in blocks if not _is_section(x)]
    got, done = {}, 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_one, blk): blk for blk in todo}
        for fut in as_completed(futs):
            _check(deadline, "取工单管理图表")
            blk = futs[fut]
            try:
                got[blk.get("chartId")] = fut.result()
            except Exception:            # 单块失败不拖垮整页
                got[blk.get("chartId")] = []
            done += 1
            if progress:
                progress("已取图表 %d/%d" % (done, len(todo)))

    out = []
    for blk in blocks:
        sec = _is_section(blk)
        out.append({
            "chartName": blk.get("chartName") or "",
            "chartType": blk.get("chartType") or "",
            "chartSize": blk.get("chartSize") or "",
            "chartOrder": blk.get("chartOrder") or 0,
            "isSection": sec,
            "data": [] if sec else got.get(blk.get("chartId"), []),
        })
    out.sort(key=lambda x: x["chartOrder"])
    return {"ok": True, "tab": "ol:work-report", "title": TAB_TITLES["ol:work-report"],
            "blocks": out, "rows": [], "columns": [], "total": len(out),
            "page": 1, "page_size": len(out), "has_next": False, "has_prev": False,
            "total_pages": 1, "online": True, "range": [b[:10], e[:10]]}


# ── 地图类报表（2026-11-16 hunter 定）────────────────────────────────
# 数据源均为内建报表接口 /api/report/outside/stats/*（第二轮抓包定位）
_STATE_COLOR = {"工作中": "rgb(110, 207, 64)", "非常忙碌": "rgb(255, 89, 0)",
                "休假中": "rgb(255, 174, 0)"}


def _place_name(city, prov, dist=None):
    """地点名归一化：city 为「市辖区/县」等非具体地名时，回退用省份（+区县）

    2026-11-16：实测天津等地地址的 city 字段是「市辖区」，
    直接拿来聚合会把不同省份的点全部混成一个，故做此回退。
    """
    city = (city or "").strip()
    prov = (prov or "").strip()
    dist = (dist or "").strip()
    bad = ("市辖区", "县", "省直辖县级行政区划", "自治区直辖县级行政区划", "市辖县")
    if not city or city in bad:
        base = prov or dist
        if dist and base and dist != base:
            return base + "·" + dist
        return base
    return city


def _map_group(items, name_of, lng_of, lat_of):
    """按地点名聚合计点：同名取首点坐标，count 累加"""
    agg = {}
    skipped = 0
    for it in items or []:
        nm = (name_of(it) or "").strip()
        lng, lat = lng_of(it), lat_of(it)
        if not nm or lng in (None, "", 0) or lat in (None, "", 0):
            skipped += 1
            continue
        g = agg.get(nm) or {"name": nm, "lng": float(lng), "lat": float(lat), "count": 0}
        g["count"] += 1
        agg[nm] = g
    return sorted(agg.values(), key=lambda x: -x["count"]), skipped


def q_task_map(params, progress, deadline):
    """工单地图：按地址城市聚合工单数（内建报表 plan/getTaskMapList）"""
    d = http_post("/api/report/outside/stats/plan/getTaskMapList",
                  {"page": 1, "pageSize": 2000})
    rows = d.get("list") or []
    pts, skipped = _map_group(
        rows,
        lambda x: _place_name((x.get("customerAddress") or {}).get("city"),
                              (x.get("customerAddress") or {}).get("province"),
                              (x.get("customerAddress") or {}).get("dist")),
        lambda x: (x.get("customerAddress") or {}).get("longitude"),
        lambda x: (x.get("customerAddress") or {}).get("latitude"))
    return {"ok": True, "tab": "ol:task-map", "title": TAB_TITLES["ol:task-map"],
            "map": {"points": pts, "unit": "单", "total": len(rows),
                    "located": len(rows) - skipped, "skipped": skipped},
            "rows": [], "columns": [], "total": len(pts), "online": True,
            "page": 1, "page_size": len(pts), "has_next": False, "has_prev": False,
            "total_pages": 1}


def q_customer_map(params, progress, deadline):
    """客户地图：按客户地址城市聚合客户数（内建报表 customer/list）"""
    rows = http_post("/api/report/outside/stats/customer/list", {})
    if isinstance(rows, dict):
        rows = rows.get("data") or []
    pts, skipped = _map_group(
        rows,
        lambda x: _place_name((x.get("customerAddress") or {}).get("adCity"),
                              (x.get("customerAddress") or {}).get("adProvince"),
                              (x.get("customerAddress") or {}).get("adDist")),
        lambda x: (x.get("customerAddress") or {}).get("adLongitude"),
        lambda x: (x.get("customerAddress") or {}).get("adLatitude"))
    return {"ok": True, "tab": "ol:customer-map", "title": TAB_TITLES["ol:customer-map"],
            "map": {"points": pts, "unit": "客户", "total": len(rows),
                    "located": len(rows) - skipped, "skipped": skipped},
            "rows": [], "columns": [], "total": len(pts), "online": True,
            "page": 1, "page_size": len(pts), "has_next": False, "has_prev": False,
            "total_pages": 1}


def q_staff_map(params, progress, deadline):
    """人员地图：一人一点 + 工作状态（内建报表 teamUser + stats/config）"""
    arr = http_post("/api/dataware/outside/user/staff/teamUser", {})
    if isinstance(arr, dict):
        arr = arr.get("data") or []
    colors = dict(_STATE_COLOR)
    try:
        cfgd = http_get("/api/report/outside/stats/config", {})
        m = (cfgd or {}).get("stateColorMap") or {}
        if m:
            colors.update(m)
    except Exception:
        pass
    pts, skipped = [], 0
    stat = {}
    for u in arr or []:
        st = (u.get("state") or "").strip() or "未设置状态"
        stat[st] = stat.get(st, 0) + 1
        lng, lat = u.get("longitude"), u.get("latitude")
        if lng in (None, "", 0) or lat in (None, "", 0):
            skipped += 1
            continue
        pts.append({"name": (u.get("displayName") or "").strip(),
                    "lng": float(lng), "lat": float(lat),
                    "count": 1, "state": st,
                    "color": colors.get(st, "#595959")})
    legend = [{"state": k, "count": v, "color": colors.get(k, "#595959")}
              for k, v in sorted(stat.items(), key=lambda kv: -kv[1])]
    return {"ok": True, "tab": "ol:staff-map", "title": TAB_TITLES["ol:staff-map"],
            "map": {"points": pts, "unit": "人", "total": len(arr or []),
                    "located": len(pts), "skipped": skipped, "legend": legend},
            "rows": [], "columns": [], "total": len(pts), "online": True,
            "page": 1, "page_size": len(pts), "has_next": False, "has_prev": False,
            "total_pages": 1}


HANDLERS = {
    "ol:task-map": q_task_map,
    "ol:customer-map": q_customer_map,
    "ol:staff-map": q_staff_map,
    "ol:work-report": q_work_report,
    "ol:trip": q_trip,
    "ol:weekly-plan": q_weekly,
    "ol:progress2": q_progress2,
    "ol:workload": q_workload,
    "ol:debug-progress": q_debug,
    "ol:maintain-progress": q_maint,
    "ol:accept-progress": q_accept,
    "ol:audit-check": q_audit_check,
    "ol:task-query": q_task_query,
    "ol:issue-tasks": q_issue_tasks,
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
