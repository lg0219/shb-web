"""周计划 / 周总结 —— 公共算法层（2026-11-12 建）

目的：本地报表与在线报表「一式两份」——同一套计算，只换数据源。
  · 在线（ol:weekly-plan）：tasks 来自云端 search + detail
  · 本地（weekly-plan）  ：tasks 来自本地 SQLite 备份库
两边都调用 build_weekly()，避免口径漂移。

输入 tasks 统一结构（dict，键名与云端列表行一致）：
  id, taskNo, templateName, executorName, executorUser{displayName},
  customer, customerEntity{name},
  state（云端英文 / 本地中文均可，只看是否「已取消」）,
  planStartTime, planEndTime, startTime, completeTime
输入 details：{task_id: {"task": {"executor": {...}, "synergies": [...]}}}
"""
import html
from datetime import datetime, timedelta

WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
_CANCELED = {"offed", "已取消"}


def esc(s):
    return html.escape("" if s is None else str(s), quote=True)


def ts_any(v):
    """毫秒时间戳 / 带时区 ISO / 'YYYY-MM-DD HH:MM:SS' → 本地 datetime 或 None"""
    if v in (None, "", 0, "0"):
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        try:
            n = v / 1000.0 if v > 1e11 else float(v)
            return datetime.fromtimestamp(n)
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit():
        return ts_any(int(s))
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s.replace("/", "-"))
        if d.tzinfo is not None:
            d = d.astimezone().replace(tzinfo=None)
        return d
    except Exception:
        return None


def task_persons(t, init):
    """一单的参与人 = 负责人（列表/详情）+ 协同人（详情 synergies）"""
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
        add((u or {}).get("displayName") or (u or {}).get("userName"))
    return out


def week_days(week_start=""):
    """任选周内一天 → 该周周一 00:00 与 7 天列表"""
    try:
        d = datetime.strptime((week_start or "").strip(), "%Y-%m-%d")
    except Exception:
        d = datetime.now()
    monday = (d - timedelta(days=d.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return monday, [monday + timedelta(days=i) for i in range(7)]


def build_weekly(tasks, details, monday, days, mode="plan", keyword=""):
    """产出周计划/周总结的行、列、人员清单（与在线版完全同算法）"""
    week_end_ts = days[-1].replace(hour=23, minute=59, second=59)
    keyword = (keyword or "").strip()

    pre = []
    for t in tasks:
        if (t.get("state") or "") in _CANCELED:
            continue
        if mode == "summary":
            st, en = ts_any(t.get("startTime")), ts_any(t.get("completeTime"))
        else:
            st, en = ts_any(t.get("planStartTime")), ts_any(t.get("planEndTime"))
        if not st:
            continue
        if not (st <= week_end_ts and (en is None or en >= monday)):
            continue
        pre.append((t, st, en))

    items = []
    for t, st, en in pre:
        # 客户名：在线是 {name} 结构，本地是纯字符串，两种都要兼容
        def _cname(v):
            return (v.get("name") or "") if isinstance(v, dict) else (v or "")
        cust = _cname(t.get("customerEntity")) or _cname(t.get("customer"))
        for person in task_persons(t, details.get(t.get("id"))):
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

    head_cols = ["负责人"] + ["%s<br>%s" % (WEEKDAY_CN[i], day.strftime("%m-%d"))
                          for i, day in enumerate(days)]

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

    return {"rows": out_rows, "columns": head_cols, "persons": persons,
            "week_start": monday.strftime("%Y-%m-%d"),
            "week_end": days[-1].strftime("%Y-%m-%d")}
