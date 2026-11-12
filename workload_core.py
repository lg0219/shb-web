"""服务人员工作量 —— 公共算法层（2026-11-12 建）

本地/在线「一式两份」：同一套计算，只换数据源。
  · 在线 ol:workload：tasks 来自云端 search + detail + 动态(出发/到达)
  · 本地 workload    ：tasks 来自本地 SQLite（tasks / task_raw / task_records）
调用方注入 4 个辅助函数（都来自 online.py，避免重复实现）：
  ts(v) → datetime|None；cards(extra) → {人:{in:[],out:[]}}；
  hours(ins,outs,arr) → (工时, 实际到场, 实际离场, 提示)；province(extra, addr) → 省
"""


def _task_customer(task):
    return task.get("customer") or {}


def _area_of(cust):
    for tag in (cust.get("tags") or []):
        nm = (tag or {}).get("tagName") or ""
        if "片区" in nm:
            return nm
    return ""


def build_workload(tasks, details, records, keyword="", *,
                   ts=None, cards=None, hours=None, province=None):
    """返回 (rows, persons)；rows 列顺序 = WORKLOAD_COLUMNS"""
    keyword = (keyword or "").strip()
    out, persons = [], set()

    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        recs = records.get(t.get("id")) or []

        dep_ts = sorted({r.get("createTime") for r in recs
                         if r.get("action") == "出发" and r.get("createTime")})
        arr_ts = sorted({r.get("createTime") for r in recs
                         if r.get("action") == "到达" and r.get("createTime")})
        dep = ts(dep_ts[0]) if dep_ts else None
        arr = ts(arr_ts[-1]) if arr_ts else None

        extra = {"cardInfo": init.get("cardInfo"),
                 "receiptTaskForUpdate": init.get("receiptTaskForUpdate")}
        cust = _task_customer(task)
        addr = ((task.get("attribute") or {}).get("taskArriveAddress")
                or task.get("taddressStr") or "")

        # 客户名：在线是 {name} 结构，本地是纯字符串，两种都兼容
        ce = t.get("customerEntity")
        ce_name = (ce.get("name") or "") if isinstance(ce, dict) else (ce or "")

        base = {
            "工单类型": t.get("templateName") or task.get("templateName") or "",
            "客户编号": cust.get("serialNumber") or "",
            "客户": ce_name or cust.get("name") or "",
            "服务部门": _area_of(cust),
            "省份": province(extra, addr),
            "出发打卡": dep.strftime("%Y-%m-%d %H:%M:%S") if dep else "",
            "到场打卡": arr.strftime("%Y-%m-%d %H:%M:%S") if arr else "",
            "途中时长": (round((arr - dep).total_seconds() / 3600.0, 1) if dep and arr else ""),
        }

        boss = (t.get("executorName")
                or (t.get("executorUser") or {}).get("displayName")
                or (task.get("executor") or {}).get("displayName") or "").strip()

        sig = cards(extra)
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
            h, used_in, used_out, tips = hours(rec["in"], rec["out"], arr)
            row = dict(base)
            row["服务人员"] = user
            row["角色"] = "负责人" if user == boss else "协同人"
            row["到场签到时间"] = "、".join(x.strftime("%m-%d %H:%M") for x in used_in)
            row["离场签到时间"] = "、".join(x.strftime("%m-%d %H:%M") for x in used_out)
            row["工单时长"] = h if h is not None else ""
            row["提示"] = "；".join(tips)
            row["__warn"] = 1 if tips else 0
            row["__tips"] = "；".join(tips)
            out.append(row)
            persons.add(user)

    if keyword:
        out = [r for r in out
               if keyword in (r.get("客户编号") or "") or keyword in (r.get("客户") or "")
               or keyword in (r.get("服务人员") or "") or keyword in (r.get("工单类型") or "")]
    return out, sorted({r["服务人员"] for r in out if r.get("服务人员")})
