"""调试进度（明细）—— 公共算法层（2026-11-12 建）

本地/在线「一式两份」：同一套计算，只换数据源。
  · 在线 ol:debug-progress：task detail（已提交）+ 节点暂存
  · 本地 debug-progress   ：本地 SQLite 的 detail_json（只有已提交，无暂存）
调用方注入 ts(v) → 'YYYY-MM-DD HH:MM:SS' 字符串。
"""

DEBUG_COLUMNS = ["序号", "调试人员", "项目编码", "客户名称", "调试内容", "调试日期"]
DBG_ITEMS_FIELD = "field_FwVetv884eAgXTfa"                  # 调试项（多行调试组件）
DBG_ITEM_CONTENT = "field45wlp82DxDQKzlmhYjFsnVXPFDeKKeBc"  # 调试内容
DBG_ITEM_TIME = "field_cJ6uku6AtKpo1XLw"                    # 调试时间
DBG_ITEM_PERSON = "field_UDTamlMA7djGJC9M"                  # 调试人


def _cname_of(t, cust):
    ce = t.get("customerEntity")
    ce_name = (ce.get("name") or "") if isinstance(ce, dict) else (ce or "")
    return ce_name or cust.get("name") or ""


def build_debug(tasks, details, tmp_map=None, keyword="", date_filter=True,
                b_day="", e_day="", ts=None):
    """返回明细行（一条调试内容一行），列顺序 = DEBUG_COLUMNS"""
    tmp_map = tmp_map or {}
    keyword = (keyword or "").strip()
    rows = []

    for t in tasks:
        init = details.get(t.get("id")) or {}
        task = init.get("task") or {}
        cust = task.get("customer") or {}
        serial = cust.get("serialNumber") or t.get("customerId") or ""
        cname = _cname_of(t, cust)

        sub_items = ((task.get("attribute") or {}).get(DBG_ITEMS_FIELD) or [])
        tmp_items = ((tmp_map.get(t.get("id")) or {}).get(DBG_ITEMS_FIELD) or [])

        # 暂存在前（同一 bizId 以暂存为准），再补已提交
        items = list(tmp_items) + list(sub_items)
        seen, uniq = set(), []
        for it in items:
            if not isinstance(it, dict):
                continue
            bid = it.get("bizId")
            if not bid:
                persons = it.get(DBG_ITEM_PERSON) if isinstance(it.get(DBG_ITEM_PERSON), list) else []
                bid = "%s|%s|%s" % (it.get(DBG_ITEM_CONTENT), it.get(DBG_ITEM_TIME),
                                    (persons[0] or {}).get("displayName") if persons else "")
            if bid in seen:
                continue
            seen.add(bid)
            uniq.append(it)

        for it in uniq:
            content = str(it.get(DBG_ITEM_CONTENT) or "").strip()
            names = "、".join([(u or {}).get("displayName") or ""
                              for u in (it.get(DBG_ITEM_PERSON) or [])]).strip("、")
            day = (ts(it.get(DBG_ITEM_TIME)) or "")[:10]
            if not content and not names:
                continue
            if date_filter and day and (day < b_day or day > e_day):
                continue
            if keyword and not any(keyword in str(x) for x in (serial, cname, names, content)):
                continue
            rows.append({"调试人员": names, "项目编码": serial, "客户名称": cname,
                         "调试内容": content, "调试日期": day})

    rows.sort(key=lambda r: (r["项目编码"] or "", r["调试日期"] or "", r["调试内容"] or ""))
    return [{"序号": i, **r} for i, r in enumerate(rows, 1)]
