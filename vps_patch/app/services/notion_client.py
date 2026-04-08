"""
app/services/notion_client.py — Streamlit依存なしのNotionクライアント
"""
import os
import requests
from typing import Any
import uuid as _uuid
import time
import json
from contextvars import ContextVar

NOTION_VERSION = "2022-06-28"

DB_KEYS = [
    "CONCERT_DB_ATLAS","CONCERT_DB_PRACTICE","CONCERT_DB_SONG",
    "CONCERT_DB_INSTRUMENT","CONCERT_DB_PLAYER","CONCERT_DB_PARTICIPANT",
    "CONCERT_DB_ATTENDANCE","CONCERT_DB_PLAYER_INSTRUMENT",
    "CONCERT_DB_PART_DEFINITION","CONCERT_DB_PREFERENCE",
    "CONCERT_DB_PART_MASTER","CONCERT_DB_HARMONIA_CONCERT",
    "CONCERT_DB_CONCERT_ASSIGNMENT","CONCERT_DB_CONCERT_SONG",
    "CONCERT_DB_SCHEDULE",
]

_QUERY_CACHE: dict[str, tuple[float, list[dict]]] = {}
_TYPE_CACHE: dict[str, tuple[float, dict]] = {}
_METRICS: ContextVar[list[dict]] = ContextVar("notion_metrics", default=[])


def _cache_ttl_seconds() -> int:
    try:
        return max(0, int(os.environ.get("NOTION_CACHE_TTL_SECONDS", "120")))
    except Exception:
        return 120


def _metric_add(kind: str, db_id: str, ms: float, cache_hit: bool = False, extra: dict | None = None) -> None:
    arr = list(_METRICS.get())
    row = {"kind": kind, "db": (db_id or "")[:8], "ms": round(ms, 2), "cache": cache_hit}
    if extra:
        row.update(extra)
    arr.append(row)
    _METRICS.set(arr)


def _metric_clear() -> None:
    _METRICS.set([])


def _metric_collect() -> list[dict]:
    return list(_METRICS.get())


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _query_all_notion(db_id: str, api_key: str,
                       filter_payload: dict | None = None) -> list[dict]:
    t0 = time.perf_counter()
    ttl = _cache_ttl_seconds()
    fkey = json.dumps(filter_payload, sort_keys=True, ensure_ascii=False) if filter_payload else ""
    cache_key = f"{db_id}::{fkey}"
    if ttl > 0:
        hit = _QUERY_CACHE.get(cache_key)
        if hit and (time.time() - hit[0] <= ttl):
            _metric_add("query_all", db_id, (time.perf_counter() - t0) * 1000, cache_hit=True, extra={"rows": len(hit[1])})
            return [dict(x) for x in hit[1]]

    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    results, cursor = [], None
    while True:
        body: dict[str, Any] = {"page_size": 100}
        if filter_payload:
            body["filter"] = filter_payload
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(url, headers=_headers(api_key), json=body, timeout=30)
        if resp.status_code != 200:
            break
        data = resp.json()
        for row in (data.get("results") or []):
            if row.get("id"):
                row["id"] = row["id"].replace("-", "")
            results.append(row)
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    if ttl > 0:
        _QUERY_CACHE[cache_key] = (time.time(), results)
    _metric_add("query_all", db_id, (time.perf_counter() - t0) * 1000, cache_hit=False, extra={"rows": len(results)})
    return results


def _get_prop_types(db_id: str, api_key: str) -> dict:
    t0 = time.perf_counter()
    ttl = _cache_ttl_seconds()
    if ttl > 0:
        hit = _TYPE_CACHE.get(db_id)
        if hit and (time.time() - hit[0] <= ttl):
            _metric_add("get_prop_types", db_id, (time.perf_counter() - t0) * 1000, cache_hit=True)
            return dict(hit[1])

    resp = requests.get(
        f"https://api.notion.com/v1/databases/{db_id}",
        headers=_headers(api_key), timeout=15,
    )
    if resp.status_code != 200:
        _metric_add("get_prop_types", db_id, (time.perf_counter() - t0) * 1000, cache_hit=False, extra={"status": resp.status_code})
        return {}
    props = resp.json().get("properties", {})
    if ttl > 0:
        _TYPE_CACHE[db_id] = (time.time(), props)
    _metric_add("get_prop_types", db_id, (time.perf_counter() - t0) * 1000, cache_hit=False, extra={"keys": len(props)})
    return props


def _extract_prop_text_any(page: dict, keys: list[str]) -> str:
    props = page.get("properties", {})
    for key in keys:
        prop = props.get(key)
        if not prop:
            continue
        ptype = prop.get("type", "")
        if ptype == "title":
            return "".join(i.get("plain_text","") for i in prop.get("title",[]))
        elif ptype == "rich_text":
            text = "".join(i.get("plain_text","") for i in prop.get("rich_text",[]))
            if text: return text
        elif ptype == "select":
            sel = prop.get("select") or {}
            if sel.get("name"): return sel["name"]
        elif ptype == "date":
            d = prop.get("date") or {}
            if d.get("start"): return d["start"]
        elif ptype == "checkbox":
            return str(prop.get("checkbox", False))
        elif ptype == "number":
            v = prop.get("number")
            if v is not None: return str(v)
        elif ptype in ("email","phone_number","url"):
            v = prop.get(ptype)
            if v: return v
        elif ptype == "formula":
            f = prop.get("formula", {})
            for ft in ["string","number","boolean","date"]:
                if ft in f: return str(f[ft]) if f[ft] is not None else ""
    return ""


def _extract_relation_ids_any(page: dict, keys: list[str]) -> list[str]:
    props = page.get("properties", {})
    for key in keys:
        prop = props.get(key)
        if prop and prop.get("type") == "relation":
            return [r.get("id","").replace("-","") for r in prop.get("relation",[])]
    return []


def _find_prop_name(type_map: dict, candidates: list[str]) -> str:
    for key in (type_map or {}):
        if any(c.lower() == key.lower() or c.lower() in key.lower()
               for c in candidates):
            return key
    return ""


def _put_prop(props: dict, type_map: dict, key: str, value: Any) -> None:
    if not key or key not in type_map:
        return
    ptype = type_map[key].get("type","")
    if ptype == "title":
        props[key] = {"title": [{"text": {"content": str(value)}}]}
    elif ptype == "rich_text":
        props[key] = {"rich_text": [{"text": {"content": str(value)}}]}
    elif ptype == "select":
        props[key] = {"select": {"name": str(value)}}
    elif ptype == "checkbox":
        props[key] = {"checkbox": bool(value)}
    elif ptype == "number":
        props[key] = {"number": float(value) if value is not None else None}
    elif ptype in ("email","phone_number","url"):
        props[key] = {ptype: str(value)}
    elif ptype == "relation":
        if isinstance(value, list):
            props[key] = {"relation": [{"id": v} for v in value]}
        else:
            props[key] = {"relation": [{"id": str(value)}]}


def _put_prop_any(props, type_map, candidates, value):
    key = _find_prop_name(type_map, candidates)
    if key:
        _put_prop(props, type_map, key, value)


def _put_key_any(props, type_map, candidates, *parts, prefix=""):
    key = _find_prop_name(type_map, candidates)
    if not key:
        return
    uid = str(_uuid.uuid4())[:8]
    label = f"{prefix}_{'_'.join(str(p)[:8] for p in parts)}_{uid}"
    _put_prop(props, type_map, key, label)


def _api_request(method, url, api_key, **kwargs):
    t0 = time.perf_counter()
    db = ""
    if "databases/" in url:
        db = url.split("databases/")[-1].split("/")[0]
    elif "pages/" in url:
        db = "pages"
    try:
        resp = requests.request(method, url, headers=_headers(api_key), timeout=30, **kwargs)
        _metric_add("api_request", db or method.upper(), (time.perf_counter() - t0) * 1000, extra={"method": method.upper(), "status": resp.status_code})
        return resp
    except Exception:
        _metric_add("api_request", db or method.upper(), (time.perf_counter() - t0) * 1000, extra={"method": method.upper(), "status": "error"})
        return None


def build_concert_ctx() -> dict:
    api_key = os.environ.get("NOTION_CONCERT_API_KEY","")

    ctx = {
        "query_all":                lambda db_id, fp=None: _query_all_notion(db_id, api_key, fp if isinstance(fp, dict) else None),
        "get_prop_types":           lambda db_id: _get_prop_types(db_id, api_key),
        "api_request":              lambda method, url, **kw: _api_request(method, url, api_key, **kw),
        "extract_prop_text_any":    _extract_prop_text_any,
        "extract_relation_ids_any": _extract_relation_ids_any,
        "find_prop_name":           _find_prop_name,
        "put_prop":                 _put_prop,
        "put_prop_any":             _put_prop_any,
        "put_key_any":              _put_key_any,
        "clear_metrics":            _metric_clear,
        "collect_metrics":          _metric_collect,
    }
    for key in DB_KEYS:
        ctx[key] = os.environ.get(key, "")
    return ctx
