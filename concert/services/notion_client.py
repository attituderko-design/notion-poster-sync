"""
concert.services.notion_client
Concert System 専用の Notion API クライアント。
既存 ArtéMis の ctx/api_request パターンを踏襲し、
Concert 用ヘッダー・DB ID を別管理する。
"""
import requests
import time
import streamlit as st
from functools import lru_cache

NOTION_VERSION = "2022-06-28"
DEFAULT_TIMEOUT = 20
_MAX_RETRIES = 3
_RETRY_STATUSES = {429, 500, 502, 503}


# ============================================================
# シークレット読み込み
# ============================================================

def get_concert_secrets() -> dict:
    """secrets.toml から Concert 用設定を取得する。
    APIトークンは ArtéMis と共用（NOTION_API_KEY）。
    """
    required_keys = ["NOTION_API_KEY"]
    missing = [k for k in required_keys if k not in st.secrets]
    if missing:
        raise KeyError(f"secrets.toml に以下のキーが見つかりません: {missing}")
    # 既存 ArtéMis DB を優先的に流用し、未設定時のみ Concert 専用DBへフォールバックする
    db_concert = st.secrets.get("NOTION_DB_ID") or st.secrets.get("CONCERT_DB_CONCERT", "")
    db_song = st.secrets.get("NOTION_SCORE_DB_ID") or st.secrets.get("CONCERT_DB_SONG", "")
    db_player = st.secrets.get("NOTION_PERFORMER_DB_ID") or st.secrets.get("CONCERT_DB_PLAYER", "")
    db_attendance = st.secrets.get("NOTION_PERFORMANCE_CAST_DB_ID") or st.secrets.get("CONCERT_DB_ATTENDANCE", "")
    db_player_instrument = st.secrets.get("NOTION_SONG_ASSIGN_DB_ID") or st.secrets.get("CONCERT_DB_PLAYER_INSTRUMENT", "")
    # 以下は現時点では Concert 専用DBを使用（既存と責務分離）
    db_practice = st.secrets.get("CONCERT_DB_PRACTICE", "")
    db_instrument = st.secrets.get("CONCERT_DB_INSTRUMENT", "")
    db_song_instrument = st.secrets.get("CONCERT_DB_SONG_INSTRUMENT", "")
    db_rental = st.secrets.get("CONCERT_DB_RENTAL", "")
    required_db = {
        "演奏会DB": db_concert,
        "練習DB": db_practice,
        "楽曲DB": db_song,
        "楽器種別DB": db_instrument,
        "曲別必要楽器DB": db_song_instrument,
        "奏者DB": db_player,
        "出欠DB": db_attendance,
        "楽器アサインDB": db_player_instrument,
        "レンタルDB": db_rental,
    }
    missing_db = [name for name, val in required_db.items() if not val]
    if missing_db:
        raise KeyError(f"secrets.toml のDB ID設定が不足しています: {', '.join(missing_db)}")
    return {
        "api_key":              st.secrets["NOTION_API_KEY"],  # ArtéMis と共用
        "db_concert":          db_concert,
        "db_practice":         db_practice,
        "db_song":             db_song,
        "db_instrument":       db_instrument,
        "db_song_instrument":  db_song_instrument,
        "db_player":           db_player,
        "db_attendance":       db_attendance,
        "db_player_instrument":db_player_instrument,
        "db_rental":           db_rental,
    }


def get_concert_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


# ============================================================
# API リクエスト（リトライ付き）
# ============================================================

def concert_api_request(method: str, url: str, headers: dict, **kwargs):
    """
    既存 app.py の api_request と同等のラッパー。
    429/5xx は指数バックオフでリトライする。
    """
    for attempt in range(_MAX_RETRIES):
        try:
            res = requests.request(
                method.upper(),
                url,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
                **kwargs,
            )
            if res.status_code not in _RETRY_STATUSES:
                return res
            if attempt < _MAX_RETRIES - 1:
                wait = (2 ** attempt) + 0.5
                time.sleep(wait)
        except requests.RequestException:
            if attempt < _MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    return None


# ============================================================
# DB プロパティ型マップ（キャッシュ付き）
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def get_concert_db_property_types(db_id: str, api_key: str) -> dict:
    """
    指定 DB のプロパティ名→型の辞書を返す。
    {"名称": "title", "カテゴリ": "select", ...}
    """
    if not db_id:
        return {}
    headers = get_concert_headers(api_key)
    res = concert_api_request("get", f"https://api.notion.com/v1/databases/{db_id}", headers=headers)
    if res is None or res.status_code != 200:
        return {}
    props = (res.json() or {}).get("properties", {})
    return {k: (v.get("type") or "") for k, v in props.items()}


# ============================================================
# ページネーション対応クエリ
# ============================================================

def query_concert_db_all(db_id: str, headers: dict, filter_payload: dict | None = None) -> list:
    """全件取得（ページネーション自動処理）。"""
    if not db_id:
        return []
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    all_results, has_more, next_cursor = [], True, None
    while has_more:
        payload: dict = {"page_size": 100}
        if filter_payload:
            payload.update(filter_payload)
        if next_cursor:
            payload["start_cursor"] = next_cursor
        res = concert_api_request("post", url, headers=headers, json=payload)
        if res is None or res.status_code != 200:
            break
        data = res.json()
        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")
    return all_results


# ============================================================
# プロパティ書き込みヘルパー
# ============================================================

def put_concert_prop(props: dict, type_map: dict, key: str, value) -> None:
    """
    key が type_map に存在する場合のみ props に Notion 形式で書き込む。
    既存 app.py の put_notion_prop と同等。
    """
    if key not in type_map:
        return
    ptype = type_map[key]

    if ptype == "title":
        props[key] = {"title": [{"text": {"content": str(value or "")}}]}

    elif ptype == "rich_text":
        props[key] = {"rich_text": [{"text": {"content": str(value or "")}}]}

    elif ptype == "number":
        props[key] = {"number": (float(value) if value not in (None, "") else None)}

    elif ptype == "select":
        props[key] = {"select": {"name": str(value)} if value else None}

    elif ptype == "multi_select":
        items = value if isinstance(value, list) else ([value] if value else [])
        props[key] = {"multi_select": [{"name": str(v)} for v in items if v]}

    elif ptype == "checkbox":
        props[key] = {"checkbox": bool(value)}

    elif ptype == "date":
        if value:
            if isinstance(value, str):
                props[key] = {"date": {"start": value}}
            else:
                # date / datetime オブジェクト
                props[key] = {"date": {"start": value.isoformat()}}
        else:
            props[key] = {"date": None}

    elif ptype == "relation":
        if value:
            ids = value if isinstance(value, list) else [value]
            props[key] = {"relation": [{"id": str(i)} for i in ids if i]}
        else:
            props[key] = {"relation": []}

    elif ptype == "email":
        props[key] = {"email": str(value) if value else None}

    elif ptype == "url":
        props[key] = {"url": str(value) if value else None}

    elif ptype == "location":
        # Notion location 型:
        # {"location":{"name","address","latitude","longitude"}}
        if isinstance(value, dict):
            payload = {}
            for fld in ("name", "address", "city", "region", "country", "postal_code"):
                if value.get(fld):
                    payload[fld] = str(value.get(fld))
            for fld in ("latitude", "longitude"):
                v = value.get(fld)
                if v not in (None, ""):
                    try:
                        payload[fld] = float(v)
                    except Exception:
                        pass
            props[key] = {"location": payload if payload else None}
        elif value:
            props[key] = {"location": {"name": str(value)}}
        else:
            props[key] = {"location": None}


# ============================================================
# プロパティ読み取りヘルパー
# ============================================================

def extract_concert_title(page: dict) -> str:
    """ページの title プロパティから表示文字列を取得する。"""
    props = page.get("properties", {})
    for meta in props.values():
        if (meta or {}).get("type") == "title":
            chunks = meta.get("title", []) or []
            return "".join((c.get("plain_text") or "") for c in chunks).strip()
    return ""


def extract_prop_text(page: dict, prop_name: str) -> str:
    """rich_text / title / select / number プロパティの文字列を返す。"""
    meta = (page.get("properties", {}) or {}).get(prop_name) or {}
    ptype = meta.get("type")
    if ptype in ("rich_text", "title"):
        return "".join((c.get("plain_text") or "") for c in (meta.get(ptype) or [])).strip()
    if ptype == "select":
        return ((meta.get("select") or {}).get("name") or "").strip()
    if ptype == "number":
        v = meta.get("number")
        return str(v) if v is not None else ""
    if ptype == "checkbox":
        return str(meta.get("checkbox", False))
    if ptype == "date":
        return ((meta.get("date") or {}).get("start") or "")
    if ptype == "email":
        return (meta.get("email") or "")
    if ptype == "url":
        return (meta.get("url") or "")
    if ptype == "formula":
        f = meta.get("formula") or {}
        ftype = f.get("type")
        if ftype == "string":
            return (f.get("string") or "")
        if ftype == "number":
            v = f.get("number")
            return str(v) if v is not None else ""
        if ftype == "boolean":
            return str(bool(f.get("boolean")))
        if ftype == "date":
            return ((f.get("date") or {}).get("start") or "")
    if ptype == "rollup":
        r = meta.get("rollup") or {}
        rtype = r.get("type")
        if rtype == "number":
            v = r.get("number")
            return str(v) if v is not None else ""
        if rtype == "date":
            return ((r.get("date") or {}).get("start") or "")
        if rtype == "array":
            arr = r.get("array") or []
            out = []
            for item in arr:
                itype = item.get("type")
                if itype in ("title", "rich_text"):
                    out.append("".join((c.get("plain_text") or "") for c in (item.get(itype) or [])))
                elif itype == "select":
                    out.append(((item.get("select") or {}).get("name") or ""))
                elif itype == "number":
                    nv = item.get("number")
                    out.append(str(nv) if nv is not None else "")
            return " / ".join([x for x in out if x])
    if ptype == "location":
        loc = meta.get("location") or {}
        # Notionのlocation型（将来拡張）: name/address を優先表示
        return (
            loc.get("name")
            or loc.get("address")
            or loc.get("city")
            or loc.get("region")
            or ""
        )
    return ""


def extract_relation_ids(page: dict, prop_name: str) -> list[str]:
    """relation プロパティから ID リストを返す。"""
    meta = (page.get("properties", {}) or {}).get(prop_name) or {}
    return [(r.get("id") or "") for r in (meta.get("relation") or []) if r.get("id")]


def find_prop_name(type_map: dict, candidates: list[str]) -> str:
    """候補名リストから、DBに存在する最初のプロパティ名を返す。"""
    if not type_map:
        return ""
    for key in candidates:
        if key in type_map:
            return key
    return ""


def extract_prop_text_any(page: dict, candidates: list[str]) -> str:
    """候補名リストのうち最初に見つかったプロパティの文字列値を返す。"""
    for key in candidates:
        v = extract_prop_text(page, key)
        if v != "":
            return v
    return ""


def extract_relation_ids_any(page: dict, candidates: list[str]) -> list[str]:
    """候補名リストのうち最初に見つかったrelation ID配列を返す。"""
    for key in candidates:
        ids = extract_relation_ids(page, key)
        if ids:
            return ids
    return []


def put_concert_prop_any(props: dict, type_map: dict, candidates: list[str], value) -> str:
    """候補名リストのうちDBに存在する最初のプロパティへ値を書き込む。"""
    key = find_prop_name(type_map, candidates)
    if not key:
        return ""
    put_concert_prop(props, type_map, key, value)
    return key


# ============================================================
# ctx ビルダー
# ============================================================

def build_concert_ctx() -> dict:
    """
    Concert 系サービス関数に渡す ctx を組み立てる。
    app.py 側の ctx ビルド規約と同じ構造を持つ。
    """
    secrets = get_concert_secrets()
    api_key = secrets["api_key"]
    headers = get_concert_headers(api_key)

    def _api_request(method, url, **kwargs):
        return concert_api_request(method, url, headers=headers, **kwargs)

    def _query_all(db_id, filter_payload=None):
        return query_concert_db_all(db_id, headers, filter_payload)

    def _get_prop_types(db_id):
        return get_concert_db_property_types(db_id, api_key)

    def _put_prop(props, type_map, key, value):
        put_concert_prop(props, type_map, key, value)

    return {
        # 認証
        "NOTION_HEADERS":              headers,
        "api_request":                 _api_request,
        # DB IDs
        "CONCERT_DB_CONCERT":          secrets["db_concert"],
        "CONCERT_DB_PRACTICE":         secrets["db_practice"],
        "CONCERT_DB_SONG":             secrets["db_song"],
        "CONCERT_DB_INSTRUMENT":       secrets["db_instrument"],
        "CONCERT_DB_SONG_INSTRUMENT":  secrets["db_song_instrument"],
        "CONCERT_DB_PLAYER":           secrets["db_player"],
        "CONCERT_DB_ATTENDANCE":       secrets["db_attendance"],
        "CONCERT_DB_PLAYER_INSTRUMENT":secrets["db_player_instrument"],
        "CONCERT_DB_RENTAL":           secrets["db_rental"],
        # ユーティリティ
        "query_all":                   _query_all,
        "get_prop_types":              _get_prop_types,
        "put_prop":                    _put_prop,
        "extract_title":               extract_concert_title,
        "extract_prop_text":           extract_prop_text,
        "extract_relation_ids":        extract_relation_ids,
        "find_prop_name":              find_prop_name,
        "extract_prop_text_any":       extract_prop_text_any,
        "extract_relation_ids_any":    extract_relation_ids_any,
        "put_prop_any":                put_concert_prop_any,
    }
