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
    """secrets.toml から Concert 用設定を取得する。"""
    return {
        "api_key":              st.secrets["NOTION_CONCERT_API_KEY"],
        "db_concert":          st.secrets["CONCERT_DB_CONCERT"],
        "db_practice":         st.secrets["CONCERT_DB_PRACTICE"],
        "db_song":             st.secrets["CONCERT_DB_SONG"],
        "db_instrument":       st.secrets["CONCERT_DB_INSTRUMENT"],
        "db_song_instrument":  st.secrets["CONCERT_DB_SONG_INSTRUMENT"],
        "db_player":           st.secrets["CONCERT_DB_PLAYER"],
        "db_attendance":       st.secrets["CONCERT_DB_ATTENDANCE"],
        "db_player_instrument":st.secrets["CONCERT_DB_PLAYER_INSTRUMENT"],
        "db_rental":           st.secrets["CONCERT_DB_RENTAL"],
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
    return ""


def extract_relation_ids(page: dict, prop_name: str) -> list[str]:
    """relation プロパティから ID リストを返す。"""
    meta = (page.get("properties", {}) or {}).get(prop_name) or {}
    return [(r.get("id") or "") for r in (meta.get("relation") or []) if r.get("id")]


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
    }
