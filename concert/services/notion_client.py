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
import re
import unicodedata
from concert.services.keys import (
    CONCERT_NAME_KEYS,
    CONCERT_DATE_KEYS,
    PRACTICE_NAME_KEYS,
    PRACTICE_DATE_KEYS,
    PRACTICE_CONCERT_REL_KEYS,
    SONG_NAME_KEYS,
    SONG_CONCERT_REL_KEYS,
    INSTRUMENT_NAME_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS,
    PARTICIPANT_CONCERT_REL_KEYS,
    ATT_PLAYER_REL_KEYS,
    ATT_PRACTICE_REL_KEYS,
    PARTDEF_CONCERT_REL_KEYS,
    PARTDEF_SONG_REL_KEYS,
    PARTDEF_INST_REL_KEYS,
    PREF_PLAYER_REL_KEYS,
    PREF_PART_REL_KEYS,
    PREF_PRIORITY_KEYS,
)

NOTION_VERSION = "2022-06-28"
DEFAULT_TIMEOUT = 20
_MAX_RETRIES = 3
_RETRY_STATUSES = {429, 500, 502, 503}

# HARMONIA 用の既定DB（secrets未設定時の最終フォールバック）
# 2026-03-23 時点: ユーザー指定DB
_DEFAULT_CONCERT_DB_IDS = {
    "concert": "2704532d7d5680ab9beed2574eb2daa5",          # ArtéMis ATLAS
    "practice": "32c4532d7d56804caac4cae1fd4ada4f",         # 練習 Practice
    "song": "3224532d7d56804a85dbd2eab6ac2050",             # ArtéMis APOLLO
    "instrument": "32c4532d7d56800cb34ac6d1b1c3ecdb",       # 楽器種別 Instrument
    "song_instrument": "32c4532d7d56803ba3e1c8c87d1cd0dc",  # PART_DEFINITION DB（SONG_INSTRUMENTの代替）
    "player": "3224532d7d568072bbb0c2cea44d67d9",           # 出演者DB
    "participant": "3224532d7d56808e8dd0eb06c11f92db",      # 演奏会参加者DB（既存）
    "attendance": "32c4532d7d5680e6813fe67bae986c39",       # 練習出欠DB
    "player_instrument": "3224532d7d5680bd9acef5bbf042daa6",# 楽曲別担当者DB（既存）
    "rental": "32c6e5f3-8885-8072-9131-ceaff635b895",       # レンタル見積 Rental
    "part_definition": "32c4532d7d56803ba3e1c8c87d1cd0dc",  # パート定義DB
    "preference": "32c4532d7d5680b1902dce3555590db3",       # 希望入力DB
    "billing": "3314532d7d5680fb9cdbebd1d2730e62",          # 見積/請求DB（任意）
    "concert_song": "3324532d7d5680f38f0fccc3adae9860",     # 演奏会×曲 管理DB
    "harmonia_concert": "3334532d7d5680589934fa73ed352551",  # HARMONIA演奏会ヘッダDB
}

_NOTION_ID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _normalize_notion_id(value: str) -> str:
    """Notion DB/Page IDを正規化（ハイフン有りUUID形式）する。"""
    raw = str(value or "").strip()
    if not raw:
        return ""
    if _NOTION_ID_PATTERN.fullmatch(raw):
        return raw.lower()
    compact = raw.replace("-", "")
    if re.fullmatch(r"[0-9a-fA-F]{32}", compact):
        return (
            f"{compact[0:8]}-{compact[8:12]}-{compact[12:16]}-"
            f"{compact[16:20]}-{compact[20:32]}"
        ).lower()
    return ""


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
    db_concert = (
        st.secrets.get("NOTION_DB_ID")
        or st.secrets.get("CONCERT_DB_CONCERT", "")
        or _DEFAULT_CONCERT_DB_IDS["concert"]
    )
    db_song = (
        st.secrets.get("NOTION_SCORE_DB_ID")
        or st.secrets.get("CONCERT_DB_SONG", "")
        or _DEFAULT_CONCERT_DB_IDS["song"]
    )
    db_player = (
        st.secrets.get("NOTION_PERFORMER_DB_ID")
        or st.secrets.get("CONCERT_DB_PLAYER", "")
        or _DEFAULT_CONCERT_DB_IDS["player"]
    )
    db_participant = (
        st.secrets.get("CONCERT_DB_PARTICIPANT", "")
        or st.secrets.get("NOTION_PERFORMANCE_CAST_DB_ID", "")
        or _DEFAULT_CONCERT_DB_IDS["participant"]
    )
    # 専用DBキーを優先（HARMONIA本設計）
    db_attendance = (
        st.secrets.get("CONCERT_DB_ATTENDANCE", "")
        or _DEFAULT_CONCERT_DB_IDS["attendance"]
    )
    db_player_instrument = (
        st.secrets.get("CONCERT_DB_PLAYER_INSTRUMENT", "")
        or st.secrets.get("NOTION_SONG_ASSIGN_DB_ID", "")
        or _DEFAULT_CONCERT_DB_IDS["player_instrument"]
    )
    db_part_definition = (
        st.secrets.get("CONCERT_DB_PART_DEFINITION", "")
        or _DEFAULT_CONCERT_DB_IDS["part_definition"]
    )
    db_preference = (
        st.secrets.get("CONCERT_DB_PREFERENCE", "")
        or _DEFAULT_CONCERT_DB_IDS["preference"]
    )
    # 以下は現時点では Concert 専用DBを使用（既存と責務分離）
    db_practice = st.secrets.get("CONCERT_DB_PRACTICE", "") or _DEFAULT_CONCERT_DB_IDS["practice"]
    db_instrument = st.secrets.get("CONCERT_DB_INSTRUMENT", "") or _DEFAULT_CONCERT_DB_IDS["instrument"]
    db_song_instrument = st.secrets.get("CONCERT_DB_SONG_INSTRUMENT", "") or _DEFAULT_CONCERT_DB_IDS["song_instrument"]
    db_rental = st.secrets.get("CONCERT_DB_RENTAL", "") or _DEFAULT_CONCERT_DB_IDS["rental"]
    db_schedule = st.secrets.get("CONCERT_DB_SCHEDULE", "") or _DEFAULT_CONCERT_DB_IDS.get("schedule", "")
    db_pi_master = st.secrets.get("CONCERT_DB_PLAYER_INSTRUMENT_MASTER", "") or _DEFAULT_CONCERT_DB_IDS.get("pi_master", "")
    db_expense   = (
        st.secrets.get("CONCERT_DB_CONCERT_EXPENSE", "")
        or st.secrets.get("CONCERT_DB_CONCERT_EXPENCE", "")
        or _DEFAULT_CONCERT_DB_IDS.get("expense", "")
    )
    db_billing = (
        st.secrets.get("CONCERT_DB_BILLING", "")
        or _DEFAULT_CONCERT_DB_IDS.get("billing", "")
    )
    db_concert_song = (
        st.secrets.get("CONCERT_DB_CONCERT_SONG", "")
        or _DEFAULT_CONCERT_DB_IDS.get("concert_song", "")
    )
    db_harmonia_concert = (
        st.secrets.get("CONCERT_DB_HARMONIA_CONCERT", "")
        or _DEFAULT_CONCERT_DB_IDS.get("harmonia_concert", "")
    )
    required_db = {
        "演奏会DB": db_concert,
        "練習DB": db_practice,
        "楽曲DB": db_song,
        "楽器種別DB": db_instrument,
        "パート定義DB(SONG_INSTRUMENT兼用)": db_song_instrument,
        "奏者DB": db_player,
        "演奏会参加者DB": db_participant,
        "出欠DB": db_attendance,
        "楽器アサインDB": db_player_instrument,
        "レンタルDB": db_rental,
        "パート定義DB": db_part_definition,
        "希望入力DB": db_preference,
        "スケジュールDB": db_schedule,
        "演奏会×曲DB": db_concert_song,
        "HARMONIA演奏会ヘッダDB": db_harmonia_concert,
    }
    normalized_required_db = {
        name: _normalize_notion_id(val) for name, val in required_db.items()
    }
    missing_db = [name for name, val in normalized_required_db.items() if not val]
    if missing_db:
        raise KeyError(f"secrets.toml のDB ID設定が不足しています: {', '.join(missing_db)}")
    return {
        "api_key":              st.secrets["NOTION_API_KEY"],  # ArtéMis と共用
        "db_concert":          normalized_required_db["演奏会DB"],
        "db_practice":         normalized_required_db["練習DB"],
        "db_song":             normalized_required_db["楽曲DB"],
        "db_instrument":       normalized_required_db["楽器種別DB"],
        "db_song_instrument":  normalized_required_db["パート定義DB(SONG_INSTRUMENT兼用)"],
        "db_player":           normalized_required_db["奏者DB"],
        "db_participant":      normalized_required_db["演奏会参加者DB"],
        "db_attendance":       normalized_required_db["出欠DB"],
        "db_player_instrument":normalized_required_db["楽器アサインDB"],
        "db_rental":           normalized_required_db["レンタルDB"],
        "db_part_definition":  normalized_required_db["パート定義DB"],
        "db_preference":       normalized_required_db["希望入力DB"],
        "db_schedule":         normalized_required_db["スケジュールDB"],
        "db_pi_master":        _normalize_notion_id(db_pi_master),
        "db_expense":          _normalize_notion_id(db_expense),
        "db_billing":          _normalize_notion_id(db_billing),
        "db_concert_song":     normalized_required_db["演奏会×曲DB"],
        "db_harmonia_concert": normalized_required_db["HARMONIA演奏会ヘッダDB"],
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
    session_stateにキャッシュして2回目以降はAPIを叩かない。
    """
    import streamlit as st
    if not db_id:
        return {}
    cache_key = f"_prop_types_{db_id}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]
    headers = get_concert_headers(api_key)
    res = concert_api_request("get", f"https://api.notion.com/v1/databases/{db_id}", headers=headers)
    if res is None or res.status_code != 200:
        return {}
    props = (res.json() or {}).get("properties", {})
    result = {k: (v.get("type") or "") for k, v in props.items()}
    st.session_state[cache_key] = result
    return result


@st.cache_data(ttl=300, show_spinner=False)
def get_concert_db_schema(db_id: str, api_key: str) -> dict:
    """DBの生スキーマ（properties）を返す。"""
    if not db_id:
        return {}
    headers = get_concert_headers(api_key)
    res = concert_api_request("get", f"https://api.notion.com/v1/databases/{db_id}", headers=headers)
    if res is None or res.status_code != 200:
        return {}
    return (res.json() or {}).get("properties", {}) or {}


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

    elif ptype == "phone_number":
        props[key] = {"phone_number": str(value) if value else None}

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
    if ptype == "phone_number":
        return (meta.get("phone_number") or "")
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


def make_concert_key(*parts, prefix: str = "") -> str:
    """
    可読性のある安定キーを生成する。
    例:
      make_concert_key("Happy Hour Orchestre お祭りコンサート", prefix="concert")
      -> concert_happy_hour_orchestre_お祭りコンサート
    """
    raw = " ".join([str(p or "").strip() for p in parts if str(p or "").strip()])
    if not raw:
        raw = "item"
    txt = unicodedata.normalize("NFKC", raw).lower()
    txt = re.sub(r"[^\w]+", "_", txt, flags=re.UNICODE)
    txt = re.sub(r"_+", "_", txt).strip("_")
    if not txt:
        txt = "item"
    if prefix:
        p = re.sub(r"[^\w]+", "_", unicodedata.normalize("NFKC", str(prefix).lower()), flags=re.UNICODE)
        p = re.sub(r"_+", "_", p).strip("_")
        if p:
            return f"{p}_{txt}"
    return txt


def put_concert_key_any(
    props: dict,
    type_map: dict,
    candidates: list[str],
    *parts,
    prefix: str = "",
) -> str:
    """
    key候補列（例: part_key / preference_key）へ自動生成したキーを格納する。
    返値: 書き込んだプロパティ名（未検出時は ""）
    """
    key_prop = find_prop_name(type_map, candidates)
    if not key_prop:
        # 緩く *_key / key を含む列へフォールバック
        for k, t in (type_map or {}).items():
            if t not in ("title", "rich_text"):
                continue
            kl = str(k).lower()
            if kl.endswith("_key") or kl == "key" or "key" in kl or "キー" in str(k):
                key_prop = k
                break
    if not key_prop:
        return ""
    put_concert_prop(props, type_map, key_prop, make_concert_key(*parts, prefix=prefix))
    return key_prop


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

    def _get_db_schema(db_id):
        return get_concert_db_schema(db_id, api_key)

    def _validate_contract() -> dict:
        """
        HARMONIA最小契約の健全性を検証する。
        - 必須プロパティ候補の存在
        - relationの向き（target DB）の一致
        """
        checks = [
            ("CONCERT_DB_CONCERT", "演奏会DB", [("any", CONCERT_NAME_KEYS), ("any", CONCERT_DATE_KEYS)]),
            (
                "CONCERT_DB_PRACTICE",
                "練習DB",
                [
                    ("any", PRACTICE_NAME_KEYS),
                    ("any", PRACTICE_DATE_KEYS),
                    ("relation", PRACTICE_CONCERT_REL_KEYS, "CONCERT_DB_CONCERT"),
                ],
            ),
            (
                "CONCERT_DB_SONG",
                "楽曲DB",
                [("any", SONG_NAME_KEYS), ("relation", SONG_CONCERT_REL_KEYS, "CONCERT_DB_CONCERT")],
            ),
            ("CONCERT_DB_INSTRUMENT", "楽器種別DB", [("any", INSTRUMENT_NAME_KEYS)]),
            (
                "CONCERT_DB_PARTICIPANT",
                "演奏会参加者DB",
                [
                    ("relation", PARTICIPANT_PLAYER_REL_KEYS, "CONCERT_DB_PLAYER"),
                    ("relation", PARTICIPANT_CONCERT_REL_KEYS, "CONCERT_DB_CONCERT"),
                ],
            ),
            (
                "CONCERT_DB_ATTENDANCE",
                "練習出欠DB",
                [
                    ("relation", ATT_PLAYER_REL_KEYS, "CONCERT_DB_PARTICIPANT"),
                    ("relation", ATT_PRACTICE_REL_KEYS, "CONCERT_DB_PRACTICE"),
                ],
            ),
            (
                "CONCERT_DB_PART_DEFINITION",
                "パート定義DB",
                [
                    ("relation", PARTDEF_CONCERT_REL_KEYS, "CONCERT_DB_CONCERT"),
                    ("relation", PARTDEF_SONG_REL_KEYS, "CONCERT_DB_SONG"),
                    ("relation", PARTDEF_INST_REL_KEYS, "CONCERT_DB_INSTRUMENT"),
                ],
            ),
            (
                "CONCERT_DB_CONCERT_SONG",
                "演奏会×曲DB",
                [
                    ("relation", ["演奏会", "FK演奏会", "concert"], "CONCERT_DB_CONCERT"),
                    ("relation", ["曲", "楽曲", "演奏曲", "song"], "CONCERT_DB_CONCERT"),  # 曲はATLAS向き
                    ("any", ["定義完了", "definition_done"]),
                ],
            ),
            (
                "CONCERT_DB_PREFERENCE",
                "希望入力DB",
                [
                    ("relation", PREF_PLAYER_REL_KEYS, "CONCERT_DB_PARTICIPANT"),
                    ("relation", PREF_PART_REL_KEYS, "CONCERT_DB_PART_DEFINITION"),
                    ("any", PREF_PRIORITY_KEYS),
                ],
            ),
            (
                "CONCERT_DB_HARMONIA_CONCERT",
                "HARMONIA演奏会ヘッダDB",
                [
                    ("any", ["concert_key", "タイトル"]),
                    ("relation", ["演奏会", "FK演奏会", "concert"], "CONCERT_DB_CONCERT"),
                    ("any", ["管理開始"]),
                    ("any", ["楽曲情報確定"]),
                    ("any", ["練習情報確定"]),
                    ("any", ["練習日確定"]),
                    ("any", ["必要楽器確定"]),
                    ("any", ["パート定義確定"]),
                    ("any", ["奏者情報確定"]),
                    ("any", ["所有楽器確定"]),
                    ("any", ["出欠確定"]),
                    ("any", ["希望入力確定"]),
                    ("any", ["持参楽器確定"]),
                    ("any", ["案提示"]),
                    ("any", ["アサイン確定"]),
                    ("any", ["収支確定"]),
                ],
            ),
        ]

        errors: list[str] = []
        warnings: list[str] = []

        def _find_rel(schema: dict, candidates: list[str]) -> tuple[str, dict]:
            for c in candidates:
                m = schema.get(c) or {}
                if m.get("type") == "relation":
                    return c, m
            # ゆるい一致（空白除去・小文字）
            norm = {re.sub(r"\s+", "", str(k)).lower(): k for k in schema.keys()}
            for c in candidates:
                key = norm.get(re.sub(r"\s+", "", str(c)).lower())
                if not key:
                    continue
                m = schema.get(key) or {}
                if m.get("type") == "relation":
                    return key, m
            return "", {}

        def _find_any(type_map: dict, candidates: list[str]) -> str:
            key = find_prop_name(type_map, candidates)
            if key:
                return key
            norm = {re.sub(r"\s+", "", str(k)).lower(): k for k in type_map.keys()}
            for c in candidates:
                got = norm.get(re.sub(r"\s+", "", str(c)).lower(), "")
                if got:
                    return got
            return ""

        for db_ctx_key, db_label, db_checks in checks:
            db_id = secrets.get(
                {
                    "CONCERT_DB_CONCERT": "db_concert",
                    "CONCERT_DB_PRACTICE": "db_practice",
                    "CONCERT_DB_SONG": "db_song",
                    "CONCERT_DB_INSTRUMENT": "db_instrument",
                    "CONCERT_DB_PLAYER": "db_player",
                    "CONCERT_DB_PARTICIPANT": "db_participant",
                    "CONCERT_DB_ATTENDANCE": "db_attendance",
                    "CONCERT_DB_PART_DEFINITION": "db_part_definition",
                    "CONCERT_DB_CONCERT_SONG": "db_concert_song",
                    "CONCERT_DB_PREFERENCE": "db_preference",
                    "CONCERT_DB_HARMONIA_CONCERT": "db_harmonia_concert",
                }.get(db_ctx_key, ""),
                "",
            )
            schema = _get_db_schema(db_id)
            type_map = {k: (v.get("type") or "") for k, v in schema.items()}
            if not schema:
                errors.append(f"{db_label}: スキーマ取得失敗（Integration接続/DB IDを確認）")
                continue

            for chk in db_checks:
                if chk[0] == "any":
                    _, candidates = chk
                    found = _find_any(type_map, candidates)
                    if not found:
                        errors.append(f"{db_label}: 必須候補が見つかりません {candidates}")
                elif chk[0] == "relation":
                    _, candidates, target_ctx_key = chk
                    found, meta = _find_rel(schema, candidates)
                    if not found:
                        errors.append(f"{db_label}: relation候補が見つかりません {candidates}")
                        continue
                    target_db = ((meta.get("relation") or {}).get("database_id") or "").lower()
                    expected_db = (
                        secrets.get(
                            {
                                "CONCERT_DB_CONCERT": "db_concert",
                                "CONCERT_DB_PRACTICE": "db_practice",
                                "CONCERT_DB_SONG": "db_song",
                                "CONCERT_DB_INSTRUMENT": "db_instrument",
                                "CONCERT_DB_PLAYER": "db_player",
                                "CONCERT_DB_PARTICIPANT": "db_participant",
                                "CONCERT_DB_ATTENDANCE": "db_attendance",
                                "CONCERT_DB_PART_DEFINITION": "db_part_definition",
                                "CONCERT_DB_CONCERT_SONG": "db_concert_song",
                                "CONCERT_DB_PREFERENCE": "db_preference",
                            }.get(target_ctx_key, ""),
                            "",
                        )
                        or ""
                    ).lower()
                    if target_db and expected_db and target_db != expected_db:
                        warnings.append(
                            f"{db_label}.{found}: relation先が想定と不一致 "
                            f"(actual={target_db}, expected={expected_db})"
                        )

        return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}

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
        "CONCERT_DB_PARTICIPANT":      secrets["db_participant"],
        "CONCERT_DB_ATTENDANCE":       secrets["db_attendance"],
        "CONCERT_DB_PLAYER_INSTRUMENT":secrets["db_player_instrument"],
        "CONCERT_DB_RENTAL":           secrets["db_rental"],
        "CONCERT_DB_PART_DEFINITION":  secrets["db_part_definition"],
        "CONCERT_DB_PREFERENCE":       secrets["db_preference"],
        "CONCERT_DB_SCHEDULE":         secrets["db_schedule"],
        "CONCERT_DB_CONCERT_EXPENSE":  secrets["db_expense"],
        "CONCERT_DB_BILLING":          secrets["db_billing"],
        "CONCERT_DB_CONCERT_SONG":     secrets["db_concert_song"],
        "CONCERT_DB_HARMONIA_CONCERT":  secrets["db_harmonia_concert"],
        "query_all":                   _query_all,
        "get_prop_types":              _get_prop_types,
        "get_db_schema":               _get_db_schema,
        "put_prop":                    _put_prop,
        "extract_title":               extract_concert_title,
        "extract_prop_text":           extract_prop_text,
        "extract_relation_ids":        extract_relation_ids,
        "find_prop_name":              find_prop_name,
        "extract_prop_text_any":       extract_prop_text_any,
        "extract_relation_ids_any":    extract_relation_ids_any,
        "put_prop_any":                put_concert_prop_any,
        "make_key":                    make_concert_key,
        "put_key_any":                 put_concert_key_any,
        "validate_contract":           _validate_contract,
    }
