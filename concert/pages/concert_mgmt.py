"""
concert.pages.concert_mgmt
演奏会・練習情報の登録・一覧・編集画面。
"""
import streamlit as st
from concert.services.keys import *  # noqa: F401,F403
from datetime import date, datetime, timezone, timedelta
import re
import requests





# ============================================================
# ヘルパー
# ============================================================

def _ss(key, default=None):
    return st.session_state.get(key, default)


def _compose_notion_date_with_optional_time(d: date, start_hhmm: str, end_hhmm: str) -> tuple[str, str]:
    """
    練習日 + 任意時刻を Notion date.start/date.end 用のISO文字列に整形する。
    return: (start_iso, end_iso)
    """
    s = (start_hhmm or "").strip()
    e = (end_hhmm or "").strip()
    hhmm_re = re.compile(r"^\d{1,2}:\d{2}$")

    def _to_dt(hhmm: str) -> datetime:
        h, m = hhmm.split(":")
        jst = timezone(timedelta(hours=9))
        return datetime(d.year, d.month, d.day, int(h), int(m), 0, tzinfo=jst)

    if s and not hhmm_re.match(s):
        raise ValueError("開始時刻は HH:MM 形式で入力してください（例: 19:30）")
    if e and not hhmm_re.match(e):
        raise ValueError("終了時刻は HH:MM 形式で入力してください（例: 21:00）")

    if s:
        sdt = _to_dt(s)
        if not (0 <= sdt.hour <= 23 and 0 <= sdt.minute <= 59):
            raise ValueError("開始時刻が不正です。")
        if e:
            edt = _to_dt(e)
            if not (0 <= edt.hour <= 23 and 0 <= edt.minute <= 59):
                raise ValueError("終了時刻が不正です。")
            if edt < sdt:
                raise ValueError("終了時刻は開始時刻以降にしてください。")
            return sdt.isoformat(), edt.isoformat()
        return sdt.isoformat(), ""

    if e:
        raise ValueError("終了時刻のみは指定できません。開始時刻も入力してください。")

    return d.isoformat(), ""


def _contains_query(values: list[str], query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return True
    blob = " ".join([(v or "") for v in values]).lower()
    return q in blob


def _normalize_page_id(v: str) -> str:
    return (v or "").replace("-", "").strip().lower()


def _practice_rel_prop_candidates(type_map: dict, ctx: dict) -> list[str]:
    out = []
    rel = ctx["find_prop_name"](type_map, PRACTICE_CONCERT_REL_KEYS)
    if rel:
        out.append(rel)
    for k, t in (type_map or {}).items():
        if t != "relation":
            continue
        ks = str(k)
        if ("演奏会" in ks) or ("出演" in ks) or ("concert" in ks.lower()) or ("fk" in ks.lower()):
            if k not in out:
                out.append(k)
    return out


def _extract_bool_any(ctx: dict, page: dict, keys: list[str], default: bool = False) -> bool:
    raw = (ctx["extract_prop_text_any"](page, keys) or "").strip().lower()
    if raw in ("true", "1", "yes", "on", "チェック済み"):
        return True
    if raw in ("false", "0", "no", "off"):
        return False
    return default


def _geocode_nominatim(query: str) -> list[dict]:
    q = (query or "").strip()
    if not q:
        return []
    try:
        res = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "jsonv2", "addressdetails": 1, "limit": 8},
            headers={"User-Agent": "artemis-harmonia/1.0"},
            timeout=10,
        )
        if res.status_code != 200:
            return []
        rows = res.json() or []
        out = []
        for r in rows:
            out.append(
                {
                    "name": r.get("display_name") or "",
                    "address": r.get("display_name") or "",
                    "lat": r.get("lat"),
                    "lon": r.get("lon"),
                }
            )
        return out
    except Exception:
        return []


def _clear_concert_cache(ctx):
    try:
        from concert.services.notion_client import get_concert_db_property_types
        get_concert_db_property_types.clear()
    except Exception:
        pass
    for k in ["concertmgmt_concert_list", "practice_list"]:
        st.session_state.pop(k, None)


def _backfill_pk_for_db(
    ctx: dict,
    db_id: str,
    key_candidates: list[str],
    prefix: str,
    parts_builder,
) -> tuple[int, int, int]:
    """
    既存レコードに key 列を付与/補正する。
    return: (scanned, updated, skipped)
    """
    t = ctx["get_prop_types"](db_id)
    rows = ctx["query_all"](db_id)
    scanned = len(rows)
    updated = 0
    skipped = 0
    for r in rows:
        parts = parts_builder(r)
        if not any(str(p or "").strip() for p in parts):
            skipped += 1
            continue
        props = {}
        key_prop = ctx["put_key_any"](props, t, key_candidates, *parts, prefix=prefix)
        if not key_prop:
            skipped += 1
            continue
        new_key = ctx["make_key"](*parts, prefix=prefix)
        cur_key = ctx["extract_prop_text"](r, key_prop)
        if cur_key == new_key:
            skipped += 1
            continue
        res = ctx["api_request"](
            "patch",
            f"https://api.notion.com/v1/pages/{r.get('id','')}",
            json={"properties": props},
        )
        if res is not None and res.status_code == 200:
            updated += 1
        else:
            skipped += 1
    return scanned, updated, skipped


def _backfill_all_concert_keys(ctx: dict) -> dict:
    """
    Concert System で使うDBの既存レコードへ key を一括反映する。
    """
    song_rel_keys = ["楽曲", "演奏曲", "FK楽曲", "作品楽章", "作品マスタ"]
    part_rel_keys = ["パート", "パート定義", "FKパート"]
    participant_player_rel = ["奏者", "出演者", "FK奏者", "演奏会参加者"]
    participant_concert_rel = ["演奏会", "出演", "FK演奏会"]
    attendance_player_rel = ["奏者", "出演者", "FK奏者", "演奏会参加者"]
    attendance_practice_rel = ["練習", "演奏会", "出演", "FK練習"]
    inst_rel_keys = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
    out = {}

    out["concert"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_CONCERT"],
        CONCERT_KEY_KEYS,
        "concert",
        lambda r: [
            ctx["extract_prop_text_any"](r, CONCERT_NAME_KEYS) or ctx["extract_title"](r),
            ctx["extract_prop_text_any"](r, CONCERT_DATE_KEYS),
        ],
    )
    out["practice"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_PRACTICE"],
        PRACTICE_KEY_KEYS,
        "practice",
        lambda r: [
            ",".join(ctx["extract_relation_ids_any"](r, PRACTICE_CONCERT_REL_KEYS)),
            ctx["extract_prop_text_any"](r, PRACTICE_NAME_KEYS) or ctx["extract_title"](r),
            ctx["extract_prop_text_any"](r, PRACTICE_DATE_KEYS),
        ],
    )
    out["song"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_SONG"],
        SONG_KEY_KEYS,
        "song",
        lambda r: [
            ctx["extract_prop_text_any"](r, ["曲名", "タイトル"]) or ctx["extract_title"](r),
            ctx["extract_prop_text_any"](r, ["作曲者", "クリエイター"]),
        ],
    )
    out["instrument"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_INSTRUMENT"],
        INSTRUMENT_KEY_KEYS,
        "inst",
        lambda r: [
            ctx["extract_prop_text_any"](r, ["楽器名", "タイトル"]) or ctx["extract_title"](r),
        ],
    )
    out["part_definition"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_PART_DEFINITION"],
        PARTDEF_KEY_KEYS,
        "part",
        lambda r: [
            ",".join(ctx["extract_relation_ids_any"](r, PRACTICE_CONCERT_REL_KEYS)),
            ",".join(ctx["extract_relation_ids_any"](r, song_rel_keys)),
            ctx["extract_prop_text_any"](r, ["パートNo", "パート番号"]),
            ctx["extract_prop_text_any"](r, ["パート名", "名称", "タイトル"]) or ctx["extract_title"](r),
            ",".join(ctx["extract_relation_ids_any"](r, inst_rel_keys)),
        ],
    )
    out["player"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_PLAYER"],
        PLAYER_KEY_KEYS,
        "player",
        lambda r: [ctx["extract_prop_text_any"](r, ["氏名", "名前", "表示名", "タイトル"]) or ctx["extract_title"](r)],
    )
    out["participant"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_PARTICIPANT"],
        PARTICIPANT_RECORD_KEYS,
        "participant",
        lambda r: [
            ",".join(ctx["extract_relation_ids_any"](r, participant_concert_rel)),
            ",".join(ctx["extract_relation_ids_any"](r, participant_player_rel)),
        ],
    )
    out["attendance"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_ATTENDANCE"],
        ATTENDANCE_KEY_KEYS,
        "att",
        lambda r: [
            ",".join(ctx["extract_relation_ids_any"](r, attendance_player_rel)),
            ",".join(ctx["extract_relation_ids_any"](r, attendance_practice_rel)),
        ],
    )
    out["assign"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_PLAYER_INSTRUMENT"],
        ASSIGN_KEY_KEYS,
        "assign",
        lambda r: [
            ",".join(ctx["extract_relation_ids_any"](r, participant_player_rel)),
            ",".join(ctx["extract_relation_ids_any"](r, inst_rel_keys)),
            ",".join(ctx["extract_relation_ids_any"](r, song_rel_keys)),
        ],
    )
    out["preference"] = _backfill_pk_for_db(
        ctx,
        ctx["CONCERT_DB_PREFERENCE"],
        PREFERENCE_KEY_KEYS,
        "pref",
        lambda r: [
            ",".join(ctx["extract_relation_ids_any"](r, participant_player_rel)),
            ",".join(ctx["extract_relation_ids_any"](r, song_rel_keys)),
            ",".join(ctx["extract_relation_ids_any"](r, part_rel_keys)),
            ",".join(ctx["extract_relation_ids_any"](r, inst_rel_keys)),
        ],
    )
    if ctx.get("CONCERT_DB_RENTAL"):
        out["rental"] = _backfill_pk_for_db(
            ctx,
            ctx["CONCERT_DB_RENTAL"],
            RENTAL_KEY_KEYS,
            "rental",
            lambda r: [
                ",".join(ctx["extract_relation_ids_any"](r, ["練習", "演奏会", "出演", "FK練習"])),
                ",".join(ctx["extract_relation_ids_any"](r, inst_rel_keys)),
                ctx["extract_prop_text_any"](r, ["業者名", "ベンダー", "vendor"]),
            ],
        )
    return out


def _concert_media_values(c: dict) -> list[str]:
    props = (c or {}).get("properties", {}) or {}
    out: list[str] = []
    for key in CONCERT_MEDIA_KEYS:
        meta = props.get(key) or {}
        ptype = meta.get("type")
        if ptype == "select":
            n = ((meta.get("select") or {}).get("name") or "").strip()
            if n:
                out.append(n)
        elif ptype == "multi_select":
            for it in (meta.get("multi_select") or []):
                n = (it.get("name") or "").strip()
                if n:
                    out.append(n)
        elif ptype in ("rich_text", "title"):
            txt = "".join((x.get("plain_text") or "") for x in (meta.get(ptype) or [])).strip()
            if txt:
                out.extend([s.strip() for s in txt.replace("／", "/").split("/") if s.strip()])
        elif ptype == "formula":
            f = meta.get("formula") or {}
            if f.get("type") == "string":
                txt = (f.get("string") or "").strip()
                if txt:
                    out.extend([s.strip() for s in txt.replace("／", "/").split("/") if s.strip()])
    return list(dict.fromkeys(out))


def _is_performance_media_concert(c: dict) -> bool:
    return "出演" in _concert_media_values(c)


def _location_payload(venue: str, address: str, lat=None, lon=None) -> dict:
    payload = {}
    if venue:
        payload["name"] = str(venue)
    if address:
        payload["address"] = str(address)
    try:
        if lat not in (None, ""):
            payload["latitude"] = float(lat)
        if lon not in (None, ""):
            payload["longitude"] = float(lon)
    except Exception:
        pass
    return payload


def _load_concerts(ctx) -> list[dict]:
    if "concertmgmt_concert_list" not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
        st.session_state["concertmgmt_concert_list"] = [r for r in rows if _is_performance_media_concert(r)]
    return st.session_state.get("concertmgmt_concert_list", [])


def _load_songs(ctx, concert_id: str) -> list[dict]:
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
        rel = ctx["find_prop_name"](t, SONG_CONCERT_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_SONG"], f)
    return st.session_state.get(key, [])


def _load_practices(ctx, concert_id: str = "") -> list[dict]:
    # 練習データは手入力→即確認の運用が多いため毎回最新を取得する
    rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"])
    if not concert_id:
        return rows

    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_PRACTICE"])
    rel_props = _practice_rel_prop_candidates(type_map, ctx)
    if not rel_props:
        return rows

    target = _normalize_page_id(concert_id)
    filtered = []
    for r in rows:
        hit = False
        for rp in rel_props:
            ids = ctx["extract_relation_ids"](r, rp)
            if any(_normalize_page_id(x) == target for x in ids):
                hit = True
                break
        if hit:
            filtered.append(r)
    return filtered


def _concert_display_name(page: dict, ctx: dict) -> str:
    name = ctx["extract_prop_text_any"](page, CONCERT_NAME_KEYS)
    if not name:
        name = ctx["extract_title"](page)
    dt = ctx["extract_prop_text_any"](page, CONCERT_DATE_KEYS)
    return f"{name}（{dt[:10] if dt else '日時未設定'}）" if name else page.get("id", "")


def _practice_display_name(page: dict, ctx: dict) -> str:
    name = ctx["extract_prop_text_any"](page, PRACTICE_NAME_KEYS)
    if not name:
        name = ctx["extract_title"](page)
    dt = ctx["extract_prop_text_any"](page, PRACTICE_DATE_KEYS)
    return f"{name}（{dt[:10] if dt else '日時未設定'}）" if name else page.get("id", "")


# ============================================================
# 演奏会 CRUD
# ============================================================

def _create_concert(
    ctx: dict, name: str, dt_start: str, dt_end: str, venue: str, address: str, memo: str, lat=None, lon=None
) -> bool:
    api   = ctx["api_request"]
    hdrs  = ctx["NOTION_HEADERS"]
    db_id = ctx["CONCERT_DB_CONCERT"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(db_id)
    if not type_map:
        st.error("演奏会DBのプロパティ取得に失敗しました。DB IDとインテグレーション接続を確認してください。")
        return False

    props: dict = {}
    ctx["put_prop_any"](props, type_map, CONCERT_NAME_KEYS, name)
    date_key = ctx["find_prop_name"](type_map, CONCERT_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    venue_key = ctx["find_prop_name"](type_map, CONCERT_VENUE_KEYS)
    address_key = ctx["find_prop_name"](type_map, CONCERT_ADDRESS_KEYS)
    if venue_key:
        if type_map.get(venue_key) == "location":
            ctx["put_prop"](props, type_map, venue_key, _location_payload(venue, address, lat, lon))
        else:
            ctx["put_prop"](props, type_map, venue_key, venue)
    if address_key and address_key != venue_key:
        ctx["put_prop"](props, type_map, address_key, address)
    ctx["put_prop_any"](props, type_map, CONCERT_MEMO_KEYS, memo)
    ctx["put_key_any"](props, type_map, CONCERT_KEY_KEYS, name, dt_start, prefix="concert")
    media_key = ctx["find_prop_name"](type_map, CONCERT_MEDIA_KEYS)
    if media_key:
        mtype = type_map.get(media_key, "")
        if mtype == "select":
            props[media_key] = {"select": {"name": "出演"}}
        elif mtype == "multi_select":
            props[media_key] = {"multi_select": [{"name": "出演"}]}

    res = api("post", "https://api.notion.com/v1/pages",
              json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_concert(
    ctx: dict, page_id: str, name: str, dt_start: str, dt_end: str, venue: str, address: str, memo: str, lat=None, lon=None
) -> bool:
    api  = ctx["api_request"]
    hdrs = ctx["NOTION_HEADERS"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(ctx["CONCERT_DB_CONCERT"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, CONCERT_NAME_KEYS, name)
    date_key = ctx["find_prop_name"](type_map, CONCERT_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    venue_key = ctx["find_prop_name"](type_map, CONCERT_VENUE_KEYS)
    address_key = ctx["find_prop_name"](type_map, CONCERT_ADDRESS_KEYS)
    if venue_key:
        if type_map.get(venue_key) == "location":
            ctx["put_prop"](props, type_map, venue_key, _location_payload(venue, address, lat, lon))
        else:
            ctx["put_prop"](props, type_map, venue_key, venue)
    if address_key and address_key != venue_key:
        ctx["put_prop"](props, type_map, address_key, address)
    ctx["put_prop_any"](props, type_map, CONCERT_MEMO_KEYS, memo)
    ctx["put_key_any"](props, type_map, CONCERT_KEY_KEYS, name, dt_start, prefix="concert")

    res = api("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 練習 CRUD
# ============================================================

def _create_practice(
    ctx: dict,
    name: str,
    concert_id: str,
    dt_start: str,
    dt_end: str,
    venue: str,
    address: str,
    is_concert_day: bool,
    is_rest_day: bool,
    memo: str,
    lat=None,
    lon=None,
    song_ids: list | None = None,
) -> str:
    api   = ctx["api_request"]
    db_id = ctx["CONCERT_DB_PRACTICE"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(db_id)
    if not type_map:
        st.error("練習DBのプロパティ取得に失敗しました。")
        return ""

    props: dict = {}
    ctx["put_prop_any"](props, type_map, PRACTICE_NAME_KEYS, name)
    if concert_id:
        rel_written = ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_REL_KEYS, concert_id)
        # 候補名不一致でも relation 型の演奏会系プロパティへフォールバック
        if not rel_written:
            for k, t in (type_map or {}).items():
                if t == "relation" and ("演奏会" in str(k) or "出演" in str(k)):
                    ctx["put_prop"](props, type_map, k, concert_id)
                    break
    date_key = ctx["find_prop_name"](type_map, PRACTICE_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    venue_key = ctx["find_prop_name"](type_map, PRACTICE_VENUE_KEYS)
    address_key = ctx["find_prop_name"](type_map, PRACTICE_ADDRESS_KEYS)
    if venue_key:
        if type_map.get(venue_key) == "location":
            ctx["put_prop"](props, type_map, venue_key, _location_payload(venue, address, lat, lon))
        else:
            ctx["put_prop"](props, type_map, venue_key, venue)
    if address_key and address_key != venue_key:
        ctx["put_prop"](props, type_map, address_key, address)
    cday_written = ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_DAY_KEYS, is_concert_day)
    if not cday_written:
        for k, t in (type_map or {}).items():
            if t == "checkbox" and ("当日" in str(k) or "本番" in str(k)):
                ctx["put_prop"](props, type_map, k, is_concert_day)
                break
    rest_written = ctx["put_prop_any"](props, type_map, PRACTICE_PERCUSSION_OFF_KEYS, is_rest_day)
    if not rest_written:
        for k, t in (type_map or {}).items():
            if t == "checkbox" and "休" in str(k):
                ctx["put_prop"](props, type_map, k, is_rest_day)
                break
    ctx["put_prop_any"](props, type_map, PRACTICE_MEMO_KEYS, memo)
    ctx["put_key_any"](props, type_map, PRACTICE_KEY_KEYS, concert_id, name, dt_start, prefix="practice")
    if song_ids:
        ctx["put_prop_any"](props, type_map, PRACTICE_SONG_REL_KEYS, song_ids)

    res = api("post", "https://api.notion.com/v1/pages",
              json={"parent": {"database_id": db_id}, "properties": props})
    if res is not None and res.status_code == 200:
        return (res.json() or {}).get("id", "")
    return ""


def _update_practice(
    ctx: dict,
    page_id: str,
    name: str,
    concert_id: str,
    dt_start: str,
    dt_end: str,
    venue: str,
    address: str,
    is_concert_day: bool,
    is_rest_day: bool,
    memo: str,
    lat=None,
    lon=None,
    song_ids: list | None = None,
) -> bool:
    api   = ctx["api_request"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(ctx["CONCERT_DB_PRACTICE"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, PRACTICE_NAME_KEYS, name)
    if concert_id:
        rel_written = ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_REL_KEYS, concert_id)
        if not rel_written:
            for k, t in (type_map or {}).items():
                if t == "relation" and ("演奏会" in str(k) or "出演" in str(k)):
                    ctx["put_prop"](props, type_map, k, concert_id)
                    break
    date_key = ctx["find_prop_name"](type_map, PRACTICE_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    venue_key = ctx["find_prop_name"](type_map, PRACTICE_VENUE_KEYS)
    address_key = ctx["find_prop_name"](type_map, PRACTICE_ADDRESS_KEYS)
    if venue_key:
        if type_map.get(venue_key) == "location":
            ctx["put_prop"](props, type_map, venue_key, _location_payload(venue, address, lat, lon))
        else:
            ctx["put_prop"](props, type_map, venue_key, venue)
    if address_key and address_key != venue_key:
        ctx["put_prop"](props, type_map, address_key, address)
    cday_written = ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_DAY_KEYS, is_concert_day)
    if not cday_written:
        for k, t in (type_map or {}).items():
            if t == "checkbox" and ("当日" in str(k) or "本番" in str(k)):
                ctx["put_prop"](props, type_map, k, is_concert_day)
                break
    rest_written = ctx["put_prop_any"](props, type_map, PRACTICE_PERCUSSION_OFF_KEYS, is_rest_day)
    if not rest_written:
        for k, t in (type_map or {}).items():
            if t == "checkbox" and "休" in str(k):
                ctx["put_prop"](props, type_map, k, is_rest_day)
                break
    ctx["put_prop_any"](props, type_map, PRACTICE_MEMO_KEYS, memo)
    ctx["put_key_any"](props, type_map, PRACTICE_KEY_KEYS, concert_id, name, dt_start, prefix="practice")
    if song_ids is not None:
        ctx["put_prop_any"](props, type_map, PRACTICE_SONG_REL_KEYS, song_ids)

    res = api("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


def _bind_practice_concert_relation(ctx: dict, page_id: str, concert_id: str) -> bool:
    """練習ページを演奏会へ強制紐付け（relationキー名ゆれ対策）。"""
    if not page_id or not concert_id:
        return False
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_PRACTICE"]) or {}
    relation_keys = [
        k for k, t in type_map.items()
        if t == "relation" and ("演奏会" in str(k) or "出演" in str(k) or "concert" in str(k).lower())
    ]
    if not relation_keys:
        return False
    for rk in relation_keys:
        res = ctx["api_request"](
            "patch",
            f"https://api.notion.com/v1/pages/{page_id}",
            json={"properties": {rk: {"relation": [{"id": concert_id}]}}},
        )
        if res is not None and res.status_code == 200:
            return True
    return False


# ============================================================
# 演奏会フォーム
# ============================================================

def _render_concert_form(ctx: dict, existing: dict | None = None):
    """演奏会の新規登録 / 編集フォーム。existing が None なら新規。"""
    is_edit = existing is not None
    prefix  = f"conc_edit_{existing.get('id','')}_" if is_edit else "conc_new_"
    ext     = ctx["extract_prop_text_any"]

    venue_default = ext(existing, CONCERT_VENUE_KEYS) if is_edit else ""
    address_default = ext(existing, CONCERT_ADDRESS_KEYS) if is_edit else ""
    # ATLAS 側が「ロケーション」単独運用のデータでも会場欄に表示する
    if is_edit:
        location_fallback = ext(existing, ["ロケーション", "場所", "Location"])
        if not venue_default and location_fallback:
            venue_default = location_fallback
        if not address_default and location_fallback:
            address_default = location_fallback

    with st.form(key=f"{prefix}form", border=True):
        name = st.text_input(
            "演奏会名 *",
            value=ext(existing, CONCERT_NAME_KEYS) if is_edit else "",
            placeholder="例：第12回定期演奏会",
            key=f"{prefix}name",
        )

        col1, col2 = st.columns(2)
        with col1:
            dt_start_str = ext(existing, CONCERT_DATE_KEYS) if is_edit else ""
            dt_start_val = date.fromisoformat(dt_start_str[:10]) if dt_start_str else date.today()
            dt_start = st.date_input("開催日 *", value=dt_start_val, key=f"{prefix}dt_start")
        with col2:
            dt_end = st.date_input("終了日（任意）", value=dt_start_val, key=f"{prefix}dt_end")

        venue   = st.text_input("会場名", value=venue_default,
                                placeholder="例：○○ホール", key=f"{prefix}venue")
        address = st.text_input("会場住所", value=address_default,
                                placeholder="任意", key=f"{prefix}address")
        memo    = st.text_area("メモ", value=ext(existing, CONCERT_MEMO_KEYS) if is_edit else "",
                               height=80, key=f"{prefix}memo")

        label = "更新" if is_edit else "登録"
        submitted = st.form_submit_button(f"💾 {label}", use_container_width=True, type="primary")

    if submitted:
        if not name.strip():
            st.error("演奏会名は必須です。")
            return
        dt_s = dt_start.isoformat()
        dt_e = dt_end.isoformat() if dt_end and dt_end != dt_start else dt_s

        with st.spinner(f"{label}中..."):
            if is_edit:
                ok = _update_concert(ctx, existing["id"], name.strip(), dt_s, dt_e,
                                     venue, address, memo, None, None)
            else:
                ok = _create_concert(ctx, name.strip(), dt_s, dt_e, venue, address, memo, None, None)

        if ok:
            st.success(f"✅ 演奏会を{label}しました。")
            _clear_concert_cache(ctx)
            st.rerun()
        else:
            st.error(f"❌ {label}に失敗しました。Notion の接続・プロパティ名を確認してください。")


# ============================================================
# 練習フォーム
# ============================================================

def _render_practice_form(ctx: dict, concerts: list[dict], existing: dict | None = None):
    """練習の新規登録 / 編集フォーム。"""
    is_edit = existing is not None
    prefix  = f"prac_edit_{existing.get('id','')}_" if is_edit else "prac_new_"
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids"]

    # 演奏会セレクタ
    concert_options = {_concert_display_name(c, ctx): c.get("id", "") for c in concerts}
    concert_names   = ["（未選択）"] + list(concert_options.keys())

    current_concert_id = ""
    if is_edit:
        ids = ctx["extract_relation_ids_any"](existing, PRACTICE_CONCERT_REL_KEYS)
        current_concert_id = ids[0] if ids else ""
    current_concert_name = next(
        (k for k, v in concert_options.items() if v == current_concert_id), "（未選択）"
    )

    venue_default = ext(existing, PRACTICE_VENUE_KEYS) if is_edit else ""
    address_default = ext(existing, PRACTICE_ADDRESS_KEYS) if is_edit else ""
    prefill_venue_key = f"{prefix}prefill_venue"
    prefill_address_key = f"{prefix}prefill_address"
    prefill_lat_key = f"{prefix}prefill_lat"
    prefill_lon_key = f"{prefix}prefill_lon"
    if _ss(prefill_venue_key):
        venue_default = _ss(prefill_venue_key, "")
    if _ss(prefill_address_key):
        address_default = _ss(prefill_address_key, "")
    if is_edit:
        location_fallback = ext(existing, ["ロケーション", "場所", "Location"])
        if not venue_default and location_fallback:
            venue_default = location_fallback
        if not address_default and location_fallback:
            address_default = location_fallback

    # 会場検索（フォーム外）
    venue_q_key = f"{prefix}venue_query"
    venue_list_key = f"{prefix}venue_candidates"
    venue_sel_key = f"{prefix}venue_candidate_index"
    with st.expander("🗺️ 会場を検索して反映（任意）", expanded=False):
        c1, c2 = st.columns([4, 1])
        c1.text_input("会場検索ワード", key=venue_q_key, placeholder="例: 門真市民文化会館")
        if c2.button("🔎 検索", key=f"{prefix}venue_search_btn", use_container_width=True):
            st.session_state[venue_list_key] = _geocode_nominatim(_ss(venue_q_key, ""))
            st.session_state[venue_sel_key] = 0
        candidates = _ss(venue_list_key, [])
        if candidates:
            labels = [c.get("name", "") for c in candidates]
            idx = st.selectbox(
                "候補",
                options=list(range(len(labels))),
                format_func=lambda i: labels[i],
                index=min(_ss(venue_sel_key, 0), max(len(labels) - 1, 0)),
                key=venue_sel_key,
            )
            picked = candidates[idx]
            if st.button("✅ この候補をフォームに反映", key=f"{prefix}apply_venue_candidate"):
                st.session_state[prefill_venue_key] = picked.get("name", "")
                st.session_state[prefill_address_key] = picked.get("address", "")
                st.session_state[prefill_lat_key] = picked.get("lat")
                st.session_state[prefill_lon_key] = picked.get("lon")
                st.session_state[f"{prefix}venue"] = picked.get("name", "")
                st.session_state[f"{prefix}address"] = picked.get("address", "")
                st.rerun()

    # 休みフラグはフォーム外に置いて、ON/OFF時に即座に入力可否へ反映
    rest_default = _extract_bool_any(ctx, existing, PRACTICE_PERCUSSION_OFF_KEYS, False) if is_edit else False
    live_rest_key = f"{prefix}rest_day_live"
    if live_rest_key not in st.session_state:
        st.session_state[live_rest_key] = rest_default
    is_rest_day = st.checkbox(
        "打楽器休み（ON時は日時以外の入力を無効化）",
        key=live_rest_key,
    )

    with st.form(key=f"{prefix}form", border=True):
        selected_concert_name = st.selectbox(
            "演奏会",
            concert_names,
            index=concert_names.index(current_concert_name) if current_concert_name in concert_names else 0,
            key=f"{prefix}concert",
        )
        selected_concert_id = concert_options.get(selected_concert_name, "")

        if is_edit:
            name = st.text_input(
                "練習名 *",
                value=ext(existing, PRACTICE_NAME_KEYS),
                placeholder="例：第3回練習",
                key=f"{prefix}name",
            )
            practice_round = None
        else:
            # 同演奏会の既存練習名から「第N回練習」を拾って次番号を提案
            max_round = 0
            if selected_concert_id:
                for row in _load_practices(ctx, selected_concert_id):
                    nm = ctx["extract_prop_text_any"](row, PRACTICE_NAME_KEYS) or ""
                    m = re.search(r"第\s*(\d+)\s*回練習", nm)
                    if m:
                        max_round = max(max_round, int(m.group(1)))
            suggested_round = max_round + 1 if max_round > 0 else 1
            practice_round = int(st.number_input(
                "練習回数 *",
                min_value=1,
                value=suggested_round,
                step=1,
                key=f"{prefix}round_no",
            ))
            auto_name = f"第{practice_round}回練習"
            name = st.text_input(
                "練習名（自動）",
                value=auto_name,
                disabled=True,
                key=f"{prefix}name_auto",
            )

        dt_start_str = ext(existing, PRACTICE_DATE_KEYS) if is_edit else ""
        dt_start_val = date.fromisoformat(dt_start_str[:10]) if dt_start_str else date.today()
        start_time_default = ""
        if dt_start_str and "T" in dt_start_str:
            try:
                start_time_default = dt_start_str.split("T", 1)[1][:5]
            except Exception:
                start_time_default = ""
        dt_start = st.date_input("練習日 *", value=dt_start_val, key=f"{prefix}dt_start")
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            start_time = st.text_input(
                "開始時刻（任意）",
                value=start_time_default,
                placeholder="例: 19:00",
                key=f"{prefix}start_time",
            )
        with col_t2:
            end_time = st.text_input(
                "終了時刻（任意）",
                value="",
                placeholder="例: 21:00",
                key=f"{prefix}end_time",
            )
        col3, col4 = st.columns(2)
        with col3:
            venue = st.text_input("会場名", value=venue_default,
                                  placeholder="例：○○スタジオ", key=f"{prefix}venue", disabled=is_rest_day)
        with col4:
            address = st.text_input("会場住所", value=address_default,
                                    placeholder="任意", key=f"{prefix}address", disabled=is_rest_day)

        is_concert_day = st.checkbox(
            "演奏会当日フラグ（本番日の場合はチェック）",
            value=_extract_bool_any(ctx, existing, PRACTICE_CONCERT_DAY_KEYS, False) if is_edit else False,
            key=f"{prefix}concert_day",
            disabled=is_rest_day,
        )
        memo = st.text_area("メモ", value=ext(existing, PRACTICE_MEMO_KEYS) if is_edit else "",
                            height=80, key=f"{prefix}memo", disabled=is_rest_day)

        # 演奏曲選択（その練習日にやる曲）
        song_opts: dict = {}
        if selected_concert_id:
            s_rows = _load_songs(ctx, selected_concert_id)
            song_opts = {ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or s.get("id",""): s.get("id","")
                         for s in s_rows}
        cur_song_ids: list[str] = []
        if is_edit:
            cur_song_ids = ctx["extract_relation_ids_any"](existing, PRACTICE_SONG_REL_KEYS)
        cur_song_names = [k for k, v in song_opts.items() if v in cur_song_ids]
        selected_songs = st.multiselect(
            "この日に練習する曲（未選択の場合は全曲対象）",
            options=list(song_opts.keys()),
            default=cur_song_names,
            key=f"{prefix}songs",
            disabled=is_rest_day,
        )
        selected_song_ids = [song_opts[s] for s in selected_songs if s in song_opts]

        label = "更新" if is_edit else "登録"
        submitted = st.form_submit_button(f"💾 {label}", use_container_width=True, type="primary")

    if submitted:
        if not name.strip():
            st.error("練習名は必須です。")
            return
        concert_id = selected_concert_id
        if not is_edit and practice_round:
            name = f"第{practice_round}回練習"
        try:
            dt_s, dt_e = _compose_notion_date_with_optional_time(dt_start, start_time, end_time)
        except ValueError as e:
            st.error(str(e))
            return

        if is_rest_day:
            venue = ""
            address = ""
            is_concert_day = False
            memo = ""

        with st.spinner(f"{label}中..."):
            chosen_lat = _ss(prefill_lat_key, None)
            chosen_lon = _ss(prefill_lon_key, None)
            if is_edit:
                ok = _update_practice(ctx, existing["id"], name.strip(), concert_id,
                                      dt_s, dt_e, venue, address, is_concert_day, is_rest_day, memo,
                                      chosen_lat, chosen_lon, selected_song_ids)
            else:
                created_id = _create_practice(ctx, name.strip(), concert_id,
                                              dt_s, dt_e, venue, address, is_concert_day, is_rest_day, memo,
                                              chosen_lat, chosen_lon, selected_song_ids)
                ok = bool(created_id)
                if ok and concert_id:
                    _bind_practice_concert_relation(ctx, created_id, concert_id)

        if ok:
            st.success(f"✅ 練習を{label}しました。")
            st.session_state.pop(prefill_venue_key, None)
            st.session_state.pop(prefill_address_key, None)
            st.session_state.pop(prefill_lat_key, None)
            st.session_state.pop(prefill_lon_key, None)
            st.session_state.pop(venue_list_key, None)
            _clear_concert_cache(ctx)
            st.rerun()
        else:
            st.error(f"❌ {label}に失敗しました。")


def _bulk_generate_practice_rows(ctx: dict, concert_page: dict, practice_count: int) -> tuple[int, int]:
    """
    指定演奏会に対して練習回を一括生成する。
    return: (created_count, skipped_count)
    """
    if not concert_page or practice_count <= 0:
        return 0, 0

    concert_id = concert_page.get("id", "")
    if not concert_id:
        return 0, 0

    existing_rows = _load_practices(ctx, concert_id)
    existing_names = set()
    for r in existing_rows:
        nm = (ctx["extract_prop_text_any"](r, PRACTICE_NAME_KEYS) or "").strip()
        if nm:
            existing_names.add(nm)

    concert_date = (ctx["extract_prop_text_any"](concert_page, CONCERT_DATE_KEYS) or "").strip()
    concert_dt = concert_date[:10] if concert_date else ""
    concert_venue = (ctx["extract_prop_text_any"](concert_page, CONCERT_VENUE_KEYS) or "").strip()
    concert_addr = (ctx["extract_prop_text_any"](concert_page, CONCERT_ADDRESS_KEYS) or "").strip()
    if not concert_venue:
        loc_fallback = (ctx["extract_prop_text_any"](concert_page, ["ロケーション", "場所", "Location"]) or "").strip()
        if loc_fallback:
            concert_venue = loc_fallback
            if not concert_addr:
                concert_addr = loc_fallback

    created = 0
    skipped = 0

    # 1..N: 通常練習（日時/会場は後で入力）
    for i in range(1, practice_count + 1):
        name = f"第{i}回練習"
        if name in existing_names:
            skipped += 1
            continue
        created_id = _create_practice(
            ctx=ctx,
            name=name,
            concert_id=concert_id,
            dt_start="",
            dt_end="",
            venue="",
            address="",
            is_concert_day=False,
            is_rest_day=False,
            memo="",
        )
        if created_id:
            _bind_practice_concert_relation(ctx, created_id, concert_id)
            created += 1
            existing_names.add(name)

    # N+1: 本番当日（演奏会情報を自動反映）
    final_name = "本番当日"
    if final_name in existing_names:
        skipped += 1
    else:
        created_id = _create_practice(
            ctx=ctx,
            name=final_name,
            concert_id=concert_id,
            dt_start=concert_dt,
            dt_end="",
            venue=concert_venue,
            address=concert_addr,
            is_concert_day=True,
            is_rest_day=False,
            memo="",
        )
        if created_id:
            _bind_practice_concert_relation(ctx, created_id, concert_id)
            # 全参加者の出欠を○に設定
            _auto_mark_concert_day_attendance(ctx, created_id, concert_id)
            created += 1
        else:
            skipped += 1

    return created, skipped


def _auto_mark_concert_day_attendance(ctx: dict, practice_id: str, concert_id: str) -> None:
    """本番当日の練習レコードに全参加者の出欠を○で一括登録する。"""
    if not practice_id:
        return
    players = _load_players(ctx)
    if not players:
        return
    att_db   = ctx["CONCERT_DB_ATTENDANCE"]
    type_map = ctx["get_prop_types"](att_db)
    if not type_map:
        return
    practice_name = "本番当日"
    for pl in players:
        pid   = pl.get("id", "")
        pname = (ctx["extract_prop_text_any"](pl, ["氏名", "名前", "Name"]) or
                 ctx["extract_title"](pl) or pid)
        if not pid:
            continue
        props: dict = {}
        ctx["put_prop_any"](props, type_map, ATT_RECORD_KEYS, f"{pname} × {practice_name}")
        ctx["put_prop_any"](props, type_map, ATT_PLAYER_REL_KEYS, pid)
        ctx["put_prop_any"](props, type_map, ATT_PRACTICE_REL_KEYS, practice_id)
        ctx["put_prop_any"](props, type_map, ATT_STATUS_KEYS, "○")
        ctx["put_prop_any"](props, type_map, ATT_NOTE_KEYS, "本番当日のため自動登録")
        ctx["api_request"](
            "post",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": att_db}, "properties": props},
        )


# ============================================================
# メイン描画
# ============================================================


# ============================================================
# スケジュール管理
# ============================================================

def _load_schedules(ctx, practice_id: str) -> list[dict]:
    key = f"schedule_list_{practice_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_SCHEDULE"])
        rel = ctx["find_prop_name"](t, SCHEDULE_PRACTICE_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": practice_id}}} if rel else None
        rows = ctx["query_all"](ctx["CONCERT_DB_SCHEDULE"], f)
        # 表示順でソート、未設定の場合は開始時刻でフォールバック
        def _sort_key(r):
            v = ctx["extract_prop_text_any"](r, SCHEDULE_ORDER_KEYS)
            try:
                if v:
                    return (int(float(v)), "")
            except: pass
            # フォールバック：開始時刻でソート
            t = ctx["extract_prop_text_any"](r, SCHEDULE_START_KEYS) or "99:99"
            return (9999, t)
        st.session_state[key] = sorted(rows, key=_sort_key)
    return st.session_state.get(key, [])


def _clear_schedule_cache(practice_id: str = ""):
    for k in list(st.session_state.keys()):
        if k.startswith("schedule_list_") and (not practice_id or practice_id in k):
            st.session_state.pop(k, None)


def _upsert_schedule(ctx, practice_id: str, practice_name: str,
                     start: str, end: str, type_: str, content: str,
                     song_id: str, order: int,
                     existing_id: str = "") -> bool:
    db_id    = ctx["CONCERT_DB_SCHEDULE"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("スケジュールDBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    label = f"{start}〜{end} {type_} {content}".strip()
    ctx["put_prop_any"](props, type_map, SCHEDULE_KEY_KEYS, label)
    ctx["put_prop_any"](props, type_map, SCHEDULE_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, type_map, SCHEDULE_START_KEYS, start)
    ctx["put_prop_any"](props, type_map, SCHEDULE_END_KEYS, end)
    ctx["put_prop_any"](props, type_map, SCHEDULE_TYPE_KEYS, type_)
    ctx["put_prop_any"](props, type_map, SCHEDULE_CONTENT_KEYS, content)
    if song_id:
        ctx["put_prop_any"](props, type_map, SCHEDULE_SONG_REL_KEYS, song_id)
    ctx["put_prop_any"](props, type_map, SCHEDULE_ORDER_KEYS, order)

    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _delete_schedule(ctx, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"archived": True})
    return res is not None and res.status_code == 200


def _render_schedule_tab(ctx: dict):
    st.caption("練習日ごとのタイムスケジュールを管理します。行を追加・削除して「保存」を押してください。")

    concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    if not concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return

    practices = _load_practices(ctx, concert_id)
    if not practices:
        st.info("練習が登録されていません。")
        return

    def _prac_date_sort(p):
        d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
        return d[:10] if d else "9999"

    practice_opts = {_practice_display_name(p, ctx): p.get("id", "")
                     for p in sorted(practices, key=_prac_date_sort)}

    col_sel, col_r = st.columns([8, 1])
    p_label = col_sel.selectbox("練習日", list(practice_opts.keys()), key="sched_practice_sel")
    p_id    = practice_opts.get(p_label, "")
    if col_r.button("🔄", key="sched_refresh", help="再読み込み"):
        _clear_schedule_cache(p_id)
        st.rerun()
    if not p_id:
        return

    # 演奏曲の選択肢
    all_songs_rows = _load_songs(ctx, concert_id)
    song_opts = {"（なし）": ""}
    for s in sorted(all_songs_rows, key=lambda x: ctx["extract_prop_text_any"](x, SONG_NAME_KEYS) or ""):
        name = ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or s.get("id", "")
        song_opts[name] = s.get("id", "")
    song_names = list(song_opts.keys())

    schedules = _load_schedules(ctx, p_id)

    # 既存データをDataFrame形式に変換
    existing_rows: list[dict] = []
    row_ids: list[str] = []
    for r in schedules:
        s_id = r.get("id", "")
        cur_song_ids = ctx["extract_relation_ids_any"](r, SCHEDULE_SONG_REL_KEYS)
        cur_song_id  = cur_song_ids[0] if cur_song_ids else ""
        cur_song_name = next((k for k, v in song_opts.items() if v == cur_song_id), "（なし）")
        cur_order_str = ctx["extract_prop_text_any"](r, SCHEDULE_ORDER_KEYS) or "0"
        try:
            cur_order = int(float(cur_order_str))
        except Exception:
            cur_order = 0
        existing_rows.append({
            "開始": ctx["extract_prop_text_any"](r, SCHEDULE_START_KEYS) or "",
            "終了": ctx["extract_prop_text_any"](r, SCHEDULE_END_KEYS) or "",
            "種別": ctx["extract_prop_text_any"](r, SCHEDULE_TYPE_KEYS) or "練習",
            "内容": ctx["extract_prop_text_any"](r, SCHEDULE_CONTENT_KEYS) or "",
            "演奏曲": cur_song_name,
            "表示順": cur_order,
        })
        row_ids.append(s_id)

    # 既存0件のときだけ空行1行
    if not existing_rows:
        existing_rows.append({
            "開始": "", "終了": "", "種別": "練習",
            "内容": "", "演奏曲": "（なし）", "表示順": 1,
        })
        row_ids.append("")

    import pandas as pd
    df_init = pd.DataFrame(existing_rows)

    edited_df = st.data_editor(
        df_init,
        num_rows="dynamic",
        use_container_width=True,
        key=f"sched_editor_{p_id}",
        column_config={
            "開始": st.column_config.TextColumn("開始", help="例: 19:00", max_chars=10),
            "終了": st.column_config.TextColumn("終了", help="例: 19:30", max_chars=10),
            "種別": st.column_config.SelectboxColumn(
                "種別", options=SCHEDULE_TYPE_OPTIONS, required=True, default="練習",
            ),
            "内容": st.column_config.TextColumn("内容（業者名など）", max_chars=100),
            "演奏曲": st.column_config.SelectboxColumn(
                "演奏曲", options=song_names, default="（なし）",
            ),
            "表示順": st.column_config.NumberColumn(
                "表示順", min_value=1, max_value=99, step=1, default=1,
            ),
        },
    )

    if st.button("💾 まとめて保存", type="primary", use_container_width=True, key=f"sched_save_{p_id}"):
        ok_n = fail_n = skip_n = 0
        with st.spinner("保存中..."):
            saved_ids: set[str] = set()
            df_reset = edited_df.reset_index(drop=True)

            for idx in range(len(df_reset)):
                row = df_reset.iloc[idx]
                start_v   = str(row.get("開始") or "").strip()
                end_v     = str(row.get("終了") or "").strip()
                type_v    = str(row.get("種別") or "練習").strip()
                content_v = str(row.get("内容") or "").strip()
                song_n    = str(row.get("演奏曲") or "（なし）").strip()
                order_v   = int(row.get("表示順") or idx + 1)
                song_id_v = song_opts.get(song_n, "")

                # 開始時刻が空はスキップ
                if not start_v:
                    skip_n += 1
                    continue

                existing_id = row_ids[idx] if idx < len(row_ids) else ""
                ok = _upsert_schedule(
                    ctx, p_id, p_label,
                    start_v, end_v, type_v, content_v,
                    song_id_v, order_v,
                    existing_id=existing_id,
                )
                if ok:
                    ok_n += 1
                    if existing_id:
                        saved_ids.add(existing_id)
                else:
                    fail_n += 1

            # 削除された既存行をアーカイブ
            for rid in row_ids:
                if rid and rid not in saved_ids:
                    _delete_schedule(ctx, rid)

        if fail_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ 成功 {ok_n} / 失敗 {fail_n} / スキップ {skip_n}")
        _clear_schedule_cache(p_id)
        st.rerun()

def render(ctx: dict):
    st.header("🗓️ 練習管理")
    global_concert_id   = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    global_concert_name = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()

    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return

    st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")

    concerts = _load_concerts(ctx)
    filter_concert_id    = global_concert_id
    selected_concert_page = next((c for c in concerts if c.get("id") == filter_concert_id), None)

    with st.expander("⚙️ 練習回を一括生成", expanded=False):
        st.caption("練習回数を入力すると「第1回練習〜第N回練習」と「本番当日」を作成します。")
        bulk_count = int(st.number_input("練習回数", min_value=1, value=3, step=1, key="practice_bulk_count"))
        if st.button("➕ 練習回を生成", key="practice_bulk_generate", type="primary", use_container_width=True):
            if not filter_concert_id or not selected_concert_page:
                st.error("先にサイドバーで演奏会を選択してください。")
            else:
                with st.spinner("練習回を生成中..."):
                    created, skipped = _bulk_generate_practice_rows(ctx, selected_concert_page, bulk_count)
                st.success(f"✅ 生成完了: 作成 {created} 件 / スキップ {skipped} 件")
                for k in list(st.session_state.keys()):
                    if k.startswith("practice_list_"):
                        st.session_state.pop(k, None)
                st.rerun()

    with st.expander("➕ 新規練習を登録", expanded=False):
        _render_practice_form(ctx, concerts)

    st.divider()

    practices = _load_practices(ctx, filter_concert_id)
    if filter_concert_id and not practices:
        fallback_all = _load_practices(ctx, "")
        if fallback_all:
            st.warning("選択演奏会へのリレーション未設定の可能性があります。未絞り込みの練習を表示します。")
            practices = fallback_all

    if not practices:
        st.info("この演奏会に練習がまだ登録されていません。")
    else:
        st.caption(f"登録済み練習（{len(practices)}件）")
        col_search, col_refresh = st.columns([8, 1])
        practice_query = col_search.text_input(
            "練習を検索",
            value=_ss("concert_mgmt_practice_query", ""),
            placeholder="例: 第3回 / 2026-07 / 本番 / スタジオ",
            key="concert_mgmt_practice_query",
        )
        if col_refresh.button("🔄", key="refresh_practices", help="一覧を再読み込み"):
            for k in list(st.session_state.keys()):
                if k.startswith("practice_list_"):
                    st.session_state.pop(k, None)
            st.rerun()

        # 練習回（第N回練習）を優先して降順表示（未設定日時でも入力しやすくする）
        def _practice_round_no(p: dict) -> int:
            nm = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ""
            m = re.search(r"第\s*(\d+)\s*回練習", nm)
            return int(m.group(1)) if m else 9999  # 本番当日など回番号なしは末尾

        def _prac_date(p: dict) -> str:
            d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
            return d[:10] if d else ""

        def _prac_sort_key(p: dict):
            is_cd = _extract_bool_any(ctx, p, PRACTICE_CONCERT_DAY_KEYS, False)
            return (1 if is_cd else 0, _practice_round_no(p), _prac_date(p))

        sorted_practices = sorted(
            practices,
            key=_prac_sort_key,
        )
        filtered_practices = []
        for p in sorted_practices:
            if not _contains_query(
                [
                    _practice_display_name(p, ctx),
                    ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS),
                    ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS),
                    ctx["extract_prop_text_any"](p, PRACTICE_VENUE_KEYS),
                    ctx["extract_prop_text_any"](p, PRACTICE_ADDRESS_KEYS),
                    ctx["extract_prop_text_any"](p, PRACTICE_MEMO_KEYS),
                ],
                practice_query,
            ):
                continue
            filtered_practices.append(p)

        st.caption(f"表示件数: {len(filtered_practices)} / {len(practices)}")
        if not filtered_practices:
            st.info("検索条件に一致する練習がありません。")
            return

        # ── data_editor形式で一覧表示・一括編集 ──────────────
        import pandas as pd
        prac_df_rows: list[dict] = []
        prac_meta:    list[dict] = []

        for p in filtered_practices:
            pid    = p.get("id", "")
            pname  = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ""
            pdate  = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or ""
            # 日時："2026-04-25T19:00:00+09:00" → "2026-04-25 19:00"
            pdate_disp = pdate[:16].replace("T", " ") if pdate else ""
            pvenue = ctx["extract_prop_text_any"](p, PRACTICE_VENUE_KEYS) or ""
            paddr  = ctx["extract_prop_text_any"](p, PRACTICE_ADDRESS_KEYS) or ""
            pmemo  = ctx["extract_prop_text_any"](p, PRACTICE_MEMO_KEYS) or ""
            is_cd  = _extract_bool_any(ctx, p, PRACTICE_CONCERT_DAY_KEYS, False)
            is_po  = _extract_bool_any(ctx, p, PRACTICE_PERCUSSION_OFF_KEYS, False)
            disp_name = "【本番当日】" if is_cd else pname
            prac_df_rows.append({
                "練習名":     disp_name,
                "日時":       pdate_disp,
                "会場名":     pvenue,
                "会場住所":   paddr,
                "本番日":     is_cd,
                "打楽器休み": is_po,
                "メモ":       pmemo,
            })
            prac_meta.append({"pid": pid, "pname": pname, "pdate": pdate})

        df_prac = pd.DataFrame(prac_df_rows)
        # keyにリロードカウンタを含めてrerun後にdata_editorをリセット
        editor_version = st.session_state.get("practice_editor_version", 0)
        edited_prac = st.data_editor(
            df_prac,
            num_rows="fixed",
            use_container_width=True,
            key=f"practice_list_editor_{filter_concert_id}_{editor_version}",
            column_config={
                "練習名":     st.column_config.TextColumn("練習名", disabled=True),
                "日時":       st.column_config.TextColumn("日時", help="例: 2026-04-25 19:00"),
                "会場名":     st.column_config.TextColumn("会場名"),
                "会場住所":   st.column_config.TextColumn("会場住所"),
                "本番日":     st.column_config.CheckboxColumn("本番日"),
                "打楽器休み": st.column_config.CheckboxColumn("打楽器休み"),
                "メモ":       st.column_config.TextColumn("メモ"),
            },
        )

        if st.button("💾 まとめて保存", type="primary", use_container_width=True,
                     key="practice_list_save"):
            ok_n = ng_n = 0
            with st.spinner("保存中..."):
                df_reset = edited_prac.reset_index(drop=True)
                for idx, meta in enumerate(prac_meta):
                    if idx >= len(df_reset): break
                    row      = df_reset.iloc[idx]
                    new_date = str(row.get("日時") or "").strip()
                    new_venue= str(row.get("会場名") or "").strip()
                    new_addr = str(row.get("会場住所") or "").strip()
                    new_cd   = bool(row.get("本番日") or False)
                    new_po   = bool(row.get("打楽器休み") or False)
                    new_memo = str(row.get("メモ") or "").strip()

                    # 日時のパース（"2026-04-25 19:00" → Notion date format）
                    dt_s = ""
                    if new_date:
                        try:
                            dt_s, _ = _compose_notion_date_with_optional_time(
                                __import__('datetime').date.fromisoformat(new_date[:10]),
                                new_date[11:16] if len(new_date) > 10 else "",
                                "",
                            )
                        except Exception:
                            dt_s = meta["pdate"]

                    ok = _update_practice(
                        ctx, meta["pid"], meta["pname"], filter_concert_id,
                        dt_s or meta["pdate"], "",
                        new_venue, new_addr,
                        new_cd, new_po, new_memo,
                    )
                    if ok and new_cd and not prac_df_rows[idx]["本番日"]:
                        # 本番フラグが新たにONになった→全参加者の出欠を○に自動登録
                        _auto_mark_concert_day_attendance(ctx, meta["pid"], filter_concert_id)
                    ok_n += 1 if ok else 0
                    ng_n += 0 if ok else 1

            if ng_n == 0:
                st.success(f"✅ {ok_n}件を保存しました。")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
            # バージョンを上げてdata_editorをリセット
            st.session_state["practice_editor_version"] = editor_version + 1
            _clear_concert_cache(ctx)
            st.rerun()

        st.divider()
        st.caption("演奏曲設定・前日共有PDFは各練習を展開してください。")
        for p in filtered_practices:
            label = _practice_display_name(p, ctx)
            is_concert_day = _extract_bool_any(ctx, p, PRACTICE_CONCERT_DAY_KEYS, False)
            if is_concert_day:
                label = "【本番当日】" + label
            with st.expander(label, expanded=False):
                p_id_pdf = p.get("id", "")
                # 演奏曲設定（multiselect必須のためexpander内に残す）
                song_opts_e: dict = {}
                if filter_concert_id:
                    s_rows_e = _load_songs(ctx, filter_concert_id)
                    song_opts_e = {ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or s.get("id",""): s.get("id","")
                                   for s in s_rows_e}
                cur_song_ids_e = ctx["extract_relation_ids_any"](p, PRACTICE_SONG_REL_KEYS)
                cur_song_names_e = [k for k, v in song_opts_e.items() if v in cur_song_ids_e]
                selected_songs_e = st.multiselect(
                    "この日に練習する曲（未選択の場合は全曲対象）",
                    options=list(song_opts_e.keys()),
                    default=cur_song_names_e,
                    key=f"prac_songs_{p_id_pdf}",
                )
                if st.button("💾 演奏曲を保存", key=f"prac_songs_save_{p_id_pdf}", use_container_width=True):
                    new_song_ids_e = [song_opts_e[s] for s in selected_songs_e if s in song_opts_e]
                    ok = _update_practice(
                        ctx, p_id_pdf,
                        ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or "",
                        filter_concert_id,
                        ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or "", "",
                        ctx["extract_prop_text_any"](p, PRACTICE_VENUE_KEYS) or "",
                        ctx["extract_prop_text_any"](p, PRACTICE_ADDRESS_KEYS) or "",
                        _extract_bool_any(ctx, p, PRACTICE_CONCERT_DAY_KEYS, False),
                        _extract_bool_any(ctx, p, PRACTICE_PERCUSSION_OFF_KEYS, False),
                        ctx["extract_prop_text_any"](p, PRACTICE_MEMO_KEYS) or "",
                        song_ids=new_song_ids_e,
                    )
                    if ok:
                        st.success("✅ 演奏曲を保存しました。")
                        _clear_concert_cache(ctx)
                        st.rerun()
                    else:
                        st.error("❌ 保存に失敗しました。")
                st.divider()
                col_pdf, col_mail, _ = st.columns([2, 2, 4])

                # ── PDF出力 ────────────────────────────────────
                if col_pdf.button("📄 前日共有PDF",
                                  key=f"practice_pdf_{p_id_pdf}",
                                  use_container_width=True):
                    with st.spinner("PDF生成中..."):
                        try:
                            from concert.services.practice_report import generate_practice_report
                            pdf_bytes = generate_practice_report(ctx, p_id_pdf)
                            fname_pdf = f"練習前日共有_{label.replace('/', '-').replace(' ', '_')}.pdf"
                            st.session_state[f"practice_pdf_bytes_{p_id_pdf}"] = pdf_bytes
                            st.session_state[f"practice_pdf_fname_{p_id_pdf}"] = fname_pdf
                        except Exception as e:
                            st.error(f"PDF生成に失敗しました: {e}")

                _pdf_ready = st.session_state.get(f"practice_pdf_bytes_{p_id_pdf}")
                if _pdf_ready:
                    st.download_button(
                        label="⬇️ ダウンロード",
                        data=_pdf_ready,
                        file_name=st.session_state.get(f"practice_pdf_fname_{p_id_pdf}", "practice.pdf"),
                        mime="application/pdf",
                        key=f"practice_pdf_dl_{p_id_pdf}",
                    )

                # ── メール一括送信 ──────────────────────────────
                if col_mail.button("✉️ メール送信",
                                   key=f"practice_mail_{p_id_pdf}",
                                   use_container_width=True):
                    with st.spinner("PDF生成・送信中..."):
                        try:
                            from concert.services.practice_report import generate_practice_report
                            from concert.services.mailer import get_recipients_from_players, send_pdf_to_all
                            _pdf = generate_practice_report(ctx, p_id_pdf)
                            _fname = f"練習前日共有_{label.replace('/', '-').replace(' ', '_')}.pdf"
                            _concert_id = (filter_concert_id or "").strip()
                            if not _concert_id:
                                st.error("演奏会が選択されていないため、送信対象を確定できません。")
                                st.stop()

                            _participant_rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"])
                            _participant_player_ids = set()
                            for _row in _participant_rows:
                                _concert_ids = ctx["extract_relation_ids_any"](_row, PARTICIPANT_CONCERT_REL_KEYS)
                                if _concert_id not in _concert_ids:
                                    continue
                                _player_ids = ctx["extract_relation_ids_any"](_row, PARTICIPANT_PLAYER_REL_KEYS)
                                if _player_ids:
                                    _participant_player_ids.add(_player_ids[0])

                            _players = [
                                _p for _p in ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
                                if _p.get("id", "") in _participant_player_ids
                            ]
                            _recipients = get_recipients_from_players(ctx, _players)
                            _with_email = [r for r in _recipients if r["email"]]
                            _no_email   = [r for r in _recipients if not r["email"]]
                            _concert_name = st.session_state.get("SELECTED_CONCERT_NAME", "演奏会")
                            _subject = f"【ArtéMis HARMONIA】{label} 前日共有資料"
                            _body = (
                                "{name} さん" + "\n\n"
                                + f"{_concert_name} の練習（{label}）前日共有資料をお送りします。\n"
                                + "添付PDFをご確認ください。\n\nArtéMis HARMONIA"
                            )
                            result = send_pdf_to_all(
                                ctx, _with_email, _subject, _body, _pdf, _fname
                            )
                            if result.sent:
                                st.success(f"✅ {len(result.sent)}件送信完了")
                            if _no_email:
                                st.warning(f"メール未登録: {', '.join(r['name'] for r in _no_email)}")
                            if result.failed:
                                st.error(f"送信失敗: {', '.join(result.failed)}")
                            if result.errors:
                                for err in result.errors:
                                    st.caption(f"⚠️ {err}")
                        except Exception as e:
                            import traceback
                            st.error(f"送信エラー: {e}")
                            st.code(traceback.format_exc())

    st.divider()
    with st.expander("📅 スケジュール管理", expanded=False):
        _render_schedule_tab(ctx)
    st.divider()
    with st.expander("📋 データチェック", expanded=False):
        try:
            _render_data_check_tab(ctx, filter_concert_id)
        except Exception as e:
            st.error(f"データチェックでエラーが発生しました: {e}")



def _render_data_check_tab(ctx: dict, concert_id: str):
    """データ完全性チェックタブ。未入力・未設定項目を一覧表示する。"""
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    col_h, col_r = st.columns([8, 1])
    col_h.caption("登録済みデータの未入力・未設定項目を検出します。")
    if col_r.button("🔄", key="data_check_refresh"):
        _clear_concert_cache(ctx)
        st.rerun()

    issues: list[dict] = []  # {"category": str, "severity": str, "message": str}

    def warn(category, message):
        issues.append({"category": category, "severity": "⚠️", "message": message})
    def error(category, message):
        issues.append({"category": category, "severity": "🔴", "message": message})
    def ok(category, message):
        issues.append({"category": category, "severity": "✅", "message": message})

    # ── 1. 奏者・参加者 ──────────────────────────────────────
    all_players    = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    participant_rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    cast = [r for r in participant_rows
            if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    player_name_map = {r.get("id",""): ext(r, PLAYER_NAME_KEYS) or r.get("id","")
                       for r in all_players}
    cast_player_ids = set()
    for r in cast:
        pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            cast_player_ids.add(pids[0])

    if not all_players:
        error("奏者", "奏者が1人も登録されていません。")
    elif not cast:
        error("参加者", "この演奏会の参加者が登録されていません。")
    else:
        ok("参加者", f"参加者 {len(cast)}名 登録済み")

    # パート・役職未入力
    no_part = [player_name_map.get((ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0], "?")
               for r in cast if not ext(r, PARTICIPANT_PART_KEYS)]
    if no_part:
        warn("パート・役職", f"パート未入力: {', '.join(no_part)}")
    else:
        ok("パート・役職", "全員パート入力済み")

    no_role = [player_name_map.get((ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0], "?")
               for r in cast if not ext(r, PARTICIPANT_ROLE_KEYS)]
    if no_role:
        warn("パート・役職", f"役職未入力: {', '.join(no_role)}")
    else:
        ok("パート・役職", "全員役職入力済み")

    # 参加費未入力（Nullのみ。0円は入力済み扱い）
    no_fee = []
    for r in cast:
        fee_s = ext(r, PARTICIPANT_FEE_KEYS)
        if fee_s is None or fee_s == "":
            pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
            no_fee.append(player_name_map.get(pids[0] if pids else "", "?"))
    if no_fee:
        warn("参加費", f"参加費未入力（Null）: {', '.join(no_fee)}")
    else:
        ok("参加費", "全員参加費入力済み（0円含む）")

    # ── 2. 出欠 ──────────────────────────────────────────────
    practices = _load_practices(ctx, concert_id)
    if not practices:
        error("出欠", "練習日が登録されていません。")
    else:
        all_att = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
        # practice_id → player_id set
        att_map: dict[str, set] = {}
        for r in all_att:
            pr_ids = ext_rel(r, ATT_PRACTICE_REL_KEYS)
            pl_ids = ext_rel(r, ATT_PLAYER_REL_KEYS)
            if not pr_ids or not pl_ids:
                continue
            att_map.setdefault(pr_ids[0], set()).add(pl_ids[0])

        missing_att = []
        for p in practices:
            pr_id   = p.get("id", "")
            pr_name = ext(p, PRACTICE_NAME_KEYS) or pr_id
            is_cd   = _extract_bool_any(ctx, p, PRACTICE_CONCERT_DAY_KEYS, False)
            recorded = att_map.get(pr_id, set())
            # 出欠レコードがない参加者を特定
            missing = [player_name_map.get(pid, pid)
                       for pid in cast_player_ids if pid not in recorded]
            if missing:
                missing_att.append(f"{pr_name}：{', '.join(missing)}")
        if missing_att:
            for m in missing_att:
                warn("出欠", f"出欠未入力 → {m}")
        else:
            ok("出欠", "全練習日・全参加者の出欠入力済み")

    # ── 3. 希望入力（Percパートのみ） ────────────────────────
    perc_pids = [
        (ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]
        for r in cast
        if (ext(r, PARTICIPANT_PART_KEYS) or "").lower() in ("perc", "percussion", "打楽器")
    ]
    if not perc_pids:
        warn("希望入力", "Percパートの参加者がいません（パート未入力の可能性あり）")
    else:
        all_pref = ctx["query_all"](ctx["CONCERT_DB_PREFERENCE"], None)
        pref_player_ids = set()
        for r in all_pref:
            pl_ids = ext_rel(r, PREF_PLAYER_REL_KEYS)
            if pl_ids:
                pref_player_ids.add(pl_ids[0])
        no_pref = [player_name_map.get(pid, pid)
                   for pid in perc_pids if pid not in pref_player_ids]
        if no_pref:
            warn("希望入力", f"希望未入力（Perc）: {', '.join(no_pref)}")
        else:
            ok("希望入力", f"Perc奏者 {len(perc_pids)}名 全員希望入力済み")

    # ── 4. 持参担当 ───────────────────────────────────────────
    songs = _load_songs(ctx, concert_id)
    required_insts: set[str] = set()
    all_partdefs = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    song_ids_set = {s.get("id","") for s in songs}
    for part in all_partdefs:
        song_rel = ext_rel(part, PARTDEF_SONG_REL_KEYS)
        if not any(sid in song_ids_set for sid in song_rel):
            continue
        iids = ext_rel(part, PARTDEF_INST_REL_KEYS)
        required_insts.update(iids)

    if not required_insts:
        warn("持参担当", "パート定義が登録されていません（必要楽器不明）")
    elif not practices:
        pass  # 既にエラー済み
    else:
        all_pi = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
        # practice_id → instrument_id → True/False（担当あり）
        bring_map: dict[str, set] = {}
        for r in all_pi:
            if ext(r, PI_BRING_ASSIGN_KEYS) != "True":
                continue
            pr_ids = ext_rel(r, PI_PRACTICE_REL_KEYS)
            i_ids  = ext_rel(r, PI_INST_REL_KEYS)
            if not pr_ids or not i_ids:
                continue
            bring_map.setdefault(pr_ids[0], set()).add(i_ids[0])

        inst_name_map2 = {r.get("id",""): ext(r, INSTRUMENT_NAME_KEYS) or r.get("id","")
                          for r in ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)}

        missing_bring = []
        for p in practices:
            if _extract_bool_any(ctx, p, PRACTICE_CONCERT_DAY_KEYS, False):
                continue
            if _extract_bool_any(ctx, p, PRACTICE_PERCUSSION_OFF_KEYS, False):
                continue
            pr_id   = p.get("id", "")
            pr_name = ext(p, PRACTICE_NAME_KEYS) or pr_id
            assigned = bring_map.get(pr_id, set())
            missing  = [inst_name_map2.get(iid, iid)
                        for iid in required_insts if iid not in assigned]
            if missing:
                missing_bring.append(f"{pr_name}：{', '.join(missing)}")

        if missing_bring:
            for m in missing_bring:
                warn("持参担当", f"担当未設定 → {m}")
        else:
            ok("持参担当", "全練習日・全必要楽器の持参担当設定済み")

    # ── 5. 表示 ───────────────────────────────────────────────
    st.divider()
    if not issues:
        st.success("チェック項目なし")
        return

    categories = list(dict.fromkeys(i["category"] for i in issues))
    for cat in categories:
        cat_issues = [i for i in issues if i["category"] == cat]
        has_error = any(i["severity"] == "🔴" for i in cat_issues)
        has_warn  = any(i["severity"] == "⚠️" for i in cat_issues)
        icon = "🔴" if has_error else ("⚠️" if has_warn else "✅")
        with st.expander(f"{icon} {cat}（{len(cat_issues)}件）",
                         expanded=(has_error or has_warn)):
            for i in cat_issues:
                if i["severity"] == "🔴":
                    st.error(i["message"])
                elif i["severity"] == "⚠️":
                    st.warning(i["message"])
                else:
                    st.success(i["message"])

    # サマリ
    n_error = sum(1 for i in issues if i["severity"] == "🔴")
    n_warn  = sum(1 for i in issues if i["severity"] == "⚠️")
    n_ok    = sum(1 for i in issues if i["severity"] == "✅")
    st.caption(f"🔴 {n_error}件  ⚠️ {n_warn}件  ✅ {n_ok}件")


def render_sidebar_summary_pdf(ctx: dict):
    """サイドバーに演奏会サマリPDF出力ボタンを描画する。app.pyから呼ぶ。"""
    concert_id   = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    concert_name = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()
    if not concert_id:
        return
    if st.sidebar.button("📊 演奏会サマリPDFを出力", key="sidebar_summary_pdf_btn",
                          use_container_width=True):
        with st.spinner("PDF生成中..."):
            try:
                from concert.services.concert_summary_report import generate_concert_summary
                pdf_bytes = generate_concert_summary(ctx, concert_id)
                fname = f"演奏会サマリ_{concert_name or concert_id}.pdf"
                st.sidebar.download_button(
                    label="⬇️ ダウンロード",
                    data=pdf_bytes,
                    file_name=fname,
                    mime="application/pdf",
                    key="sidebar_summary_pdf_dl",
                )
            except Exception as e:
                st.sidebar.error(f"PDF生成に失敗しました: {e}")
