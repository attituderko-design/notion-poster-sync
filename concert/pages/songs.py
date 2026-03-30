"""
concert.pages.songs
楽曲・楽器種別の登録、曲別必要楽器（SongInstrument）の設定画面。
"""
import streamlit as st
from concert.services.keys import *  # noqa: F401,F403


APOLLO_ATLAS_SONG_REL_KEYS = ["演奏曲", "曲", "作品楽章", "作品", "ATLAS曲"]

CONCERT_SONG_SONGINFO_KEYS = [
    "楽曲確定",
    "楽曲情報確定",
    "曲確定",
    "song_confirmed",
    "songs_confirmed",
    "song_info_confirmed",
]


def _norm_loose_label(v: str) -> str:
    return str(v or "").replace(" ", "").replace("　", "").strip().lower()


def _find_prop_name_loose(ctx: dict, type_map: dict, candidates: list[str]) -> str:
    key = ctx["find_prop_name"](type_map, candidates)
    if key:
        return key
    norm_map = {_norm_loose_label(k): k for k in (type_map or {}).keys()}
    for c in candidates:
        got = norm_map.get(_norm_loose_label(c))
        if got:
            return got
    return ""


def _find_prop_name_from_rows_loose(rows: list[dict], candidates: list[str]) -> str:
    if not rows:
        return ""
    for row in rows:
        props = (row.get("properties") or {})
        if not props:
            continue
        norm_map = {_norm_loose_label(k): k for k in props.keys()}
        for c in candidates:
            got = norm_map.get(_norm_loose_label(c))
            if got:
                return got
    return ""


def _clear_property_type_cache():
    try:
        from concert.services.notion_client import get_concert_db_property_types
        get_concert_db_property_types.clear()
    except Exception:
        pass


def _extract_checkbox_value(page: dict, prop_name: str) -> bool:
    if not page or not prop_name:
        return False
    meta = ((page.get("properties") or {}).get(prop_name) or {})
    if meta.get("type") == "checkbox":
        return bool(meta.get("checkbox"))
    raw = str(meta or "").strip().lower()
    return raw in {"true", "1", "yes", "on", "はい"}



def _norm_notion_id(v: str) -> str:
    return (v or "").replace("-", "").strip().lower()


def _extract_apollo_atlas_song_ids(song_row: dict, ctx: dict) -> list[str]:
    """
    APOLLO（演奏曲）行が参照している ATLAS 曲ID を返す。
    現行DBでは APOLLO.演奏曲 relation が ATLAS 向き。
    """
    if not song_row:
        return []
    return ctx["extract_relation_ids_any"](song_row, APOLLO_ATLAS_SONG_REL_KEYS)


def _resolve_atlas_song_ids(ctx: dict, concert_id: str, song_id: str) -> list[str]:
    """
    入力 song_id が APOLLO 行ID / ATLAS 曲ID のどちらでも、
    対応する ATLAS 曲IDの配列を返す。
    """
    if not song_id:
        return []

    target_norm = _norm_notion_id(song_id)
    out: list[str] = []
    seen: set[str] = set()

    rows = _load_songs(ctx, concert_id)
    if concert_id:
        extra = _load_songs(ctx, "")
        rows = rows + [r for r in extra if r.get("id", "") not in {x.get("id", "") for x in rows}]

    for row in rows:
        rid = row.get("id", "")
        atlas_ids = _extract_apollo_atlas_song_ids(row, ctx)
        atlas_norms = {_norm_notion_id(x) for x in atlas_ids if x}
        rid_norm = _norm_notion_id(rid)

        # APOLLO行ID指定
        if rid_norm and rid_norm == target_norm:
            for aid in atlas_ids:
                an = _norm_notion_id(aid)
                if aid and an not in seen:
                    seen.add(an)
                    out.append(aid)

        # すでにATLAS曲ID指定
        if target_norm in atlas_norms:
            for aid in atlas_ids:
                an = _norm_notion_id(aid)
                if aid and an not in seen:
                    seen.add(an)
                    out.append(aid)

    if out:
        return out

    # 最終救済: すでにATLAS曲IDだった可能性
    return [song_id]








# ============================================================
# キャッシュ／ロードヘルパー
# ============================================================

def _clear_song_cache():
    _clear_property_type_cache()
    for k in list(st.session_state.keys()):
        if k.startswith(("song_list", "instrument_list", "si_list_", "concert_song_rows_")):
            st.session_state.pop(k, None)
    st.session_state.pop("songs_concert_list", None)


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


def _load_concerts(ctx) -> list[dict]:
    if "songs_concert_list" not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
        st.session_state["songs_concert_list"] = [r for r in rows if _is_performance_media_concert(r)]
    return st.session_state.get("songs_concert_list", [])


def _load_songs(ctx, concert_id: str = "") -> list[dict]:
    """
    演奏会に紐づく楽曲を CONCERT_SONG 経由で取得する。
    CONCERT_SONG.曲 は ATLAS 向き、APOLLO.演奏曲 relation も ATLAS 向きで対応付ける。
    """
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        songs: list[dict] = []

        if concert_id and ctx.get("CONCERT_DB_CONCERT_SONG"):
            cs_type_map = ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT_SONG"])
            cs_concert_rel = ctx["find_prop_name"](cs_type_map, CONCERT_SONG_CONCERT_REL_KEYS)
            cs_filter = None
            if cs_concert_rel:
                cs_filter = {"filter": {"property": cs_concert_rel, "relation": {"contains": concert_id}}}
            cs_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT_SONG"], cs_filter)

            if cs_rows:
                atlas_song_ids: list[str] = []
                for cs in cs_rows:
                    atlas_song_ids.extend(ctx["extract_relation_ids_any"](cs, CONCERT_SONG_SONG_REL_KEYS))
                atlas_song_ids = list(dict.fromkeys([x for x in atlas_song_ids if x]))

                if atlas_song_ids:
                    atlas_id_set = {_norm_notion_id(sid) for sid in atlas_song_ids}
                    all_apollo = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)

                    for row in all_apollo:
                        apollo_atlas_ids = ctx["extract_relation_ids_any"](row, APOLLO_ATLAS_SONG_REL_KEYS)
                        apollo_atlas_norm = {_norm_notion_id(x) for x in apollo_atlas_ids if x}
                        if not apollo_atlas_norm.intersection(atlas_id_set):
                            continue

                        # 演奏会 relation があるなら対象演奏会にも一致させる
                        concert_rel_ids = ctx["extract_relation_ids_any"](row, SONG_CONCERT_REL_KEYS)
                        if concert_rel_ids and concert_id not in concert_rel_ids:
                            continue

                        songs.append(row)

                    found_atlas_norms = set()
                    for row in songs:
                        found_atlas_norms.update(
                            _norm_notion_id(x) for x in ctx["extract_relation_ids_any"](row, APOLLO_ATLAS_SONG_REL_KEYS) if x
                        )
                    missing = [sid for sid in atlas_song_ids if _norm_notion_id(sid) not in found_atlas_norms]
                    if missing:
                        st.caption(f"⚠️ APOLLO未登録の曲が{len(missing)}件あります（ATLAS IDで表示）")

        if not songs:
            f = None
            if concert_id:
                type_map = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
                rel_prop = ctx["find_prop_name"](type_map, SONG_CONCERT_REL_KEYS)
                if rel_prop:
                    f = {"filter": {"property": rel_prop, "relation": {"contains": concert_id}}}
            songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], f)

        st.session_state[key] = songs
    return st.session_state.get(key, [])


def _resolve_apollo_song_ids(ctx, concert_id: str, atlas_song_id: str) -> list[str]:
    """
    ATLAS曲IDに対応する APOLLO 演奏曲DB行IDを返す。
    入力がすでに APOLLO 行ID の場合もそのまま救済する。
    """
    if not atlas_song_id:
        return []

    target_norm = _norm_notion_id(atlas_song_id)
    out: list[str] = []
    seen: set[str] = set()

    rows = _load_songs(ctx, concert_id)
    if concert_id:
        extra = _load_songs(ctx, "")
        rows = rows + [r for r in extra if r.get("id", "") not in {x.get("id", "") for x in rows}]

    for row in rows:
        rid = row.get("id", "")
        rid_norm = _norm_notion_id(rid)
        atlas_ids = _extract_apollo_atlas_song_ids(row, ctx)
        atlas_norms = {_norm_notion_id(x) for x in atlas_ids if x}

        if rid_norm == target_norm or target_norm in atlas_norms:
            if rid and rid not in seen:
                seen.add(rid)
                out.append(rid)

    if not out and atlas_song_id:
        out = [atlas_song_id]
    return out


def _load_instruments(ctx) -> list[dict]:
    if "instrument_list" not in st.session_state:
        st.session_state["instrument_list"] = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    return st.session_state.get("instrument_list", [])


def _load_partdefs(ctx, concert_id: str = "", song_id: str = "") -> list[dict]:
    """
    パート定義を取得する。

    song_id は画面上の ATLAS 曲ID。
    PART_DEFINITION.演奏曲 は APOLLO 向きのため、_resolve_apollo_song_ids() で変換する。
    変換失敗時は ATLAS ID をそのまま使って旧データを救済する。

    フィルタ優先順位:
      1. 演奏会 + APOLLO曲ID（正常系）
      2. 演奏会 + ATLAS曲ID（旧データ互換・フォールバック）
      3. APOLLO曲IDのみ（演奏会relation未設定の旧データ救済）
    """
    key = f"partdef_list_{concert_id}_{song_id}"
    if key not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"])
        t     = ctx["get_prop_types"](ctx["CONCERT_DB_PART_DEFINITION"])
        c_rel = ctx["find_prop_name"](t, PARTDEF_CONCERT_REL_KEYS)
        s_rel = ctx["find_prop_name"](t, PARTDEF_SONG_REL_KEYS)

        # ATLAS曲ID → APOLLO曲IDに変換（旧データ互換で両方を候補に含める）
        target_song_ids = set()
        if song_id:
            apollo_ids = _resolve_apollo_song_ids(ctx, concert_id, song_id)
            target_song_ids.update(apollo_ids)
            target_song_ids.add(song_id)  # ATLAS IDも候補に（旧データ互換）

        def _matches(r: dict) -> bool:
            ok = True
            if concert_id and c_rel:
                ok = concert_id in ctx["extract_relation_ids"](r, c_rel)
            if ok and target_song_ids and s_rel:
                ok = bool(target_song_ids.intersection(set(ctx["extract_relation_ids"](r, s_rel))))
            return ok

        out = [r for r in rows if _matches(r)]

        # 演奏会relationが未設定の旧データを救済（曲IDのみでマッチ）
        if not out and target_song_ids and s_rel:
            out = [r for r in rows
                   if bool(target_song_ids.intersection(set(ctx["extract_relation_ids"](r, s_rel))))]

        st.session_state[key] = out
    return st.session_state.get(key, [])


def _concert_name(c: dict, ctx: dict) -> str:
    n  = ctx["extract_prop_text_any"](c, ["名称", "タイトル"]) or ctx["extract_title"](c)
    dt = ctx["extract_prop_text_any"](c, CONCERT_DATE_KEYS)
    return f"{n}（{dt[:10] if dt else '日時未設定'}）"


def _song_name(s: dict, ctx: dict) -> str:
    return ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or ctx["extract_title"](s) or s.get("id", "")


def _instrument_name(i: dict, ctx: dict) -> str:
    return ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ctx["extract_title"](i) or i.get("id", "")


def _get_global_concert_filter(ctx: dict, concert_opts: dict[str, str]) -> tuple[str, str]:
    gid = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    gname = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()
    if not gid:
        return "", ""
    if not gname:
        for n, cid in (concert_opts or {}).items():
            if cid == gid:
                gname = n
                break
    return gid, gname


# ============================================================
# 楽曲 CRUD
# ============================================================

def _create_song(ctx: dict, title: str, concert_ids: list[str],
                 composer: str, duration_sec: int | None, note: str) -> bool:
    db_id    = ctx["CONCERT_DB_SONG"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("楽曲DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    ctx["put_prop_any"](props, type_map, SONG_NAME_KEYS, title)
    if concert_ids:
        ctx["put_prop_any"](props, type_map, SONG_CONCERT_REL_KEYS, concert_ids)
    ctx["put_prop_any"](props, type_map, SONG_COMPOSER_KEYS, composer)
    if duration_sec is not None:
        ctx["put_prop_any"](props, type_map, SONG_DURATION_KEYS, duration_sec)
    ctx["put_prop_any"](props, type_map, SONG_NOTE_KEYS, note)
    ctx["put_key_any"](props, type_map, SONG_KEY_KEYS, title, composer, prefix="song")
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_song(ctx: dict, page_id: str, title: str, concert_ids: list[str],
                 composer: str, duration_sec: int | None, note: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, SONG_NAME_KEYS, title)
    if concert_ids:
        ctx["put_prop_any"](props, type_map, SONG_CONCERT_REL_KEYS, concert_ids)
    ctx["put_prop_any"](props, type_map, SONG_COMPOSER_KEYS, composer)
    if duration_sec is not None:
        ctx["put_prop_any"](props, type_map, SONG_DURATION_KEYS, duration_sec)
    ctx["put_prop_any"](props, type_map, SONG_NOTE_KEYS, note)
    ctx["put_key_any"](props, type_map, SONG_KEY_KEYS, title, composer, prefix="song")
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 楽器種別 CRUD
# ============================================================

INSTRUMENT_CATEGORIES = ["鍵盤楽器", "膜鳴楽器", "金属楽器", "小物楽器", "特殊楽器・効果音", "備品", "その他"]


def _create_instrument(ctx: dict, name: str, category: str, memo: str) -> bool:
    db_id    = ctx["CONCERT_DB_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("楽器種別DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    ctx["put_prop_any"](props, type_map, INSTRUMENT_NAME_KEYS, name)
    ctx["put_prop_any"](props, type_map, INSTRUMENT_CATEGORY_KEYS, category)
    ctx["put_prop_any"](props, type_map, INSTRUMENT_MEMO_KEYS, memo)
    ctx["put_key_any"](props, type_map, INSTRUMENT_KEY_KEYS, name, prefix="inst")
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_instrument(ctx: dict, page_id: str, name: str, category: str, memo: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_INSTRUMENT"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, INSTRUMENT_NAME_KEYS, name)
    ctx["put_prop_any"](props, type_map, INSTRUMENT_CATEGORY_KEYS, category)
    ctx["put_prop_any"](props, type_map, INSTRUMENT_MEMO_KEYS, memo)
    ctx["put_key_any"](props, type_map, INSTRUMENT_KEY_KEYS, name, prefix="inst")
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 曲別必要楽器 CRUD
# ============================================================

def _delete_page(ctx: dict, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"archived": True})
    return res is not None and res.status_code == 200


def _find_concert_song_row(ctx: dict, concert_id: str, song_id: str) -> dict | None:
    """
    CONCERT_SONG DB から、指定の 演奏会 + 曲(ATLAS) に対応する1行を返す。
    song_id は APOLLO 行ID / ATLAS 曲ID のどちらでも受ける。
    """
    db_id = ctx.get("CONCERT_DB_CONCERT_SONG")
    if not db_id or not concert_id or not song_id:
        return None

    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        return None

    concert_rel = ctx["find_prop_name"](type_map, CONCERT_SONG_CONCERT_REL_KEYS)
    song_rel = ctx["find_prop_name"](type_map, CONCERT_SONG_SONG_REL_KEYS)
    target_atlas_ids = _resolve_atlas_song_ids(ctx, concert_id, song_id)
    target_atlas_norms = {_norm_notion_id(x) for x in target_atlas_ids if x}

    rows = ctx["query_all"](db_id)
    for row in rows:
        ok = True

        if concert_rel:
            row_concert_ids = ctx["extract_relation_ids"](row, concert_rel)
            ok = concert_id in row_concert_ids

        if ok and song_rel:
            row_song_norms = {_norm_notion_id(x) for x in ctx["extract_relation_ids"](row, song_rel)}
            ok = bool(row_song_norms.intersection(target_atlas_norms))

        if ok:
            return row

    return None

    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        return None

    concert_rel = ctx["find_prop_name"](type_map, CONCERT_SONG_CONCERT_REL_KEYS)
    song_rel = ctx["find_prop_name"](type_map, CONCERT_SONG_SONG_REL_KEYS)

    rows = ctx["query_all"](db_id)
    for row in rows:
        ok = True

        if concert_rel:
            ok = concert_id in ctx["extract_relation_ids"](row, concert_rel)
        if ok and song_rel:
            ok = song_id in ctx["extract_relation_ids"](row, song_rel)

        if ok:
            return row

    return None


def _set_concert_song_partdef_completed(
    ctx: dict,
    concert_id: str,
    song_id: str,
    completed: bool = True,
    note: str = "",
) -> bool:
    """
    CONCERT_SONG.定義完了 を更新する。
    note が指定された場合は 備考 も更新する。
    """
    row = _find_concert_song_row(ctx, concert_id, song_id)
    if not row:
        st.error("CONCERT_SONG に対応する行が見つかりません。演奏会と曲の紐づきを確認してください。")
        return False

    page_id = row.get("id", "")
    if not page_id:
        st.error("CONCERT_SONG の対象ページIDが取得できませんでした。")
        return False

    db_id = ctx["CONCERT_DB_CONCERT_SONG"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("CONCERT_SONG DB のプロパティ取得に失敗しました。")
        return False

    props: dict = {}

    done_prop = ctx["find_prop_name"](type_map, CONCERT_SONG_DONE_KEYS)
    if done_prop:
        props[done_prop] = {"checkbox": bool(completed)}
    else:
        st.error("CONCERT_SONG DB に『定義完了』プロパティが見つかりません。")
        return False

    if note.strip():
        note_prop = ctx["find_prop_name"](type_map, CONCERT_SONG_NOTE_KEYS)
        if note_prop:
            props[note_prop] = {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": note.strip()},
                    }
                ]
            }

    res = ctx["api_request"](
        "patch",
        f"https://api.notion.com/v1/pages/{page_id}",
        json={"properties": props},
    )
    return res is not None and res.status_code == 200


def _load_concert_song_rows(ctx: dict, concert_id: str) -> list[dict]:
    if not concert_id or not ctx.get("CONCERT_DB_CONCERT_SONG"):
        return []
    key = f"concert_song_rows_{concert_id}"
    if key not in st.session_state:
        db_id = ctx["CONCERT_DB_CONCERT_SONG"]
        type_map = ctx["get_prop_types"](db_id) or {}
        rel_key = _find_prop_name_loose(ctx, type_map, CONCERT_SONG_CONCERT_REL_KEYS)
        rows = []
        if rel_key:
            rows = ctx["query_all"](db_id, {"filter": {"property": rel_key, "relation": {"contains": concert_id}}})
        if not rows:
            rows = ctx["query_all"](db_id)
        target = _norm_notion_id(concert_id)
        filtered = []
        for row in rows:
            ids = ctx["extract_relation_ids_any"](row, [rel_key] if rel_key else CONCERT_SONG_CONCERT_REL_KEYS)
            if any(_norm_notion_id(x) == target for x in ids):
                filtered.append(row)
        st.session_state[key] = filtered
    return st.session_state.get(key, [])


def _clear_concert_song_cache(concert_id: str = ""):
    for k in list(st.session_state.keys()):
        if k.startswith("concert_song_rows_") and (not concert_id or k == f"concert_song_rows_{concert_id}"):
            st.session_state.pop(k, None)


def _find_harmonia_concert_row(ctx: dict, concert_id: str) -> dict | None:
    if not concert_id or not ctx.get("CONCERT_DB_HARMONIA_CONCERT"):
        return None
    db_id = ctx["CONCERT_DB_HARMONIA_CONCERT"]
    type_map = ctx["get_prop_types"](db_id) or {}
    rel_key = _find_prop_name_loose(ctx, type_map, HARMONIA_CONCERT_CONCERT_REL_KEYS)
    rows = []
    if rel_key:
        rows = ctx["query_all"](db_id, {"filter": {"property": rel_key, "relation": {"contains": concert_id}}})
    if not rows:
        rows = ctx["query_all"](db_id)
    target = _norm_notion_id(concert_id)
    for row in rows:
        ids = ctx["extract_relation_ids_any"](row, [rel_key] if rel_key else HARMONIA_CONCERT_CONCERT_REL_KEYS)
        if any(_norm_notion_id(x) == target for x in ids):
            return row
    return None


def _set_harmonia_concert_checkbox(ctx: dict, concert_id: str, key_candidates: list[str], checked: bool) -> bool:
    row = _find_harmonia_concert_row(ctx, concert_id)
    if not row:
        return False
    row_id = row.get("id", "")
    if not row_id:
        return False
    db_id = ctx["CONCERT_DB_HARMONIA_CONCERT"]
    type_map = ctx["get_prop_types"](db_id) or {}
    flag_key = _find_prop_name_loose(ctx, type_map, key_candidates)
    if not flag_key:
        return False
    res = ctx["api_request"](
        "patch",
        f"https://api.notion.com/v1/pages/{row_id}",
        json={"properties": {flag_key: {"checkbox": bool(checked)}}},
    )
    return res is not None and res.status_code == 200


def _refresh_harmonia_song_info_status(ctx: dict, concert_id: str) -> bool:
    rows = _load_concert_song_rows(ctx, concert_id)
    if not rows:
        return _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_SONG_INFO_KEYS, False)
    db_id = ctx["CONCERT_DB_CONCERT_SONG"]
    type_map = ctx["get_prop_types"](db_id) or {}
    flag_key = _find_prop_name_loose(ctx, type_map, CONCERT_SONG_SONGINFO_KEYS) or _find_prop_name_from_rows_loose(rows, CONCERT_SONG_SONGINFO_KEYS)
    if not flag_key:
        return False
    all_done = all(_extract_checkbox_value(r, flag_key) for r in rows)
    return _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_SONG_INFO_KEYS, all_done)


def _set_concert_song_song_confirmed(
    ctx: dict,
    concert_id: str,
    checked: bool,
    song_id: str = "",
) -> tuple[int, int]:
    rows = _load_concert_song_rows(ctx, concert_id)
    if not rows or not ctx.get("CONCERT_DB_CONCERT_SONG"):
        return 0, 0
    db_id = ctx["CONCERT_DB_CONCERT_SONG"]
    type_map = ctx["get_prop_types"](db_id) or {}
    flag_key = _find_prop_name_loose(ctx, type_map, CONCERT_SONG_SONGINFO_KEYS) or _find_prop_name_from_rows_loose(rows, CONCERT_SONG_SONGINFO_KEYS)
    if not flag_key:
        return 0, 0

    target_atlas_norms = {_norm_notion_id(x) for x in _resolve_atlas_song_ids(ctx, concert_id, song_id)} if song_id else set()

    def _matches_song(row: dict) -> bool:
        if not target_atlas_norms:
            return True
        row_song_ids = ctx["extract_relation_ids_any"](row, CONCERT_SONG_SONG_REL_KEYS)
        row_song_norms = {_norm_notion_id(x) for x in row_song_ids if x}
        return bool(row_song_norms.intersection(target_atlas_norms))

    targets = [r for r in rows if _matches_song(r)]
    updated = 0
    for row in targets:
        row_id = row.get("id", "")
        if not row_id:
            continue
        res = ctx["api_request"](
            "patch",
            f"https://api.notion.com/v1/pages/{row_id}",
            json={"properties": {flag_key: {"checkbox": bool(checked)}}},
        )
        if res is not None and res.status_code == 200:
            updated += 1

    _clear_concert_song_cache(concert_id)
    _refresh_harmonia_song_info_status(ctx, concert_id)
    return len(targets), updated


def _get_concert_song_confirmation_stats(ctx: dict, concert_id: str) -> dict:
    rows = _load_concert_song_rows(ctx, concert_id)
    db_id = ctx.get("CONCERT_DB_CONCERT_SONG", "")
    type_map = ctx["get_prop_types"](db_id) or {} if db_id else {}
    flag_key = _find_prop_name_loose(ctx, type_map, CONCERT_SONG_SONGINFO_KEYS) or _find_prop_name_from_rows_loose(rows, CONCERT_SONG_SONGINFO_KEYS)
    confirmed = 0
    detail_rows = []
    for row in rows:
        atlas_song_ids = ctx["extract_relation_ids_any"](row, CONCERT_SONG_SONG_REL_KEYS)
        song_names = []
        for aid in atlas_song_ids:
            apollo_ids = _resolve_apollo_song_ids(ctx, concert_id, aid)
            matched = next((s for s in _load_songs(ctx, concert_id) if s.get("id", "") in set(apollo_ids)), None)
            if matched:
                song_names.append(_song_name(matched, ctx))
        if not song_names:
            song_names = atlas_song_ids or [row.get("id", "")]
        is_done = _extract_checkbox_value(row, flag_key) if flag_key else False
        confirmed += 1 if is_done else 0
        detail_rows.append({"row": row, "name": " / ".join(song_names), "confirmed": is_done})
    return {
        "rows": rows,
        "total": len(rows),
        "confirmed": confirmed,
        "unconfirmed": max(len(rows) - confirmed, 0),
        "flag_key": flag_key,
        "details": detail_rows,
    }


# ============================================================
# 演奏時間ユーティリティ
# ============================================================

def _sec_to_mmss(sec: int | None) -> str:
    if sec is None or sec <= 0:
        return ""
    return f"{sec // 60}:{sec % 60:02d}"


def _mmss_to_sec(mmss: str) -> int | None:
    """'5:30' → 330、空文字 → None"""
    s = mmss.strip()
    if not s:
        return None
    try:
        if ":" in s:
            parts = s.split(":")
            return int(parts[0]) * 60 + int(parts[1])
        return int(s)
    except ValueError:
        return None


# ============================================================
# 楽曲タブ
# ============================================================


# ============================================================
# 楽曲タブ
# ============================================================

def _render_song_tab(ctx: dict):
    """楽曲タブ：一覧・確定管理・新規登録をサブタブで分離。"""
    concerts = _load_concerts(ctx)
    all_concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_concert_opts)
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    filter_concert_id = global_concert_id

    sub_list, sub_new = st.tabs(["📋 楽曲一覧", "➕ 新規登録"])

    # ── サブタブ: 楽曲一覧 ──────────────────────────────────
    with sub_list:
        songs = _load_songs(ctx, filter_concert_id)

        # 確定サマリーをコンパクトに表示
        song_confirm_stats = _get_concert_song_confirmation_stats(ctx, filter_concert_id)
        if song_confirm_stats["rows"] and song_confirm_stats["flag_key"]:
            confirmed = song_confirm_stats["confirmed"]
            total     = song_confirm_stats["total"]
            if confirmed == total:
                st.success(f"✅ 楽曲情報確定：{confirmed} / {total} 曲（全曲完了）")
            else:
                pending = [d["name"] for d in song_confirm_stats["details"] if not d["confirmed"]]
                st.warning(
                    f"楽曲情報確定：{confirmed} / {total} 曲　"
                    f"未確定：{'、'.join(pending[:5])}{'…' if len(pending) > 5 else ''}"
                )

        col_h, col_r = st.columns([8, 1])
        col_h.subheader(f"楽曲一覧（{len(songs)}件）" if songs else "楽曲一覧")
        if col_r.button("🔄", key="refresh_songs", help="再読み込み"):
            _clear_song_cache()
            st.rerun()

        if not songs:
            st.info("楽曲がまだ登録されていません。「新規登録」タブから追加してください。")
            return

        # 検索
        song_query = st.text_input(
            "検索",
            value=st.session_state.get("songs_song_search", ""),
            key="songs_song_search",
            placeholder="曲名 / 作曲者",
        ).strip().lower()
        sorted_songs = sorted(songs, key=lambda x: _song_name(x, ctx))
        if song_query:
            sorted_songs = [
                s for s in sorted_songs
                if song_query in (_song_name(s, ctx) or "").lower()
                or song_query in (ctx["extract_prop_text_any"](s, SONG_COMPOSER_KEYS) or "").lower()
            ]
        if not sorted_songs:
            st.info("検索条件に一致する楽曲がありません。")
            return
        st.caption(f"{len(sorted_songs)} / {len(songs)} 件表示")

        # 楽曲ごとに1行ずつ表示
        for s in sorted_songs:
            _render_song_row(ctx, s, all_concert_opts, filter_concert_id, song_confirm_stats)

    # ── サブタブ: 新規登録 ───────────────────────────────────
    with sub_new:
        st.subheader("楽曲を新規登録")
        st.caption("APOLLO（演奏曲DB）に登録済みの曲を追加する場合は、出演登録フロー経由で行ってください。")
        with st.form("song_new_form", border=True):
            title    = st.text_input("曲名 *", placeholder="例：マリンバ協奏曲", key="sn_title")
            composer = st.text_input("作曲者", placeholder="例：安倍圭子", key="sn_composer")
            duration_str = st.text_input(
                "演奏時間", placeholder="例：5:30（分:秒）または 330（秒）", key="sn_duration"
            )
            concert_sel = st.multiselect(
                "紐づける演奏会",
                list(all_concert_opts.keys()),
                default=[k for k, v in all_concert_opts.items() if v == filter_concert_id],
                key="sn_concerts",
            )
            note = st.text_area("難易度メモ", height=60, key="sn_note")
            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                if not title.strip():
                    st.error("曲名は必須です。")
                else:
                    duration_sec = _mmss_to_sec(duration_str)
                    concert_ids  = [all_concert_opts[n] for n in concert_sel if all_concert_opts.get(n)]
                    with st.spinner("登録中..."):
                        ok = _create_song(ctx, title.strip(), concert_ids, composer, duration_sec, note)
                    if ok:
                        st.success("✅ 楽曲を登録しました。")
                        _clear_song_cache()
                        st.rerun()
                    else:
                        st.error("❌ 登録に失敗しました。")


def _render_song_row(
    ctx: dict,
    s: dict,
    all_concert_opts: dict[str, str],
    concert_id: str,
    song_confirm_stats: dict,
):
    """楽曲1件の行表示。確定ボタンを行内に統合。"""
    song_id    = s.get("id", "")
    song_label = _song_name(s, ctx)
    composer   = ctx["extract_prop_text_any"](s, SONG_COMPOSER_KEYS) or ""
    dur_sec_str = ctx["extract_prop_text_any"](s, SONG_DURATION_KEYS)
    dur_disp   = _sec_to_mmss(int(float(dur_sec_str)) if dur_sec_str else None)
    sub_cap    = "　".join(filter(None, [composer, dur_disp]))

    # 確定状態を取得
    is_confirmed = False
    has_flag_key = bool(song_confirm_stats.get("flag_key"))
    if has_flag_key:
        target_norms = {_norm_notion_id(x) for x in _resolve_atlas_song_ids(ctx, concert_id, song_id)}
        row_detail = next(
            (d for d in song_confirm_stats["details"]
             if {_norm_notion_id(x) for x in ctx["extract_relation_ids_any"](d["row"], CONCERT_SONG_SONG_REL_KEYS)}.intersection(target_norms)),
            None
        )
        is_confirmed = row_detail["confirmed"] if row_detail else False

    confirm_badge = "✅" if is_confirmed else "⬜"
    expander_label = f"{confirm_badge} {song_label}" + (f"　*{sub_cap}*" if sub_cap else "")

    with st.expander(expander_label, expanded=False):
        # 確定ボタン（行内）
        if has_flag_key:
            c_conf1, c_conf2 = st.columns(2)
            if not is_confirmed:
                if c_conf1.button("✅ 楽曲情報を確定", key=f"song_confirm_{concert_id}_{song_id}",
                                   use_container_width=True, type="primary"):
                    total, updated = _set_concert_song_song_confirmed(ctx, concert_id, True, song_id=song_id)
                    st.success(f"✅ {updated}件を更新しました。") if updated else st.error("CONCERT_SONG 行が見つかりません。")
                    st.rerun()
            else:
                if c_conf2.button("↩ 確定を解除", key=f"song_unconfirm_{concert_id}_{song_id}",
                                   use_container_width=True):
                    total, updated = _set_concert_song_song_confirmed(ctx, concert_id, False, song_id=song_id)
                    st.success(f"✅ {updated}件を更新しました。") if updated else st.error("CONCERT_SONG 行が見つかりません。")
                    st.rerun()

        # 編集フォーム
        existing_concert_ids   = ctx["extract_relation_ids_any"](s, SONG_CONCERT_REL_KEYS)
        existing_concert_names = [k for k, v in all_concert_opts.items() if v in existing_concert_ids]
        with st.form(f"song_edit_{song_id}", border=True):
            title   = st.text_input("曲名 *", value=song_label, key=f"se_title_{song_id}")
            composer_v = st.text_input("作曲者", value=composer, key=f"se_composer_{song_id}")
            dur_str = st.text_input(
                "演奏時間", value=dur_disp or "", placeholder="例：5:30",
                key=f"se_duration_{song_id}",
            )
            concert_sel = st.multiselect(
                "紐づける演奏会", list(all_concert_opts.keys()),
                default=existing_concert_names, key=f"se_concerts_{song_id}",
            )
            note = st.text_area("難易度メモ",
                                value=ctx["extract_prop_text_any"](s, SONG_NOTE_KEYS) or "",
                                height=60, key=f"se_note_{song_id}")
            if st.form_submit_button("💾 更新", use_container_width=True):
                if not title.strip():
                    st.error("曲名は必須です。")
                else:
                    duration_sec = _mmss_to_sec(dur_str)
                    concert_ids  = [all_concert_opts[n] for n in concert_sel if all_concert_opts.get(n)]
                    with st.spinner("更新中..."):
                        ok = _update_song(ctx, song_id, title.strip(), concert_ids,
                                          composer_v, duration_sec, note)
                    if ok:
                        st.success("✅ 更新しました。")
                        _clear_song_cache()
                        st.rerun()
                    else:
                        st.error("❌ 更新に失敗しました。")


# ============================================================
# パート定義タブ
# ============================================================

def _render_partdef_tab(ctx: dict):
    """パート定義タブ：曲選択→パート一覧→追加。楽器追加は楽器タブへ。"""
    pending_inst_search = st.session_state.pop("partdef_inst_search_next", "")
    if pending_inst_search:
        st.session_state["partdef_inst_search"] = pending_inst_search

    concerts = _load_concerts(ctx)
    all_concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_concert_opts)
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    c_id = global_concert_id

    songs = _load_songs(ctx, c_id)
    if not songs:
        st.info("この演奏会に楽曲がありません。先に「楽曲」タブで登録してください。")
        return

    instruments = _load_instruments(ctx)
    if not instruments:
        st.info("楽器種別が登録されていません。先に「楽器種別」タブで登録してください。")
        return
    inst_opts_all = {
        _instrument_name(i, ctx): i.get("id", "")
        for i in sorted(instruments, key=lambda x: _instrument_name(x, ctx))
    }

    # 曲選択
    song_opts = {_song_name(s, ctx): s for s in songs}
    s_name = st.selectbox("楽曲を選択", list(song_opts.keys()), key="partdef_song_sel")
    s = song_opts[s_name]
    selected_apollo_song_id = s.get("id", "")
    atlas_song_ids = _extract_apollo_atlas_song_ids(s, ctx)
    s_id = atlas_song_ids[0] if atlas_song_ids else selected_apollo_song_id

    st.divider()

    # CONCERT_SONG 定義完了状態
    cs_row = _find_concert_song_row(ctx, c_id, s_id)
    if cs_row:
        cs_props      = cs_row.get("properties", {}) or {}
        done_prop_name = ctx["find_prop_name"](
            ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT_SONG"]), CONCERT_SONG_DONE_KEYS
        )
        current_done = (
            bool(cs_props[done_prop_name].get("checkbox"))
            if done_prop_name and done_prop_name in cs_props
               and cs_props[done_prop_name].get("type") == "checkbox"
            else False
        )
        badge = "✅ 定義完了" if current_done else "⬜ 未完了"
        col_badge, col_btn1, col_btn2 = st.columns([3, 2, 2])
        col_badge.markdown(f"**パート定義状態：{badge}**")
        complete_note = st.text_input(
            "完了メモ（任意）",
            key=f"cs_complete_note_{c_id}_{s_id}",
            placeholder="例: 2026-03-30 確認済み",
        )
        if not current_done:
            if col_btn1.button("✅ 完了にする", use_container_width=True,
                               key=f"cs_done_{c_id}_{s_id}", type="primary"):
                ok = _set_concert_song_partdef_completed(ctx, c_id, s_id, True, note=complete_note)
                st.success("✅ 更新しました。") if ok else st.error("❌ 更新に失敗しました。")
                if ok: st.rerun()
        else:
            if col_btn2.button("↩ 完了を取り消す", use_container_width=True,
                               key=f"cs_undone_{c_id}_{s_id}"):
                ok = _set_concert_song_partdef_completed(ctx, c_id, s_id, False, note=complete_note)
                st.success("✅ 更新しました。") if ok else st.error("❌ 更新に失敗しました。")
                if ok: st.rerun()
    else:
        st.warning("CONCERT_SONG に対応する行がありません。完了フラグの更新はできません。")

    st.divider()

    # 楽器検索（パート定義タブ内の絞り込みのみ）
    inst_search = st.text_input(
        "楽器を絞り込む",
        value=st.session_state.get("partdef_inst_search", ""),
        key="partdef_inst_search",
        placeholder="例: snare / cymbal",
    ).strip().lower()
    inst_opts = {k: v for k, v in inst_opts_all.items() if inst_search in k.lower()}         if inst_search else dict(inst_opts_all)
    if not inst_opts:
        st.warning("条件に一致する楽器がありません。「楽器種別」タブで追加してください。")

    # パート追加フォーム
    with st.expander("➕ パートを追加", expanded=False):
        with st.form(f"partdef_new_{c_id}_{s_id}", border=True):
            p_name = st.text_input("パート名 *", placeholder="例: Part1 1stTimp.")
            i_names = st.multiselect(
                "担当楽器（複数選択可）", list(inst_opts.keys()),
                help="候補が多いときは上の絞り込みを使ってください。",
            )
            need = st.number_input("必要人数", min_value=1, max_value=20, value=1, step=1)
            note = st.text_input("備考", placeholder="任意")
            if st.form_submit_button("💾 追加", type="primary", use_container_width=True):
                if not p_name.strip():
                    st.error("パート名は必須です。")
                elif not i_names:
                    st.error("担当楽器を1つ以上選択してください。")
                else:
                    ok = _upsert_partdef(
                        ctx, concert_id=c_id, song_id=s_id, song_name=s_name,
                        part_name=p_name.strip(),
                        inst_ids=[inst_opts[n] for n in i_names if inst_opts.get(n)],
                        inst_names=i_names, need_count=int(need), note=note,
                    )
                    if ok:
                        _set_concert_song_partdef_completed(ctx, c_id, s_id, False)
                        st.success("✅ パートを追加しました。")
                        st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                        st.rerun()
                    else:
                        st.error("❌ 追加に失敗しました。")

    # 登録済みパート一覧
    part_rows = _load_partdefs(ctx, c_id, s_id)
    st.subheader(f"登録済みパート（{len(part_rows)}件）")
    if not part_rows:
        st.info("まだパートが登録されていません。上の「パートを追加」から登録してください。")
    else:
        for r in part_rows:
            rid = r.get("id", "")
            part_name_disp = (
                ctx["extract_prop_text_any"](r, PARTDEF_NAME_KEYS)
                or ctx["extract_prop_text_any"](r, PARTDEF_RECORD_KEYS)
                or ctx["extract_title"](r)
            )
            row_title      = ctx["extract_prop_text_any"](r, PARTDEF_RECORD_KEYS) or part_name_disp
            cur_inst_ids   = ctx["extract_relation_ids_any"](r, PARTDEF_INST_REL_KEYS)
            cur_inst_names = [k for k, v in inst_opts_all.items() if v in set(cur_inst_ids)]
            cur_note       = ctx["extract_prop_text_any"](r, PARTDEF_NOTE_KEYS) or ""
            with st.expander(row_title, expanded=False):
                with st.form(f"partdef_edit_{rid}", border=True):
                    n_name = st.text_input("パート名 *", value=part_name_disp)
                    n_inst = st.multiselect(
                        "担当楽器（複数選択可）", list(inst_opts.keys()),
                        default=[x for x in cur_inst_names if x in inst_opts],
                    )
                    n_note = st.text_input("備考", value=cur_note)
                    c1, c2 = st.columns(2)
                    if c1.form_submit_button("💾 更新", use_container_width=True):
                        if not n_inst:
                            st.error("担当楽器を1つ以上選択してください。")
                        else:
                            ok = _upsert_partdef(
                                ctx, concert_id=c_id, song_id=s_id, song_name=s_name,
                                part_name=n_name.strip() or part_name_disp,
                                inst_ids=[inst_opts[x] for x in n_inst if inst_opts.get(x)],
                                inst_names=n_inst, need_count=1, note=n_note,
                                existing_id=rid,
                            )
                            if ok:
                                _set_concert_song_partdef_completed(ctx, c_id, s_id, False)
                                st.success("✅ 更新しました。")
                                st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                                st.rerun()
                            else:
                                st.error("❌ 更新に失敗しました。")
                    if c2.form_submit_button("🗑 削除", use_container_width=True):
                        ok = _delete_page(ctx, rid)
                        if ok:
                            _set_concert_song_partdef_completed(ctx, c_id, s_id, False)
                            st.success("✅ 削除しました。")
                            st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                            st.rerun()
                        else:
                            st.error("❌ 削除に失敗しました。")

    # デバッグ情報（通常時は非表示）
    with st.expander("🔧 デバッグ情報", expanded=False):
        apollo_song_ids = _resolve_apollo_song_ids(ctx, c_id, s_id)
        if apollo_song_ids:
            st.caption(f"対応する APOLLO 演奏曲ID: {len(apollo_song_ids)} 件")
            for aid in apollo_song_ids:
                st.caption(f"  - {aid}")
        else:
            st.warning("対応する APOLLO 演奏曲が見つかりません。出演登録フロー経由の登録が必要です。")
        if cs_row:
            st.caption(f"CONCERT_SONG ID: {cs_row.get('id', '')}")


# ============================================================
# 楽器種別タブ
# ============================================================

def _render_instrument_tab(ctx: dict):
    """楽器種別タブ：楽器マスタの管理 + パート定義タブ向けの新規追加を統合。"""
    instruments = _load_instruments(ctx)

    sub_list, sub_new = st.tabs(["📋 楽器一覧", "➕ 新規登録"])

    # ── サブタブ: 楽器一覧 ──────────────────────────────────
    with sub_list:
        col_h, col_r = st.columns([8, 1])
        col_h.subheader(f"楽器種別一覧（{len(instruments)}件）")
        if col_r.button("🔄", key="refresh_instruments", help="再読み込み"):
            st.session_state.pop("instrument_list", None)
            st.rerun()

        if not instruments:
            st.info("楽器種別がまだ登録されていません。「新規登録」タブから追加してください。")
            return

        q = st.text_input(
            "検索", value=st.session_state.get("instrument_search", ""),
            key="instrument_search", placeholder="例: marimba / cymbal",
        ).strip().lower()
        edit_mode = st.toggle("編集モード", value=False, key="instrument_edit_mode")

        filtered = instruments
        if q:
            def _hit(inst: dict) -> bool:
                name = (_instrument_name(inst, ctx) or "").lower()
                cat  = (ctx["extract_prop_text_any"](inst, INSTRUMENT_CATEGORY_KEYS) or "").lower()
                memo = (ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS) or "").lower()
                return q in name or q in cat or q in memo
            filtered = [i for i in instruments if _hit(i)]

        st.caption(f"{len(filtered)} / {len(instruments)} 件表示")
        if not filtered:
            st.info("検索条件に一致する楽器がありません。")
            return

        # カテゴリごとにグループ表示
        by_cat: dict[str, list] = {c: [] for c in INSTRUMENT_CATEGORIES}
        for i in filtered:
            cat = ctx["extract_prop_text_any"](i, INSTRUMENT_CATEGORY_KEYS) or "その他"
            by_cat.setdefault(cat, []).append(i)

        if not edit_mode:
            # 読み取り専用
            for cat in INSTRUMENT_CATEGORIES:
                items = by_cat.get(cat, [])
                if not items:
                    continue
                st.markdown(f"**{cat}**")
                for inst in sorted(items, key=lambda x: _instrument_name(x, ctx)):
                    label = _instrument_name(inst, ctx)
                    memo  = ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS)
                    if memo:
                        memo_html = f"- {label}  \n  <span style='color:#9aa0a6'>{memo}</span>"
                        st.markdown(memo_html, unsafe_allow_html=True)
                    else:
                        st.markdown(f"- {label}")
        else:
            # data_editorで一括編集
            import pandas as pd
            edit_rows: list[dict] = []
            edit_meta: list[dict] = []
            for cat in INSTRUMENT_CATEGORIES:
                for inst in sorted(by_cat.get(cat, []), key=lambda x: _instrument_name(x, ctx)):
                    iid      = inst.get("id", "")
                    label    = _instrument_name(inst, ctx)
                    cur_cat  = ctx["extract_prop_text_any"](inst, INSTRUMENT_CATEGORY_KEYS) or "その他"
                    cur_memo = ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS) or ""
                    edit_rows.append({"楽器名": label, "カテゴリ": cur_cat, "メモ": cur_memo})
                    edit_meta.append({"iid": iid, "iname": label, "icat": cur_cat, "imemo": cur_memo})

            df_inst = pd.DataFrame(edit_rows)
            edited_inst = st.data_editor(
                df_inst, num_rows="fixed", use_container_width=True,
                key=f"inst_edit_df_{q}",
                column_config={
                    "楽器名": st.column_config.TextColumn("楽器名", max_chars=50),
                    "カテゴリ": st.column_config.SelectboxColumn("カテゴリ", options=INSTRUMENT_CATEGORIES),
                    "メモ": st.column_config.TextColumn("メモ", max_chars=100),
                },
            )
            if st.button("💾 まとめて保存", type="primary", use_container_width=True,
                         key="inst_bulk_save"):
                ok_n = ng_n = skip_n = 0
                with st.spinner("保存中..."):
                    df_reset = edited_inst.reset_index(drop=True)
                    for idx, meta in enumerate(edit_meta):
                        if idx >= len(df_reset):
                            break
                        row   = df_reset.iloc[idx]
                        new_n = str(row.get("楽器名") or "").strip()
                        new_c = str(row.get("カテゴリ") or "").strip()
                        new_m = str(row.get("メモ") or "").strip()
                        if not new_n:
                            skip_n += 1
                            continue
                        if new_n == meta["iname"] and new_c == meta["icat"] and new_m == meta["imemo"]:
                            skip_n += 1
                            continue
                        ok = _update_instrument(ctx, meta["iid"], new_n, new_c, new_m)
                        ok_n  += 1 if ok else 0
                        ng_n  += 0 if ok else 1
                if ng_n == 0:
                    st.success(f"✅ {ok_n}件を保存しました。（変更なし {skip_n}件はスキップ）")
                else:
                    st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
                st.session_state.pop("instrument_list", None)
                st.rerun()

    # ── サブタブ: 新規登録 ───────────────────────────────────
    with sub_new:
        st.subheader("楽器種別を新規登録")
        st.caption("パート定義タブの楽器選択に反映されます。")
        with st.form("inst_new_form", border=True):
            name     = st.text_input("楽器名 *", placeholder="例：マリンバ", key="in_name")
            category = st.selectbox("カテゴリ", INSTRUMENT_CATEGORIES, key="in_cat")
            memo     = st.text_area("メモ", height=60, key="in_memo")
            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                if not name.strip():
                    st.error("楽器名は必須です。")
                else:
                    with st.spinner("登録中..."):
                        ok = _create_instrument(ctx, name.strip(), category, memo)
                    if ok:
                        st.success("✅ 楽器種別を登録しました。")
                        st.session_state.pop("instrument_list", None)
                        # パート定義タブの検索語を新楽器名にセット
                        st.session_state["partdef_inst_search_next"] = name.strip()
                        st.rerun()
                    else:
                        st.error("❌ 登録に失敗しました。")


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎵 楽曲・楽器管理")
    global_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    tab_song, tab_partdef, tab_instrument = st.tabs(["🎵 楽曲", "🧩 パート定義", "🎹 楽器種別"])
    with tab_song:
        _render_song_tab(ctx)
    with tab_partdef:
        _render_partdef_tab(ctx)
    with tab_instrument:
        _render_instrument_tab(ctx)