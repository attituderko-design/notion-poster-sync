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

def _render_song_tab(ctx: dict):
    concerts = _load_concerts(ctx)
    all_concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_concert_opts)
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")
    filter_concert_id = global_concert_id

    songs = _load_songs(ctx, filter_concert_id)

    st.markdown("### 楽曲情報の確定")
    song_confirm_stats = _get_concert_song_confirmation_stats(ctx, filter_concert_id)
    if not song_confirm_stats["rows"]:
        st.info("この演奏会の CONCERT_SONG がまだありません。楽曲情報は未着手です。")
    elif not song_confirm_stats["flag_key"]:
        st.warning("CONCERT_SONG DB に『楽曲確定』プロパティが見つかりません。")
    else:
        st.caption(
            f"楽曲確定 {song_confirm_stats['confirmed']} / {song_confirm_stats['total']} 曲"
        )
        pending_names = [d["name"] for d in song_confirm_stats["details"] if not d["confirmed"]]
        if pending_names:
            st.caption("未確定: " + "、".join(pending_names[:8]) + (" …" if len(pending_names) > 8 else ""))
        c_sc1, c_sc2 = st.columns(2)
        if c_sc1.button("✅ 楽曲情報を確定", use_container_width=True, key=f"song_confirm_all_{filter_concert_id}"):
            total, updated = _set_concert_song_song_confirmed(ctx, filter_concert_id, True)
            if total == 0:
                st.error("CONCERT_SONG の対象行が見つかりません。")
            elif updated == total:
                st.success(f"✅ {updated}件の楽曲を確定しました。")
                st.rerun()
            else:
                st.warning(f"⚠️ {updated} / {total} 件の更新に成功しました。")
                st.rerun()
        if c_sc2.button("↩ 楽曲情報確定を解除", use_container_width=True, key=f"song_unconfirm_all_{filter_concert_id}"):
            total, updated = _set_concert_song_song_confirmed(ctx, filter_concert_id, False)
            if total == 0:
                st.error("CONCERT_SONG の対象行が見つかりません。")
            elif updated == total:
                st.success(f"✅ {updated}件の楽曲確定を解除しました。")
                st.rerun()
            else:
                st.warning(f"⚠️ {updated} / {total} 件の更新に成功しました。")
                st.rerun()

    with st.expander("➕ 新規楽曲を登録（簡易・手動）", expanded=(len(songs) == 0)):
        st.caption("※ ここで設定する「必要楽器」は曲側の必要編成（人数・台数）です。")
        st.caption("　誰が何を担当するかは「奏者・出欠・アサイン」画面で設定します。")
        with st.form("song_new_form", border=True):
            title    = st.text_input("曲名 *", placeholder="例：マリンバ協奏曲", key="sn_title")
            composer = st.text_input("作曲者", placeholder="例：安倍圭子", key="sn_composer")

            duration_str = st.text_input(
                "演奏時間", placeholder="例：5:30（分:秒）または 330（秒）", key="sn_duration"
            )

            concert_sel = st.multiselect(
                "紐づける演奏会",
                list(all_concert_opts.keys()),
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
                        ok = _create_song(ctx, title.strip(), concert_ids,
                                          composer, duration_sec, note)
                    if ok:
                        st.success("✅ 楽曲を登録しました。")
                        _clear_song_cache()
                        st.rerun()
                    else:
                        st.error("❌ 登録に失敗しました。")

    st.divider()

    if not songs:
        st.info("楽曲がまだ登録されていません。")
        return

    col_h, col_r = st.columns([8, 1])
    col_h.subheader(f"登録済み楽曲（{len(songs)}件）")
    if col_r.button("🔄", key="refresh_songs", help="再読み込み"):
        _clear_song_cache()
        st.rerun()
    song_query = st.text_input(
        "楽曲を検索",
        value=st.session_state.get("songs_song_search", ""),
        key="songs_song_search",
        placeholder="例: Jupiter / marimba / concerto",
    ).strip().lower()

    sorted_songs = sorted(songs, key=lambda x: _song_name(x, ctx))
    if song_query:
        sorted_songs = [
            s for s in sorted_songs
            if song_query in (_song_name(s, ctx) or "").lower()
            or song_query in (ctx["extract_prop_text_any"](s, SONG_COMPOSER_KEYS) or "").lower()
        ]
    st.caption(f"表示件数: {len(sorted_songs)} / {len(songs)}")
    if not sorted_songs:
        st.info("検索条件に一致する楽曲がありません。")
        return

    # 重い全件描画を避け、基本は1件選択編集
    mode_all = st.toggle("全件展開モード（重い）", value=False, key="songs_all_mode")
    if not mode_all:
        sel_map = {}
        labels = []
        for s in sorted_songs:
            sid = s.get("id", "")
            label = _song_name(s, ctx)
            comp = ctx["extract_prop_text_any"](s, SONG_COMPOSER_KEYS)
            labels.append(f"{label} / {comp}" if comp else label)
            sel_map[labels[-1]] = sid
        pick = st.selectbox("編集対象の楽曲", ["（選択してください）"] + labels, key="songs_pick_one")
        if pick == "（選択してください）":
            st.info("上のプルダウンから1件選ぶと編集フォームを表示します。")
            return
        song_id = sel_map.get(pick, "")
        target = next((s for s in sorted_songs if s.get("id", "") == song_id), None)
        if not target:
            st.warning("選択した楽曲が見つかりません。再読み込みしてください。")
            return
        _render_song_editor(ctx, target, all_concert_opts)
        return

    for s in sorted_songs:
        _render_song_editor(ctx, s, all_concert_opts)


def _render_song_editor(ctx: dict, s: dict, all_concert_opts: dict[str, str]):
    song_id    = s.get("id", "")
    song_label = _song_name(s, ctx)
    composer   = ctx["extract_prop_text_any"](s, SONG_COMPOSER_KEYS)
    dur_sec_str = ctx["extract_prop_text_any"](s, SONG_DURATION_KEYS)
    dur_disp   = _sec_to_mmss(int(float(dur_sec_str)) if dur_sec_str else None)
    caption    = f"{composer}　{dur_disp}" if composer or dur_disp else ""

    with st.expander(f"{song_label}　{f'*{caption}*' if caption else ''}", expanded=True):
        concert_ids_current = list(all_concert_opts.values())
        active_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
        if active_concert_id:
            song_confirm_stats = _get_concert_song_confirmation_stats(ctx, active_concert_id)
            target_norms = {_norm_notion_id(x) for x in _resolve_atlas_song_ids(ctx, active_concert_id, song_id)}
            row_detail = next((d for d in song_confirm_stats["details"] if {_norm_notion_id(x) for x in ctx["extract_relation_ids_any"](d["row"], CONCERT_SONG_SONG_REL_KEYS)}.intersection(target_norms)), None)
            if row_detail and song_confirm_stats["flag_key"]:
                st.caption(f"この曲の楽曲確定: {'✅ 確定' if row_detail['confirmed'] else '⬜ 未確定'}")
                c1_song, c2_song = st.columns(2)
                if c1_song.button("✅ この曲を確定", key=f"song_confirm_one_{active_concert_id}_{song_id}", use_container_width=True):
                    total, updated = _set_concert_song_song_confirmed(ctx, active_concert_id, True, song_id=song_id)
                    if total == 0:
                        st.error("CONCERT_SONG 行が見つかりません。")
                    else:
                        st.success(f"✅ {updated}件を更新しました。")
                        st.rerun()
                if c2_song.button("↩ この曲の確定を解除", key=f"song_unconfirm_one_{active_concert_id}_{song_id}", use_container_width=True):
                    total, updated = _set_concert_song_song_confirmed(ctx, active_concert_id, False, song_id=song_id)
                    if total == 0:
                        st.error("CONCERT_SONG 行が見つかりません。")
                    else:
                        st.success(f"✅ {updated}件を更新しました。")
                        st.rerun()

        # 既存紐づき演奏会
        existing_concert_ids = ctx["extract_relation_ids_any"](s, SONG_CONCERT_REL_KEYS)
        existing_concert_names = [k for k, v in all_concert_opts.items() if v in existing_concert_ids]

        with st.form(f"song_edit_{song_id}", border=True):
            title    = st.text_input("曲名 *", value=_song_name(s, ctx), key=f"se_title_{song_id}")
            composer = st.text_input("作曲者", value=ctx["extract_prop_text_any"](s, SONG_COMPOSER_KEYS),
                                     key=f"se_composer_{song_id}")
            dur_str  = st.text_input(
                "演奏時間",
                value=_sec_to_mmss(int(float(dur_sec_str)) if dur_sec_str else None),
                placeholder="例：5:30",
                key=f"se_duration_{song_id}",
            )
            concert_sel = st.multiselect(
                "紐づける演奏会",
                list(all_concert_opts.keys()),
                default=existing_concert_names,
                key=f"se_concerts_{song_id}",
            )
            note = st.text_area("難易度メモ",
                                value=ctx["extract_prop_text_any"](s, SONG_NOTE_KEYS),
                                height=60, key=f"se_note_{song_id}")

            if st.form_submit_button("💾 更新", use_container_width=True):
                if not title.strip():
                    st.error("曲名は必須です。")
                else:
                    duration_sec = _mmss_to_sec(dur_str)
                    concert_ids  = [all_concert_opts[n] for n in concert_sel if all_concert_opts.get(n)]
                    with st.spinner("更新中..."):
                        ok = _update_song(ctx, song_id, title.strip(), concert_ids,
                                          composer, duration_sec, note)
                    if ok:
                        st.success("✅ 更新しました。")
                        _clear_song_cache()
                        st.rerun()
                    else:
                        st.error("❌ 更新に失敗しました。")



def _upsert_partdef(
    ctx: dict,
    concert_id: str,
    song_id: str,
    song_name: str,
    part_name: str,
    inst_ids: list[str],
    inst_names: list[str],
    need_count: int,
    note: str,
    existing_id: str = "",
) -> bool:
    db_id = ctx["CONCERT_DB_PART_DEFINITION"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        st.error("パート定義DBのプロパティ取得に失敗しました。")
        return False
    clean_inst_ids = [x for x in (inst_ids or []) if x]
    if not clean_inst_ids:
        st.error("担当楽器を1つ以上選択してください。")
        return False
    apollo_song_ids = _resolve_apollo_song_ids(ctx, concert_id, song_id)
    if apollo_song_ids:
        target_song_id = apollo_song_ids[0]
    else:
        # APOLLO IDが取得できない場合はATLAS IDで代替（過渡期フォールバック）
        st.warning("対応する APOLLO 演奏曲が見つからないため、ATLAS IDで代替登録します。出演登録フロー経由で APOLLO 演奏曲DBへの登録を確認してください。")
        target_song_id = song_id
    inst_label = " / ".join([x for x in (inst_names or []) if x]) or "楽器未設定"
    props = {}
    ctx["put_prop_any"](props, t, PARTDEF_RECORD_KEYS, f"{song_name} / {part_name} / {inst_label}")
    ctx["put_prop_any"](props, t, PARTDEF_NAME_KEYS, part_name)
    ctx["put_prop_any"](props, t, PARTDEF_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PARTDEF_SONG_REL_KEYS, target_song_id)
    ctx["put_prop_any"](props, t, PARTDEF_INST_REL_KEYS, clean_inst_ids)
    ctx["put_prop_any"](props, t, PARTDEF_NOTE_KEYS, note)
    ctx["put_key_any"](
        props,
        t,
        PARTDEF_KEY_KEYS,
        concert_id,
        target_song_id,
        part_name,
        "|".join(clean_inst_ids),
        prefix="part",
    )
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}", json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _render_partdef_tab(ctx: dict):
    st.subheader("🧩 パート定義")
    st.caption("楽曲ごとに、担当パート（楽器・必要人数）を明示管理します。")

    # Streamlitのwidget keyへ直接代入すると例外になるため、
    # 追加直後の検索語反映は「次回反映キー」を経由して先頭で適用する。
    pending_inst_search = st.session_state.pop("partdef_inst_search_next", "")
    if pending_inst_search:
        st.session_state["partdef_inst_search"] = pending_inst_search

    concerts = _load_concerts(ctx)
    all_concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_concert_opts)
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    c_name = global_concert_name or "（選択中）"
    c_id = global_concert_id
    st.caption(f"対象演奏会: {c_name}")
    if not c_id:
        return

    songs = _load_songs(ctx, c_id)
    if not songs:
        st.info("この演奏会に紐づく楽曲がありません。先に楽曲を登録してください。")
        return
    song_opts = {_song_name(s, ctx): s for s in songs}
    s_name = st.selectbox("楽曲", list(song_opts.keys()), key="partdef_song_sel")
    s = song_opts[s_name]
    selected_apollo_song_id = s.get("id", "")
    atlas_song_ids = _extract_apollo_atlas_song_ids(s, ctx)
    s_id = atlas_song_ids[0] if atlas_song_ids else selected_apollo_song_id

    instruments = _load_instruments(ctx)
    if not instruments:
        st.info("先に楽器種別を登録してください。")
        return
    inst_opts_all = {
        _instrument_name(i, ctx): i.get("id", "")
        for i in sorted(instruments, key=lambda x: _instrument_name(x, ctx))
    }

    st.caption("担当楽器は複数選択できます（例: `Tamb. + Guiro + A.Cym.`）。")
    inst_search = st.text_input(
        "楽器を検索",
        value=st.session_state.get("partdef_inst_search", ""),
        key="partdef_inst_search",
        placeholder="例: snare / cymbal / tam",
    ).strip().lower()
    if inst_search:
        inst_opts = {k: v for k, v in inst_opts_all.items() if inst_search in k.lower()}
    else:
        inst_opts = dict(inst_opts_all)

    c_new1, c_new2, c_new3 = st.columns([4, 2, 2])
    new_inst_name = c_new1.text_input(
        "楽器マスタに新規追加",
        value=st.session_state.get("partdef_new_inst_name", ""),
        key="partdef_new_inst_name",
        placeholder="候補に無い楽器名を入力",
    ).strip()
    new_inst_cat = c_new2.selectbox(
        "カテゴリ",
        INSTRUMENT_CATEGORIES,
        key="partdef_new_inst_cat",
    )
    if c_new3.button("➕ 楽器を追加", use_container_width=True, key="partdef_add_inst_btn"):
        if not new_inst_name:
            st.warning("新規追加する楽器名を入力してください。")
        else:
            with st.spinner("楽器マスタに追加中..."):
                ok_add = _create_instrument(ctx, new_inst_name, new_inst_cat, "")
            if ok_add:
                st.success(f"✅ 楽器マスタへ追加しました: {new_inst_name}")
                st.session_state.pop("instrument_list", None)
                st.session_state["partdef_inst_search_next"] = new_inst_name
                st.rerun()
            else:
                st.error("❌ 楽器マスタへの追加に失敗しました。")

    with st.form(f"partdef_new_{c_id}_{s_id}", border=True):
        p_name = st.text_input("パート名 *", placeholder="例: Part1 1stTimp.")
        i_names = st.multiselect(
            "担当楽器（複数選択可）",
            list(inst_opts.keys()),
            help="候補が多いときは上の「楽器を検索」で絞り込んでください。",
        )
        need = st.number_input("必要人数", min_value=1, max_value=20, value=1, step=1)
        note = st.text_input("備考", placeholder="任意")
        if st.form_submit_button("💾 パートを追加", type="primary", use_container_width=True):
            if not p_name.strip():
                st.error("パート名は必須です。")
            elif not i_names:
                st.error("担当楽器を1つ以上選択してください。")
            else:
                ok = _upsert_partdef(
                    ctx,
                    concert_id=c_id,
                    song_id=s_id,
                    song_name=s_name,
                    part_name=p_name.strip(),
                    inst_ids=[inst_opts[n] for n in i_names if inst_opts.get(n)],
                    inst_names=i_names,
                    need_count=int(need),
                    note=note,
                )
                if ok:
                    _set_concert_song_partdef_completed(
                        ctx,
                        concert_id=c_id,
                        song_id=s_id,
                        completed=False,
                    )
                    st.success("✅ パート定義を追加しました。")
                    st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                    st.rerun()
                else:
                    st.error("❌ 追加に失敗しました。")

    st.divider()
    st.markdown("### CONCERT_SONG への反映")

    cs_row = _find_concert_song_row(ctx, c_id, s_id)
    if cs_row:
        cs_props = (cs_row.get("properties", {}) or {})
        done_prop_name = ctx["find_prop_name"](ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT_SONG"]), CONCERT_SONG_DONE_KEYS)
        current_done = False
        if done_prop_name and done_prop_name in cs_props and cs_props[done_prop_name].get("type") == "checkbox":
            current_done = bool(cs_props[done_prop_name].get("checkbox"))

        st.caption(f"現在の定義完了状態: {'✅ 完了' if current_done else '⬜ 未完了'}")
    else:
        st.warning("CONCERT_SONG に対応する行が見つかりません。完了反映はできません。")

    complete_note = st.text_input(
        "完了時メモ（任意）",
        key=f"concert_song_complete_note_{c_id}_{s_id}",
        placeholder="例: 2026-03-30 パート定義確認済み",
    )

    c_done1, c_done2 = st.columns(2)

    if c_done1.button("✅ この曲のパート定義を完了にする", use_container_width=True):
        if not cs_row:
            st.error("CONCERT_SONG 行が無いため、完了反映できません。")
        else:
            ok = _set_concert_song_partdef_completed(
                ctx,
                concert_id=c_id,
                song_id=s_id,
                completed=True,
                note=complete_note,
            )
            if ok:
                st.success("✅ CONCERT_SONG の『定義完了』を更新しました。")
                st.rerun()
            else:
                st.error("❌ CONCERT_SONG の更新に失敗しました。")

    if c_done2.button("↩ 完了を取り消す", use_container_width=True):
        if not cs_row:
            st.error("CONCERT_SONG 行が無いため、完了取消できません。")
        else:
            ok = _set_concert_song_partdef_completed(
                ctx,
                concert_id=c_id,
                song_id=s_id,
                completed=False,
                note=complete_note,
            )
            if ok:
                st.success("✅ CONCERT_SONG の『定義完了』を取り消しました。")
                st.rerun()
            else:
                st.error("❌ CONCERT_SONG の更新に失敗しました。")

    apollo_song_ids = _resolve_apollo_song_ids(ctx, c_id, s_id)
    if apollo_song_ids:
        st.caption(f"対応する APOLLO 演奏曲: {len(apollo_song_ids)} 件")
    else:
        st.warning("対応する APOLLO 演奏曲が見つかりません。出演登録フロー経由の演奏曲DB作成が必要です。")

    part_rows = _load_partdefs(ctx, c_id, s_id)
    st.caption(f"登録済みパート: {len(part_rows)}件")
    if not part_rows:
        return
    for r in part_rows:
        rid = r.get("id", "")
        part_name_disp = (
            ctx["extract_prop_text_any"](r, PARTDEF_NAME_KEYS)
            or ctx["extract_prop_text_any"](r, PARTDEF_RECORD_KEYS)
            or ctx["extract_title"](r)
        )
        row_title = ctx["extract_prop_text_any"](r, PARTDEF_RECORD_KEYS) or part_name_disp
        cur_inst_ids = ctx["extract_relation_ids_any"](r, PARTDEF_INST_REL_KEYS)
        cur_inst_names = [k for k, v in inst_opts_all.items() if v in set(cur_inst_ids)]
        cur_note = ctx["extract_prop_text_any"](r, PARTDEF_NOTE_KEYS)
        with st.expander(row_title, expanded=True):
            with st.form(f"partdef_edit_{rid}", border=True):
                n_name = st.text_input("パート名 *", value=part_name_disp)
                n_inst = st.multiselect(
                    "担当楽器（複数選択可）",
                    list(inst_opts.keys()),
                    default=[x for x in cur_inst_names if x in inst_opts],
                )
                n_need = 1  # パートNoフィールド廃止のため固定
                n_note = st.text_input("備考", value=cur_note)
                c1, c2 = st.columns(2)
                if c1.form_submit_button("💾 更新", use_container_width=True):
                    if not n_inst:
                        st.error("担当楽器を1つ以上選択してください。")
                        ok = False
                    else:
                        ok = _upsert_partdef(
                            ctx,
                            concert_id=c_id,
                            song_id=s_id,
                            song_name=s_name,
                            part_name=n_name.strip() or part_name_disp,
                            inst_ids=[inst_opts[x] for x in n_inst if inst_opts.get(x)],
                            inst_names=n_inst,
                            need_count=int(n_need),
                            note=n_note,
                            existing_id=rid,
                        )
                    if ok:
                        _set_concert_song_partdef_completed(
                            ctx,
                            concert_id=c_id,
                            song_id=s_id,
                            completed=False,
                        )
                        st.success("✅ 更新しました。")
                        st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                        st.rerun()
                    elif n_inst:
                        st.error("❌ 更新に失敗しました。")
                if c2.form_submit_button("🗑 削除", use_container_width=True):
                    ok = _delete_page(ctx, rid)
                    if ok:
                        _set_concert_song_partdef_completed(
                            ctx,
                            concert_id=c_id,
                            song_id=s_id,
                            completed=False,
                        )
                        st.success("✅ 削除しました。")
                        st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                        st.rerun()
                    else:
                        st.error("❌ 削除に失敗しました。")



def _render_instrument_tab(ctx: dict):
    instruments = _load_instruments(ctx)

    with st.expander("➕ 新規楽器種別を登録", expanded=(len(instruments) == 0)):
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
                        st.rerun()
                    else:
                        st.error("❌ 登録に失敗しました。")

    st.divider()

    if not instruments:
        st.info("楽器種別がまだ登録されていません。")
        return

    col_h, col_r = st.columns([8, 1])
    col_h.subheader(f"登録済み楽器種別（{len(instruments)}件）")
    if col_r.button("🔄", key="refresh_instruments", help="再読み込み"):
        st.session_state.pop("instrument_list", None)
        st.rerun()

    q = st.text_input(
        "楽器種別を検索",
        value=st.session_state.get("instrument_search", ""),
        key="instrument_search",
        placeholder="例: marimba / cymbal / membrane",
    ).strip().lower()
    edit_mode = st.toggle("編集フォームを表示（重い場合はOFF推奨）", value=False, key="instrument_edit_mode")

    if q:
        def _hit(inst: dict) -> bool:
            name = (_instrument_name(inst, ctx) or "").lower()
            cat = (ctx["extract_prop_text_any"](inst, INSTRUMENT_CATEGORY_KEYS) or "").lower()
            memo = (ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS) or "").lower()
            return q in name or q in cat or q in memo
        instruments = [i for i in instruments if _hit(i)]
    st.caption(f"表示件数: {len(instruments)}")
    if not instruments:
        st.info("検索条件に一致する楽器種別がありません。")
        return

    # カテゴリごとにグループ表示
    by_cat: dict[str, list] = {c: [] for c in INSTRUMENT_CATEGORIES}
    for i in instruments:
        cat = ctx["extract_prop_text_any"](i, INSTRUMENT_CATEGORY_KEYS) or "その他"
        by_cat.setdefault(cat, []).append(i)

    # 読み取り専用の軽量表示
    if not edit_mode:
        for cat in INSTRUMENT_CATEGORIES:
            items = by_cat.get(cat, [])
            if not items:
                continue
            st.markdown(f"**{cat}**")
            for inst in sorted(items, key=lambda x: _instrument_name(x, ctx)):
                label = _instrument_name(inst, ctx)
                memo = ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS)
                if memo:
                    st.markdown(f"- {label}  \n  <span style='color:#9aa0a6'>{memo}</span>", unsafe_allow_html=True)
                else:
                    st.markdown(f"- {label}")
        return

    import pandas as pd
    # 全楽器をdata_editorで一括編集
    edit_rows: list[dict] = []
    edit_meta: list[dict] = []
    for cat in INSTRUMENT_CATEGORIES:
        for inst in sorted(by_cat.get(cat, []), key=lambda x: _instrument_name(x, ctx)):
            iid   = inst.get("id", "")
            label = _instrument_name(inst, ctx)
            cur_cat  = ctx["extract_prop_text_any"](inst, INSTRUMENT_CATEGORY_KEYS) or "その他"
            cur_memo = ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS) or ""
            edit_rows.append({"楽器名": label, "カテゴリ": cur_cat, "メモ": cur_memo})
            edit_meta.append({"iid": iid, "iname": label, "icat": cur_cat, "imemo": cur_memo})

    df_inst = pd.DataFrame(edit_rows)
    edited_inst = st.data_editor(
        df_inst,
        num_rows="fixed",
        use_container_width=True,
        key=f"inst_edit_df_{q}",
        column_config={
            "楽器名": st.column_config.TextColumn("楽器名", max_chars=50),
            "カテゴリ": st.column_config.SelectboxColumn(
                "カテゴリ", options=INSTRUMENT_CATEGORIES,
            ),
            "メモ": st.column_config.TextColumn("メモ", max_chars=100),
        },
    )

    if st.button("💾 まとめて保存", type="primary", use_container_width=True, key="inst_bulk_save"):
        ok_n = ng_n = skip_n = 0
        with st.spinner("保存中..."):
            df_reset = edited_inst.reset_index(drop=True)
            for idx, meta in enumerate(edit_meta):
                if idx >= len(df_reset): break
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
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（変更なし {skip_n}件はスキップ）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        st.session_state.pop("instrument_list", None)
        st.rerun()


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎵 楽曲・楽器管理")
    global_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    tab_song, tab_partdef, tab_instrument = st.tabs(["楽曲", "パート定義", "楽器種別"])

    with tab_song:
        _render_song_tab(ctx)

    with tab_partdef:
        _render_partdef_tab(ctx)

    with tab_instrument:
        _render_instrument_tab(ctx)
