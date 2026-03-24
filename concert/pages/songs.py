"""
concert.pages.songs
楽曲・楽器種別の登録、曲別必要楽器（SongInstrument）の設定画面。
"""
import streamlit as st

SONG_NAME_KEYS = ["曲名", "タイトル"]
SONG_COMPOSER_KEYS = ["作曲者", "クリエイター"]
SONG_DURATION_KEYS = ["演奏時間（秒）", "演奏時間", "DurationSec"]
SONG_NOTE_KEYS = ["難易度メモ", "メモ", "備考"]
SONG_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
CONCERT_DATE_KEYS = ["日時", "日付", "出演日", "体験日", "リリース日"]
CONCERT_MEDIA_KEYS = ["媒体", "MEDIA_TYPE", "メディア", "種類"]

INSTRUMENT_NAME_KEYS = ["楽器名", "タイトル", "PK楽器名"]
INSTRUMENT_CATEGORY_KEYS = ["カテゴリ"]
INSTRUMENT_MEMO_KEYS = ["メモ", "備考"]

SONG_INSTR_RECORD_KEYS = ["レコード名", "タイトル"]
SONG_INSTR_SONG_REL_KEYS = ["楽曲", "演奏曲", "FK楽曲"]
SONG_INSTR_INST_REL_KEYS = ["楽器種別", "楽器", "FK楽器種別"]
SONG_INSTR_QTY_KEYS = ["必要台数", "台数"]
SONG_INSTR_NOTE_KEYS = ["備考", "メモ"]

PARTDEF_RECORD_KEYS = ["レコード名", "タイトル", "名称", "パート名"]
PARTDEF_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
PARTDEF_SONG_REL_KEYS = ["演奏曲", "楽曲", "FK楽曲", "作品楽章", "作品マスタ"]
PARTDEF_INST_REL_KEYS = ["必要楽器", "楽器種別", "楽器", "FK楽器種別", "担当楽器"]
PARTDEF_COUNT_KEYS = ["必要人数", "必要台数", "台数", "人数"]
PARTDEF_NOTE_KEYS = ["備考", "メモ"]

SONG_KEY_KEYS = ["song_key", "SongKey", "PK曲キー", "曲キー"]
INSTRUMENT_KEY_KEYS = ["instrument_key", "InstrumentKey", "PK楽器キー", "楽器キー"]
PARTDEF_KEY_KEYS = ["part_key", "PartKey", "PKパートキー", "パートキー"]


# ============================================================
# キャッシュ／ロードヘルパー
# ============================================================

def _clear_song_cache():
    for k in list(st.session_state.keys()):
        if k.startswith(("song_list", "instrument_list", "si_list_")):
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
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        f = None
        if concert_id:
            type_map = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
            rel_prop = ctx["find_prop_name"](type_map, SONG_CONCERT_REL_KEYS)
            if rel_prop:
                f = {"filter": {"property": rel_prop, "relation": {"contains": concert_id}}}
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_SONG"], f)
    return st.session_state.get(key, [])


def _load_instruments(ctx) -> list[dict]:
    if "instrument_list" not in st.session_state:
        st.session_state["instrument_list"] = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    return st.session_state.get("instrument_list", [])


def _load_song_instruments(ctx, song_id: str) -> list[dict]:
    key = f"si_list_{song_id}"
    if key not in st.session_state:
        type_map = ctx["get_prop_types"](ctx["CONCERT_DB_SONG_INSTRUMENT"])
        rel_prop = ctx["find_prop_name"](type_map, SONG_INSTR_SONG_REL_KEYS)
        filter_payload = {"filter": {"property": rel_prop, "relation": {"contains": song_id}}} if rel_prop else None
        rows = ctx["query_all"](
            ctx["CONCERT_DB_SONG_INSTRUMENT"],
            filter_payload,
        )
        st.session_state[key] = rows
    return st.session_state.get(key, [])


def _load_partdefs(ctx, concert_id: str = "", song_id: str = "") -> list[dict]:
    key = f"partdef_list_{concert_id}_{song_id}"
    if key not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"])
        t = ctx["get_prop_types"](ctx["CONCERT_DB_PART_DEFINITION"])
        c_rel = ctx["find_prop_name"](t, PARTDEF_CONCERT_REL_KEYS)
        s_rel = ctx["find_prop_name"](t, PARTDEF_SONG_REL_KEYS)
        out = []
        for r in rows:
            ok = True
            if concert_id and c_rel:
                ok = concert_id in ctx["extract_relation_ids"](r, c_rel)
            if ok and song_id and s_rel:
                ok = song_id in ctx["extract_relation_ids"](r, s_rel)
            if ok:
                out.append(r)
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

INSTRUMENT_CATEGORIES = ["鍵盤楽器", "膜鳴楽器", "金属楽器", "小物楽器", "特殊楽器・効果音"]


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

def _upsert_song_instrument(ctx: dict, song_id: str, song_name: str,
                             instrument_id: str, instrument_name: str,
                             qty: int, note: str,
                             existing_id: str = "") -> bool:
    db_id    = ctx["CONCERT_DB_SONG_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("曲別必要楽器DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    ctx["put_prop_any"](props, type_map, SONG_INSTR_RECORD_KEYS, f"{song_name} × {instrument_name}")
    ctx["put_prop_any"](props, type_map, SONG_INSTR_SONG_REL_KEYS, song_id)
    ctx["put_prop_any"](props, type_map, SONG_INSTR_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, type_map, SONG_INSTR_QTY_KEYS, qty)
    ctx["put_prop_any"](props, type_map, SONG_INSTR_NOTE_KEYS, note)
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _delete_page(ctx: dict, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"archived": True})
    return res is not None and res.status_code == 200


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

    if global_concert_id:
        concert_opts = {global_concert_name or "（選択中）": global_concert_id}
        st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")
    else:
        concert_search = st.text_input(
            "演奏会を検索",
            value=st.session_state.get("songs_concert_search", ""),
            key="songs_concert_search",
            placeholder="例: 2026 / Osaka / 定期 / Happy Hour",
        ).strip().lower()
        if concert_search:
            concert_opts = {
                k: v for k, v in all_concert_opts.items()
                if concert_search in k.lower()
            }
        else:
            concert_opts = all_concert_opts
    if not concert_opts:
        st.warning("演奏会検索の条件に一致する候補がありません。絞り込みを緩めてください。")

    st.info(
        "🎼 楽曲の正式登録は ArtéMis MUSE（媒体=演奏曲）を推奨します。"
        " MUSE経由だと MusicBrainz / 初演情報 / 肖像画 / 作品・楽章マスタ連動まで一括反映されます。"
    )
    st.caption(
        "この画面の「新規楽曲を登録」は簡易手動登録です。"
        "急ぎの追記や、MUSE未収載データの暫定入力に使ってください。"
    )

    # 絞り込み
    if global_concert_id:
        filter_concert_id = global_concert_id
    else:
        filter_opts = {"すべて": ""} | concert_opts
        selected_filter = st.selectbox("絞り込み：演奏会", list(filter_opts.keys()), key="song_filter")
        filter_concert_id = filter_opts.get(selected_filter, "")

    songs = _load_songs(ctx, filter_concert_id)

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

        # 必要楽器サブセクション
        st.caption("📋 この曲に必要な楽器")
        _render_song_instrument_section(ctx, song_id, song_label)


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
    inst_label = " / ".join([x for x in (inst_names or []) if x]) or "楽器未設定"
    props = {}
    ctx["put_prop_any"](props, t, PARTDEF_RECORD_KEYS, f"{song_name} / {part_name} / {inst_label}")
    ctx["put_prop_any"](props, t, PARTDEF_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PARTDEF_SONG_REL_KEYS, song_id)
    ctx["put_prop_any"](props, t, PARTDEF_INST_REL_KEYS, clean_inst_ids)
    ctx["put_prop_any"](props, t, PARTDEF_COUNT_KEYS, int(max(need_count, 1)))
    ctx["put_prop_any"](props, t, PARTDEF_NOTE_KEYS, note)
    ctx["put_key_any"](
        props,
        t,
        PARTDEF_KEY_KEYS,
        concert_id,
        song_id,
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
    if global_concert_id:
        concert_opts = {global_concert_name or "（選択中）": global_concert_id}
        st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")
    else:
        c_query = st.text_input(
            "演奏会を検索",
            value=st.session_state.get("partdef_concert_search", ""),
            key="partdef_concert_search",
            placeholder="例: 2026 / 定期 / Happy Hour",
        ).strip().lower()
        if c_query:
            concert_opts = {k: v for k, v in all_concert_opts.items() if c_query in k.lower()}
        else:
            concert_opts = all_concert_opts
    if not concert_opts:
        st.warning("一致する演奏会がありません。")
        return

    if global_concert_id:
        c_name = global_concert_name or next(iter(concert_opts.keys()))
        c_id = global_concert_id
    else:
        c_name = st.selectbox("演奏会", list(concert_opts.keys()), key="partdef_concert_sel")
        c_id = concert_opts.get(c_name, "")
    if not c_id:
        return

    songs = _load_songs(ctx, c_id)
    if not songs:
        st.info("この演奏会に紐づく楽曲がありません。先に楽曲を登録してください。")
        return
    song_opts = {_song_name(s, ctx): s for s in songs}
    s_name = st.selectbox("楽曲", list(song_opts.keys()), key="partdef_song_sel")
    s = song_opts[s_name]
    s_id = s.get("id", "")

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
                    st.success("✅ パート定義を追加しました。")
                    st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                    st.rerun()
                else:
                    st.error("❌ 追加に失敗しました。")

    part_rows = _load_partdefs(ctx, c_id, s_id)
    st.caption(f"登録済みパート: {len(part_rows)}件")
    if not part_rows:
        return
    for r in part_rows:
        rid = r.get("id", "")
        p_name = ctx["extract_prop_text_any"](r, PARTDEF_RECORD_KEYS) or ctx["extract_title"](r)
        cur_inst_ids = ctx["extract_relation_ids_any"](r, PARTDEF_INST_REL_KEYS)
        cur_inst_names = [k for k, v in inst_opts_all.items() if v in set(cur_inst_ids)]
        cur_need = ctx["extract_prop_text_any"](r, PARTDEF_COUNT_KEYS) or "1"
        try:
            cur_need_i = int(float(cur_need))
        except Exception:
            cur_need_i = 1
        cur_note = ctx["extract_prop_text_any"](r, PARTDEF_NOTE_KEYS)
        with st.expander(p_name, expanded=False):
            with st.form(f"partdef_edit_{rid}", border=True):
                n_name = st.text_input("パート名 *", value=p_name)
                n_inst = st.multiselect(
                    "担当楽器（複数選択可）",
                    list(inst_opts.keys()),
                    default=[x for x in cur_inst_names if x in inst_opts],
                )
                n_need = st.number_input("必要人数", min_value=1, max_value=20, value=max(cur_need_i, 1), step=1)
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
                            part_name=n_name.strip() or p_name,
                            inst_ids=[inst_opts[x] for x in n_inst if inst_opts.get(x)],
                            inst_names=n_inst,
                            need_count=int(n_need),
                            note=n_note,
                            existing_id=rid,
                        )
                    if ok:
                        st.success("✅ 更新しました。")
                        st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                        st.rerun()
                    elif n_inst:
                        st.error("❌ 更新に失敗しました。")
                if c2.form_submit_button("🗑 削除", use_container_width=True):
                    ok = _delete_page(ctx, rid)
                    if ok:
                        st.success("✅ 削除しました。")
                        st.session_state.pop(f"partdef_list_{c_id}_{s_id}", None)
                        st.rerun()
                    else:
                        st.error("❌ 削除に失敗しました。")


def _render_song_instrument_section(ctx: dict, song_id: str, song_label: str):
    """曲の下に展開する必要楽器設定UI。"""
    instruments = _load_instruments(ctx)
    if not instruments:
        st.info("楽器種別を先に登録してください。")
        return

    si_rows = _load_song_instruments(ctx, song_id)
    si_by_inst: dict[str, dict] = {}
    for row in si_rows:
        iids = ctx["extract_relation_ids_any"](row, SONG_INSTR_INST_REL_KEYS)
        if iids:
            si_by_inst[iids[0]] = row

    inst_opts = {_instrument_name(i, ctx): i.get("id", "")
                 for i in sorted(instruments, key=lambda x: _instrument_name(x, ctx))}

    with st.form(f"si_form_{song_id}", border=True):
        changes: list[dict] = []
        for inst_name, inst_id in inst_opts.items():
            existing = si_by_inst.get(inst_id)
            cur_qty  = int(float(ctx["extract_prop_text_any"](existing, SONG_INSTR_QTY_KEYS) or "0")) if existing else 0
            cur_note = ctx["extract_prop_text_any"](existing, SONG_INSTR_NOTE_KEYS) if existing else ""

            col_inst, col_qty, col_note = st.columns([3, 1, 4])
            col_inst.markdown(f"**{inst_name}**")
            qty = col_qty.number_input(
                "台数", min_value=0, max_value=20, value=cur_qty, step=1,
                label_visibility="collapsed",
                key=f"si_qty_{song_id}_{inst_id}",
            )
            note = col_note.text_input(
                "備考", value=cur_note, placeholder="3oct可・アンプ必要等",
                label_visibility="collapsed",
                key=f"si_note_{song_id}_{inst_id}",
            )
            changes.append({
                "inst_id":     inst_id,
                "inst_name":   inst_name,
                "qty":         qty,
                "note":        note,
                "existing_id": existing.get("id", "") if existing else "",
            })

        if st.form_submit_button("💾 必要楽器を保存", use_container_width=True):
            success, fail = 0, 0
            with st.spinner("保存中..."):
                for ch in changes:
                    if ch["qty"] == 0 and not ch["existing_id"]:
                        continue  # 0台かつ未登録はスキップ
                    if ch["qty"] == 0 and ch["existing_id"]:
                        # 0台に変更 → アーカイブ（削除）
                        ok = _delete_page(ctx, ch["existing_id"])
                    else:
                        ok = _upsert_song_instrument(
                            ctx,
                            song_id=song_id,
                            song_name=song_label,
                            instrument_id=ch["inst_id"],
                            instrument_name=ch["inst_name"],
                            qty=ch["qty"],
                            note=ch["note"],
                            existing_id=ch["existing_id"],
                        )
                    if ok:
                        success += 1
                    else:
                        fail += 1

            if fail == 0:
                st.success(f"✅ {success}件を保存しました。")
            else:
                st.warning(f"⚠️ {success}件成功、{fail}件失敗。")
            st.session_state.pop(f"si_list_{song_id}", None)
            st.rerun()


# ============================================================
# 楽器種別タブ
# ============================================================

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

    for cat in INSTRUMENT_CATEGORIES:
        items = by_cat.get(cat, [])
        if not items:
            continue
        st.markdown(f"**{cat}**")
        for inst in sorted(items, key=lambda x: _instrument_name(x, ctx)):
            iid   = inst.get("id", "")
            label = _instrument_name(inst, ctx)
            with st.expander(label, expanded=False):
                cur_cat = ctx["extract_prop_text_any"](inst, INSTRUMENT_CATEGORY_KEYS) or "その他"
                cat_idx = INSTRUMENT_CATEGORIES.index(cur_cat) if cur_cat in INSTRUMENT_CATEGORIES else 0
                with st.form(f"inst_edit_{iid}", border=True):
                    name     = st.text_input("楽器名 *", value=label, key=f"ie_name_{iid}")
                    category = st.selectbox("カテゴリ", INSTRUMENT_CATEGORIES,
                                            index=cat_idx, key=f"ie_cat_{iid}")
                    memo     = st.text_area("メモ", value=ctx["extract_prop_text_any"](inst, INSTRUMENT_MEMO_KEYS),
                                            height=60, key=f"ie_memo_{iid}")
                    if st.form_submit_button("💾 更新", use_container_width=True):
                        if not name.strip():
                            st.error("楽器名は必須です。")
                        else:
                            with st.spinner("更新中..."):
                                ok = _update_instrument(ctx, iid, name.strip(), category, memo)
                            if ok:
                                st.success("✅ 更新しました。")
                                st.session_state.pop("instrument_list", None)
                                st.rerun()
                            else:
                                st.error("❌ 更新に失敗しました。")


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
