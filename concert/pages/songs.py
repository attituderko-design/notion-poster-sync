"""
concert.pages.songs
楽曲・楽器種別の登録、曲別必要楽器（SongInstrument）の設定画面。
"""
import streamlit as st


# ============================================================
# キャッシュ／ロードヘルパー
# ============================================================

def _clear_song_cache():
    for k in list(st.session_state.keys()):
        if k.startswith(("song_list", "instrument_list", "si_list_")):
            st.session_state.pop(k, None)


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        st.session_state["concert_list"] = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
    return st.session_state.get("concert_list", [])


def _load_songs(ctx, concert_id: str = "") -> list[dict]:
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        f = {"filter": {"property": "演奏会", "relation": {"contains": concert_id}}} if concert_id else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_SONG"], f)
    return st.session_state.get(key, [])


def _load_instruments(ctx) -> list[dict]:
    if "instrument_list" not in st.session_state:
        st.session_state["instrument_list"] = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    return st.session_state.get("instrument_list", [])


def _load_song_instruments(ctx, song_id: str) -> list[dict]:
    key = f"si_list_{song_id}"
    if key not in st.session_state:
        rows = ctx["query_all"](
            ctx["CONCERT_DB_SONG_INSTRUMENT"],
            {"filter": {"property": "楽曲", "relation": {"contains": song_id}}},
        )
        st.session_state[key] = rows
    return st.session_state.get(key, [])


def _concert_name(c: dict, ctx: dict) -> str:
    n  = ctx["extract_prop_text"](c, "名称") or ctx["extract_title"](c)
    dt = ctx["extract_prop_text"](c, "日時")
    return f"{n}（{dt[:10] if dt else '日時未設定'}）"


def _song_name(s: dict, ctx: dict) -> str:
    return ctx["extract_prop_text"](s, "曲名") or ctx["extract_title"](s) or s.get("id", "")


def _instrument_name(i: dict, ctx: dict) -> str:
    return ctx["extract_prop_text"](i, "楽器名") or ctx["extract_title"](i) or i.get("id", "")


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
    ctx["put_prop"](props, type_map, "曲名", title)
    if concert_ids:
        ctx["put_prop"](props, type_map, "演奏会", concert_ids)
    ctx["put_prop"](props, type_map, "作曲者", composer)
    if duration_sec is not None:
        ctx["put_prop"](props, type_map, "演奏時間（秒）", duration_sec)
    ctx["put_prop"](props, type_map, "難易度メモ", note)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_song(ctx: dict, page_id: str, title: str, concert_ids: list[str],
                 composer: str, duration_sec: int | None, note: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
    props: dict = {}
    ctx["put_prop"](props, type_map, "曲名", title)
    if concert_ids:
        ctx["put_prop"](props, type_map, "演奏会", concert_ids)
    ctx["put_prop"](props, type_map, "作曲者", composer)
    if duration_sec is not None:
        ctx["put_prop"](props, type_map, "演奏時間（秒）", duration_sec)
    ctx["put_prop"](props, type_map, "難易度メモ", note)
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 楽器種別 CRUD
# ============================================================

INSTRUMENT_CATEGORIES = ["鍵盤打楽器", "膜鳴", "金属打楽器", "小物打楽器", "その他"]


def _create_instrument(ctx: dict, name: str, category: str, memo: str) -> bool:
    db_id    = ctx["CONCERT_DB_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("楽器種別DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    ctx["put_prop"](props, type_map, "楽器名", name)
    ctx["put_prop"](props, type_map, "カテゴリ", category)
    ctx["put_prop"](props, type_map, "メモ", memo)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_instrument(ctx: dict, page_id: str, name: str, category: str, memo: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_INSTRUMENT"])
    props: dict = {}
    ctx["put_prop"](props, type_map, "楽器名", name)
    ctx["put_prop"](props, type_map, "カテゴリ", category)
    ctx["put_prop"](props, type_map, "メモ", memo)
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
    ctx["put_prop"](props, type_map, "レコード名", f"{song_name} × {instrument_name}")
    ctx["put_prop"](props, type_map, "楽曲", song_id)
    ctx["put_prop"](props, type_map, "楽器種別", instrument_id)
    ctx["put_prop"](props, type_map, "必要台数", qty)
    ctx["put_prop"](props, type_map, "備考", note)
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
    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}

    # 絞り込み
    filter_opts = {"すべて": ""} | concert_opts
    selected_filter = st.selectbox("絞り込み：演奏会", list(filter_opts.keys()), key="song_filter")
    filter_concert_id = filter_opts.get(selected_filter, "")

    songs = _load_songs(ctx, filter_concert_id)

    with st.expander("➕ 新規楽曲を登録", expanded=(len(songs) == 0)):
        with st.form("song_new_form", border=True):
            title    = st.text_input("曲名 *", placeholder="例：マリンバ協奏曲", key="sn_title")
            composer = st.text_input("作曲者", placeholder="例：安倍圭子", key="sn_composer")

            duration_str = st.text_input(
                "演奏時間", placeholder="例：5:30（分:秒）または 330（秒）", key="sn_duration"
            )

            concert_sel = st.multiselect(
                "紐づける演奏会",
                list(concert_opts.keys()),
                key="sn_concerts",
            )
            note = st.text_area("難易度メモ", height=60, key="sn_note")

            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                if not title.strip():
                    st.error("曲名は必須です。")
                else:
                    duration_sec = _mmss_to_sec(duration_str)
                    concert_ids  = [concert_opts[n] for n in concert_sel if concert_opts.get(n)]
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

    for s in sorted(songs, key=lambda x: _song_name(x, ctx)):
        song_id    = s.get("id", "")
        song_label = _song_name(s, ctx)
        composer   = ctx["extract_prop_text"](s, "作曲者")
        dur_sec_str = ctx["extract_prop_text"](s, "演奏時間（秒）")
        dur_disp   = _sec_to_mmss(int(float(dur_sec_str)) if dur_sec_str else None)
        caption    = f"{composer}　{dur_disp}" if composer or dur_disp else ""

        with st.expander(f"{song_label}　{f'*{caption}*' if caption else ''}", expanded=False):
            # 既存紐づき演奏会
            existing_concert_ids = ctx["extract_relation_ids"](s, "演奏会")
            existing_concert_names = [k for k, v in concert_opts.items() if v in existing_concert_ids]

            with st.form(f"song_edit_{song_id}", border=True):
                title    = st.text_input("曲名 *", value=_song_name(s, ctx), key=f"se_title_{song_id}")
                composer = st.text_input("作曲者", value=ctx["extract_prop_text"](s, "作曲者"),
                                         key=f"se_composer_{song_id}")
                dur_str  = st.text_input(
                    "演奏時間",
                    value=_sec_to_mmss(int(float(dur_sec_str)) if dur_sec_str else None),
                    placeholder="例：5:30",
                    key=f"se_duration_{song_id}",
                )
                concert_sel = st.multiselect(
                    "紐づける演奏会",
                    list(concert_opts.keys()),
                    default=existing_concert_names,
                    key=f"se_concerts_{song_id}",
                )
                note = st.text_area("難易度メモ",
                                    value=ctx["extract_prop_text"](s, "難易度メモ"),
                                    height=60, key=f"se_note_{song_id}")

                if st.form_submit_button("💾 更新", use_container_width=True):
                    if not title.strip():
                        st.error("曲名は必須です。")
                    else:
                        duration_sec = _mmss_to_sec(dur_str)
                        concert_ids  = [concert_opts[n] for n in concert_sel if concert_opts.get(n)]
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


def _render_song_instrument_section(ctx: dict, song_id: str, song_label: str):
    """曲の下に展開する必要楽器設定UI。"""
    instruments = _load_instruments(ctx)
    if not instruments:
        st.info("楽器種別を先に登録してください。")
        return

    si_rows = _load_song_instruments(ctx, song_id)
    si_by_inst: dict[str, dict] = {}
    for row in si_rows:
        iids = ctx["extract_relation_ids"](row, "楽器種別")
        if iids:
            si_by_inst[iids[0]] = row

    inst_opts = {_instrument_name(i, ctx): i.get("id", "")
                 for i in sorted(instruments, key=lambda x: _instrument_name(x, ctx))}

    with st.form(f"si_form_{song_id}", border=True):
        changes: list[dict] = []
        for inst_name, inst_id in inst_opts.items():
            existing = si_by_inst.get(inst_id)
            cur_qty  = int(float(ctx["extract_prop_text"](existing, "必要台数") or "0")) if existing else 0
            cur_note = ctx["extract_prop_text"](existing, "備考") if existing else ""

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

    # カテゴリごとにグループ表示
    by_cat: dict[str, list] = {c: [] for c in INSTRUMENT_CATEGORIES}
    for i in instruments:
        cat = ctx["extract_prop_text"](i, "カテゴリ") or "その他"
        by_cat.setdefault(cat, []).append(i)

    for cat in INSTRUMENT_CATEGORIES:
        items = by_cat.get(cat, [])
        if not items:
            continue
        st.markdown(f"**{cat}**")
        for inst in sorted(items, key=lambda x: _instrument_name(x, ctx)):
            iid   = inst.get("id", "")
            label = _instrument_name(inst, ctx)
            with st.expander(label, expanded=False):
                cur_cat = ctx["extract_prop_text"](inst, "カテゴリ") or "その他"
                cat_idx = INSTRUMENT_CATEGORIES.index(cur_cat) if cur_cat in INSTRUMENT_CATEGORIES else 0
                with st.form(f"inst_edit_{iid}", border=True):
                    name     = st.text_input("楽器名 *", value=label, key=f"ie_name_{iid}")
                    category = st.selectbox("カテゴリ", INSTRUMENT_CATEGORIES,
                                            index=cat_idx, key=f"ie_cat_{iid}")
                    memo     = st.text_area("メモ", value=ctx["extract_prop_text"](inst, "メモ"),
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

    tab_song, tab_instrument = st.tabs(["楽曲", "楽器種別"])

    with tab_song:
        _render_song_tab(ctx)

    with tab_instrument:
        _render_instrument_tab(ctx)
