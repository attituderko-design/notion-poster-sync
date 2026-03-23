"""
concert.pages.concert_mgmt
演奏会・練習情報の登録・一覧・編集画面。
"""
import streamlit as st
from datetime import date, datetime


# ============================================================
# ヘルパー
# ============================================================

def _ss(key, default=None):
    return st.session_state.get(key, default)

def _clear_concert_cache(ctx):
    try:
        from concert.services.notion_client import get_concert_db_property_types
        get_concert_db_property_types.clear()
    except Exception:
        pass
    for k in ["concert_list", "practice_list"]:
        st.session_state.pop(k, None)


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
        st.session_state["concert_list"] = rows
    return st.session_state.get("concert_list", [])


def _load_practices(ctx, concert_id: str = "") -> list[dict]:
    cache_key = f"practice_list_{concert_id}"
    if cache_key not in st.session_state:
        if concert_id:
            rows = ctx["query_all"](
                ctx["CONCERT_DB_PRACTICE"],
                {"filter": {"property": "演奏会", "relation": {"contains": concert_id}}},
            )
        else:
            rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"])
        st.session_state[cache_key] = rows
    return st.session_state.get(cache_key, [])


def _concert_display_name(page: dict, ctx: dict) -> str:
    name = ctx["extract_prop_text"](page, "名称")
    if not name:
        name = ctx["extract_title"](page)
    dt = ctx["extract_prop_text"](page, "日時")
    return f"{name}（{dt[:10] if dt else '日時未設定'}）" if name else page.get("id", "")


def _practice_display_name(page: dict, ctx: dict) -> str:
    name = ctx["extract_prop_text"](page, "練習名")
    if not name:
        name = ctx["extract_title"](page)
    dt = ctx["extract_prop_text"](page, "日時")
    return f"{name}（{dt[:10] if dt else '日時未設定'}）" if name else page.get("id", "")


# ============================================================
# 演奏会 CRUD
# ============================================================

def _create_concert(ctx: dict, name: str, dt_start: str, dt_end: str, venue: str, address: str, memo: str) -> bool:
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
    put_p(props, type_map, "名称", name)
    if dt_start:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props["日時"] = {"date": date_val}
    put_p(props, type_map, "会場名", venue)
    put_p(props, type_map, "会場住所", address)
    put_p(props, type_map, "メモ", memo)

    res = api("post", "https://api.notion.com/v1/pages",
              json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_concert(ctx: dict, page_id: str, name: str, dt_start: str, dt_end: str, venue: str, address: str, memo: str) -> bool:
    api  = ctx["api_request"]
    hdrs = ctx["NOTION_HEADERS"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(ctx["CONCERT_DB_CONCERT"])
    props: dict = {}
    put_p(props, type_map, "名称", name)
    if dt_start:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props["日時"] = {"date": date_val}
    put_p(props, type_map, "会場名", venue)
    put_p(props, type_map, "会場住所", address)
    put_p(props, type_map, "メモ", memo)

    res = api("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 練習 CRUD
# ============================================================

def _create_practice(ctx: dict, name: str, concert_id: str, dt_start: str, dt_end: str,
                     venue: str, address: str, is_concert_day: bool, memo: str) -> bool:
    api   = ctx["api_request"]
    db_id = ctx["CONCERT_DB_PRACTICE"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(db_id)
    if not type_map:
        st.error("練習DBのプロパティ取得に失敗しました。")
        return False

    props: dict = {}
    put_p(props, type_map, "練習名", name)
    if concert_id:
        put_p(props, type_map, "演奏会", concert_id)
    if dt_start:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props["日時"] = {"date": date_val}
    put_p(props, type_map, "会場名", venue)
    put_p(props, type_map, "会場住所", address)
    put_p(props, type_map, "演奏会当日フラグ", is_concert_day)
    put_p(props, type_map, "メモ", memo)

    res = api("post", "https://api.notion.com/v1/pages",
              json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_practice(ctx: dict, page_id: str, name: str, concert_id: str,
                     dt_start: str, dt_end: str, venue: str, address: str,
                     is_concert_day: bool, memo: str) -> bool:
    api   = ctx["api_request"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(ctx["CONCERT_DB_PRACTICE"])
    props: dict = {}
    put_p(props, type_map, "練習名", name)
    if concert_id:
        put_p(props, type_map, "演奏会", concert_id)
    if dt_start:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props["日時"] = {"date": date_val}
    put_p(props, type_map, "会場名", venue)
    put_p(props, type_map, "会場住所", address)
    put_p(props, type_map, "演奏会当日フラグ", is_concert_day)
    put_p(props, type_map, "メモ", memo)

    res = api("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 演奏会フォーム
# ============================================================

def _render_concert_form(ctx: dict, existing: dict | None = None):
    """演奏会の新規登録 / 編集フォーム。existing が None なら新規。"""
    is_edit = existing is not None
    prefix  = f"conc_edit_{existing.get('id','')}_" if is_edit else "conc_new_"
    ext     = ctx["extract_prop_text"]

    with st.form(key=f"{prefix}form", border=True):
        name = st.text_input(
            "演奏会名 *",
            value=ext(existing, "名称") if is_edit else "",
            placeholder="例：第12回定期演奏会",
            key=f"{prefix}name",
        )

        col1, col2 = st.columns(2)
        with col1:
            dt_start_str = ext(existing, "日時") if is_edit else ""
            dt_start_val = date.fromisoformat(dt_start_str[:10]) if dt_start_str else date.today()
            dt_start = st.date_input("開催日 *", value=dt_start_val, key=f"{prefix}dt_start")
        with col2:
            dt_end = st.date_input("終了日（任意）", value=dt_start_val, key=f"{prefix}dt_end")

        venue   = st.text_input("会場名", value=ext(existing, "会場名") if is_edit else "",
                                placeholder="例：○○ホール", key=f"{prefix}venue")
        address = st.text_input("会場住所", value=ext(existing, "会場住所") if is_edit else "",
                                placeholder="任意", key=f"{prefix}address")
        memo    = st.text_area("メモ", value=ext(existing, "メモ") if is_edit else "",
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
                                     venue, address, memo)
            else:
                ok = _create_concert(ctx, name.strip(), dt_s, dt_e, venue, address, memo)

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
    ext     = ctx["extract_prop_text"]
    ext_rel = ctx["extract_relation_ids"]

    # 演奏会セレクタ
    concert_options = {_concert_display_name(c, ctx): c.get("id", "") for c in concerts}
    concert_names   = ["（未選択）"] + list(concert_options.keys())

    current_concert_id = ""
    if is_edit:
        ids = ext_rel(existing, "演奏会")
        current_concert_id = ids[0] if ids else ""
    current_concert_name = next(
        (k for k, v in concert_options.items() if v == current_concert_id), "（未選択）"
    )

    with st.form(key=f"{prefix}form", border=True):
        name = st.text_input(
            "練習名 *",
            value=ext(existing, "練習名") if is_edit else "",
            placeholder="例：第3回練習",
            key=f"{prefix}name",
        )

        selected_concert_name = st.selectbox(
            "演奏会",
            concert_names,
            index=concert_names.index(current_concert_name) if current_concert_name in concert_names else 0,
            key=f"{prefix}concert",
        )

        col1, col2 = st.columns(2)
        with col1:
            dt_start_str = ext(existing, "日時") if is_edit else ""
            dt_start_val = date.fromisoformat(dt_start_str[:10]) if dt_start_str else date.today()
            dt_start = st.date_input("練習日 *", value=dt_start_val, key=f"{prefix}dt_start")
        with col2:
            dt_end = st.date_input("終了日（任意）", value=dt_start_val, key=f"{prefix}dt_end")

        col3, col4 = st.columns(2)
        with col3:
            venue = st.text_input("会場名", value=ext(existing, "会場名") if is_edit else "",
                                  placeholder="例：○○スタジオ", key=f"{prefix}venue")
        with col4:
            address = st.text_input("会場住所", value=ext(existing, "会場住所") if is_edit else "",
                                    placeholder="任意", key=f"{prefix}address")

        is_concert_day = st.checkbox(
            "演奏会当日フラグ（本番日の場合はチェック）",
            value=(ext(existing, "演奏会当日フラグ") == "True") if is_edit else False,
            key=f"{prefix}concert_day",
        )
        memo = st.text_area("メモ", value=ext(existing, "メモ") if is_edit else "",
                            height=80, key=f"{prefix}memo")

        label = "更新" if is_edit else "登録"
        submitted = st.form_submit_button(f"💾 {label}", use_container_width=True, type="primary")

    if submitted:
        if not name.strip():
            st.error("練習名は必須です。")
            return
        concert_id = concert_options.get(selected_concert_name, "")
        dt_s = dt_start.isoformat()
        dt_e = dt_end.isoformat() if dt_end and dt_end != dt_start else dt_s

        with st.spinner(f"{label}中..."):
            if is_edit:
                ok = _update_practice(ctx, existing["id"], name.strip(), concert_id,
                                      dt_s, dt_e, venue, address, is_concert_day, memo)
            else:
                ok = _create_practice(ctx, name.strip(), concert_id,
                                      dt_s, dt_e, venue, address, is_concert_day, memo)

        if ok:
            st.success(f"✅ 練習を{label}しました。")
            _clear_concert_cache(ctx)
            st.rerun()
        else:
            st.error(f"❌ {label}に失敗しました。")


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎼 演奏会・練習管理")

    tab_concert, tab_practice = st.tabs(["演奏会", "練習"])

    # ── 演奏会タブ ────────────────────────────────────────────
    with tab_concert:
        concerts = _load_concerts(ctx)

        with st.expander("➕ 新規演奏会を登録", expanded=(len(concerts) == 0)):
            _render_concert_form(ctx)

        st.divider()

        if not concerts:
            st.info("演奏会がまだ登録されていません。")
        else:
            st.subheader(f"登録済み演奏会（{len(concerts)}件）")
            col_refresh = st.columns([8, 1])[1]
            if col_refresh.button("🔄", key="refresh_concerts", help="一覧を再読み込み"):
                st.session_state.pop("concert_list", None)
                st.rerun()

            for c in concerts:
                label = _concert_display_name(c, ctx)
                with st.expander(label, expanded=False):
                    _render_concert_form(ctx, existing=c)

    # ── 練習タブ ──────────────────────────────────────────────
    with tab_practice:
        concerts = _load_concerts(ctx)

        # 演奏会フィルタ
        concert_filter_opts = {"すべて": ""} | {
            _concert_display_name(c, ctx): c.get("id", "") for c in concerts
        }
        selected_filter = st.selectbox(
            "絞り込み：演奏会",
            list(concert_filter_opts.keys()),
            key="practice_filter_concert",
        )
        filter_concert_id = concert_filter_opts.get(selected_filter, "")

        with st.expander("➕ 新規練習を登録", expanded=False):
            _render_practice_form(ctx, concerts)

        st.divider()

        practices = _load_practices(ctx, filter_concert_id)

        if not practices:
            st.info("練習がまだ登録されていません。")
        else:
            st.subheader(f"登録済み練習（{len(practices)}件）")
            col_refresh = st.columns([8, 1])[1]
            if col_refresh.button("🔄", key="refresh_practices", help="一覧を再読み込み"):
                for k in list(st.session_state.keys()):
                    if k.startswith("practice_list_"):
                        st.session_state.pop(k, None)
                st.rerun()

            # 日付順ソート
            def _prac_date(p):
                d = ctx["extract_prop_text"](p, "日時")
                return d[:10] if d else "9999"

            for p in sorted(practices, key=_prac_date):
                label = _practice_display_name(p, ctx)
                is_concert_day = ctx["extract_prop_text"](p, "演奏会当日フラグ") == "True"
                if is_concert_day:
                    label = "🎼 " + label + "  【本番】"
                with st.expander(label, expanded=False):
                    _render_practice_form(ctx, concerts, existing=p)
