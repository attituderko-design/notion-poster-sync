"""
concert.pages.players
奏者の登録・出欠入力・楽器アサイン画面。
"""
import streamlit as st


# ============================================================
# キャッシュ／ロードヘルパー
# ============================================================

def _clear_player_cache():
    for k in list(st.session_state.keys()):
        if k.startswith(("player_list", "attendance_list_", "pi_list_")):
            st.session_state.pop(k, None)


def _load_players(ctx) -> list[dict]:
    if "player_list" not in st.session_state:
        st.session_state["player_list"] = ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
    return st.session_state.get("player_list", [])


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        st.session_state["concert_list"] = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
    return st.session_state.get("concert_list", [])


def _load_practices(ctx, concert_id: str) -> list[dict]:
    key = f"practice_list_{concert_id}"
    if key not in st.session_state:
        f = {"filter": {"property": "演奏会", "relation": {"contains": concert_id}}} if concert_id else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], f)
    return st.session_state.get(key, [])


def _load_instruments(ctx) -> list[dict]:
    if "instrument_list" not in st.session_state:
        st.session_state["instrument_list"] = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    return st.session_state.get("instrument_list", [])


def _load_attendance(ctx, practice_id: str) -> list[dict]:
    key = f"attendance_list_{practice_id}"
    if key not in st.session_state:
        rows = ctx["query_all"](
            ctx["CONCERT_DB_ATTENDANCE"],
            {"filter": {"property": "練習", "relation": {"contains": practice_id}}},
        )
        st.session_state[key] = rows
    return st.session_state.get(key, [])


def _load_player_instruments(ctx, player_id: str) -> list[dict]:
    key = f"pi_list_{player_id}"
    if key not in st.session_state:
        rows = ctx["query_all"](
            ctx["CONCERT_DB_PLAYER_INSTRUMENT"],
            {"filter": {"property": "奏者", "relation": {"contains": player_id}}},
        )
        st.session_state[key] = rows
    return st.session_state.get(key, [])


def _player_name(p: dict, ctx: dict) -> str:
    return ctx["extract_prop_text"](p, "氏名") or ctx["extract_title"](p) or p.get("id", "")


def _concert_name(c: dict, ctx: dict) -> str:
    n = ctx["extract_prop_text"](c, "名称") or ctx["extract_title"](c)
    dt = ctx["extract_prop_text"](c, "日時")
    return f"{n}（{dt[:10] if dt else '日時未設定'}）"


def _practice_name(p: dict, ctx: dict) -> str:
    n = ctx["extract_prop_text"](p, "練習名") or ctx["extract_title"](p)
    dt = ctx["extract_prop_text"](p, "日時")
    suffix = "【本番】" if ctx["extract_prop_text"](p, "演奏会当日フラグ") == "True" else ""
    return f"{n}（{dt[:10] if dt else ''}）{suffix}"


def _instrument_name(i: dict, ctx: dict) -> str:
    return ctx["extract_prop_text"](i, "楽器名") or ctx["extract_title"](i) or i.get("id", "")


# ============================================================
# 奏者 CRUD
# ============================================================

def _create_player(ctx: dict, name: str, email: str, memo: str) -> bool:
    db_id    = ctx["CONCERT_DB_PLAYER"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("奏者DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    ctx["put_prop"](props, type_map, "氏名", name)
    ctx["put_prop"](props, type_map, "メールアドレス", email)
    ctx["put_prop"](props, type_map, "メモ", memo)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_player(ctx: dict, page_id: str, name: str, email: str, memo: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
    props: dict = {}
    ctx["put_prop"](props, type_map, "氏名", name)
    ctx["put_prop"](props, type_map, "メールアドレス", email)
    ctx["put_prop"](props, type_map, "メモ", memo)
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 出欠 CRUD
# ============================================================

def _upsert_attendance(ctx: dict, player_id: str, player_name: str,
                       practice_id: str, practice_name: str,
                       status: str, note: str,
                       existing_id: str = "") -> bool:
    """出欠レコードの新規作成 or 更新。existing_id があれば PATCH。"""
    db_id    = ctx["CONCERT_DB_ATTENDANCE"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("出欠DBのプロパティ取得に失敗しました。")
        return False

    props: dict = {}
    ctx["put_prop"](props, type_map, "レコード名", f"{player_name} × {practice_name}")
    ctx["put_prop"](props, type_map, "奏者", player_id)
    ctx["put_prop"](props, type_map, "練習", practice_id)
    ctx["put_prop"](props, type_map, "参加可否", status)
    ctx["put_prop"](props, type_map, "備考", note)

    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 楽器アサイン CRUD
# ============================================================

def _upsert_player_instrument(ctx: dict, player_id: str, player_name: str,
                               instrument_id: str, instrument_name: str,
                               is_assign: bool, can_bring: bool, note: str,
                               existing_id: str = "") -> bool:
    db_id    = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("楽器アサインDBのプロパティ取得に失敗しました。")
        return False

    props: dict = {}
    ctx["put_prop"](props, type_map, "レコード名", f"{player_name} × {instrument_name}")
    ctx["put_prop"](props, type_map, "奏者", player_id)
    ctx["put_prop"](props, type_map, "楽器種別", instrument_id)
    ctx["put_prop"](props, type_map, "担当フラグ", is_assign)
    ctx["put_prop"](props, type_map, "持参可フラグ", can_bring)
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
# 奏者タブ
# ============================================================

def _render_player_tab(ctx: dict):
    players = _load_players(ctx)

    with st.expander("➕ 新規奏者を登録", expanded=(len(players) == 0)):
        with st.form("player_new_form", border=True):
            name  = st.text_input("氏名 *", placeholder="例：山田 太郎", key="player_new_name")
            email = st.text_input("メールアドレス", placeholder="任意", key="player_new_email")
            memo  = st.text_area("メモ", height=60, key="player_new_memo")
            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                if not name.strip():
                    st.error("氏名は必須です。")
                else:
                    with st.spinner("登録中..."):
                        ok = _create_player(ctx, name.strip(), email, memo)
                    if ok:
                        st.success("✅ 奏者を登録しました。")
                        _clear_player_cache()
                        st.rerun()
                    else:
                        st.error("❌ 登録に失敗しました。")

    st.divider()

    if not players:
        st.info("奏者がまだ登録されていません。")
        return

    col_h, col_r = st.columns([8, 1])
    col_h.subheader(f"登録済み奏者（{len(players)}件）")
    if col_r.button("🔄", key="refresh_players", help="再読み込み"):
        st.session_state.pop("player_list", None)
        st.rerun()

    for p in sorted(players, key=lambda x: _player_name(x, ctx)):
        name_label = _player_name(p, ctx)
        with st.expander(name_label, expanded=False):
            pid = p.get("id", "")
            ext = ctx["extract_prop_text"]
            with st.form(f"player_edit_{pid}", border=True):
                name  = st.text_input("氏名 *", value=ext(p, "氏名"), key=f"pe_name_{pid}")
                email = st.text_input("メールアドレス", value=ext(p, "メールアドレス"), key=f"pe_email_{pid}")
                memo  = st.text_area("メモ", value=ext(p, "メモ"), height=60, key=f"pe_memo_{pid}")
                if st.form_submit_button("💾 更新", use_container_width=True):
                    if not name.strip():
                        st.error("氏名は必須です。")
                    else:
                        with st.spinner("更新中..."):
                            ok = _update_player(ctx, pid, name.strip(), email, memo)
                        if ok:
                            st.success("✅ 更新しました。")
                            _clear_player_cache()
                            st.rerun()
                        else:
                            st.error("❌ 更新に失敗しました。")


# ============================================================
# 出欠タブ
# ============================================================

def _render_attendance_tab(ctx: dict):
    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("先に演奏会を登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    selected_concert = st.selectbox("演奏会を選択", list(concert_opts.keys()), key="att_concert_sel")
    concert_id = concert_opts.get(selected_concert, "")
    if not concert_id:
        return

    practices = _load_practices(ctx, concert_id)
    if not practices:
        st.info("この演奏会に練習が登録されていません。")
        return

    def _prac_date(p):
        d = ctx["extract_prop_text"](p, "日時")
        return d[:10] if d else "9999"

    practice_opts = {_practice_name(p, ctx): p.get("id", "")
                     for p in sorted(practices, key=_prac_date)}
    selected_practice = st.selectbox("練習日を選択", list(practice_opts.keys()), key="att_practice_sel")
    practice_id = practice_opts.get(selected_practice, "")
    if not practice_id:
        return

    players   = _load_players(ctx)
    if not players:
        st.info("先に奏者を登録してください。")
        return

    # この練習日の出欠レコードを取得し、奏者IDで引けるようにする
    att_rows  = _load_attendance(ctx, practice_id)
    att_by_player: dict[str, dict] = {}
    for row in att_rows:
        pids = ctx["extract_relation_ids"](row, "奏者")
        if pids:
            att_by_player[pids[0]] = row

    st.subheader(f"出欠入力：{selected_practice}")
    st.caption("変更後「保存」を押してください。")

    STATUS_OPTIONS = ["○", "×", "△"]

    with st.form(f"attendance_form_{practice_id}", border=True):
        changes: list[dict] = []
        for pl in sorted(players, key=lambda x: _player_name(x, ctx)):
            pid   = pl.get("id", "")
            pname = _player_name(pl, ctx)
            existing = att_by_player.get(pid)
            current_status = ctx["extract_prop_text"](existing, "参加可否") if existing else "△"
            current_note   = ctx["extract_prop_text"](existing, "備考") if existing else ""
            if current_status not in STATUS_OPTIONS:
                current_status = "△"

            col_name, col_status, col_note = st.columns([3, 2, 5])
            col_name.markdown(f"**{pname}**")
            status = col_status.radio(
                pname,
                STATUS_OPTIONS,
                index=STATUS_OPTIONS.index(current_status),
                horizontal=True,
                label_visibility="collapsed",
                key=f"att_status_{practice_id}_{pid}",
            )
            note = col_note.text_input(
                "備考",
                value=current_note,
                placeholder="遅刻・早退等",
                label_visibility="collapsed",
                key=f"att_note_{practice_id}_{pid}",
            )
            changes.append({
                "player_id":   pid,
                "player_name": pname,
                "status":      status,
                "note":        note,
                "existing_id": existing.get("id", "") if existing else "",
            })

        submitted = st.form_submit_button("💾 出欠を保存", use_container_width=True, type="primary")

    if submitted:
        practice_label = selected_practice
        success, fail = 0, 0
        with st.spinner("保存中..."):
            for ch in changes:
                ok = _upsert_attendance(
                    ctx,
                    player_id=ch["player_id"],
                    player_name=ch["player_name"],
                    practice_id=practice_id,
                    practice_name=practice_label,
                    status=ch["status"],
                    note=ch["note"],
                    existing_id=ch["existing_id"],
                )
                if ok:
                    success += 1
                else:
                    fail += 1

        if fail == 0:
            st.success(f"✅ {success}件の出欠を保存しました。")
            st.session_state.pop(f"attendance_list_{practice_id}", None)
            st.rerun()
        else:
            st.warning(f"⚠️ {success}件成功、{fail}件失敗しました。")
            st.session_state.pop(f"attendance_list_{practice_id}", None)


# ============================================================
# 楽器アサインタブ
# ============================================================

def _render_assign_tab(ctx: dict):
    players     = _load_players(ctx)
    instruments = _load_instruments(ctx)

    if not players:
        st.info("先に奏者を登録してください。")
        return
    if not instruments:
        st.info("先に楽器種別を登録してください（楽曲・楽器管理 画面）。")
        return

    inst_opts = {_instrument_name(i, ctx): i.get("id", "") for i in
                 sorted(instruments, key=lambda x: _instrument_name(x, ctx))}

    player_opts = {_player_name(p, ctx): p.get("id", "") for p in
                   sorted(players, key=lambda x: _player_name(x, ctx))}

    selected_player_name = st.selectbox("奏者を選択", list(player_opts.keys()), key="assign_player_sel")
    player_id = player_opts.get(selected_player_name, "")
    if not player_id:
        return

    pi_rows = _load_player_instruments(ctx, player_id)
    # 楽器IDで既存レコードを引けるようにする
    pi_by_inst: dict[str, dict] = {}
    for row in pi_rows:
        iids = ctx["extract_relation_ids"](row, "楽器種別")
        if iids:
            pi_by_inst[iids[0]] = row

    st.subheader(f"楽器アサイン：{selected_player_name}")
    st.caption("担当フラグ＝この奏者がその楽器パートを担当　持参可フラグ＝実物を持参できる")

    with st.form(f"assign_form_{player_id}", border=True):
        changes: list[dict] = []
        for inst_name, inst_id in inst_opts.items():
            existing = pi_by_inst.get(inst_id)
            cur_assign = (ctx["extract_prop_text"](existing, "担当フラグ") == "True") if existing else False
            cur_bring  = (ctx["extract_prop_text"](existing, "持参可フラグ") == "True") if existing else False
            cur_note   = ctx["extract_prop_text"](existing, "備考") if existing else ""

            col_inst, col_asgn, col_bring, col_note = st.columns([3, 1, 1, 4])
            col_inst.markdown(f"**{inst_name}**")
            is_assign = col_asgn.checkbox("担当", value=cur_assign,
                                          key=f"asgn_assign_{player_id}_{inst_id}")
            can_bring = col_bring.checkbox("持参可", value=cur_bring,
                                           key=f"asgn_bring_{player_id}_{inst_id}")
            note = col_note.text_input("備考", value=cur_note, placeholder="マレット等",
                                       label_visibility="collapsed",
                                       key=f"asgn_note_{player_id}_{inst_id}")
            changes.append({
                "inst_id":     inst_id,
                "inst_name":   inst_name,
                "is_assign":   is_assign,
                "can_bring":   can_bring,
                "note":        note,
                "existing_id": existing.get("id", "") if existing else "",
            })

        submitted = st.form_submit_button("💾 アサインを保存", use_container_width=True, type="primary")

    if submitted:
        success, fail = 0, 0
        with st.spinner("保存中..."):
            for ch in changes:
                # 担当も持参も false で既存レコードなし → スキップ（空レコード作らない）
                if not ch["is_assign"] and not ch["can_bring"] and not ch["existing_id"]:
                    continue
                ok = _upsert_player_instrument(
                    ctx,
                    player_id=player_id,
                    player_name=selected_player_name,
                    instrument_id=ch["inst_id"],
                    instrument_name=ch["inst_name"],
                    is_assign=ch["is_assign"],
                    can_bring=ch["can_bring"],
                    note=ch["note"],
                    existing_id=ch["existing_id"],
                )
                if ok:
                    success += 1
                else:
                    fail += 1

        if fail == 0:
            st.success(f"✅ {success}件を保存しました。")
            st.session_state.pop(f"pi_list_{player_id}", None)
            st.rerun()
        else:
            st.warning(f"⚠️ {success}件成功、{fail}件失敗しました。")
            st.session_state.pop(f"pi_list_{player_id}", None)


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎻 奏者・出欠・アサイン")

    tab_player, tab_attendance, tab_assign = st.tabs(["奏者管理", "出欠入力", "楽器アサイン"])

    with tab_player:
        _render_player_tab(ctx)

    with tab_attendance:
        _render_attendance_tab(ctx)

    with tab_assign:
        _render_assign_tab(ctx)
