"""
concert.pages.players
奏者の登録・出欠入力・楽器アサイン画面。
既存ArtéMis DBのプロパティ名ゆれに対応。
"""
import streamlit as st

PLAYER_NAME_KEYS = ["氏名", "名前", "表示名", "タイトル"]
PLAYER_EMAIL_KEYS = ["メールアドレス", "Email", "email"]
PLAYER_MEMO_KEYS = ["メモ", "備考"]

CONCERT_NAME_KEYS = ["名称", "タイトル", "演奏会名", "PK名称"]
CONCERT_DATE_KEYS = ["日時", "日付", "出演日", "体験日", "リリース日"]

PRACTICE_NAME_KEYS = ["練習名", "タイトル", "PK練習名"]
PRACTICE_DATE_KEYS = ["日時", "日付"]
PRACTICE_CONCERT_DAY_KEYS = ["演奏会当日フラグ", "本番フラグ"]
PRACTICE_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]

INSTRUMENT_NAME_KEYS = ["楽器名", "タイトル", "PK楽器名"]

ATT_RECORD_KEYS = ["レコード名", "タイトル"]
ATT_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者"]
ATT_PRACTICE_REL_KEYS = ["練習", "演奏会", "出演", "FK練習"]
ATT_STATUS_KEYS = ["参加可否", "出欠", "参加状況"]
ATT_NOTE_KEYS = ["備考", "メモ"]

PARTICIPANT_RECORD_KEYS = ["レコード名", "タイトル", "名称"]
PARTICIPANT_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者", "演奏会参加者"]
PARTICIPANT_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
PARTICIPANT_INST_KEYS = ["担当楽器", "楽器", "楽器種別"]
PARTICIPANT_NOTE_KEYS = ["備考", "メモ"]

PI_RECORD_KEYS = ["レコード名", "タイトル"]
PI_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者"]
PI_INST_REL_KEYS = ["楽器種別", "楽器", "担当楽器", "FK楽器種別"]
PI_ASSIGN_KEYS = ["担当フラグ", "担当", "担当有無"]
PI_BRING_KEYS = ["持参可フラグ", "持参可", "持参"]
PI_NOTE_KEYS = ["備考", "メモ"]


def _first_prop_by_type(type_map: dict, ptype: str) -> str:
    for k, t in (type_map or {}).items():
        if t == ptype:
            return k
    return ""


def _find_relation_prop(type_map: dict, candidates: list[str], keywords: list[str], exclude: set[str] | None = None) -> str:
    exclude = exclude or set()
    found = [ctx_k for ctx_k in candidates if ctx_k in (type_map or {}) and (type_map or {}).get(ctx_k) == "relation"]
    for k in found:
        if k not in exclude:
            return k
    for k, t in (type_map or {}).items():
        if t != "relation" or k in exclude:
            continue
        ks = str(k).lower()
        if any(kw.lower() in ks for kw in keywords):
            return k
    for k, t in (type_map or {}).items():
        if t == "relation" and k not in exclude:
            return k
    return ""


def _response_error_message(res) -> str:
    if res is None:
        return "API応答なし（None）"
    try:
        js = res.json() or {}
        msg = js.get("message") or js.get("code") or ""
        if msg:
            return f"HTTP {res.status_code}: {msg}"
    except Exception:
        pass
    txt = (res.text or "").strip()
    if txt:
        return f"HTTP {res.status_code}: {txt[:200]}"
    return f"HTTP {res.status_code}"


def _clear_player_cache():
    for k in list(st.session_state.keys()):
        if k.startswith(("player_list", "attendance_list_", "pi_list_", "participant_list_", "practice_list_")):
            st.session_state.pop(k, None)


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


def _load_players(ctx) -> list[dict]:
    if "player_list" not in st.session_state:
        st.session_state["player_list"] = ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
    return st.session_state.get("player_list", [])


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        st.session_state["concert_list"] = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
    return st.session_state.get("concert_list", [])


def _load_practices(ctx, concert_id: str) -> list[dict]:
    rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"])
    if not concert_id:
        return rows

    t = ctx["get_prop_types"](ctx["CONCERT_DB_PRACTICE"])
    rel_props = _practice_rel_prop_candidates(t, ctx)
    if not rel_props:
        return rows

    target = _normalize_page_id(concert_id)
    out = []
    for r in rows:
        hit = False
        for rp in rel_props:
            ids = ctx["extract_relation_ids"](r, rp)
            if any(_normalize_page_id(x) == target for x in ids):
                hit = True
                break
        if hit:
            out.append(r)
    return out


def _load_instruments(ctx) -> list[dict]:
    if "instrument_list" not in st.session_state:
        st.session_state["instrument_list"] = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    return st.session_state.get("instrument_list", [])


def _load_attendance(ctx, practice_id: str) -> list[dict]:
    key = f"attendance_list_{practice_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_ATTENDANCE"])
        rel = ctx["find_prop_name"](t, ATT_PRACTICE_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": practice_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], f)
    return st.session_state.get(key, [])


def _load_participants(ctx, concert_id: str) -> list[dict]:
    key = f"participant_list_{concert_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_PARTICIPANT"])
        rel = ctx["find_prop_name"](t, PARTICIPANT_CONCERT_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], f)
    return st.session_state.get(key, [])


def _load_player_instruments(ctx, player_id: str) -> list[dict]:
    key = f"pi_list_{player_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"])
        rel = ctx["find_prop_name"](t, PI_PLAYER_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": player_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], f)
    return st.session_state.get(key, [])


def _player_name(p: dict, ctx: dict) -> str:
    return ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or ctx["extract_title"](p) or p.get("id", "")


def _concert_name(c: dict, ctx: dict) -> str:
    n = ctx["extract_prop_text_any"](c, CONCERT_NAME_KEYS) or ctx["extract_title"](c)
    d = ctx["extract_prop_text_any"](c, CONCERT_DATE_KEYS)
    return f"{n}（{d[:10] if d else '日時未設定'}）"


def _practice_name(p: dict, ctx: dict) -> str:
    n = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ctx["extract_title"](p)
    d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
    suffix = "【本番】" if ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS) == "True" else ""
    return f"{n}（{d[:10] if d else ''}）{suffix}"


def _instrument_name(i: dict, ctx: dict) -> str:
    return ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ctx["extract_title"](i) or i.get("id", "")


def _create_player(ctx: dict, name: str, email: str, memo: str) -> bool:
    db_id = ctx["CONCERT_DB_PLAYER"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        st.error("奏者DBのプロパティ取得に失敗しました。")
        return False
    props = {}
    ctx["put_prop_any"](props, t, PLAYER_NAME_KEYS, name)
    ctx["put_prop_any"](props, t, PLAYER_EMAIL_KEYS, email)
    ctx["put_prop_any"](props, t, PLAYER_MEMO_KEYS, memo)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_player(ctx: dict, page_id: str, name: str, email: str, memo: str) -> bool:
    t = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
    props = {}
    ctx["put_prop_any"](props, t, PLAYER_NAME_KEYS, name)
    ctx["put_prop_any"](props, t, PLAYER_EMAIL_KEYS, email)
    ctx["put_prop_any"](props, t, PLAYER_MEMO_KEYS, memo)
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


def _upsert_attendance(ctx: dict, player_id: str, player_name: str, practice_id: str, practice_name: str, status: str, note: str, existing_id: str = "") -> bool:
    db_id = ctx["CONCERT_DB_ATTENDANCE"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        st.error("出欠DBのプロパティ取得に失敗しました。")
        return False

    record_key = ctx["find_prop_name"](t, ATT_RECORD_KEYS) or _first_prop_by_type(t, "title")
    practice_rel_key = _find_relation_prop(t, ATT_PRACTICE_REL_KEYS, ["練習", "practice", "fk"])
    player_rel_key = _find_relation_prop(t, ATT_PLAYER_REL_KEYS, ["奏者", "出演者", "player", "participant"], exclude={practice_rel_key} if practice_rel_key else set())
    status_key = ctx["find_prop_name"](t, ATT_STATUS_KEYS)
    if not status_key:
        for k, typ in (t or {}).items():
            if typ == "select" and ("出欠" in str(k) or "参加" in str(k)):
                status_key = k
                break
        if not status_key:
            status_key = _first_prop_by_type(t, "select")
    note_key = ctx["find_prop_name"](t, ATT_NOTE_KEYS) or _first_prop_by_type(t, "rich_text")

    if not practice_rel_key or not player_rel_key:
        st.error(
            "出欠DBのrelation列を特定できません。"
            f" DB={db_id} / player_rel={player_rel_key or '未検出'} / practice_rel={practice_rel_key or '未検出'}"
        )
        return False

    props = {}
    if record_key:
        ctx["put_prop"](props, t, record_key, f"{player_name} × {practice_name}")
    ctx["put_prop"](props, t, player_rel_key, player_id)
    ctx["put_prop"](props, t, practice_rel_key, practice_id)
    if status_key:
        ctx["put_prop"](props, t, status_key, status)
    if note_key:
        ctx["put_prop"](props, t, note_key, note)

    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}", json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    ok = res is not None and res.status_code == 200
    if not ok:
        st.error(f"出欠保存に失敗: {_response_error_message(res)} / DB={db_id}")
    return ok


def _upsert_participant(
    ctx: dict,
    concert_id: str,
    concert_name: str,
    player_id: str,
    player_name: str,
    existing_id: str = "",
) -> bool:
    db_id = ctx["CONCERT_DB_PARTICIPANT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        st.error("演奏会参加者DBのプロパティ取得に失敗しました。")
        return False
    props = {}
    ctx["put_prop_any"](props, t, PARTICIPANT_RECORD_KEYS, f"{player_name} × {concert_name}")
    ctx["put_prop_any"](props, t, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PARTICIPANT_PLAYER_REL_KEYS, player_id)
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}", json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _archive_participant(ctx: dict, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"archived": True})
    return res is not None and res.status_code == 200


def _upsert_player_instrument(ctx: dict, player_id: str, player_name: str, instrument_id: str, instrument_name: str, is_assign: bool, can_bring: bool, note: str, existing_id: str = "") -> bool:
    db_id = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        st.error("楽器アサインDBのプロパティ取得に失敗しました。")
        return False
    props = {}
    ctx["put_prop_any"](props, t, PI_RECORD_KEYS, f"{player_name} × {instrument_name}")
    ctx["put_prop_any"](props, t, PI_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, PI_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, t, PI_ASSIGN_KEYS, is_assign)
    ctx["put_prop_any"](props, t, PI_BRING_KEYS, can_bring)
    ctx["put_prop_any"](props, t, PI_NOTE_KEYS, note)
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}", json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _render_player_tab(ctx: dict):
    players = _load_players(ctx)
    with st.expander("➕ 新規奏者を登録", expanded=(len(players) == 0)):
        with st.form("player_new_form", border=True):
            name = st.text_input("氏名 *", placeholder="例：山田 太郎")
            email = st.text_input("メールアドレス", placeholder="任意")
            memo = st.text_area("メモ", height=60)
            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                if not name.strip():
                    st.error("氏名は必須です。")
                elif _create_player(ctx, name.strip(), email, memo):
                    st.success("✅ 奏者を登録しました。")
                    _clear_player_cache()
                    st.rerun()
                else:
                    st.error("❌ 登録に失敗しました。")
    st.divider()
    if not players:
        st.info("奏者がまだ登録されていません。")
        return
    st.subheader(f"登録済み奏者（{len(players)}件）")
    for p in sorted(players, key=lambda x: _player_name(x, ctx)):
        pid = p.get("id", "")
        with st.expander(_player_name(p, ctx), expanded=False):
            with st.form(f"player_edit_{pid}", border=True):
                name = st.text_input("氏名 *", value=ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS))
                email = st.text_input("メールアドレス", value=ctx["extract_prop_text_any"](p, PLAYER_EMAIL_KEYS))
                memo = st.text_area("メモ", value=ctx["extract_prop_text_any"](p, PLAYER_MEMO_KEYS), height=60)
                if st.form_submit_button("💾 更新", use_container_width=True):
                    if not name.strip():
                        st.error("氏名は必須です。")
                    elif _update_player(ctx, pid, name.strip(), email, memo):
                        st.success("✅ 更新しました。")
                        _clear_player_cache()
                        st.rerun()
                    else:
                        st.error("❌ 更新に失敗しました。")


def _render_attendance_tab(ctx: dict):
    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("先に演奏会を登録してください。")
        return
    all_c_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    c_query = st.text_input(
        "演奏会を検索",
        value=st.session_state.get("players_concert_search", ""),
        key="players_concert_search",
        placeholder="例: 2026 / 定期 / Happy Hour / Osaka",
    ).strip().lower()
    if c_query:
        c_opts = {k: v for k, v in all_c_opts.items() if c_query in k.lower()}
    else:
        c_opts = all_c_opts
    if not c_opts:
        st.warning("検索条件に一致する演奏会がありません。絞り込みを緩めてください。")
        return
    c_name = st.selectbox("演奏会を選択", list(c_opts.keys()), key="att_concert_sel")
    c_id = c_opts.get(c_name, "")
    if not c_id:
        return
    with st.expander("🔍 出欠DBデバッグ", expanded=False):
        st.caption(
            f"CONCERT_DB_CONCERT: `{ctx['CONCERT_DB_CONCERT']}` / "
            f"CONCERT_DB_PRACTICE: `{ctx['CONCERT_DB_PRACTICE']}` / "
            f"CONCERT_DB_ATTENDANCE: `{ctx['CONCERT_DB_ATTENDANCE']}`"
        )
        t_att = ctx["get_prop_types"](ctx["CONCERT_DB_ATTENDANCE"])
        rels = [k for k, v in (t_att or {}).items() if v == "relation"]
        st.caption(f"出欠DB relation候補: {', '.join(rels) if rels else '(なし)'}")
    practices = _load_practices(ctx, c_id)
    if not practices:
        st.info("この演奏会に練習が登録されていません。")
        with st.expander("🔍 練習読込デバッグ", expanded=False):
            all_rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"])
            st.caption(
                f"CONCERT_DB_CONCERT: `{ctx['CONCERT_DB_CONCERT']}` / "
                f"CONCERT_DB_PRACTICE: `{ctx['CONCERT_DB_PRACTICE']}`"
            )
            st.caption(f"選択演奏会ID: `{c_id or '(未選択)'}`")
            st.caption(f"練習DB全件数: {len(all_rows)}")
            if all_rows:
                t = ctx["get_prop_types"](ctx["CONCERT_DB_PRACTICE"])
                rel_props = _practice_rel_prop_candidates(t, ctx)
                st.caption(f"練習DB relation候補: {', '.join(rel_props) if rel_props else '(なし)'}")
                sample = all_rows[0]
                st.caption(f"サンプル練習ID: `{sample.get('id', '')}`")
                if rel_props:
                    rel_dump = {rp: ctx["extract_relation_ids"](sample, rp) for rp in rel_props}
                    st.json(rel_dump)
        return

    all_players = _load_players(ctx)
    player_name_map = {p.get("id", ""): _player_name(p, ctx) for p in all_players}

    st.markdown("### 演奏会参加者")
    participants = _load_participants(ctx, c_id)
    part_player_ids = []
    part_row_by_pid = {}
    for row in participants:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if not pids:
            continue
        pid = pids[0]
        part_player_ids.append(pid)
        part_row_by_pid[pid] = row
    part_player_ids = sorted(set(part_player_ids), key=lambda x: player_name_map.get(x, x))

    if all_players:
        selectable = { _player_name(p, ctx): p.get("id", "") for p in sorted(all_players, key=lambda x: _player_name(x, ctx)) if p.get("id") }
        default_names = [name for name, pid in selectable.items() if pid in part_player_ids]
        with st.form(f"participant_form_{c_id}", border=True):
            sel_names = st.multiselect(
                "この演奏会の参加者を選択",
                list(selectable.keys()),
                default=default_names,
                key=f"participant_select_{c_id}",
            )
            remove_unselected = st.checkbox("未選択の既存参加者をアーカイブ", value=False, key=f"participant_remove_{c_id}")
            if st.form_submit_button("💾 参加者を保存", type="primary", use_container_width=True):
                selected_ids = {selectable[n] for n in sel_names if selectable.get(n)}
                ok_n, ng_n = 0, 0
                for pid in selected_ids:
                    pname = player_name_map.get(pid, pid)
                    ex = part_row_by_pid.get(pid)
                    ok = _upsert_participant(ctx, c_id, c_name, pid, pname, ex.get("id", "") if ex else "")
                    ok_n += 1 if ok else 0
                    ng_n += 0 if ok else 1
                if remove_unselected:
                    for pid, row in part_row_by_pid.items():
                        if pid in selected_ids:
                            continue
                        ok = _archive_participant(ctx, row.get("id", ""))
                        ok_n += 1 if ok else 0
                        ng_n += 0 if ok else 1
                if ng_n == 0:
                    st.success(f"✅ 参加者更新完了: {ok_n}件")
                else:
                    st.warning(f"⚠️ 参加者更新: 成功 {ok_n} / 失敗 {ng_n}")
                st.session_state.pop(f"participant_list_{c_id}", None)
                st.rerun()

    def _d(p):
        x = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
        return x[:10] if x else "9999"

    p_opts = {_practice_name(p, ctx): p.get("id", "") for p in sorted(practices, key=_d)}
    p_name = st.selectbox("練習日を選択", list(p_opts.keys()), key="att_practice_sel")
    p_id = p_opts.get(p_name, "")
    if not p_id:
        return

    if not part_player_ids:
        st.info("先にこの演奏会の参加者を登録してください。")
        return
    players = [p for p in all_players if p.get("id", "") in set(part_player_ids)]
    att = _load_attendance(ctx, p_id)
    by_player = {}
    for row in att:
        pids = ctx["extract_relation_ids_any"](row, ATT_PLAYER_REL_KEYS)
        if pids:
            by_player[pids[0]] = row

    statuses = ["○", "×", "△"]
    with st.form(f"attendance_form_{p_id}", border=True):
        changes = []
        for pl in sorted(players, key=lambda x: _player_name(x, ctx)):
            pid = pl.get("id", "")
            pname = _player_name(pl, ctx)
            ex = by_player.get(pid)
            cur_s = ctx["extract_prop_text_any"](ex, ATT_STATUS_KEYS) if ex else "△"
            cur_n = ctx["extract_prop_text_any"](ex, ATT_NOTE_KEYS) if ex else ""
            if cur_s not in statuses:
                cur_s = "△"
            c1, c2, c3 = st.columns([3, 2, 5])
            c1.markdown(f"**{pname}**")
            s = c2.radio(pname, statuses, index=statuses.index(cur_s), horizontal=True, label_visibility="collapsed", key=f"att_{p_id}_{pid}")
            n = c3.text_input("備考", value=cur_n, label_visibility="collapsed", key=f"att_note_{p_id}_{pid}")
            changes.append({"player_id": pid, "player_name": pname, "status": s, "note": n, "existing_id": ex.get("id", "") if ex else ""})
        if st.form_submit_button("💾 出欠を保存", use_container_width=True, type="primary"):
            ok_n, ng_n = 0, 0
            for ch in changes:
                ok = _upsert_attendance(ctx, ch["player_id"], ch["player_name"], p_id, p_name, ch["status"], ch["note"], ch["existing_id"])
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件の出欠を保存しました。")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗しました。")
            st.session_state.pop(f"attendance_list_{p_id}", None)
            st.rerun()


def _render_assign_tab(ctx: dict):
    players = _load_players(ctx)
    insts = _load_instruments(ctx)
    if not players:
        st.info("先に奏者を登録してください。")
        return
    if not insts:
        st.info("先に楽器種別を登録してください（楽曲・楽器管理 画面）。")
        return
    i_opts = {_instrument_name(i, ctx): i.get("id", "") for i in sorted(insts, key=lambda x: _instrument_name(x, ctx))}
    p_opts = {_player_name(p, ctx): p.get("id", "") for p in sorted(players, key=lambda x: _player_name(x, ctx))}
    p_name = st.selectbox("奏者を選択", list(p_opts.keys()), key="assign_player_sel")
    p_id = p_opts.get(p_name, "")
    if not p_id:
        return
    rows = _load_player_instruments(ctx, p_id)
    by_inst = {}
    for row in rows:
        iids = ctx["extract_relation_ids_any"](row, PI_INST_REL_KEYS)
        if iids:
            by_inst[iids[0]] = row

    with st.form(f"assign_form_{p_id}", border=True):
        changes = []
        for iname, iid in i_opts.items():
            ex = by_inst.get(iid)
            cur_a = (ctx["extract_prop_text_any"](ex, PI_ASSIGN_KEYS) == "True") if ex else False
            cur_b = (ctx["extract_prop_text_any"](ex, PI_BRING_KEYS) == "True") if ex else False
            cur_n = ctx["extract_prop_text_any"](ex, PI_NOTE_KEYS) if ex else ""
            c1, c2, c3, c4 = st.columns([3, 1, 1, 4])
            c1.markdown(f"**{iname}**")
            a = c2.checkbox("担当", value=cur_a, key=f"a_{p_id}_{iid}")
            b = c3.checkbox("持参可", value=cur_b, key=f"b_{p_id}_{iid}")
            n = c4.text_input("備考", value=cur_n, label_visibility="collapsed", key=f"n_{p_id}_{iid}")
            changes.append({"iid": iid, "iname": iname, "a": a, "b": b, "n": n, "eid": ex.get("id", "") if ex else ""})
        if st.form_submit_button("💾 アサインを保存", use_container_width=True, type="primary"):
            ok_n, ng_n = 0, 0
            for ch in changes:
                if not ch["a"] and not ch["b"] and not ch["eid"]:
                    continue
                ok = _upsert_player_instrument(ctx, p_id, p_name, ch["iid"], ch["iname"], ch["a"], ch["b"], ch["n"], ch["eid"])
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件を保存しました。")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗しました。")
            st.session_state.pop(f"pi_list_{p_id}", None)
            st.rerun()


def render(ctx: dict):
    st.header("🎻 奏者・出欠・アサイン")
    t1, t2, t3 = st.tabs(["奏者管理", "出欠入力", "楽器アサイン"])
    with t1:
        _render_player_tab(ctx)
    with t2:
        _render_attendance_tab(ctx)
    with t3:
        _render_assign_tab(ctx)
