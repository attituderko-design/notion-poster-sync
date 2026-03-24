"""
concert.pages.players
奏者の登録・出欠入力・楽器アサイン画面。
既存ArtéMis DBのプロパティ名ゆれに対応。
"""
import streamlit as st
from concert.services.keys import *  # noqa: F401,F403










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


def _is_truthy_text(v: str) -> bool:
    return str(v or "").strip().lower() in {"true", "1", "yes", "on", "はい"}


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


def _load_concert_songs(ctx, concert_id: str) -> list[dict]:
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
        rel = ctx["find_prop_name"](t, SONG_CONCERT_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_SONG"], f)
    return st.session_state.get(key, [])


def _load_partdefs_for_song(ctx, song_id: str) -> list[dict]:
    key = f"partdef_list_{song_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_PART_DEFINITION"])
        rel = ctx["find_prop_name"](t, PARTDEF_SONG_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": song_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], f)
    return st.session_state.get(key, [])


def _load_player_instruments_for_concert(ctx, concert_id: str, player_id: str, participant_id: str = "") -> list[dict]:
    key = f"pi_list_{concert_id}_{player_id}_{participant_id or 'nop'}"
    if key not in st.session_state:
        db_id = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
        t = ctx["get_prop_types"](db_id)
        rel_player = ctx["find_prop_name"](t, PI_PLAYER_REL_KEYS)
        rel_participant = ctx["find_prop_name"](t, PI_PARTICIPANT_REL_KEYS)
        rel_concert = ctx["find_prop_name"](t, PI_CONCERT_REL_KEYS)
        filters = []
        if rel_concert and concert_id:
            filters.append({"property": rel_concert, "relation": {"contains": concert_id}})
        if rel_participant and participant_id:
            filters.append({"property": rel_participant, "relation": {"contains": participant_id}})
        elif rel_player and player_id:
            filters.append({"property": rel_player, "relation": {"contains": player_id}})
        f = {"filter": {"and": filters}} if filters else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


def _player_name(p: dict, ctx: dict) -> str:
    return ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or ctx["extract_title"](p) or p.get("id", "")


def _concert_name(c: dict, ctx: dict) -> str:
    n = ctx["extract_prop_text_any"](c, CONCERT_NAME_KEYS) or ctx["extract_title"](c)
    d = ctx["extract_prop_text_any"](c, CONCERT_DATE_KEYS)
    return f"{n}（{d[:10] if d else '日時未設定'}）"


def _concert_media_values(c: dict) -> list[str]:
    props = (c or {}).get("properties", {}) or {}
    out = []
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
    medias = _concert_media_values(c)
    return "出演" in medias


def _practice_name(p: dict, ctx: dict) -> str:
    n = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ctx["extract_title"](p)
    d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
    suffix = "【本番】" if _is_truthy_text(ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS)) else ""
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
    ctx["put_key_any"](props, t, PLAYER_KEY_KEYS, name, prefix="player")
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_player(ctx: dict, page_id: str, name: str, email: str, memo: str) -> bool:
    t = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
    props = {}
    ctx["put_prop_any"](props, t, PLAYER_NAME_KEYS, name)
    ctx["put_prop_any"](props, t, PLAYER_EMAIL_KEYS, email)
    ctx["put_prop_any"](props, t, PLAYER_MEMO_KEYS, memo)
    ctx["put_key_any"](props, t, PLAYER_KEY_KEYS, name, prefix="player")
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


def _upsert_attendance(
    ctx: dict,
    player_id: str,
    player_name: str,
    practice_id: str,
    practice_name: str,
    status: str,
    note: str,
    existing_id: str = "",
    participant_id: str = "",
) -> bool:
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
    # 出欠DBのrelation先が「演奏会参加者DB」の場合は participant_id を優先して紐づける
    rel_target_id = participant_id or player_id
    ctx["put_prop"](props, t, player_rel_key, rel_target_id)
    ctx["put_prop"](props, t, practice_rel_key, practice_id)
    if status_key:
        ctx["put_prop"](props, t, status_key, status)
    if note_key:
        ctx["put_prop"](props, t, note_key, note)
    ctx["put_key_any"](props, t, ATTENDANCE_KEY_KEYS, rel_target_id, practice_id, prefix="att")

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
    ctx["put_key_any"](props, t, PARTICIPANT_KEY_KEYS, concert_id, player_id, prefix="participant")
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
    ctx["put_key_any"](props, t, ASSIGN_KEY_KEYS, player_id, instrument_id, prefix="assign")
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}", json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _upsert_player_bring_for_concert(
    ctx: dict,
    concert_id: str,
    concert_name: str,
    player_id: str,
    player_name: str,
    participant_id: str,
    instrument_id: str,
    instrument_name: str,
    can_bring: bool,
    note: str,
    existing_id: str = "",
) -> bool:
    db_id = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        st.error("楽器アサインDBのプロパティ取得に失敗しました。")
        return False
    props = {}
    ctx["put_prop_any"](props, t, PI_RECORD_KEYS, f"{player_name} × {concert_name} × {instrument_name}")
    if participant_id:
        ctx["put_prop_any"](props, t, PI_PARTICIPANT_REL_KEYS, participant_id)
    ctx["put_prop_any"](props, t, PI_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, PI_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PI_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, t, PI_ASSIGN_KEYS, False)
    ctx["put_prop_any"](props, t, PI_BRING_KEYS, can_bring)
    ctx["put_prop_any"](props, t, PI_NOTE_KEYS, note)
    key_seed = participant_id or player_id
    ctx["put_key_any"](props, t, ASSIGN_KEY_KEYS, concert_id, key_seed, instrument_id, prefix="bring")
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}", json={"properties": props})
    else:
        res = ctx["api_request"](
            "post",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": db_id}, "properties": props},
        )
    ok = res is not None and res.status_code == 200
    if not ok:
        st.error(f"持参可保存に失敗: {_response_error_message(res)}")
    return ok


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
    concerts = [c for c in _load_concerts(ctx) if _is_performance_media_concert(c)]
    if not concerts:
        st.info("媒体=出演 の演奏会が見つかりません。ATLASで媒体設定を確認してください。")
        return
    all_c_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_c_opts)
    if global_concert_id:
        c_id = global_concert_id
        c_name = global_concert_name or global_concert_id
        st.caption(f"対象演奏会: {c_name}")
    else:
        c_query = st.text_input(
            "演奏会を検索",
            value=st.session_state.get("players_concert_search", ""),
            key="players_concert_search",
            placeholder="例: 2026 / 定期 / Happy Hour / Osaka",
        ).strip().lower()
        c_opts = {k: v for k, v in all_c_opts.items() if (not c_query) or (c_query in k.lower())}
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
    participant_id_by_player_id = {}
    for row in participants:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if not pids:
            continue
        participant_id_by_player_id[pids[0]] = row.get("id", "")

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
    practice_row = next((p for p in practices if p.get("id", "") == p_id), None)

    if not part_player_ids:
        st.info("先にこの演奏会の参加者を登録してください。")
        return
    players = [p for p in all_players if p.get("id", "") in set(part_player_ids)]
    att = _load_attendance(ctx, p_id)
    by_player = {}
    participant_to_player = {}
    for pid, part_id in participant_id_by_player_id.items():
        if part_id:
            participant_to_player[part_id] = pid
    for row in att:
        pids = ctx["extract_relation_ids_any"](row, ATT_PLAYER_REL_KEYS)
        if not pids:
            continue
        rel_id = pids[0]
        # 出欠DBが「演奏会参加者DB」を参照している場合は player_id に戻して扱う
        pid = participant_to_player.get(rel_id, rel_id)
        by_player[pid] = row

    # 打楽器休み日は全員を自動で×固定にする
    if practice_row and _is_truthy_text(ctx["extract_prop_text_any"](practice_row, PRACTICE_PERCUSSION_OFF_KEYS)):
        fixed_note = "楽器手配の無い日のため全員×"
        changed = 0
        failed = 0
        for pl in sorted(players, key=lambda x: _player_name(x, ctx)):
            pid = pl.get("id", "")
            pname = _player_name(pl, ctx)
            ex = by_player.get(pid)
            cur_s = ctx["extract_prop_text_any"](ex, ATT_STATUS_KEYS) if ex else ""
            cur_n = ctx["extract_prop_text_any"](ex, ATT_NOTE_KEYS) if ex else ""
            if cur_s == "×" and cur_n == fixed_note:
                continue
            ok = _upsert_attendance(
                ctx,
                pid,
                pname,
                p_id,
                p_name,
                "×",
                fixed_note,
                ex.get("id", "") if ex else "",
                participant_id_by_player_id.get(pid, ""),
            )
            if ok:
                changed += 1
            else:
                failed += 1
        st.session_state.pop(f"attendance_list_{p_id}", None)
        if failed == 0:
            st.info("この練習日は「打楽器休み」のため、出欠は全員「×」で自動固定されています。")
            if changed > 0:
                st.success(f"✅ 自動反映: {changed}件")
        else:
            st.warning(f"⚠️ 自動反映: 成功 {changed} / 失敗 {failed}")
        return

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
            changes.append({
                "player_id": pid,
                "player_name": pname,
                "status": s,
                "note": n,
                "existing_id": ex.get("id", "") if ex else "",
                "participant_id": participant_id_by_player_id.get(pid, ""),
            })
        if st.form_submit_button("💾 出欠を保存", use_container_width=True, type="primary"):
            ok_n, ng_n = 0, 0
            for ch in changes:
                ok = _upsert_attendance(
                    ctx,
                    ch["player_id"],
                    ch["player_name"],
                    p_id,
                    p_name,
                    ch["status"],
                    ch["note"],
                    ch["existing_id"],
                    ch.get("participant_id", ""),
                )
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件の出欠を保存しました。")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗しました。")
            st.session_state.pop(f"attendance_list_{p_id}", None)
            st.rerun()


def _render_assign_tab(ctx: dict):
    st.caption("このタブは、選択演奏会で必要な楽器に対して『各奏者が持参可能か』のみを整理します。")
    st.caption("パート希望・アサイン確定は『アサイン検討』画面で行います。")

    concerts = [c for c in _load_concerts(ctx) if _is_performance_media_concert(c)]
    if not concerts:
        st.info("媒体=出演 の演奏会が見つかりません。")
        return

    all_c_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_c_opts)
    if global_concert_id:
        c_id = global_concert_id
        c_name = global_concert_name or global_concert_id
        st.caption(f"対象演奏会: {c_name}")
    else:
        c_query = st.text_input(
            "演奏会を検索",
            value=st.session_state.get("bring_concert_search", ""),
            key="bring_concert_search",
            placeholder="例: Happy Hour / 2026 / 定期",
        ).strip().lower()
        c_opts = {k: v for k, v in all_c_opts.items() if (not c_query) or (c_query in k.lower())}
        if not c_opts:
            st.warning("検索条件に一致する演奏会がありません。")
            return
        c_name = st.selectbox("演奏会を選択", list(c_opts.keys()), key="bring_concert_sel")
        c_id = c_opts.get(c_name, "")
    if not c_id:
        return

    participants = _load_participants(ctx, c_id)
    if not participants:
        st.info("先に『出欠入力』タブでこの演奏会の参加者を保存してください。")
        return
    all_players = _load_players(ctx)
    player_name_map = {p.get("id", ""): _player_name(p, ctx) for p in all_players}
    participant_id_by_player_id = {}
    player_ids = []
    for row in participants:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if not pids:
            continue
        pid = pids[0]
        player_ids.append(pid)
        participant_id_by_player_id[pid] = row.get("id", "")
    player_ids = sorted(set(player_ids), key=lambda x: player_name_map.get(x, x))
    if not player_ids:
        st.info("演奏会参加者DBの出演者リレーションが空です。『出欠入力』で参加者を再保存してください。")
        return

    songs = _load_concert_songs(ctx, c_id)
    required_inst_ids = set()

    for s in songs:
        sid = s.get("id", "")
        for part in _load_partdefs_for_song(ctx, sid):
            iids = ctx["extract_relation_ids_any"](part, PARTDEF_INST_REL_KEYS)
            if iids:
                required_inst_ids.update([x for x in iids if x])
    if not required_inst_ids:
        st.info("この演奏会の必要楽器（パート定義）が見つかりません。先に『楽曲・楽器管理』でパート定義を行ってください。")
        return

    inst_rows = _load_instruments(ctx)
    inst_map = {i.get("id", ""): i for i in inst_rows}
    inst_names = {iid: _instrument_name(inst_map.get(iid, {}), ctx) for iid in required_inst_ids}
    ordered_inst_ids = sorted(required_inst_ids, key=lambda x: inst_names.get(x, x))

    p_opts = {player_name_map.get(pid, pid): pid for pid in player_ids}
    p_name = st.selectbox("奏者を選択", list(p_opts.keys()), key="bring_player_sel")
    p_id = p_opts.get(p_name, "")
    if not p_id:
        return
    participant_id = participant_id_by_player_id.get(p_id, "")

    rows = _load_player_instruments_for_concert(ctx, c_id, p_id, participant_id)
    by_inst = {}
    for row in rows:
        iids = ctx["extract_relation_ids_any"](row, PI_INST_REL_KEYS)
        if iids:
            by_inst[iids[0]] = row

    with st.form(f"bring_form_{c_id}_{p_id}", border=True):
        changes = []
        for iid in ordered_inst_ids:
            iname = inst_names.get(iid, iid)
            ex = by_inst.get(iid)
            cur_b = (ctx["extract_prop_text_any"](ex, PI_BRING_KEYS) == "True") if ex else False
            cur_n = ctx["extract_prop_text_any"](ex, PI_NOTE_KEYS) if ex else ""
            c1, c2, c3 = st.columns([4, 1, 5])
            c1.markdown(f"**{iname}**")
            b = c2.checkbox("持参可", value=cur_b, key=f"bring_{c_id}_{p_id}_{iid}", label_visibility="collapsed")
            n = c3.text_input("備考", value=cur_n, label_visibility="collapsed", key=f"bring_note_{c_id}_{p_id}_{iid}")
            changes.append({
                "iid": iid, "iname": iname,
                "b": b, "n": n,
                "cur_b": cur_b, "cur_n": cur_n,
                "eid": ex.get("id", "") if ex else "",
            })
        if st.form_submit_button("💾 持参可を保存", use_container_width=True, type="primary"):
            ok_n = ng_n = skip_n = 0
            for ch in changes:
                # 変更がない行はスキップ（差分保存）
                no_change = (ch["b"] == ch["cur_b"]) and (ch["n"] == ch["cur_n"])
                if no_change and not ch["eid"] and not ch["b"] and not ch["n"]:
                    skip_n += 1
                    continue
                if no_change and ch["eid"]:
                    skip_n += 1
                    continue
                ok = _upsert_player_bring_for_concert(
                    ctx, c_id, c_name, p_id, p_name,
                    participant_id,
                    ch["iid"], ch["iname"],
                    ch["b"], ch["n"], ch["eid"],
                )
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件を保存しました。（変更なし {skip_n}件はスキップ）")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗しました。")
            _clear_player_cache()
            st.rerun()


def render(ctx: dict):
    st.header("🎻 奏者・出欠・持参楽器")
    global_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    t1, t2, t3 = st.tabs(["奏者管理", "出欠入力", "持参楽器整理"])
    with t1:
        _render_player_tab(ctx)
    with t2:
        _render_attendance_tab(ctx)
    with t3:
        _render_assign_tab(ctx)
