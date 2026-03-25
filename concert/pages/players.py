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
    is_extra: bool = False,
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
    ctx["put_key_any"](props, t, PARTICIPANT_RECORD_KEYS, concert_id, player_id, prefix="participant")
    # 新規登録時のみ：ATLASの確定参加費を参照してセット（既存レコードは絶対に上書きしない）
    if not existing_id:
        confirmed_fee = st.session_state.get(f"confirmed_fee_{concert_id}", 0)
        if confirmed_fee == 0:
            # session_stateにない場合はATLASから取得
            try:
                t_c = ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT"])
                res_c = ctx["api_request"]("get", f"https://api.notion.com/v1/pages/{concert_id}")
                if res_c and res_c.status_code == 200:
                    fee_key = ctx["find_prop_name"](t_c, CONCERT_CONFIRMED_FEE_KEYS) if t_c else None
                    if fee_key:
                        num = res_c.json().get("properties", {}).get(fee_key, {}).get("number")
                        confirmed_fee = int(num) if num else 0
            except Exception:
                confirmed_fee = 0
        if confirmed_fee > 0:
            ctx["put_prop_any"](props, t, PARTICIPANT_FEE_KEYS, confirmed_fee)
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
    ctx["put_prop_any"](props, t, PI_NOTE_KEYS, note)
    if practice_id:
        ctx["put_prop_any"](props, t, PI_PRACTICE_REL_KEYS, practice_id)
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
    own_count: int = 1,
    bring_assign: bool = False,
    bring_count: int = 0,
    practice_id: str = "",
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
    ctx["put_prop_any"](props, t, PI_OWN_COUNT_KEYS, own_count)
    ctx["put_prop_any"](props, t, PI_BRING_ASSIGN_KEYS, bring_assign)
    ctx["put_prop_any"](props, t, PI_BRING_COUNT_KEYS, bring_count if bring_assign else 0)
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
    """奏者マスタ管理 + この演奏会の参加者設定・パート・役職設定を統合。"""
    import pandas as pd

    global_concert_id   = (ctx.get("SELECTED_CONCERT_ID")   or "").strip()
    global_concert_name = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()

    players = _load_players(ctx)

    # ── 奏者マスタ（全奏者）────────────────────────────────────
    st.markdown("### 奏者マスタ")
    with st.expander("➕ 新規奏者を登録", expanded=(len(players) == 0)):
        with st.form("player_new_form", border=True):
            name  = st.text_input("氏名 *", placeholder="例：山田 太郎")
            email = st.text_input("メールアドレス", placeholder="任意")
            memo  = st.text_area("メモ", height=60)
            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                if not name.strip():
                    st.error("氏名は必須です。")
                elif _create_player(ctx, name.strip(), email, memo):
                    st.success("✅ 奏者を登録しました。")
                    _clear_player_cache()
                    st.rerun()
                else:
                    st.error("❌ 登録に失敗しました。")

    if not players:
        st.info("奏者がまだ登録されていません。")
        return

    # 検索
    col_s, col_r = st.columns([8, 1])
    search = col_s.text_input("奏者を検索", placeholder="氏名・メモ", key="player_master_search").strip().lower()
    if col_r.button("🔄", key="player_master_refresh"):
        _clear_player_cache()
        st.rerun()

    sorted_players = sorted(players, key=lambda x: _player_name(x, ctx))
    if search:
        sorted_players = [p for p in sorted_players
                          if search in _player_name(p, ctx).lower()
                          or search in (ctx["extract_prop_text_any"](p, PLAYER_MEMO_KEYS) or "").lower()]

    st.caption(f"表示 {len(sorted_players)} / {len(players)} 件")

    # data_editor形式で全奏者を表示・編集
    master_rows = []
    master_meta = []
    for p in sorted_players:
        pid = p.get("id", "")
        master_rows.append({
            "氏名":           ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or "",
            "メールアドレス": ctx["extract_prop_text_any"](p, PLAYER_EMAIL_KEYS) or "",
            "メモ":           ctx["extract_prop_text_any"](p, PLAYER_MEMO_KEYS) or "",
        })
        master_meta.append({"pid": pid,
                             "cur_name":  ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or "",
                             "cur_email": ctx["extract_prop_text_any"](p, PLAYER_EMAIL_KEYS) or "",
                             "cur_memo":  ctx["extract_prop_text_any"](p, PLAYER_MEMO_KEYS) or ""})

    master_version = st.session_state.get("player_master_version", 0)
    df_master = pd.DataFrame(master_rows)
    edited_master = st.data_editor(
        df_master, num_rows="fixed", use_container_width=True,
        key=f"player_master_editor_{master_version}",
        column_config={
            "氏名":           st.column_config.TextColumn("氏名 *", max_chars=50),
            "メールアドレス": st.column_config.TextColumn("メールアドレス", max_chars=100),
            "メモ":           st.column_config.TextColumn("メモ", max_chars=200),
        },
    )
    if st.button("💾 奏者マスタを保存", type="primary", use_container_width=True, key="player_master_save"):
        ok_n = ng_n = skip_n = 0
        df_reset = edited_master.reset_index(drop=True)
        for idx, meta in enumerate(master_meta):
            if idx >= len(df_reset): break
            row = df_reset.iloc[idx]
            new_name  = str(row.get("氏名") or "").strip()
            new_email = str(row.get("メールアドレス") or "").strip()
            new_memo  = str(row.get("メモ") or "").strip()
            if not new_name:
                skip_n += 1; continue
            if new_name == meta["cur_name"] and new_email == meta["cur_email"] and new_memo == meta["cur_memo"]:
                skip_n += 1; continue
            ok = _update_player(ctx, meta["pid"], new_name, new_email, new_memo)
            ok_n += 1 if ok else 0
            ng_n += 0 if ok else 1
        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        st.session_state["player_master_version"] = master_version + 1
        _clear_player_cache()
        st.rerun()

    if not global_concert_id:
        return

    # ── 演奏会参加者設定 ──────────────────────────────────────
    st.divider()
    st.markdown(f"### 演奏会参加者設定")
    st.caption("この演奏会に参加する奏者を選択し、パート・役職を設定してください。")

    participants = _load_participants(ctx, global_concert_id)
    part_row_by_pid: dict[str, dict] = {}
    for row in participants:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            part_row_by_pid[pids[0]] = row
    current_pids = set(part_row_by_pid.keys())

    # Notionのselect選択肢を動的取得
    part_opts     = _get_select_options(ctx, ctx["CONCERT_DB_PARTICIPANT"], PARTICIPANT_PART_KEYS)
    role_m_opts   = _get_select_options(ctx, ctx["CONCERT_DB_PARTICIPANT"], PARTICIPANT_ROLE_KEYS)
    role_ops_opts = _get_select_options(ctx, ctx["CONCERT_DB_PARTICIPANT"], PARTICIPANT_ROLE_OPS_KEYS)

    cast_rows = []
    cast_meta = []
    for p in sorted(players, key=lambda x: _player_name(x, ctx)):
        pid   = p.get("id", "")
        pname = _player_name(p, ctx)
        row   = part_row_by_pid.get(pid, {})
        rid   = row.get("id", "") if row else ""
        in_cast = pid in current_pids
        cur_part    = ctx["extract_prop_text_any"](row, PARTICIPANT_PART_KEYS)  if row else ""
        cur_role_m  = ctx["extract_prop_text_any"](row, PARTICIPANT_ROLE_KEYS)  if row else ""
        cur_role_o  = ctx["extract_prop_text_any"](row, PARTICIPANT_ROLE_OPS_KEYS) if row else ""
        fee_s       = ctx["extract_prop_text_any"](row, PARTICIPANT_FEE_KEYS)   if row else ""
        paid        = ctx["extract_prop_text_any"](row, PARTICIPANT_PAID_KEYS) == "True" if row else False
        try: fee = int(float(fee_s)) if fee_s != "" else None
        except: fee = None
        is_extra = (fee == 0) if fee is not None else False

        cast_rows.append({
            "参加":     in_cast,
            "エキストラ": is_extra,
            "氏名":     pname,
            "パート":   cur_part or "",
            "役職(音楽)": cur_role_m or "",
            "役職(運営)": cur_role_o or "",
        })
        cast_meta.append({
            "pid": pid, "pname": pname, "rid": rid,
            "in_cast": in_cast, "is_extra": is_extra,
            "cur_part": cur_part, "cur_role_m": cur_role_m,
            "cur_role_o": cur_role_o, "fee": fee, "paid": paid,
        })

    cast_version = st.session_state.get(f"cast_editor_version_{global_concert_id}", 0)
    df_cast = pd.DataFrame(cast_rows)

    col_cfg = {
        "参加":       st.column_config.CheckboxColumn("参加", default=False),
        "エキストラ": st.column_config.CheckboxColumn("エキストラ", default=False,
                       help="チェックすると参加費0円で登録"),
        "氏名":       st.column_config.TextColumn("氏名", disabled=True),
        "パート":     st.column_config.SelectboxColumn("パート", options=part_opts) if part_opts
                       else st.column_config.TextColumn("パート", max_chars=30),
        "役職(音楽)": st.column_config.SelectboxColumn("役職(音楽)", options=role_m_opts) if role_m_opts
                       else st.column_config.TextColumn("役職(音楽)", max_chars=30),
        "役職(運営)": st.column_config.SelectboxColumn("役職(運営)", options=role_ops_opts) if role_ops_opts
                       else st.column_config.TextColumn("役職(運営)", max_chars=30),
    }
    edited_cast = st.data_editor(
        df_cast, num_rows="fixed", use_container_width=True,
        key=f"cast_editor_{global_concert_id}_{cast_version}",
        column_config=col_cfg,
    )

    if st.button("💾 参加者・パート・役職を保存", type="primary",
                 use_container_width=True, key=f"cast_save_{global_concert_id}"):
        ok_n = ng_n = skip_n = 0

        # ATLASの確定参加費を取得してsession_stateにキャッシュ
        try:
            tc2   = ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT"])
            res_c = ctx["api_request"]("get", f"https://api.notion.com/v1/pages/{global_concert_id}")
            if res_c and res_c.status_code == 200 and tc2:
                fee_key2 = ctx["find_prop_name"](tc2, CONCERT_CONFIRMED_FEE_KEYS)
                if fee_key2:
                    num = res_c.json().get("properties", {}).get(fee_key2, {}).get("number")
                    if num is not None:
                        st.session_state[f"confirmed_fee_{global_concert_id}"] = int(num)
        except Exception:
            pass

        with st.spinner("保存中..."):
            df_reset = edited_cast.reset_index(drop=True)
            for idx, meta in enumerate(cast_meta):
                if idx >= len(df_reset): break
                row        = df_reset.iloc[idx]
                new_in     = bool(row.get("参加") or False)
                new_extra  = bool(row.get("エキストラ") or False)
                new_part   = str(row.get("パート")     or "").strip()
                new_role_m = str(row.get("役職(音楽)") or "").strip()
                new_role_o = str(row.get("役職(運営)") or "").strip()

                if not new_in and not meta["in_cast"]:
                    skip_n += 1; continue

                # 参加→非参加：アーカイブ
                if not new_in and meta["in_cast"] and meta["rid"]:
                    ok = _archive_participant(ctx, meta["rid"])
                    ok_n += 1 if ok else 0
                    ng_n += 0 if ok else 1
                    continue

                # 新規参加 or 既存更新
                no_change = (meta["in_cast"] and
                             new_extra == meta["is_extra"] and
                             new_part   == (meta["cur_part"]   or "") and
                             new_role_m == (meta["cur_role_m"] or "") and
                             new_role_o == (meta["cur_role_o"] or ""))
                if no_change:
                    skip_n += 1; continue

                ok = _upsert_participant(
                    ctx, global_concert_id, global_concert_name,
                    meta["pid"], meta["pname"], meta["rid"],
                    is_extra=new_extra,
                    part=new_part, role_music=new_role_m, role_ops=new_role_o,
                )
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1

        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        st.session_state[f"cast_editor_version_{global_concert_id}"] = cast_version + 1
        st.session_state.pop(f"participant_list_{global_concert_id}", None)
        st.rerun()


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
            extra_names = st.multiselect(
                "エキストラ（参加費0円）",
                list(selectable.keys()),
                default=[name for name, pid in selectable.items()
                         if pid in part_player_ids and
                         ctx["extract_prop_text_any"](part_row_by_pid.get(pid, {}), PARTICIPANT_FEE_KEYS) == "0"],
                key=f"participant_extra_{c_id}",
                help="エキストラの方は参加費が0円で登録されます",
            )
            remove_unselected = st.checkbox("未選択の既存参加者をアーカイブ", value=False, key=f"participant_remove_{c_id}")
            if st.form_submit_button("💾 参加者を保存", type="primary", use_container_width=True):
                selected_ids = {selectable[n] for n in sel_names if selectable.get(n)}
                extra_ids    = {selectable[n] for n in extra_names if selectable.get(n)}
                ok_n, ng_n = 0, 0
                # ATLASの確定参加費をsession_stateにキャッシュ
                try:
                    t_c   = ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT"])
                    res_c = ctx["api_request"]("get", f"https://api.notion.com/v1/pages/{c_id}")
                    if res_c and res_c.status_code == 200:
                        fee_key = ctx["find_prop_name"](t_c, CONCERT_CONFIRMED_FEE_KEYS) if t_c else None
                        if fee_key:
                            num = res_c.json().get("properties", {}).get(fee_key, {}).get("number")
                            if num is not None:
                                st.session_state[f"confirmed_fee_{c_id}"] = int(num)
                except Exception:
                    pass

                for pid in selected_ids:
                    pname = player_name_map.get(pid, pid)
                    ex    = part_row_by_pid.get(pid)
                    is_extra = pid in extra_ids
                    ok = _upsert_participant(ctx, c_id, c_name, pid, pname,
                                            ex.get("id", "") if ex else "",
                                            is_extra=is_extra)
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

    # ── 参加者別パート・役職設定 ──────────────────────────────
    if part_player_ids:
        st.markdown("### パート・役職設定")
        st.caption("Notionに登録済みの選択肢が表示されます。新しい値はNotionに追加後、🔄で反映されます。")
        part_opts = _get_select_options(ctx, ctx["CONCERT_DB_PARTICIPANT"], PARTICIPANT_PART_KEYS)
        role_opts = _get_select_options(ctx, ctx["CONCERT_DB_PARTICIPANT"], PARTICIPANT_ROLE_KEYS)

        import pandas as pd
        pr_rows: list[dict] = []
        pr_meta: list[dict] = []
        for pid in part_player_ids:
            pname = player_name_map.get(pid, pid)
            row   = part_row_by_pid.get(pid, {})
            rid   = row.get("id", "") if row else ""
            cur_part = ctx["extract_prop_text_any"](row, PARTICIPANT_PART_KEYS) if row else ""
            cur_role = ctx["extract_prop_text_any"](row, PARTICIPANT_ROLE_KEYS) if row else ""
            pr_rows.append({"氏名": pname, "パート": cur_part, "役職": cur_role})
            pr_meta.append({"rid": rid, "pid": pid, "cur_part": cur_part, "cur_role": cur_role})

        df_pr = pd.DataFrame(pr_rows)
        col_config_pr = {
            "氏名":   st.column_config.TextColumn("氏名", disabled=True),
            "パート": st.column_config.SelectboxColumn("パート", options=part_opts) if part_opts
                       else st.column_config.TextColumn("パート", max_chars=30),
            "役職":   st.column_config.SelectboxColumn("役職",  options=role_opts) if role_opts
                       else st.column_config.TextColumn("役職",  max_chars=30),
        }
        pr_version = st.session_state.get(f"pr_editor_version_{c_id}", 0)
        edited_pr = st.data_editor(
            df_pr, num_rows="fixed", use_container_width=True,
            key=f"pr_editor_{c_id}_{pr_version}",
            column_config=col_config_pr,
        )
        if st.button("💾 パート・役職を保存", type="primary", use_container_width=True,
                     key=f"pr_save_{c_id}"):
            ok_n = ng_n = skip_n = 0
            with st.spinner("保存中..."):
                df_reset = edited_pr.reset_index(drop=True)
                for idx, meta in enumerate(pr_meta):
                    if idx >= len(df_reset): break
                    row      = df_reset.iloc[idx]
                    new_part = str(row.get("パート") or "").strip()
                    new_role = str(row.get("役職")   or "").strip()
                    if new_part == meta["cur_part"] and new_role == meta["cur_role"]:
                        skip_n += 1
                        continue
                    if not meta["rid"]:
                        skip_n += 1
                        continue
                    t_p  = ctx["get_prop_types"](ctx["CONCERT_DB_PARTICIPANT"])
                    props: dict = {}
                    ctx["put_prop_any"](props, t_p, PARTICIPANT_PART_KEYS, new_part)
                    ctx["put_prop_any"](props, t_p, PARTICIPANT_ROLE_KEYS, new_role)
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{meta['rid']}",
                        json={"properties": props})
                    ok = res is not None and res.status_code == 200
                    ok_n += 1 if ok else 0
                    ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
            st.session_state[f"pr_editor_version_{c_id}"] = pr_version + 1
            st.session_state.pop(f"participant_list_{c_id}", None)
            st.rerun()

        st.divider()

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
    import pandas as pd

    # data_editor形式に変更
    sorted_players = sorted(players, key=lambda x: _player_name(x, ctx))
    att_rows: list[dict] = []
    att_row_meta: list[dict] = []  # player_id・existing_id・participant_idを保持

    for pl in sorted_players:
        pid   = pl.get("id", "")
        pname = _player_name(pl, ctx)
        ex    = by_player.get(pid)
        cur_s = ctx["extract_prop_text_any"](ex, ATT_STATUS_KEYS) if ex else "△"
        cur_n = ctx["extract_prop_text_any"](ex, ATT_NOTE_KEYS) if ex else ""
        if cur_s not in statuses:
            cur_s = "△"
        att_rows.append({"奏者": pname, "参加可否": cur_s, "備考": cur_n})
        att_row_meta.append({
            "player_id":      pid,
            "player_name":    pname,
            "existing_id":    ex.get("id", "") if ex else "",
            "participant_id": participant_id_by_player_id.get(pid, ""),
        })

    df_att = pd.DataFrame(att_rows)
    edited_att = st.data_editor(
        df_att,
        num_rows="fixed",
        use_container_width=True,
        key=f"att_editor_{p_id}",
        column_config={
            "奏者": st.column_config.TextColumn("奏者", disabled=True),
            "参加可否": st.column_config.SelectboxColumn(
                "参加可否",
                options=statuses,
                required=True,
                default="△",
            ),
            "備考": st.column_config.TextColumn("備考", max_chars=100),
        },
    )

    if st.button("💾 出欠を保存", use_container_width=True, type="primary",
                 key=f"att_save_{p_id}"):
        ok_n = ng_n = 0
        with st.spinner("保存中..."):
            df_reset = edited_att.reset_index(drop=True)
            for idx, meta in enumerate(att_row_meta):
                if idx >= len(df_reset): break
                row   = df_reset.iloc[idx]
                new_s = str(row.get("参加可否") or "△").strip()
                new_n = str(row.get("備考") or "").strip()
                ok = _upsert_attendance(
                    ctx,
                    meta["player_id"], meta["player_name"],
                    p_id, p_name,
                    new_s, new_n,
                    meta["existing_id"],
                    meta["participant_id"],
                )
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
        if ng_n == 0:
            st.success(f"✅ {ok_n}件の出欠を保存しました。")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗しました。")
        st.session_state.pop(f"attendance_list_{p_id}", None)
        st.rerun()


def _get_select_options(ctx, db_id: str, field_keys: list) -> list[str]:
    """NotionのselectフィールドのオプションをAPIから取得する。"""
    try:
        t = ctx["get_prop_types"](db_id)
        if not t:
            return []
        field_name = ctx["find_prop_name"](t, field_keys)
        if not field_name:
            return []
        res = ctx["api_request"]("get", f"https://api.notion.com/v1/databases/{db_id}")
        if not res or res.status_code != 200:
            return []
        props = res.json().get("properties", {})
        prop  = props.get(field_name, {})
        opts  = prop.get("select", {}).get("options", [])
        return [o["name"] for o in opts if o.get("name")]
    except Exception:
        return []


# ============================================================
# 所有楽器マスタ
# ============================================================

def _load_pi_master(ctx, player_id: str) -> list[dict]:
    """奏者の所有楽器マスタを取得。"""
    db_id = ctx.get("CONCERT_DB_PI_MASTER", "")
    if not db_id:
        return []
    key = f"pi_master_{player_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](db_id)
        rel = ctx["find_prop_name"](t, MASTER_PLAYER_REL_KEYS) if t else None
        f = {"filter": {"property": rel, "relation": {"contains": player_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


def _upsert_pi_master(ctx, player_id: str, player_name: str,
                      instrument_id: str, instrument_name: str,
                      own_count: int, note: str,
                      existing_id: str = "") -> bool:
    db_id = ctx.get("CONCERT_DB_PI_MASTER", "")
    if not db_id:
        st.error("所有楽器マスタDBのIDが未設定です。secrets.tomlに CONCERT_DB_PI_MASTER を追加してください。")
        return False
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    props: dict = {}
    ctx["put_prop_any"](props, t, MASTER_KEY_KEYS, f"{player_name} × {instrument_name}")
    ctx["put_prop_any"](props, t, MASTER_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, MASTER_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, t, MASTER_OWN_COUNT_KEYS, own_count)
    ctx["put_prop_any"](props, t, MASTER_NOTE_KEYS, note)
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _render_pi_master_tab(ctx: dict):
    """所有楽器マスタ管理タブ。"""
    db_id = ctx.get("CONCERT_DB_PI_MASTER", "")
    if not db_id:
        st.warning("所有楽器マスタDBが未設定です。secrets.tomlに `CONCERT_DB_PI_MASTER` を追加してください。")
        return

    st.caption("奏者ごとの所有楽器・台数を管理するマスタです。演奏会をまたいで流用できます。")

    all_players = _load_players(ctx)
    if not all_players:
        st.info("奏者が登録されていません。")
        return

    player_name_map = {p.get("id", ""): _player_name(p, ctx) for p in all_players}
    p_opts = {_player_name(p, ctx): p.get("id", "") for p in sorted(all_players, key=lambda x: _player_name(x, ctx))}

    col_sel, col_r = st.columns([8, 1])
    p_name = col_sel.selectbox("奏者を選択", list(p_opts.keys()), key="master_player_sel")
    if col_r.button("🔄", key="master_refresh"):
        for k in list(st.session_state.keys()):
            if k.startswith("pi_master_"):
                st.session_state.pop(k, None)
        st.rerun()

    p_id = p_opts.get(p_name, "")
    if not p_id:
        return

    # 全楽器一覧
    inst_rows  = _load_instruments(ctx)
    inst_map   = {i.get("id", ""): i for i in inst_rows}
    inst_names = {i.get("id", ""): _instrument_name(i, ctx) for i in inst_rows}
    ordered_inst_ids = sorted(inst_map.keys(), key=lambda x: inst_names.get(x, x))

    # 既存マスタ
    master_rows = _load_pi_master(ctx, p_id)
    by_inst: dict[str, dict] = {}
    for r in master_rows:
        iids = ctx["extract_relation_ids_any"](r, MASTER_INST_REL_KEYS)
        if iids:
            by_inst[iids[0]] = r

    import pandas as pd
    df_rows: list[dict] = []
    df_meta: list[dict] = []

    for iid in ordered_inst_ids:
        iname = inst_names.get(iid, iid)
        ex    = by_inst.get(iid)
        cur_own_str = ctx["extract_prop_text_any"](ex, MASTER_OWN_COUNT_KEYS) if ex else "0"
        try: cur_own = int(float(cur_own_str))
        except: cur_own = 0
        cur_n = ctx["extract_prop_text_any"](ex, MASTER_NOTE_KEYS) if ex else ""
        df_rows.append({"楽器": iname, "所有台数": cur_own, "備考": cur_n})
        df_meta.append({
            "iid": iid, "iname": iname,
            "eid": ex.get("id", "") if ex else "",
            "cur_own": cur_own, "cur_n": cur_n,
        })

    df = pd.DataFrame(df_rows)
    edited = st.data_editor(
        df, num_rows="fixed", use_container_width=True,
        key=f"master_editor_{p_id}",
        column_config={
            "楽器":     st.column_config.TextColumn("楽器", disabled=True),
            "所有台数": st.column_config.NumberColumn("所有台数", min_value=0, max_value=20, step=1, default=0),
            "備考":     st.column_config.TextColumn("備考", max_chars=100),
        },
    )

    if st.button("💾 マスタを保存", type="primary", use_container_width=True, key=f"master_save_{p_id}"):
        ok_n = ng_n = skip_n = 0
        with st.spinner("保存中..."):
            df_reset = edited.reset_index(drop=True)
            for idx, meta in enumerate(df_meta):
                if idx >= len(df_reset): break
                row     = df_reset.iloc[idx]
                new_own = int(row.get("所有台数") or 0)
                new_n   = str(row.get("備考") or "").strip()
                if new_own == meta["cur_own"] and new_n == meta["cur_n"]:
                    skip_n += 1
                    continue
                if new_own == 0 and not meta["eid"]:
                    skip_n += 1
                    continue
                ok = _upsert_pi_master(ctx, p_id, p_name,
                                       meta["iid"], meta["iname"],
                                       new_own, new_n, meta["eid"])
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        for k in list(st.session_state.keys()):
            if k.startswith("pi_master_"):
                st.session_state.pop(k, None)
        st.rerun()


def _render_assign_tab(ctx: dict):
    st.caption("奏者ごとに所有楽器・台数を登録します。持参担当の設定は『練習日別持参担当』タブで行います。")

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
    # Percパートのみに絞り込み
    player_ids = _filter_perc_players(ctx, player_ids, participants)
    if not player_ids:
        st.info("打楽器パート（Perc）の参加者が登録されていません。出欠入力タブでパートを設定してください。")
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
    col_ps, col_copy = st.columns([6, 2])
    p_name = col_ps.selectbox("奏者を選択", list(p_opts.keys()), key="bring_player_sel")
    p_id = p_opts.get(p_name, "")
    if not p_id:
        return
    participant_id = participant_id_by_player_id.get(p_id, "")

    # マスタからコピー
    if col_copy.button("📋 マスタからコピー", key=f"copy_from_master_{p_id}",
                       use_container_width=True, help="所有楽器マスタの台数をこの演奏会にコピーします"):
        master_rows = _load_pi_master(ctx, p_id)
        if not master_rows:
            st.warning("所有楽器マスタにデータがありません。先に『所有楽器マスタ』タブで登録してください。")
        else:
            ok_n = ng_n = 0
            rows_current = _load_player_instruments_for_concert(ctx, c_id, p_id, participant_id)
            by_inst_cur = {}
            for r in rows_current:
                iids = ctx["extract_relation_ids_any"](r, PI_INST_REL_KEYS)
                if iids: by_inst_cur[iids[0]] = r
            with st.spinner("コピー中..."):
                for mr in master_rows:
                    iids = ctx["extract_relation_ids_any"](mr, MASTER_INST_REL_KEYS)
                    if not iids: continue
                    iid   = iids[0]
                    iname = inst_names.get(iid, iid)
                    own_str = ctx["extract_prop_text_any"](mr, MASTER_OWN_COUNT_KEYS) or "0"
                    try: own = int(float(own_str))
                    except: own = 0
                    note  = ctx["extract_prop_text_any"](mr, MASTER_NOTE_KEYS) or ""
                    ex    = by_inst_cur.get(iid)
                    ok = _upsert_player_bring_for_concert(
                        ctx, c_id, c_name, p_id, p_name, participant_id,
                        iid, iname, own >= 1, note,
                        ex.get("id", "") if ex else "",
                        own_count=own,
                    )
                    ok_n += 1 if ok else 0
                    ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件をコピーしました。")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
            for k in list(st.session_state.keys()):
                if k.startswith(f"pi_list_"):
                    st.session_state.pop(k, None)
            st.rerun()

    rows = _load_player_instruments_for_concert(ctx, c_id, p_id, participant_id)
    by_inst = {}
    for row in rows:
        iids = ctx["extract_relation_ids_any"](row, PI_INST_REL_KEYS)
        if iids:
            by_inst[iids[0]] = row

    import pandas as pd

    bring_rows_data: list[dict] = []
    bring_row_meta: list[dict] = []

    for iid in ordered_inst_ids:
        iname = inst_names.get(iid, iid)
        ex    = by_inst.get(iid)
        cur_ba = (ctx["extract_prop_text_any"](ex, PI_BRING_ASSIGN_KEYS) == "True") if ex else False
        cur_own_str = ctx["extract_prop_text_any"](ex, PI_OWN_COUNT_KEYS) if ex else ""
        cur_cnt_str = ctx["extract_prop_text_any"](ex, PI_BRING_COUNT_KEYS) if ex else ""
        try:
            cur_own = int(float(cur_own_str)) if cur_own_str else 1
        except ValueError:
            cur_own = 1
        try:
            cur_cnt = int(float(cur_cnt_str)) if cur_cnt_str else 0
        except ValueError:
            cur_cnt = 0
        cur_n = ctx["extract_prop_text_any"](ex, PI_NOTE_KEYS) if ex else ""
        bring_rows_data.append({
            "楽器":     iname,
            "所有台数": cur_own,
            "備考":     cur_n,
        })
        bring_row_meta.append({
            "iid":     iid,
            "iname":   iname,
            "eid":     ex.get("id", "") if ex else "",
            "cur_own": cur_own,
            "cur_n":   cur_n,
        })

    df_bring = pd.DataFrame(bring_rows_data)
    edited_bring = st.data_editor(
        df_bring,
        num_rows="fixed",
        use_container_width=True,
        key=f"bring_editor_{c_id}_{p_id}",
        column_config={
            "楽器":     st.column_config.TextColumn("楽器", disabled=True),
            "所有台数": st.column_config.NumberColumn("所有台数", min_value=0, max_value=20, step=1, default=0),
            "備考":     st.column_config.TextColumn("備考", max_chars=100),
        },
    )

    if st.button("💾 所有楽器を保存", use_container_width=True, type="primary",
                 key=f"bring_save_{c_id}_{p_id}"):
        ok_n = ng_n = skip_n = 0
        with st.spinner("保存中..."):
            df_reset = edited_bring.reset_index(drop=True)
            for idx, meta in enumerate(bring_row_meta):
                if idx >= len(df_reset): break
                row     = df_reset.iloc[idx]
                new_own = int(row.get("所有台数") or 0)
                new_n   = str(row.get("備考") or "").strip()
                no_change = (new_own == meta["cur_own"] and new_n == meta["cur_n"])
                if no_change and not meta["eid"] and new_own == 0:
                    skip_n += 1
                    continue
                if no_change and meta["eid"]:
                    skip_n += 1
                    continue
                ok = _upsert_player_bring_for_concert(
                    ctx, c_id, c_name, p_id, p_name,
                    participant_id,
                    meta["iid"], meta["iname"],
                    new_own >= 1, new_n, meta["eid"],
                    own_count=new_own,
                )
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
            if ng_n == 0:
                st.success(f"✅ {ok_n}件を保存しました。（変更なし {skip_n}件はスキップ）")
            else:
                st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗しました。")
            _clear_player_cache()
            st.rerun()


def _render_practice_bring_tab(ctx: dict):
    """練習日を選んで楽器ごとの持参担当を設定するタブ。"""
    st.caption("練習日を選択して、各楽器の持参担当者を設定します。")

    concerts = [c for c in _load_concerts(ctx) if _is_performance_media_concert(c)]
    if not concerts:
        st.info("媒体=出演 の演奏会が見つかりません。")
        return

    all_c_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, all_c_opts)
    if global_concert_id:
        c_id   = global_concert_id
        c_name = global_concert_name or global_concert_id
        st.caption(f"対象演奏会: {c_name}")
    else:
        c_name = st.selectbox("演奏会を選択", list(all_c_opts.keys()), key="pb_concert_sel")
        c_id = all_c_opts.get(c_name, "")
    if not c_id:
        return

    # 練習一覧（本番当日除く、日付順）
    all_practices = _load_practices(ctx, c_id)
    practices = [p for p in all_practices
                 if not _is_truthy_text(ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS))]
    practices = sorted(practices, key=lambda p: ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or "9999")
    if not practices:
        st.info("練習が登録されていません。")
        return

    # 練習日選択
    prac_opts = {}
    for p in practices:
        pname  = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ""
        pdate  = (ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or "")[:10]
        prac_opts[f"{pname}（{pdate}）"] = p
    col_sel, col_r = st.columns([8, 1])
    p_label = col_sel.selectbox("練習日を選択", list(prac_opts.keys()), key="pb_prac_sel")
    if col_r.button("🔄", key="pb_refresh", help="再読み込み"):
        for k in list(st.session_state.keys()):
            if k.startswith("pi_practice_") or k.startswith("attendance_list_"):
                st.session_state.pop(k, None)
        st.rerun()
    practice = prac_opts.get(p_label)
    if not practice:
        return
    pr_id = practice.get("id", "")

    # 出欠確認
    att_rows = _load_attendance(ctx, pr_id)
    participants = _load_participants(ctx, c_id)
    part_to_player = {
        row.get("id", ""): (ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]
        for row in participants
    }
    att_status: dict[str, str] = {}
    for r in att_rows:
        raw  = (ctx["extract_relation_ids_any"](r, ATT_PLAYER_REL_KEYS) or [""])[0]
        plid = part_to_player.get(raw, raw)
        att_status[plid] = ctx["extract_prop_text_any"](r, ATT_STATUS_KEYS) or "△"

    # 出席確定（○）の奏者
    attending_pids = {pid for pid, s in att_status.items() if s == "○"}

    # この日の演奏曲
    song_ids = ctx["extract_relation_ids_any"](practice, PRACTICE_SONG_REL_KEYS)
    if not song_ids:
        # 未設定の場合は演奏会の全曲
        all_songs = _load_concert_songs(ctx, c_id)
        song_ids = [s.get("id", "") for s in all_songs]

    # 必要楽器をパート定義から収集
    # instrument_id → [(part_name, song_name, assigned_player_ids)]
    inst_parts: dict[str, list[dict]] = {}
    all_songs_rows = _load_concert_songs(ctx, c_id)
    song_name_map = {s.get("id", ""): ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or "" for s in all_songs_rows}

    for sid in song_ids:
        sname = song_name_map.get(sid, sid)
        for part in _load_partdefs_for_song(ctx, sid):
            iids   = ctx["extract_relation_ids_any"](part, PARTDEF_INST_REL_KEYS)
            pname  = ctx["extract_prop_text_any"](part, PARTDEF_NAME_KEYS) or ""
            part_id = part.get("id", "")
            for iid in iids:
                if iid not in inst_parts:
                    inst_parts[iid] = []
                inst_parts[iid].append({
                    "part_name": pname,
                    "song_name": sname,
                    "part_id":   part_id,
                })

    if not inst_parts:
        st.info("この練習日に演奏曲・パート定義が登録されていません。")
        return

    # 奏者情報
    all_players   = _load_players(ctx)
    player_name_map = {p.get("id", ""): _player_name(p, ctx) for p in all_players}
    participant_id_by_player_id = {
        (ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]: row.get("id", "")
        for row in participants
    }

    # PLAYER_INSTRUMENTから担当フラグ・持参可フラグを取得
    all_pi = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    # instrument_id → player_id → {assign, can_bring, own_count}
    pi_by_inst: dict[str, dict[str, dict]] = {}
    for r in all_pi:
        c_ids = ctx["extract_relation_ids_any"](r, PI_CONCERT_REL_KEYS)
        if c_id not in c_ids: continue
        iids = ctx["extract_relation_ids_any"](r, PI_INST_REL_KEYS)
        pids = ctx["extract_relation_ids_any"](r, PI_PLAYER_REL_KEYS)
        if not iids or not pids: continue
        iid = iids[0]; pid = pids[0]
        pi_by_inst.setdefault(iid, {})[pid] = {
            "assign":    ctx["extract_prop_text_any"](r, PI_ASSIGN_KEYS) == "True",
            "can_bring": int(float(ctx["extract_prop_text_any"](r, PI_OWN_COUNT_KEYS) or "0")) >= 1,
            "own_count": ctx["extract_prop_text_any"](r, PI_OWN_COUNT_KEYS) or "0",
        }

    # 既存の持参担当レコード（この練習日）
    existing_by_inst: dict[str, dict] = {}  # instrument_id → {player_id, bring_count, record_id}
    for r in all_pi:
        iids   = ctx["extract_relation_ids_any"](r, PI_INST_REL_KEYS)
        pids   = ctx["extract_relation_ids_any"](r, PI_PLAYER_REL_KEYS)
        pr_ids = ctx["extract_relation_ids_any"](r, PI_PRACTICE_REL_KEYS)
        if not iids or not pids or not pr_ids: continue
        if pr_id not in pr_ids: continue
        if ctx["extract_prop_text_any"](r, PI_BRING_ASSIGN_KEYS) != "True": continue
        existing_by_inst[iids[0]] = {
            "player_id":   pids[0],
            "bring_count": ctx["extract_prop_text_any"](r, PI_BRING_COUNT_KEYS) or "0",
            "record_id":   r.get("id", ""),
        }

    # 楽器種別情報
    inst_rows = _load_instruments(ctx)
    inst_name_map = {i.get("id", ""): _instrument_name(i, ctx) for i in inst_rows}

    # UI構築
    st.subheader(f"{p_label} の持参担当設定")
    NONE_LABEL = "（なし）"
    import pandas as pd

    df_rows:  list[dict] = []
    df_meta:  list[dict] = []

    for iid, parts in sorted(inst_parts.items(), key=lambda x: inst_name_map.get(x[0], x[0])):
        iname     = inst_name_map.get(iid, iid)
        pi_for_inst = pi_by_inst.get(iid, {})

        # アサイン済み奏者を確認
        assigned_pids = [pid for pid, info in pi_for_inst.items() if info["assign"]]
        if not assigned_pids:
            df_rows.append({
                "楽器":     iname,
                "担当者":   "⚠️ 先に楽器担当者をアサインしてください",
                "持参台数": 0,
            })
            df_meta.append({"iid": iid, "iname": iname, "skip": True,
                            "existing": existing_by_inst.get(iid)})
            continue

        # 持参可能な奏者（アサイン済み × 出席○ × 持参可フラグTrue）
        bringable_pids = [
            pid for pid in assigned_pids
            if pid in attending_pids and pi_for_inst.get(pid, {}).get("can_bring", False)  # 所有台数≥1
        ]
        bringable_names = [player_name_map.get(pid, pid) for pid in bringable_pids]
        player_name_to_id = {player_name_map.get(pid, pid): pid for pid in bringable_pids}

        # 既存の担当
        ex = existing_by_inst.get(iid)
        cur_pid   = ex["player_id"]   if ex else ""
        cur_cnt_s = ex["bring_count"] if ex else "0"
        try: cur_cnt = int(float(cur_cnt_s))
        except: cur_cnt = 0
        cur_name  = player_name_map.get(cur_pid, "") if cur_pid else NONE_LABEL
        if cur_name not in bringable_names:
            cur_name = NONE_LABEL

        opts = [NONE_LABEL] + bringable_names

        df_rows.append({
            "楽器":     iname,
            "担当者":   cur_name,
            "持参台数": cur_cnt,
        })
        df_meta.append({
            "iid":              iid,
            "iname":            iname,
            "skip":             False,
            "opts":             opts,
            "player_name_to_id": player_name_to_id,
            "existing":         ex,
            "cur_pid":          cur_pid,
            "cur_cnt":          cur_cnt,
        })

    if not df_rows:
        st.info("表示できる楽器がありません。")
        return

    # 警告行はdisabledで表示、通常行はdata_editorで編集
    # data_editorはSelecboxColumnのoptionsが行ごとに異なる場合に対応できないため
    # 全行共通の選択肢（全出席○かつ持参可の奏者）を使う
    all_bringable = sorted(set(
        name for meta in df_meta if not meta.get("skip", False)
        for name in meta.get("opts", [])
        if name != NONE_LABEL
    ))
    all_opts = [NONE_LABEL] + all_bringable

    df = pd.DataFrame(df_rows)
    edited = st.data_editor(
        df,
        num_rows="fixed",
        use_container_width=True,
        key=f"pb_editor_{c_id}_{pr_id}",
        column_config={
            "楽器":     st.column_config.TextColumn("楽器", disabled=True),
            "担当者":   st.column_config.SelectboxColumn("担当者", options=all_opts, default=NONE_LABEL),
            "持参台数": st.column_config.NumberColumn("持参台数", min_value=0, max_value=20, step=1, default=1),
        },
        disabled=["楽器"],
    )

    # アサイン未設定の行を警告
    for meta in df_meta:
        if meta.get("skip"):
            st.caption(f"⚠️ **{meta['iname']}**：担当者がアサインされていません。アサイン検討画面で設定してください。")

    # 選択した担当者が持参可能でない場合の警告
    df_reset = edited.reset_index(drop=True)
    for idx, meta in enumerate(df_meta):
        if meta.get("skip"): continue
        if idx >= len(df_reset): break
        sel_name = str(df_reset.iloc[idx].get("担当者") or NONE_LABEL).strip()
        if sel_name != NONE_LABEL and sel_name not in meta.get("opts", []):
            st.warning(f"⚠️ {meta['iname']}：{sel_name} は持参可能ではありません。")

    if st.button("💾 まとめて保存", type="primary", use_container_width=True,
                 key=f"pb_save_{c_id}_{pr_id}"):
        ok_n = ng_n = skip_n = 0
        with st.spinner("保存中..."):
            df_reset = edited.reset_index(drop=True)
            for idx, meta in enumerate(df_meta):
                if meta.get("skip"):
                    skip_n += 1
                    continue
                if idx >= len(df_reset): break
                row      = df_reset.iloc[idx]
                sel_name = str(row.get("担当者") or NONE_LABEL).strip()
                new_cnt  = int(row.get("持参台数") or 0)
                new_pid  = meta["player_name_to_id"].get(sel_name, "")
                ex       = meta["existing"]

                no_change = (new_pid == meta["cur_pid"] and new_cnt == meta["cur_cnt"])
                if no_change:
                    skip_n += 1
                    continue

                # 担当なし → 既存レコードをアーカイブ
                if not new_pid:
                    if ex:
                        res = ctx["api_request"]("patch",
                            f"https://api.notion.com/v1/pages/{ex['record_id']}",
                            json={"archived": True})
                        ok_n += 1 if (res and res.status_code == 200) else 0
                        ng_n += 0 if (res and res.status_code == 200) else 1
                    else:
                        skip_n += 1
                    continue

                new_pname = player_name_map.get(new_pid, "")
                ok = _upsert_player_bring_for_concert(
                    ctx, c_id, c_name, new_pid, new_pname,
                    participant_id_by_player_id.get(new_pid, ""),
                    meta["iid"], meta["iname"],
                    True, "", ex["record_id"] if ex else "",
                    own_count=0, bring_assign=True,
                    bring_count=new_cnt,
                    practice_id=pr_id,
                )
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1

        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        for k in list(st.session_state.keys()):
            if k.startswith("pi_practice_"):
                st.session_state.pop(k, None)
        st.rerun()


def _filter_perc_players(ctx, player_ids: list[str], participants: list[dict]) -> list[str]:
    """CONCERT_CASTのパートがPercの奏者のみに絞り込む。未設定の場合は全員対象。"""
    ext = ctx["extract_prop_text_any"]
    part_set = {}
    for row in participants:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            part_set[pids[0]] = (ext(row, PARTICIPANT_PART_KEYS) or "").strip()
    # パートが設定されている奏者がいる場合のみフィルタ
    if any(v for v in part_set.values()):
        return [pid for pid in player_ids
                if part_set.get(pid, "").lower() in ("perc", "percussion", "打楽器", "")]
    return player_ids  # 全員未設定なら全員対象



def render(ctx: dict):
    st.header("🎻 奏者・出欠・持参楽器")
    global_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    t1, t2, t3, t4, t5 = st.tabs(["奏者管理", "出欠入力", "所有楽器整理", "練習日別持参担当", "所有楽器マスタ"])
    with t1:
        _render_player_tab(ctx)
    with t2:
        _render_attendance_tab(ctx)
    with t3:
        st.caption("※ 打楽器パート（Perc）の奏者のみ対象です。")
        _render_assign_tab(ctx)
    with t4:
        st.caption("※ 打楽器パート（Perc）の奏者のみ対象です。")
        _render_practice_bring_tab(ctx)
    with t5:
        _render_pi_master_tab(ctx)
