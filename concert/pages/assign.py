"""
concert.pages.assign
パート割当画面。
  タブ1：希望入力（管理者が転記）
  タブ2：アルゴリズム実行・候補案表示
  タブ3：割当結果確認（マトリクス表示）
"""
import streamlit as st
from concert.services.keys import *  # noqa: F401,F403
from collections import defaultdict
import re


# ============================================================
# 定数
# ============================================================

PRIORITY_OPTIONS = ["未回答", "第1希望", "第2希望", "第3希望", "希望なし/降り番でも可", "NG"]
PRIORITY_TO_INT  = {
    "第1希望": 1,
    "第2希望": 2,
    "第3希望": 3,
    "希望なし/降り番でも可": 0,
    "降り番希望": 0,  # 旧ラベル互換
    "NG": -1,
    "絶対NG": -1,      # 旧ラベル互換
    "なし": None,      # 旧データ互換
}
INT_TO_PRIORITY  = {1: "第1希望", 2: "第2希望", 3: "第3希望", 0: "希望なし/降り番でも可", -1: "NG"}
SCORE_LABEL      = {3.0: "第1希望", 2.0: "第2希望", 1.0: "第3希望", 0.5: "フォールバック", 0.0: "希望なし/降り番でも可", -9999.0: "NG"}


# PLAYER_INSTRUMENT DB用キー（players.pyと共通）


def _norm_prop_key(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip().lower()


def _strip_song_prefix(part_name: str, song_name: str) -> str:
    """パート名から曲名プレフィックスを除去する。
    例: "Japanese Rhapsody / Tam-Tam / Tam-Tam" → "Tam-Tam"
        "Timp. / Timpani" → "Timp. / Timpani"（曲名なし→そのまま）
    同じ名前が重複している場合は1つに統一。
    """
    if not part_name:
        return part_name
    # 曲名プレフィックスを除去
    if song_name and part_name.startswith(song_name):
        part_name = part_name[len(song_name):].lstrip(" /").strip()
    # "A / A" のような重複を除去
    parts = [p.strip() for p in part_name.split("/")]
    seen = []
    for p in parts:
        if p and p not in seen:
            seen.append(p)
    return " / ".join(seen) if seen else part_name


def _find_prop_name_loose(ctx, type_map: dict, candidates: list[str]) -> str:
    key = ctx["find_prop_name"](type_map, candidates)
    if key:
        return key
    if not type_map:
        return ""
    norm_to_actual = {_norm_prop_key(k): k for k in type_map.keys()}
    for c in candidates:
        got = norm_to_actual.get(_norm_prop_key(c), "")
        if got:
            return got
    return ""


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
        filtered = []
        for r in rows:
            medias = _extract_concert_media_labels(r, ctx)
            title_hint = (ctx["extract_prop_text_any"](r, ["名称", "演奏会名", "タイトル"]) or ctx["extract_title"](r) or "")
            if ("出演" in medias) or ("出演" in title_hint):
                filtered.append(r)
        st.session_state["concert_list"] = filtered
    return st.session_state.get("concert_list", [])


def _extract_concert_media_labels(c: dict, ctx) -> list[str]:
    labels = []
    for k in CONCERT_MEDIA_KEYS:
        v = ctx["extract_prop_text"](c, k)
        if v:
            labels.extend([x.strip() for x in str(v).replace("／", "/").split("/") if x.strip()])
    props = (c or {}).get("properties", {}) or {}
    for pname, meta in props.items():
        ptype = (meta or {}).get("type")
        # 媒体っぽい列名は候補に追加
        if ("媒体" not in str(pname)) and ("media" not in str(pname).lower()):
            continue
        if ptype == "select":
            n = ((meta.get("select") or {}).get("name") or "").strip()
            if n:
                labels.append(n)
        elif ptype == "multi_select":
            for it in (meta.get("multi_select") or []):
                n = (it.get("name") or "").strip()
                if n:
                    labels.append(n)
        elif ptype in ("rich_text", "title"):
            txt = "".join((x.get("plain_text") or "") for x in (meta.get(ptype) or [])).strip()
            if txt:
                labels.extend([x.strip() for x in txt.replace("／", "/").split("/") if x.strip()])
    # 重複除去
    return list(dict.fromkeys(labels))


def _select_concert_with_search(ctx, concerts: list[dict], key_prefix: str):
    global_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    global_concert_name = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()
    if global_concert_id:
        if not global_concert_name:
            for c in concerts:
                if c.get("id", "") == global_concert_id:
                    global_concert_name = _concert_name(c, ctx)
                    break
        st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")
        return global_concert_name, global_concert_id

    all_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    q = st.text_input(
        "演奏会を検索",
        key=f"{key_prefix}_concert_search",
        placeholder="例: Happy Hour / 2026 / 定期",
    ).strip().lower()
    opts = {k: v for k, v in all_opts.items() if (not q) or (q in k.lower())}
    if not opts:
        st.warning("検索条件に一致する出演演奏会がありません。")
        return "", ""
    selected = st.selectbox("演奏会を選択", list(opts.keys()), key=f"{key_prefix}_concert_sel")
    return selected, opts.get(selected, "")


def _clear_assign_cache():
    """希望入力・アサイン関連のセッションキャッシュをクリア。"""
    for k in list(st.session_state.keys()):
        if any(k.startswith(p) for p in (
            "pi_list_", "confirmed_rows_", "assign_result_",
            "pref_list_", "participant_list_",
        )):
            st.session_state.pop(k, None)


def _load_players(ctx) -> list[dict]:
    if "player_list" not in st.session_state:
        st.session_state["player_list"] = ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
    return st.session_state.get("player_list", [])


def _load_participants(ctx, concert_id: str, db_id_override: str = "") -> list[dict]:
    db_id = (db_id_override or ctx["CONCERT_DB_PARTICIPANT"]).strip()
    key = f"participant_list_{db_id}_{concert_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](db_id)
        rel = ctx["find_prop_name"](t, PARTICIPANT_CONCERT_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


def _load_songs(ctx, concert_id: str) -> list[dict]:
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
        rel = ctx["find_prop_name"](t, SONG_CONCERT_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_SONG"], f)
    return st.session_state.get(key, [])


def _load_song_instruments(ctx, song_id: str) -> list[dict]:
    key = f"si_list_{song_id}"
    if key not in st.session_state:
        db_id = ctx["CONCERT_DB_PART_DEFINITION"]
        t = ctx["get_prop_types"](db_id)
        rel = ctx["find_prop_name"](t, PARTDEF_SONG_REL_KEYS)
        f = {"filter": {"property": rel, "relation": {"contains": song_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


def _load_player_instruments(ctx, concert_id: str) -> list[dict]:
    """PREFERENCEから演奏会参加者経由で希望データを取得する。
    PREFERENCEには演奏会リレーションがないため、
    先に演奏会参加者IDセットを取得してフィルタする。
    """
    key = f"pi_list_{concert_id}"
    if key not in st.session_state:
        db_id = ctx["CONCERT_DB_PREFERENCE"]
        # 演奏会参加者DBから該当演奏会の参加者IDセットを取得
        participant_rows = ctx["query_all"](
            ctx["CONCERT_DB_PARTICIPANT"],
            {"filter": {"property": ctx["find_prop_name"](
                ctx["get_prop_types"](ctx["CONCERT_DB_PARTICIPANT"]),
                PARTICIPANT_CONCERT_REL_KEYS
            ) or "演奏会", "relation": {"contains": concert_id}}},
        )
        participant_ids = {r.get("id", "") for r in participant_rows if r.get("id")}
        if not participant_ids:
            st.session_state[key] = []
        else:
            # PREFERENCEを全件取得して参加者IDで絞り込む
            all_prefs = ctx["query_all"](db_id)
            t = ctx["get_prop_types"](db_id)
            player_rel = ctx["find_prop_name"](t, PREF_PLAYER_REL_KEYS)
            filtered = []
            for r in all_prefs:
                pids = ctx["extract_relation_ids"](r, player_rel) if player_rel else []
                if pids and pids[0] in participant_ids:
                    filtered.append(r)
            st.session_state[key] = filtered
    return st.session_state.get(key, [])


def _concert_name(c, ctx) -> str:
    n = (
        ctx["extract_prop_text_any"](c, ["名称", "演奏会名", "タイトル", "PK名称"])
        or ctx["extract_title"](c)
    )
    dt = ctx["extract_prop_text_any"](c, CONCERT_DATE_KEYS)
    return f"{n}（{dt[:10] if dt else '日時未設定'}）"


def _player_name(p, ctx) -> str:
    return ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or ctx["extract_title"](p) or p.get("id", "")


def _song_name(s, ctx) -> str:
    return ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or ctx["extract_title"](s) or s.get("id", "")


def _instrument_name(i, ctx) -> str:
    return ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ctx["extract_title"](i) or i.get("id", "")


# ============================================================
# Notionへの希望保存
# ============================================================

def _save_preference(ctx, player_id: str, player_name: str,
                     song_id: str, song_name: str,
                     part_id: str, part_name: str,
                     instrument_id: str, instrument_name: str,
                     priority_int: int,
                     participant_id: str = "",
                     existing_id: str = "") -> bool:
    """PlayerInstrumentに希望順位を書き込む。"""
    db_id    = ctx["CONCERT_DB_PREFERENCE"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        return False
    props: dict = {}
    # PREFERENCEの構造: preference_key, 演奏会参加者, パート定義, 希望順位, 備考
    ctx["put_prop_any"](props, type_map, PREFERENCE_KEY_KEYS,
                        f"{player_name} × {song_name} × {part_name}")
    if participant_id:
        ctx["put_prop_any"](props, type_map, PREF_PLAYER_REL_KEYS, participant_id)
    ctx["put_prop_any"](props, type_map, PREF_PART_REL_KEYS, part_id)
    ctx["put_prop_any"](props, type_map, PREF_PRIORITY_KEYS,
                        INT_TO_PRIORITY.get(priority_int, "希望なし/降り番でも可"))
    ctx["put_key_any"](
        props,
        type_map,
        PREFERENCE_KEY_KEYS,
        player_id,
        song_id,
        part_id,
        prefix="pref",
    )

    if existing_id:
        res = ctx["api_request"]("patch",
                                 f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post",
                                 "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id},
                                       "properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# assign_solver用データ変換
# ============================================================

def _build_solver_input(ctx, concert_id: str, songs: list, players: list):
    """
    NotionのDBからsolve_all()に渡すPrefs/Requirementsを構築する。
    assign_solver.pyのPref/Requirementデータクラスを直接使わず、
    dictで渡してsolve_all内で変換する形にする。
    """
    from concert.services.assign_solver import Pref, Requirement

    # 楽器マスタ
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}

    # Requirements: パート定義DBから構築
    requirements: list[Requirement] = []
    for song in songs:
        sid = song.get("id", "")
        sname = _song_name(song, ctx)
        part_rows = _load_song_instruments(ctx, sid)
        for part in part_rows:
            iids = ctx["extract_relation_ids_any"](part, PARTDEF_INST_REL_KEYS)
            iid   = iids[0] if iids else ""
            iname = inst_name_map.get(iid, iid) if iid else ""
            qty_str = ctx["extract_prop_text_any"](part, ["必要台数", "必要人数", "台数", "人数"])
            try:
                qty = max(int(float(qty_str)), 1) if qty_str else 1
            except ValueError:
                qty = 1
            note = ctx["extract_prop_text_any"](part, PARTDEF_NOTE_KEYS) or ""
            part_id   = part.get("id", "")
            pnm = ctx["extract_prop_text_any"](part, PARTDEF_NAME_KEYS) or iname or "Part"
            pnm = _strip_song_prefix(pnm, sname)
            part_name = f"{pnm}（{note}）" if note else pnm
            requirements.append(Requirement(
                song_id=sid, song_name=sname,
                part_id=part_id, part_name=part_name,
                instrument_id=iid, instrument_name=iname,
                required_count=qty,
            ))

    # 参加者DB（演奏会参加者ID -> 奏者ID）マップ
    participant_rows = _load_participants(ctx, concert_id)
    participant_to_player: dict[str, str] = {}
    for row in participant_rows:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            participant_to_player[row.get("id", "")] = pids[0]

    # Prefs: 希望入力DBの希望順位から構築（パート単位）
    # PREFERENCEには演奏曲リレーションがないため、パート定義→演奏曲の逆引きマップを使う
    prefs: list[Pref] = []
    pi_rows = _load_player_instruments(ctx, concert_id)

    player_name_map = {p.get("id"): _player_name(p, ctx) for p in players}
    song_name_map   = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_id_set     = {s.get("id") for s in songs}

    # パート定義→演奏曲の逆引きマップ（重複を避けるため再構築）
    _partdef_to_song: dict[str, str] = {}
    _partdef_to_inst: dict[str, str] = {}
    for song in songs:
        _sid = song.get("id", "")
        for part in _load_song_instruments(ctx, _sid):
            _pid = part.get("id", "")
            _partdef_to_song[_pid] = _sid
            iids = ctx["extract_relation_ids_any"](part, PARTDEF_INST_REL_KEYS)
            if iids:
                _partdef_to_inst[_pid] = iids[0]

    for pi in pi_rows:
        player_ids = ctx["extract_relation_ids_any"](pi, PREF_PLAYER_REL_KEYS)
        part_ids   = ctx["extract_relation_ids_any"](pi, PREF_PART_REL_KEYS)
        if not (player_ids and part_ids):
            continue
        pid_raw = player_ids[0]
        pid     = participant_to_player.get(pid_raw, pid_raw)
        part_id = part_ids[0]
        sid     = _partdef_to_song.get(part_id, "")
        iid     = _partdef_to_inst.get(part_id, "")
        if not sid or sid not in song_id_set:
            continue

        priority_str = ctx["extract_prop_text_any"](pi, PREF_PRIORITY_KEYS)
        priority_int = PRIORITY_TO_INT.get(priority_str)
        if priority_int is None:
            continue  # 「なし」はスキップ

        # part_idは希望入力DBのrelationを正とする
        matching_reqs = [r for r in requirements if r.song_id == sid and r.part_id == part_id]
        part_name = matching_reqs[0].part_name if matching_reqs else part_id

        prefs.append(Pref(
            player_id=pid,
            player_name=player_name_map.get(pid, pid),
            song_id=sid,
            song_name=song_name_map.get(sid, sid),
            part_id=part_id,
            part_name=part_name,
            instrument_id=iid,
            instrument_name=inst_name_map.get(iid, iid) if iid else "",
            priority=priority_int,
            can_bring=False,  # 持参可フラグはスコア計算から除外済み
        ))

    return prefs, requirements


# ============================================================
# タブ1：希望入力
# ============================================================

def _render_pref_tab(ctx: dict):
    st.caption("管理者がアンケート結果を転記する画面です。奏者×曲×楽器ごとに希望順位を選択して保存してください。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("先に演奏会を登録してください。")
        return

    selected_concert, concert_id = _select_concert_with_search(ctx, concerts, "pref")
    if not concert_id:
        return

    players = _load_players(ctx)
    songs   = _load_songs(ctx, concert_id)
    if not players:
        st.info("奏者を先に登録してください。")
        return
    if not songs:
        st.info("この演奏会に楽曲が登録されていません。")
        return

    # 演奏会参加者DBに紐づいた奏者のみを希望入力対象にする
    participant_rows = _load_participants(ctx, concert_id)
    player_ids_in_concert: set[str] = set()
    participant_id_by_player_id: dict[str, str] = {}
    participant_to_player: dict[str, str] = {}
    for row in participant_rows:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if not pids:
            continue
        pid = pids[0]
        player_ids_in_concert.add(pid)
        participant_id_by_player_id[pid] = row.get("id", "")
        participant_to_player[row.get("id", "")] = pid

    if not player_ids_in_concert:
        st.info("この演奏会の参加者が未登録です。先に「出欠入力」で参加者を保存してください。")
        return

    players = [p for p in players if p.get("id", "") in player_ids_in_concert]
    if not players:
        st.info("この演奏会に紐づく奏者が見つかりませんでした。参加者DBのリレーションを確認してください。")
        return

    # パートフィルタ：CONCERT_CASTからパート一覧を取得して選択
    part_by_player: dict[str, str] = {}
    for row in participant_rows:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            part_by_player[pids[0]] = ctx["extract_prop_text_any"](row, PARTICIPANT_PART_KEYS) or ""
    all_parts = sorted({p for p in part_by_player.values() if p})
    if all_parts:
        selected_part = st.selectbox(
            "パートを選択", all_parts,
            key="pref_part_filter",
            help="選択したパートの奏者のみ希望入力・入力状況に表示されます。"
        )
        players = [p for p in players
                   if part_by_player.get(p.get("id",""), "") == selected_part]
        if not players:
            st.info(f"「{selected_part}」の参加者が見つかりません。")
            return

    # 楽器マスタ
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}

    # パート定義→演奏曲の逆引きマップを構築
    # PREFERENCEには演奏曲リレーションがないため、パート定義IDから演奏曲IDを引く
    partdef_to_song: dict[str, str] = {}
    for song in songs:
        sid = song.get("id", "")
        for part in _load_song_instruments(ctx, sid):
            partdef_to_song[part.get("id", "")] = sid

    # 既存の希望入力を取得（奏者×パート定義 → レコード）
    pi_rows = _load_player_instruments(ctx, concert_id)
    pi_lookup: dict[tuple, dict] = {}  # (player_id, song_id, part_id) → row
    for pi in pi_rows:
        pids     = ctx["extract_relation_ids_any"](pi, PREF_PLAYER_REL_KEYS)
        part_ids = ctx["extract_relation_ids_any"](pi, PREF_PART_REL_KEYS)
        if pids and part_ids:
            pid_raw = pids[0]
            pid     = participant_to_player.get(pid_raw, pid_raw)
            part_id = part_ids[0]
            sid     = partdef_to_song.get(part_id, "")
            if sid:
                pi_lookup[(pid, sid, part_id)] = pi

    # 入力状況サマリー（奏者 × 演奏曲）
    song_part_ids: dict[str, set[str]] = {}
    for song in songs:
        sid = song.get("id", "")
        req_rows = _load_song_instruments(ctx, sid)
        song_part_ids[sid] = {r.get("id", "") for r in req_rows if r.get("id", "")}
    per_player_song_parts: dict[tuple[str, str], set[str]] = defaultdict(set)
    for (pid, sid, part_id), _row in pi_lookup.items():
        if pid and sid and part_id:
            per_player_song_parts[(pid, sid)].add(part_id)

    with st.expander("📊 入力状況（奏者 × 演奏曲）", expanded=False):
        status_song_opts = {"（全演奏曲）": ""}
        for s in sorted(songs, key=lambda x: _song_name(x, ctx)):
            status_song_opts[_song_name(s, ctx)] = s.get("id", "")
        selected_status_song_label = st.selectbox(
            "演奏曲を指定",
            list(status_song_opts.keys()),
            key="pref_status_song_sel",
        )
        selected_status_song_id = status_song_opts.get(selected_status_song_label, "")

        summary_rows = []
        for p in sorted(players, key=lambda x: _player_name(x, ctx)):
            pid = p.get("id", "")
            pname = _player_name(p, ctx)
            if selected_status_song_id:
                total = len(song_part_ids.get(selected_status_song_id, set()))
                done = len(per_player_song_parts.get((pid, selected_status_song_id), set()))
                if total == 0:
                    status = "パート未定義"
                elif done > 0:
                    status = f"入力済 ({done}/{total})"
                else:
                    status = "未入力"
                summary_rows.append({
                    "奏者": pname,
                    "演奏曲": selected_status_song_label,
                    "状態": status,
                })
            else:
                total_songs = sum(1 for s in songs if len(song_part_ids.get(s.get("id", ""), set())) > 0)
                done_songs = 0
                pending_song_names = []
                for s in songs:
                    sid = s.get("id", "")
                    req = song_part_ids.get(sid, set())
                    done = per_player_song_parts.get((pid, sid), set())
                    if len(req) == 0:
                        continue
                    if len(done) > 0:
                        done_songs += 1
                    else:
                        pending_song_names.append(_song_name(s, ctx))
                if total_songs == 0:
                    status = "対象なし"
                elif done_songs == total_songs:
                    status = f"入力済 ({done_songs}/{total_songs})"
                else:
                    status = f"未完了 ({done_songs}/{total_songs})"
                summary_rows.append({
                    "奏者": pname,
                    "状態": status,
                    "未入力の演奏曲": " / ".join(pending_song_names) if pending_song_names else "—",
                })

        st.dataframe(summary_rows, use_container_width=True, hide_index=True)

    player_opts = {_player_name(p, ctx): p.get("id", "") for p in
                   sorted(players, key=lambda x: _player_name(x, ctx))}
    col_sel, col_r = st.columns([8, 1])
    selected_player_name = col_sel.selectbox("奏者を選択", list(player_opts.keys()), key="pref_player_sel")
    if col_r.button("🔄", key="pref_refresh", help="パート定義・希望データを再読み込み"):
        for k in list(st.session_state.keys()):
            if k.startswith("si_list_") or k.startswith("pi_list_"):
                st.session_state.pop(k, None)
        st.rerun()
    player_id = player_opts.get(selected_player_name, "")
    if not player_id:
        return

    st.subheader(f"希望入力：{selected_player_name}")

    import pandas as pd

    # 全曲のパートをまとめて1つのdata_editorに表示
    all_pref_rows: list[dict] = []
    all_pref_meta: list[dict] = []

    for song in sorted(songs, key=lambda x: _song_name(x, ctx)):
        sid   = song.get("id", "")
        sname = _song_name(song, ctx)
        si_rows = _load_song_instruments(ctx, sid)
        if not si_rows:
            continue
        for si_idx, si in enumerate(si_rows):
            part_id = si.get("id", "")
            iids  = ctx["extract_relation_ids_any"](si, PARTDEF_INST_REL_KEYS)
            iid   = iids[0] if iids else ""
            iname = inst_name_map.get(iid, iid) if iid else ""
            note  = ctx["extract_prop_text_any"](si, PARTDEF_NOTE_KEYS) or ""
            pname = ctx["extract_prop_text_any"](si, PARTDEF_NAME_KEYS) or iname or "Part"
            pname = _strip_song_prefix(pname, sname)
            label = f"{pname}（{note}）" if note else pname

            existing = pi_lookup.get((player_id, sid, part_id))
            has_record = existing is not None
            cur_p = ctx["extract_prop_text_any"](existing, PREF_PRIORITY_KEYS) if existing else "未回答"
            if cur_p not in PRIORITY_OPTIONS:
                cur_p = "未回答"
            status = "" if has_record else "🔴"

            all_pref_rows.append({"状態": status, "曲": sname, "パート": label, "希望": cur_p})
            all_pref_meta.append({
                "sid": sid, "sname": sname,
                "part_id": part_id, "part_name": pname,
                "iid": iid, "iname": iname,
                "existing_id": existing.get("id", "") if existing else "",
                "cur_p": cur_p,
            })

    if not all_pref_rows:
        st.info("パート定義が登録されていません。先に楽曲・楽器管理でパート定義を行ってください。")
        return

    df_pref = pd.DataFrame(all_pref_rows)
    edited_pref = st.data_editor(
        df_pref,
        num_rows="fixed",
        use_container_width=True,
        key=f"pref_editor_{player_id}_{concert_id}",
        column_config={
            "状態": st.column_config.TextColumn("状態", disabled=True, width="small"),
            "曲":   st.column_config.TextColumn("曲", disabled=True),
            "パート": st.column_config.TextColumn("パート", disabled=True),
            "希望": st.column_config.SelectboxColumn(
                "希望", options=PRIORITY_OPTIONS,
                required=True, default="未回答",
            ),
        },
    )
    unanswered = sum(1 for r in all_pref_meta if not r["existing_id"])
    if unanswered:
        st.caption(f"🔴 未回答 {unanswered}件")

    if st.button("💾 まとめて保存", use_container_width=True, type="primary",
                 key=f"pref_save_{player_id}"):
        ok_count = fail_count = 0
        with st.spinner("保存中..."):
            df_reset = edited_pref.reset_index(drop=True)
            for idx, meta in enumerate(all_pref_meta):
                if idx >= len(df_reset): break
                row = df_reset.iloc[idx]
                new_p = str(row.get("希望") or "希望なし/降り番でも可").strip()
                pri_int = PRIORITY_TO_INT.get(new_p)

                # 変更なしはスキップ
                if new_p == meta["cur_p"]:
                    continue

                # 「未回答」はスキップ（既存レコードがあればアーカイブ）
                if new_p == "未回答":
                    if meta["existing_id"]:
                        res = ctx["api_request"](
                            "patch",
                            f"https://api.notion.com/v1/pages/{meta['existing_id']}",
                            json={"archived": True},
                        )
                        ok_count += 1 if (res and res.status_code == 200) else 0
                        fail_count += 0 if (res and res.status_code == 200) else 1
                    continue
                if pri_int is None and meta["existing_id"]:
                    # 「なし」に変更 → アーカイブ
                    res = ctx["api_request"](
                        "patch",
                        f"https://api.notion.com/v1/pages/{meta['existing_id']}",
                        json={"archived": True},
                    )
                    ok_count += 1 if (res and res.status_code == 200) else 0
                    fail_count += 0 if (res and res.status_code == 200) else 1
                    continue
                if pri_int is None:
                    continue

                ok = _save_preference(
                    ctx, player_id, selected_player_name,
                    meta["sid"], meta["sname"],
                    meta["part_id"], meta["part_name"],
                    meta["iid"], meta["iname"],
                    pri_int,
                    participant_id_by_player_id.get(player_id, ""),
                    meta["existing_id"],
                )
                ok_count += 1 if ok else 0
                fail_count += 0 if ok else 1

        if fail_count == 0:
            st.success(f"✅ {ok_count}件を保存しました。")
            st.session_state.pop(f"pi_list_{concert_id}", None)
        else:
            st.warning(f"⚠️ {ok_count}件成功、{fail_count}件失敗。")
        st.rerun()


# ============================================================
# タブ2：アルゴリズム実行
# ============================================================

def _render_solver_tab(ctx: dict):
    st.caption("希望入力が完了したら、アルゴリズムを実行して候補案を生成します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    selected_concert, concert_id = _select_concert_with_search(ctx, concerts, "solver")
    if not concert_id:
        return

    players = _load_players(ctx)
    songs   = _load_songs(ctx, concert_id)
    if not players or not songs:
        st.info("奏者・楽曲が登録されていません。")
        return

    solve_mode = st.radio(
        "解法モード",
        ["⚡ 高速モード（貪欲法＋局所探索）", "🎯 厳密モード（整数計画法・小規模向け）"],
        index=0, horizontal=True,
        help="厳密モードは数学的に最適な解を求めます。目安：奏者×パート数が200以下なら数秒以内。",
        key=f"solve_mode_{concert_id}",
    )
    use_exact = "厳密モード" in solve_mode

    if st.button("▶ 割当を実行", type="primary", key=f"run_solver_{concert_id}"):
        with st.spinner("アルゴリズム実行中..."):
            try:
                from concert.services.assign_solver import (
                    greedy_solve, local_search, _calc_stats, _build_absent_set,
                    _first_choice_rate, _total_score, score_assignment,
                    _min_player_score, _bring_count, _rest_std,
                    solve_exact,
                )
                absent   = _build_absent_set(ctx, concert_id)
                prefs, requirements = _build_solver_input(ctx, concert_id, songs, players)
                if not prefs:
                    st.warning("希望データがありません。先に希望入力を行ってください。")
                    return
                if not requirements:
                    st.warning("必要楽器が登録されていません。楽曲・楽器管理から登録してください。")
                    return

                # 参加者DB全員リスト（希望未提出も含む）をfallback候補に
                # ※ all_pidsより先に定義する必要がある
                _all_cast = _load_participants(ctx, concert_id)
                _part_to_pl = {}
                for _r in _all_cast:
                    _pids = ctx["extract_relation_ids_any"](_r, PARTICIPANT_PLAYER_REL_KEYS)
                    if _pids:
                        _pid = _pids[0]
                        _pobj = next((p for p in players if p.get("id","") == _pid), None)
                        _pname = _player_name(_pobj, ctx) if _pobj else ""
                        # playersリストにない場合はPERFORMER DBから直接取得
                        if not _pname:
                            _all_pl = _load_players(ctx)
                            _pobj2 = next((p for p in _all_pl if p.get("id","") == _pid), None)
                            _pname = _player_name(_pobj2, ctx) if _pobj2 else _pid
                        _part_to_pl[_pid] = _pname
                _all_participants = sorted(_part_to_pl.items(), key=lambda x: x[1])

                # 公平性・降り番評価は参加者DB全員（希望未提出も含む）を対象に
                all_pids = sorted({pid for pid, _ in _all_participants} |
                                  {p.player_id for p in prefs})
                st.session_state[f"assign_all_pids_{concert_id}"] = all_pids
                pref_map = {(p.player_id, p.song_id, p.part_id): p for p in prefs}
                # 複数初期解で最良を採用（走査順ランダム化）
                def _total_sc_v(sol):
                    return sum(max(score_assignment(a, pref_map), 0) for a in sol)
                base = greedy_solve(prefs, requirements, absent, _all_participants)
                _best_sc = _total_sc_v(base)
                for _seed in range(1, 8):
                    _cand = greedy_solve(prefs, requirements, absent,
                                         _all_participants, shuffle_seed=_seed)
                    _cand_sc = _total_sc_v(_cand)
                    if _cand_sc > _best_sc:
                        base = _cand
                        _best_sc = _cand_sc

                def obj_a(sol):
                    return _first_choice_rate(sol, pref_map) * 10000 + _total_score(sol, pref_map)
                def obj_b(sol):
                    return _total_score(sol, pref_map)
                def obj_c(sol):
                    return _min_player_score(sol, pref_map, all_pids) * 1000 + _total_score(sol, pref_map)
                def obj_d(sol):
                    return int((1000 - _rest_std(sol, all_pids) * 100) * 100) + _total_score(sol, pref_map)

                if use_exact:
                    with st.spinner("厳密解を計算中... しばらくお待ちください"):
                        results = solve_exact(
                            prefs, requirements, absent,
                            all_player_ids=all_pids,
                            time_limit_sec=60.0,
                        )
                    # player_name補完
                    _name_map_e = {p.get("id",""): _player_name(p, ctx) for p in players}
                    for _pid, _pname in _all_participants:
                        if _pid not in _name_map_e or not _name_map_e[_pid]:
                            _name_map_e[_pid] = _pname
                    for _r in results:
                        for _a in _r["assignments"]:
                            if not _a.get("player_name") or _a["player_name"] == _a["player_id"]:
                                _a["player_name"] = _name_map_e.get(_a["player_id"], _a["player_id"])
                        _r["pref_map"] = {str(k): v.__dict__ for k, v in pref_map.items()}
                    if not results:
                        st.error("❌ 厳密解の計算に失敗しました。高速モードをお試しください。")
                        return
                else:
                    variants = [
                        ("候補A：第1希望率最大", obj_a),
                        ("候補B：総スコア最大",  obj_b),
                        ("候補C：公平性重視",    obj_c),
                        ("候補D：降り番均等",    obj_d),
                    ]
                    _name_map = {p.get("id",""): _player_name(p, ctx) for p in players}
                    for _pid, _pname in _all_participants:
                        if _pid not in _name_map or not _name_map[_pid]:
                            _name_map[_pid] = _pname
                    results = []
                    for label, fn in variants:
                        _ls_ret = local_search(base, pref_map, fn, max_iter=250,
                                                absent_players=absent, all_player_ids=all_pids,
                                                verbose=True)
                        sol, _ls_log = _ls_ret if isinstance(_ls_ret, tuple) else (_ls_ret, {})
                        stats = _calc_stats(sol, pref_map, all_pids)
                        stats["_ls_log"] = _ls_log  # 改善ログをstatsに付帯
                        # player_nameがIDになっている場合をplayer_name_mapで補完
                        _assignments_fixed = []
                        for _a in sol:
                            _d = _a.__dict__.copy()
                            if not _d.get("player_name") or _d["player_name"] == _d["player_id"]:
                                _d["player_name"] = _name_map.get(_d["player_id"], _d["player_id"])
                            _assignments_fixed.append(_d)
                        results.append({
                            "label":       label,
                            "assignments": _assignments_fixed,
                            "stats":       stats,
                            "pref_map":    {str(k): v.__dict__ for k, v in pref_map.items()},
                        })
                st.session_state[f"assign_result_{concert_id}"] = results
                # モード別に保存（検証用に両方保持）
                if use_exact:
                    st.session_state[f"assign_result_exact_{concert_id}"] = results
                else:
                    st.session_state[f"assign_result_heuristic_{concert_id}"] = results
                st.success("✅ 候補案を生成しました。")
            except Exception as e:
                import traceback
                st.error(f"❌ 実行エラー：{e}")
                st.code(traceback.format_exc())
                return

    results = st.session_state.get(f"assign_result_{concert_id}")
    if not results:
        return

    st.divider()

    # デバッグ：pref_mapの中身確認
    with st.expander("🔍 pref_mapデバッグ（希望データ確認）", expanded=False):
        if results:
            pm = results[0]["pref_map"]
            st.caption(f"登録された希望件数: {len(pm)}件")
            for k, v in list(pm.items())[:20]:
                st.caption(f"`{k}` → player:{v.get('player_name','?')} priority:{v.get('priority','?')}")

    # 検証：共通再採点で解の品質を比較
    with st.expander("🔬 解の検証（共通再採点）", expanded=False):
        try:
            from concert.services.verify_results import verify
            _v_pids = st.session_state.get(f"assign_all_pids_{concert_id}")
            _h_results = st.session_state.get(f"assign_result_heuristic_{concert_id}", [])
            _e_results = st.session_state.get(f"assign_result_exact_{concert_id}", [])

            def _show_verify(label, rlist, color):
                if not rlist:
                    return
                st.markdown(f"**{label}**")
                for r in rlist:
                    v = verify(r["assignments"], r["pref_map"], _v_pids)
                    st.markdown(f"<span style='color:{color}'>{r['label']}</span>",
                                unsafe_allow_html=True)
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("総スコア",    f"{v['total_score']:.1f}")
                    col2.metric("第1希望本数", f"{v['first_choice_count']}件")
                    col3.metric("第1希望率",   f"{v['first_choice_rate']:.1%}")
                    col4.metric("最低スコア",  f"{v['min_player_score']:.1f}")
                    # 局所探索改善ログ
                    ls_log = r.get("stats", {}).get("_ls_log", {})
                    if ls_log:
                        st.caption(
                            f"局所探索: {ls_log.get('total_iterations',0)}回反復 / "
                            f"改善{ls_log.get('total_improvements',0)}回 "
                            f"（N1:{ls_log.get('n1_swap_count',0)} "
                            f"N2:{ls_log.get('n2_replace_count',0)} "
                            f"N3:{ls_log.get('n3_crosssong_count',0)} "
                            f"N4:{ls_log.get('n4_uplift_count',0)}）"
                            f"　最終目的値: {ls_log.get('final_objective',0):.2f}"
                        )

            if not _h_results and not _e_results:
                st.caption("候補案を生成すると検証結果が表示されます。")
            _show_verify("⚡ ヒューリスティック解", _h_results, "#1A3D7C")
            if _h_results and _e_results:
                st.divider()
            _show_verify("🎯 厳密解", _e_results, "#5A1A7C")
        except Exception as _ve:
            st.warning(f"検証エラー: {_ve}")

    # PDFダウンロードボタン
    col_title, col_pdf = st.columns([6, 2])
    col_title.subheader("候補案比較")
    try:
        from concert.services.report import generate_assign_report
        concert_name = ctx.get("SELECTED_CONCERT_NAME", "演奏会")
        pdf_bytes = generate_assign_report(concert_name, results, songs, players, ctx)
        col_pdf.download_button(
            "📄 PDF出力",
            data=pdf_bytes,
            file_name=f"アサイン候補案_{concert_name}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    except Exception as e:
        col_pdf.caption(f"PDF生成エラー: {e}")

    # サマリーカード
    cols = st.columns(len(results))
    for i, (col, r) in enumerate(zip(cols, results)):
        st._arrow = None
        s = r["stats"]
        col.metric(r["label"].split("：")[1],
                   f"{s['total_score']:.1f}点",
                   f"第1希望率 {s['first_choice_rate']*100:.0f}%")

    st.divider()

    # 各候補の詳細
    tabs = st.tabs([r["label"] for r in results])
    song_name_map = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_order    = [s.get("id") for s in sorted(songs, key=lambda x: _song_name(x, ctx))]

    CANDIDATE_DESC = {
        "候補A：第1希望率最大": (
            "第1希望が叶う人数を最大化する案。"
            "「絶対にこれがやりたい」という強い希望をできるだけ通す。"
            "第2・第3希望は後回しになりやすい。"
        ),
        "候補B：総スコア最大": (
            "全員の希望スコア合計を最大化する案。"
            "第1希望×3点・第2希望×2点・第3希望×1点の総和を最大にする。"
            "全体として最も「満足度の総量」が高い割当。"
        ),
        "候補C：公平性重視": (
            "最も不満な人のスコアを底上げする案。"
            "「誰か一人が割を食う」状況を避けることを優先する。"
            "希望を出したのに乗れなかった曲数（希望不成立）も最小化しようとする。"
        ),
        "候補D：降り番均等": (
            "降り番（割当なし）の偏りを最小化する案。"
            "特定の人だけ多くの曲で降り番にならないよう、"
            "割当件数の標準偏差を小さくすることを優先する。"
        ),
        "候補E：降り番均等": (
            "降り番（割当なし）の偏りを最小化する案。"
            "特定の人だけ多くの曲で降り番にならないよう、"
            "割当件数の標準偏差を小さくすることを優先する。"
        ),
    }

    for tab, result in zip(tabs, results):
        with tab:
            desc = CANDIDATE_DESC.get(result["label"], "")
            if desc:
                st.caption(desc)
            s = result["stats"]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("総スコア",   f"{s['total_score']:.1f}点")
            m2.metric("第1希望率",  f"{s['first_choice_rate']*100:.1f}%")
            m3.metric("最低スコア", f"{s['min_score']:.1f}点")
            m4.metric("FB件数",     f"{sum(1 for a in result['assignments'] if a['source'] in ('fallback','swap'))}件")

            # 曲ごとの割当（HTML表示）
            by_song: dict[str, list] = defaultdict(list)
            for a in result["assignments"]:
                by_song[a["song_id"]].append(a)

            for sid in song_order:
                items = by_song.get(sid, [])
                if not items:
                    continue
                st.markdown(f"**{song_name_map.get(sid, sid)}**")
                st.markdown(_render_assignment_html(items, result["pref_map"]),
                            unsafe_allow_html=True)

            # 奏者別スコア（割り当てられなかった人も含めて表示）
            st.markdown("**奏者別スコア**")
            player_scores: dict[str, float]   = defaultdict(float)
            player_fb: dict[str, int]          = defaultdict(int)
            player_names: dict[str, str]       = {}
            player_unassigned: dict[str, int]  = defaultdict(int)

            # まず全奏者の名前をplayersリストから初期化（希望未提出者も含む）
            for _p in players:
                _pid = _p.get("id","")
                if _pid:
                    player_names[_pid] = _player_name(_p, ctx)
            # _all_participantsにある名前も補完
            for _pid, _pname in _all_participants:
                if _pid and _pname and _pid not in player_names:
                    player_names[_pid] = _pname

            # 割当済みスコアと曲セットを集計
            assigned_songs_per_player: dict[str, set] = defaultdict(set)
            for a in result["assignments"]:
                pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                pref = result["pref_map"].get(pk)
                sc   = ({1:3.0,2:2.0,3:1.0}.get(pref["priority"],0.0)
                        if pref and pref["priority"] > 0 else 0.5)
                player_scores[a["player_id"]] += sc
                if a["source"] in ("fallback", "swap"):
                    player_fb[a["player_id"]] += 1
                player_names[a["player_id"]] = a["player_name"]
                assigned_songs_per_player[a["player_id"]].add(a["song_id"])

            # 希望不成立：希望を出した曲のうち割り当てられなかった曲数（曲単位）
            wanted_songs_per_player: dict[str, set] = defaultdict(set)
            for pk_str, pref in result["pref_map"].items():
                if pref["priority"] <= 0:
                    continue
                pid = pref["player_id"]
                wanted_songs_per_player[pid].add(pref["song_id"])
                if pid not in player_names:
                    player_names[pid] = pref["player_name"]

            for pid, wanted in wanted_songs_per_player.items():
                unmet = len(wanted - assigned_songs_per_player.get(pid, set()))
                if unmet > 0:
                    player_unassigned[pid] = unmet

            # 希望を出した奏者全員を表示対象に
            for pk_str, pref in result["pref_map"].items():
                if pref["priority"] > 0 and pref["player_id"] not in player_names:
                    player_names[pref["player_id"]] = pref["player_name"]

            st.markdown(_render_player_score_html(
                player_scores, player_fb, player_names, player_unassigned),
                unsafe_allow_html=True)

            # 採用ボタン
            st.divider()
            if st.button(f"✅ この案を採用してNotionに書き込む",
                         key=f"adopt_{result['label']}", type="primary"):
                _write_assignments_to_notion(ctx, result["assignments"], result["pref_map"])


def _render_assignment_html(items: list[dict], pref_map: dict) -> str:
    """曲ごとの割当をHTMLカード形式で返す。"""
    BADGE = {
        "第1希望":      ("background:#EEEDFE;color:#3C3489", "第1希望"),
        "第2希望":      ("background:#E1F5EE;color:#085041", "第2希望"),
        "第3希望":      ("background:#FAEEDA;color:#633806", "第3希望"),
        "フォールバック":("background:#FCEBEB;color:#A32D2D", "FB"),
        "希望なし/降り番でも可":   ("background:#F1EFE8;color:#5F5E5A", "降り番"),
    }
    rows_html = ""
    for a in items:
        pk   = str((a["player_id"], a["song_id"], a["part_id"]))
        pref = pref_map.get(pk)
        if pref and pref["priority"] > 0:
            hope = INT_TO_PRIORITY.get(pref["priority"], "—")
            sc   = {1: 3.0, 2: 2.0, 3: 1.0}.get(pref["priority"], 0.0)
        elif pref and pref["priority"] == 0:
            hope = "希望なし/降り番でも可"
            sc   = 0.0
        elif a["source"] == "fallback":
            hope = "フォールバック"
            sc   = 0.5
        elif a["source"] in ("swap", "exact"):
            hope = "補完"
            sc   = 0.5
        else:
            hope = "希望なし/降り番でも可"
            sc   = 0.0

        badge_style, badge_text = BADGE.get(hope, ("background:#F1EFE8;color:#5F5E5A", hope))
        sc_color = "#3C3489" if sc >= 3.0 else ("#085041" if sc >= 2.0 else
                   ("#633806" if sc >= 1.0 else "#A32D2D" if sc > 0 else "#888780"))

        # 同点タイブレーク発生時のハイライト
        is_tied = a.get("tied", False)
        tied_candidates = a.get("tied_candidates", [])
        row_style = "background:rgba(250,238,218,0.3);" if is_tied else ""
        tied_badge = ""
        if is_tied and tied_candidates:
            others = " / ".join(tied_candidates)
            tied_badge = (f'<span style="font-size:10px;padding:2px 6px;border-radius:99px;'
                         f'background:#FAEEDA;color:#633806;margin-left:6px;" '
                         f'title="同点候補: {others}">⚠️ 同点</span>')

        rows_html += f"""
        <tr style="{row_style}">
          <td style="padding:7px 12px;border-bottom:1px solid rgba(0,0,0,0.12);
                     font-size:13px;color:var(--color-text-primary);">
            {a["player_name"]}{tied_badge}
          </td>
          <td style="padding:7px 12px;border-bottom:1px solid rgba(0,0,0,0.12);
                     font-size:13px;color:var(--color-text-secondary);">{a["part_name"]}</td>
          <td style="padding:7px 12px;border-bottom:1px solid rgba(0,0,0,0.12);">
            <span style="font-size:11px;padding:2px 8px;border-radius:99px;
                         {badge_style}">{badge_text}</span>
          </td>
          <td style="padding:7px 12px;border-bottom:1px solid rgba(0,0,0,0.12);
                     font-size:13px;font-weight:500;color:{sc_color};
                     text-align:right;">{sc:.1f}</td>
        </tr>"""

    return f"""
<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;margin-bottom:1rem;">
<div style="background:var(--color-background-primary);
            border:0.5px solid rgba(0,0,0,0.1);
            border-radius:10px;overflow:hidden;min-width:380px;">
  <table style="width:100%;border-collapse:collapse;">
    <thead>
      <tr style="background:rgba(100,90,180,0.10);">
        <th style="padding:6px 12px;font-size:11px;font-weight:500;
                   color:var(--color-text-secondary);text-align:left;white-space:nowrap;">奏者</th>
        <th style="padding:6px 12px;font-size:11px;font-weight:500;
                   color:var(--color-text-secondary);text-align:left;">パート</th>
        <th style="padding:6px 12px;font-size:11px;font-weight:500;
                   color:var(--color-text-secondary);text-align:left;white-space:nowrap;">希望</th>
        <th style="padding:6px 12px;font-size:11px;font-weight:500;
                   color:var(--color-text-secondary);text-align:right;">点数</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
</div>"""


def _render_player_score_html(scores: dict, fb_counts: dict, names: dict,
                              unassigned: dict | None = None) -> str:
    """奏者別スコアをHTMLカード形式で返す。割当なしの人も含めて全員表示。"""
    all_pids = set(scores.keys()) | set(names.keys())
    sorted_players = sorted(all_pids, key=lambda pid: -scores.get(pid, 0))
    rows_html = ""
    for pid in sorted_players:
        sc     = scores.get(pid, 0.0)
        name   = names.get(pid, pid)
        fb     = fb_counts.get(pid, 0)
        ua     = (unassigned or {}).get(pid, 0)
        fb_str = (f'<span style="font-size:11px;padding:2px 7px;border-radius:99px;'
                  f'background:#FCEBEB;color:#A32D2D;margin-left:6px;">FB {fb}件</span>'
                  if fb > 0 else "")
        ua_str = (f'<span style="font-size:11px;padding:2px 7px;border-radius:99px;'
                  f'background:#FAEEDA;color:#633806;margin-left:6px;">希望不成立 {ua}曲</span>'
                  if ua > 0 else "")
        bar_w  = int(sc / 9.0 * 100)
        sc_color = "#3C3489" if sc >= 7 else ("#085041" if sc >= 4 else "#888780")
        # 全曲降り番（割当なし・希望なし）の場合は「降り番」バッジ
        is_rest = sc == 0.0 and fb == 0 and pid not in scores
        rest_str = ('<span style="font-size:11px;padding:2px 7px;border-radius:99px;'
                    'background:#F1EFE8;color:#5F5E5A;margin-left:6px;">降り番</span>'
                    if is_rest else "")
        rows_html += f"""
        <tr>
          <td style="padding:7px 12px;border-bottom:1px solid rgba(0,0,0,0.12);
                     font-size:13px;color:var(--color-text-primary);white-space:nowrap;">
            {name}{fb_str}{ua_str}{rest_str}
          </td>
          <td style="padding:7px 12px;border-bottom:1px solid rgba(0,0,0,0.12);">
            <div style="display:flex;align-items:center;gap:8px;">
              <div style="flex:1;background:rgba(0,0,0,0.06);border-radius:4px;height:6px;">
                <div style="width:{bar_w}%;background:{sc_color};
                            border-radius:4px;height:6px;"></div>
              </div>
              <span style="font-size:13px;font-weight:500;color:{sc_color};
                           min-width:36px;text-align:right;">{sc:.1f}点</span>
            </div>
          </td>
        </tr>"""

    return f"""
<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;margin-bottom:1.5rem;">
<div style="background:var(--color-background-primary);
            border:0.5px solid rgba(0,0,0,0.1);
            border-radius:10px;overflow:hidden;min-width:300px;">
  <table style="width:100%;border-collapse:collapse;">
    <thead>
      <tr style="background:rgba(100,90,180,0.10);">
        <th style="padding:6px 12px;font-size:11px;font-weight:500;
                   color:var(--color-text-secondary);text-align:left;white-space:nowrap;">奏者</th>
        <th style="padding:6px 12px;font-size:11px;font-weight:500;
                   color:var(--color-text-secondary);text-align:left;">スコア</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
</div>"""


def _write_assignments_to_notion(ctx: dict, assignments: list[dict], pref_map: dict):
    """採用した割当をPlayerInstrumentの担当フラグに書き込む。
    レコードの一意キー：演奏会 + 奏者 + パート定義（曲+楽器+パートを包含）
    同一奏者が同一楽器で複数曲を担当する場合も正しく区別できる。
    """
    db_id    = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("PlayerInstrument DBのプロパティ取得に失敗しました。")
        return

    concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()

    # 書き込み前に既存の担当フラグを全リセット
    if concert_id:
        existing = ctx["query_all"](db_id)
        with st.spinner("既存の担当フラグをリセット中..."):
            for r in existing:
                c_ids = ctx["extract_relation_ids_any"](r, PI_CONCERT_REL_KEYS)
                if concert_id not in (c_ids or []):
                    continue
                if ctx["extract_prop_text_any"](r, PI_ASSIGN_KEYS) != "True":
                    continue
                props: dict = {}
                ctx["put_prop_any"](props, type_map, PI_ASSIGN_KEYS, False)
                ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{r.get('id','')}",
                                   json={"properties": props})

    # 既存レコードを「奏者+パート定義」で引ける辞書を構築
    all_pi = ctx["query_all"](db_id)
    # key: (player_id, part_id) → row
    existing_map: dict[tuple[str, str], dict] = {}
    for r in all_pi:
        c_ids = ctx["extract_relation_ids_any"](r, PI_CONCERT_REL_KEYS)
        if concert_id and concert_id not in (c_ids or []):
            continue
        p_ids   = ctx["extract_relation_ids_any"](r, PI_PLAYER_REL_KEYS)
        pt_ids  = ctx["extract_relation_ids_any"](r, PI_PART_REL_KEYS)
        if p_ids and pt_ids:
            existing_map[(p_ids[0], pt_ids[0])] = r

    ok = fail = 0
    with st.spinner("Notionに書き込み中..."):
        for a in assignments:
            player_id = a["player_id"]
            part_id   = a["part_id"]
            song_id   = a.get("song_id", "")
            inst_id   = a.get("instrument_id", "")

            props: dict = {}
            ctx["put_prop_any"](props, type_map, PI_ASSIGN_KEYS, True)
            ctx["put_prop_any"](props, type_map, PI_NOTE_KEYS, a.get("part_name", ""))

            existing_row = existing_map.get((player_id, part_id))
            if existing_row:
                # 既存レコードを更新
                rid = existing_row.get("id", "")
                res = ctx["api_request"]("patch",
                                         f"https://api.notion.com/v1/pages/{rid}",
                                         json={"properties": props})
            else:
                # 新規作成：奏者+パート定義+演奏会+楽器+演奏曲を保存
                ctx["put_key_any"](props, type_map, ASSIGN_KEY_KEYS,
                                   concert_id, player_id, part_id, prefix="assign")
                ctx["put_prop_any"](props, type_map, PI_RECORD_KEYS,
                                    f"{a['player_name']} × {a['song_name']} × {a['part_name']}")
                ctx["put_prop_any"](props, type_map, PI_PLAYER_REL_KEYS,  player_id)
                ctx["put_prop_any"](props, type_map, PI_INST_REL_KEYS,    inst_id)
                ctx["put_prop_any"](props, type_map, PI_CONCERT_REL_KEYS, concert_id)
                ctx["put_prop_any"](props, type_map, PI_PART_REL_KEYS,    part_id)
                ctx["put_prop_any"](props, type_map, PI_SONG_REL_KEYS,    song_id)
                res = ctx["api_request"]("post",
                                         "https://api.notion.com/v1/pages",
                                         json={"parent": {"database_id": db_id},
                                               "properties": props})
                if res and res.status_code == 200:
                    # 新規作成したレコードをexisting_mapに追加
                    new_id = res.json().get("id", "")
                    if new_id:
                        existing_map[(player_id, part_id)] = {"id": new_id}
            if res and res.status_code == 200:
                ok += 1
            else:
                fail += 1

    if fail == 0:
        st.success(f"✅ {ok}件の担当フラグを書き込みました。")
        _clear_assign_cache()
    else:
        st.warning(f"⚠️ {ok}件成功、{fail}件失敗。")


# ============================================================
# タブ3：アサイン確定
# ============================================================

def _render_result_tab(ctx: dict):
    st.caption("担当フラグが立っているレコードを表示します。手動修正後「更新」で上書き保存できます。")

    concert_id   = (ctx.get("SELECTED_CONCERT_ID")   or "").strip()
    concert_name = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()
    if not concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    st.caption(f"対象演奏会: {concert_name or concert_id}")

    col_h, col_r = st.columns([8, 1])
    col_h.subheader("担当パート一覧")
    if col_r.button("🔄", key="refresh_result", help="再読み込み"):
        _clear_assign_cache()
        st.rerun()

    players = _load_players(ctx)
    songs   = _load_songs(ctx, concert_id)
    if not players or not songs:
        st.info("奏者・楽曲が登録されていません。")
        return

    inst_rows     = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}
    inst_opts     = {_instrument_name(r, ctx): r.get("id","") for r in sorted(inst_rows, key=lambda x: _instrument_name(x, ctx))}

    player_name_map = {p.get("id"): _player_name(p, ctx) for p in players}
    player_opts     = {_player_name(p, ctx): p.get("id","") for p in sorted(players, key=lambda x: _player_name(x, ctx))}
    song_name_map   = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_id_set     = {s.get("id") for s in songs}

    db_id   = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    t_map   = ctx["get_prop_types"](db_id)
    ext_any = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    # 担当フラグ=Trueのレコードを取得（演奏会フィルタ）
    pi_rows = ctx["query_all"](db_id)
    assigned_rows = []
    for r in pi_rows:
        c_ids = ext_rel(r, PI_CONCERT_REL_KEYS)
        if concert_id and c_ids and concert_id not in c_ids:
            continue
        if ext_any(r, PI_ASSIGN_KEYS) == "True":
            assigned_rows.append(r)

    if not assigned_rows:
        st.info("まだ担当が確定していません。「アルゴリズム実行」タブから割当を実行してください。")

        # リセットボタン
        if st.button("🗑️ 全担当フラグをリセット", key="reset_assign", type="secondary"):
            with st.spinner("リセット中..."):
                # 必ずこの演奏会のレコードのみに絞る
                all_rows = [
                    r for r in pi_rows
                    if ext_any(r, PI_ASSIGN_KEYS) == "True"
                    and (not concert_id or concert_id in ext_rel(r, PI_CONCERT_REL_KEYS))
                ]
                ok = fail = 0
                for r in all_rows:
                    rid = r.get("id","")
                    props: dict = {}
                    ctx["put_prop_any"](props, t_map, PI_ASSIGN_KEYS, False)
                    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{rid}",
                                             json={"properties": props})
                    if res and res.status_code == 200: ok += 1
                    else: fail += 1
            st.success(f"✅ {ok}件リセットしました。")
            _clear_assign_cache()
            st.rerun()
        return

    # パート定義を先読みしてpart_id → song_idのマップを作る
    pd_all = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    partdef_to_song: dict[str, str] = {}
    for pd in pd_all:
        pdid = pd.get("id", "")
        s_ids = ctx["extract_relation_ids_any"](pd, PARTDEF_SONG_REL_KEYS)
        if pdid and s_ids:
            partdef_to_song[pdid] = s_ids[0]

    # 曲ごとにまとめて表示＋手動修正フォーム
    # PI_SONG_REL_KEYSに演奏曲が保存されているので直接参照
    # フォールバック：パート定義経由で楽器→曲を特定
    by_song: dict[str, list] = defaultdict(list)
    for r in assigned_rows:
        # 優先1: 演奏曲リレーションから直接取得
        s_ids = ext_rel(r, PI_SONG_REL_KEYS)
        matched_sid = s_ids[0] if s_ids and s_ids[0] in song_id_set else ""

        # フォールバック: パート定義リレーション経由
        if not matched_sid:
            pt_ids = ext_rel(r, PI_PART_REL_KEYS)
            if pt_ids:
                matched_sid = partdef_to_song.get(pt_ids[0], "")

        # 最終フォールバック: 楽器→パート定義→曲（旧ロジック、最も精度が低い）
        if not matched_sid:
            i_ids = ext_rel(r, PI_INST_REL_KEYS)
            for sid in song_id_set:
                for pd in pd_all:
                    pd_sids = ctx["extract_relation_ids_any"](pd, PARTDEF_SONG_REL_KEYS)
                    pd_iids = ctx["extract_relation_ids_any"](pd, PARTDEF_INST_REL_KEYS)
                    if sid in pd_sids and i_ids and i_ids[0] in pd_iids:
                        matched_sid = sid
                        break
                if matched_sid:
                    break

        if matched_sid:
            by_song[matched_sid].append(r)

    for song in sorted(songs, key=lambda x: _song_name(x, ctx)):
        sid   = song.get("id","")
        sname = _song_name(song, ctx)
        rows  = by_song.get(sid, [])
        with st.expander(f"**{sname}**　{len(rows)}パート", expanded=True):
            if not rows:
                st.caption("割当なし")
                continue
            for r in rows:
                rid   = r.get("id","")
                p_ids = ext_rel(r, PI_PLAYER_REL_KEYS)
                i_ids = ext_rel(r, PI_INST_REL_KEYS)
                cur_pname = player_name_map.get(p_ids[0], "不明") if p_ids else "不明"
                cur_iname = inst_name_map.get(i_ids[0], "不明") if i_ids else "不明"
                note      = ext_any(r, PI_NOTE_KEYS) or ""

                with st.form(f"assign_edit_{rid}", border=False):
                    c1, c2, c3, c4 = st.columns([3, 3, 4, 1])
                    new_pname = c1.selectbox(
                        "奏者", list(player_opts.keys()),
                        index=list(player_opts.keys()).index(cur_pname) if cur_pname in player_opts else 0,
                        key=f"ae_p_{rid}", label_visibility="collapsed",
                    )
                    new_iname = c2.selectbox(
                        "楽器", list(inst_opts.keys()),
                        index=list(inst_opts.keys()).index(cur_iname) if cur_iname in inst_opts else 0,
                        key=f"ae_i_{rid}", label_visibility="collapsed",
                    )
                    new_note = c3.text_input("備考", value=note, key=f"ae_n_{rid}",
                                             label_visibility="collapsed", placeholder="備考")
                    if c4.form_submit_button("💾", help="更新"):
                        new_pid = player_opts.get(new_pname, p_ids[0] if p_ids else "")
                        new_iid = inst_opts.get(new_iname, i_ids[0] if i_ids else "")
                        props: dict = {}
                        ctx["put_prop_any"](props, t_map, PI_PLAYER_REL_KEYS, new_pid)
                        ctx["put_prop_any"](props, t_map, PI_INST_REL_KEYS,   new_iid)
                        ctx["put_prop_any"](props, t_map, PI_NOTE_KEYS,       new_note)
                        res = ctx["api_request"]("patch",
                                                  f"https://api.notion.com/v1/pages/{rid}",
                                                  json={"properties": props})
                        if res and res.status_code == 200:
                            st.success("✅ 更新しました。")
                            _clear_assign_cache()
                            st.rerun()
                        else:
                            st.error("❌ 更新に失敗しました。")

    st.divider()
    if st.button("🗑️ 全担当フラグをリセット", key="reset_assign_bottom", type="secondary"):
        with st.spinner("リセット中..."):
            ok = fail = 0
            for r in assigned_rows:
                rid = r.get("id","")
                props: dict = {}
                ctx["put_prop_any"](props, t_map, PI_ASSIGN_KEYS, False)
                res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{rid}",
                                         json={"properties": props})
                if res and res.status_code == 200: ok += 1
                else: fail += 1
        st.success(f"✅ {ok}件リセットしました。")
        _clear_assign_cache()
        st.rerun()


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎯 パート割当")

    tab_pref, tab_solver, tab_result = st.tabs([
        "希望入力",
        "アルゴリズム実行",
        "アサイン確定",
    ])

    with tab_pref:
        _render_pref_tab(ctx)

    with tab_solver:
        _render_solver_tab(ctx)

    with tab_result:
        _render_result_tab(ctx)
