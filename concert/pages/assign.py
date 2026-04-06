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
from itertools import zip_longest
import re
import math
try:
    from streamlit_sortables import sort_items as _sort_items
except Exception:
    _sort_items = None


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
ALL_PART_LABEL   = "（全パート）"


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




def _load_harmonia_concert_row(ctx: dict, concert_id: str) -> dict:
    if not concert_id or not ctx.get("CONCERT_DB_HARMONIA_CONCERT"):
        return {}
    db_id = ctx["CONCERT_DB_HARMONIA_CONCERT"]
    t = ctx["get_prop_types"](db_id) or {}
    rel_key = _find_prop_name_loose(ctx, t, HARMONIA_CONCERT_CONCERT_REL_KEYS)
    target = _normalize_page_id(concert_id)
    rows = []
    if rel_key:
        rows = ctx["query_all"](db_id, {"filter": {"property": rel_key, "relation": {"contains": concert_id}}})
    if not rows:
        rows = ctx["query_all"](db_id)
    for r in rows:
        ids = ctx["extract_relation_ids_any"](r, [rel_key] if rel_key else HARMONIA_CONCERT_CONCERT_REL_KEYS)
        if any(_normalize_page_id(x) == target for x in ids):
            return r
    return {}


def _ensure_harmonia_concert_row(ctx: dict, concert_id: str, concert_name: str = "") -> tuple[dict, bool]:
    row = _load_harmonia_concert_row(ctx, concert_id)
    if row:
        return row, False
    db_id = ctx.get("CONCERT_DB_HARMONIA_CONCERT", "")
    if not db_id:
        return {}, False
    t = ctx["get_prop_types"](db_id) or {}
    props: dict = {}
    ctx["put_key_any"](props, t, HARMONIA_CONCERT_KEY_KEYS, concert_id, concert_name or concert_id, prefix="harmonia")
    ctx["put_prop_any"](props, t, HARMONIA_CONCERT_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, HARMONIA_CONCERT_MANAGED_KEYS, True)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    if res is not None and res.status_code == 200:
        return res.json() or {}, True
    return {}, False


def _set_harmonia_concert_checkbox(ctx: dict, concert_id: str, key_candidates: list[str], checked: bool, concert_name: str = "") -> bool:
    row, _ = _ensure_harmonia_concert_row(ctx, concert_id, concert_name)
    if not row:
        return False
    db_id = ctx.get("CONCERT_DB_HARMONIA_CONCERT", "")
    t = ctx["get_prop_types"](db_id) or {}
    flag_key = _find_prop_name_loose(ctx, t, key_candidates)
    if not flag_key:
        return False
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{row.get('id','')}", json={"properties": {flag_key: {"checkbox": bool(checked)}}})
    return res is not None and res.status_code == 200

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
            "pref_list_", "participant_list_", "assign_manual_",
        )):
            st.session_state.pop(k, None)
        if k.startswith("pref_editor_version_") or k.startswith("pref_editor_"):
            st.session_state.pop(k, None)


def _clear_manual_assignment_state(concert_id: str):
    prefix = f"assign_manual_{concert_id}_"
    for k in list(st.session_state.keys()):
        if k.startswith(prefix):
            st.session_state.pop(k, None)


def _normalize_page_id(v: str) -> str:
    return (v or "").replace("-", "").strip().lower()


def _is_perc_part(part_name: str) -> bool:
    """パート名が打楽器（Perc）かどうかを判定する。未設定の場合は対象外とする。"""
    name = (part_name or "").strip().lower()
    if not name:
        return False  # パート未設定はPercでないとみなす
    return name in ("perc", "percussion", "打楽器")


def _manual_assignment_state_key(concert_id: str, result_label: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_]+", "_", (result_label or "").strip())
    return f"assign_manual_{concert_id}_{safe}"


def _manual_assignment_sig(assignments: list[dict]) -> tuple:
    sig_items = []
    for idx, a in enumerate(assignments):
        sig_items.append((
            idx,
            a.get("song_id", ""),
            a.get("part_id", ""),
            a.get("instrument_id", ""),
            a.get("player_id", ""),
        ))
    return tuple(sig_items)


def _slot_key(a: dict) -> tuple[str, str, str]:
    return (
        a.get("song_id", ""),
        a.get("part_id", ""),
        a.get("instrument_id", ""),
    )


def _collect_assignment_changes(base_assignments: list[dict], edited_assignments: list[dict]) -> list[dict]:
    out: list[dict] = []
    base_by_slot: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    edited_by_slot: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for a in base_assignments:
        base_by_slot[_slot_key(a)].append(a)
    for a in edited_assignments:
        edited_by_slot[_slot_key(a)].append(a)

    for sk in sorted(set(base_by_slot.keys()) | set(edited_by_slot.keys())):
        b_rows = base_by_slot.get(sk, [])
        e_rows = edited_by_slot.get(sk, [])
        b_cnt: dict[str, int] = defaultdict(int)
        e_cnt: dict[str, int] = defaultdict(int)
        b_name: dict[str, str] = {}
        e_name: dict[str, str] = {}
        for r in b_rows:
            pid = r.get("player_id", "")
            b_cnt[pid] += 1
            b_name[pid] = r.get("player_name", pid or "—")
        for r in e_rows:
            pid = r.get("player_id", "")
            e_cnt[pid] += 1
            e_name[pid] = r.get("player_name", pid or "—")
        if dict(b_cnt) == dict(e_cnt):
            continue

        removed: list[str] = []
        added: list[str] = []
        for pid, n in b_cnt.items():
            d = n - e_cnt.get(pid, 0)
            if d > 0:
                removed.extend([b_name.get(pid, pid)] * d)
        for pid, n in e_cnt.items():
            d = n - b_cnt.get(pid, 0)
            if d > 0:
                added.extend([e_name.get(pid, pid)] * d)

        part_name = (e_rows[0].get("part_name", "—") if e_rows else (b_rows[0].get("part_name", "—") if b_rows else "—"))
        song_id = sk[0]
        for frm, to in zip_longest(removed, added, fillvalue="—"):
            out.append({
                "song_id": song_id,
                "part_name": part_name,
                "from_player": frm,
                "to_player": to,
            })
    return out


def _find_song_player_duplicates(assignments: list[dict]) -> dict[str, list[str]]:
    by_song: dict[str, dict[str, int]] = {}
    by_name: dict[str, dict[str, str]] = {}
    for a in assignments:
        sid = a.get("song_id", "")
        pid = a.get("player_id", "")
        pname = a.get("player_name", pid or "—")
        if not sid or not pid:
            continue
        by_song.setdefault(sid, {})
        by_name.setdefault(sid, {})
        by_song[sid][pid] = by_song[sid].get(pid, 0) + 1
        by_name[sid][pid] = pname
    out: dict[str, list[str]] = {}
    for sid, cnts in by_song.items():
        dup_names = [by_name[sid].get(pid, pid) for pid, n in cnts.items() if n > 1]
        if dup_names:
            out[sid] = dup_names
    return out


def _get_manual_assignments(concert_id: str, result_label: str, base_assignments: list[dict]) -> list[dict]:
    key = _manual_assignment_state_key(concert_id, result_label)
    sig_key = f"{key}_sig"
    dirty_key = f"{key}_dirty"
    gen_key = f"{key}_gen"
    cur_gen = int(st.session_state.get(f"assign_generation_{concert_id}", 0) or 0)
    base_sig = _manual_assignment_sig(base_assignments)
    # dirtyフラグが無い旧セッション状態は破棄して初期化する
    if dirty_key not in st.session_state:
        st.session_state[key] = [dict(a) for a in base_assignments]
        st.session_state[sig_key] = base_sig
        st.session_state[dirty_key] = False
        st.session_state[gen_key] = cur_gen
    elif int(st.session_state.get(gen_key, -1)) != cur_gen:
        st.session_state[key] = [dict(a) for a in base_assignments]
        st.session_state[sig_key] = base_sig
        st.session_state[dirty_key] = False
        st.session_state[gen_key] = cur_gen
    elif (key not in st.session_state) or (st.session_state.get(sig_key) != base_sig):
        st.session_state[key] = [dict(a) for a in base_assignments]
        st.session_state[sig_key] = base_sig
        st.session_state[dirty_key] = False
    return st.session_state.get(key, [])


def _render_manual_assignment_editor(
    concert_id: str,
    result_label: str,
    song_order: list[str],
    song_name_map: dict[str, str],
    base_assignments: list[dict],
    player_label_map: dict[str, str],
) -> list[dict]:
    key = _manual_assignment_state_key(concert_id, result_label)
    open_key = f"{key}_open"
    dirty_key = f"{key}_dirty"
    if key not in st.session_state:
        st.session_state[key] = [dict(a) for a in base_assignments]
    if open_key not in st.session_state:
        st.session_state[open_key] = True
    if dirty_key not in st.session_state:
        st.session_state[dirty_key] = False
    edited = st.session_state.get(key, [])
    if not edited:
        return edited

    if not player_label_map:
        return edited

    with st.expander("🛠 手動でパートを設定する（この候補のみ）", expanded=bool(st.session_state.get(open_key, True))):
        st.caption("ここで変更した内容はこの候補にのみ反映されます。『採用』時にこの内容で書き込みます。")
        if st.button("↩ この候補の手動変更をリセット", key=f"manual_reset_{key}"):
            st.session_state[key] = [dict(a) for a in base_assignments]
            st.session_state[open_key] = True
            st.session_state[dirty_key] = False
            st.rerun()

        for sid in song_order:
            song_rows = [i for i, a in enumerate(edited) if a.get("song_id") == sid]
            if not song_rows:
                continue
            h1, h2 = st.columns([8, 2])
            h1.markdown(f"**{song_name_map.get(sid, sid)}**")
            if h2.button("↩ この曲を元に戻す", key=f"manual_song_reset_{key}_{sid}", use_container_width=True):
                _base_q: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
                for _b in base_assignments:
                    if _b.get("song_id", "") != sid:
                        continue
                    _base_q[_slot_key(_b)].append(_b)
                for row_idx in song_rows:
                    a = edited[row_idx]
                    k = _slot_key(a)
                    if _base_q.get(k):
                        b = _base_q[k].pop(0)
                        edited[row_idx]["player_id"] = b.get("player_id", "")
                        edited[row_idx]["player_name"] = b.get("player_name", b.get("player_id", ""))
                st.session_state[key] = edited
                st.session_state[open_key] = True
                st.session_state[dirty_key] = True
                st.rerun()

            if len(song_rows) >= 2 and _sort_items is not None:
                st.caption("ドラッグして並べ替え: 右側の順番が、左のパート順にそのまま割り当てられます。")
                part_labels = [edited[i].get("part_name", "—") for i in song_rows]
                cur_pids = [edited[i].get("player_id", "") for i in song_rows]
                cur_pnames = [edited[i].get("player_name", pid or "—") for i, pid in zip(song_rows, cur_pids)]
                if len(set(cur_pids)) != len(cur_pids):
                    st.error("同一曲で同じ奏者が重複しています。まず「この曲を元に戻す」で解消してください。")
                    st.markdown("---")
                    continue
                token_to_pid: dict[str, str] = {}
                player_tokens: list[str] = []
                _name_seen: dict[str, int] = {}
                for row_idx, pname, pid in zip(song_rows, cur_pnames, cur_pids):
                    _name_seen[pname] = _name_seen.get(pname, 0) + 1
                    if _name_seen[pname] == 1:
                        token = pname
                    else:
                        token = f"{pname} ({_name_seen[pname]})"
                    player_tokens.append(token)
                    token_to_pid[token] = pid

                d1, d2 = st.columns([5, 5])
                with d1:
                    st.caption("パート順（固定）")
                    for p in part_labels:
                        st.markdown(f"- {p}")
                with d2:
                    st.caption("担当者（ドラッグ）")
                    ordered_tokens = _sort_items(player_tokens, direction="vertical", key=f"manual_dnd_{key}_{sid}")
                if isinstance(ordered_tokens, list) and len(ordered_tokens) == len(song_rows):
                    _same_order = (ordered_tokens == player_tokens)
                    _base_song_counter: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
                    for _b in base_assignments:
                        if _b.get("song_id", "") != sid:
                            continue
                        _base_song_counter[_slot_key(_b)][_b.get("player_id", "")] += 1
                    _song_changed_flags: dict[int, bool] = {}
                    for row_idx in song_rows:
                        a = edited[row_idx]
                        k = _slot_key(a)
                        pid = a.get("player_id", "")
                        if _base_song_counter[k].get(pid, 0) > 0:
                            _base_song_counter[k][pid] -= 1
                            _song_changed_flags[row_idx] = False
                        else:
                            _song_changed_flags[row_idx] = True
                    _song_changed_count = sum(1 for _v in _song_changed_flags.values() if _v)
                    if _same_order and _song_changed_count == 0:
                        st.caption("現在の割当順と同じです（この曲の手動変更はありません）。")
                    elif _same_order and _song_changed_count > 0:
                        st.caption(f"現在の割当順と同じです（この曲は手動変更 {_song_changed_count}件を保持中）。")
                    else:
                        st.info("並べ替え結果を確認して「この順序を反映」を押すと割当を更新します。")
                    if st.button(
                        "✅ この順序を反映",
                        key=f"manual_apply_order_{key}_{sid}",
                        use_container_width=True,
                        disabled=_same_order,
                    ):
                        for pos, row_idx in enumerate(song_rows):
                            tok = ordered_tokens[pos]
                            new_pid = token_to_pid.get(tok, edited[row_idx].get("player_id", ""))
                            edited[row_idx]["player_id"] = new_pid
                            edited[row_idx]["player_name"] = player_label_map.get(new_pid, new_pid)
                        st.session_state[key] = edited
                        st.session_state[open_key] = True
                        st.session_state[dirty_key] = True
                        st.rerun()
            elif len(song_rows) >= 2 and _sort_items is None:
                st.info("ドラッグUIが利用できません。`streamlit-sortables` の導入状態を確認してください。")

            st.caption("現在の割当（変更点は ✨）")
            _base_song_counter2: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for _b in base_assignments:
                if _b.get("song_id", "") != sid:
                    continue
                _base_song_counter2[_slot_key(_b)][_b.get("player_id", "")] += 1
            for row_idx in song_rows:
                a = edited[row_idx]
                k = _slot_key(a)
                pid = a.get("player_id", "")
                if _base_song_counter2[k].get(pid, 0) > 0:
                    _base_song_counter2[k][pid] -= 1
                    changed = False
                else:
                    changed = True
                mark = "✨ " if changed else ""
                st.markdown(f"- {mark}{a.get('part_name', '—')} ← {a.get('player_name', '—')}")
            st.markdown("---")
        st.session_state[key] = edited
        # 差分が無ければdirtyを落として「未操作状態」として扱う
        st.session_state[dirty_key] = bool(_collect_assignment_changes(base_assignments, edited))
    return edited


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


def _load_part_master_name_map(ctx) -> dict[str, str]:
    key = "assign_part_master_name_map"
    if key not in st.session_state:
        db_id = (ctx.get("CONCERT_DB_PART_MASTER", "") or "").strip()
        name_map: dict[str, str] = {}
        if db_id:
            rows = ctx["query_all"](db_id, None)
            for r in rows:
                rid = r.get("id", "")
                if not rid:
                    continue
                name_map[rid] = (
                    ctx["extract_prop_text_any"](r, PARTMASTER_NAME_KEYS)
                    or ctx["extract_title"](r)
                    or rid
                )
        st.session_state[key] = name_map
    return st.session_state.get(key, {})


def _participant_part_name(ctx, participant_row: dict, part_master_name_map: dict[str, str]) -> str:
    rel_ids = ctx["extract_relation_ids_any"](participant_row, PARTICIPANT_PART_REL_KEYS)
    for rid in rel_ids:
        if rid in part_master_name_map:
            return part_master_name_map[rid]
    return (ctx["extract_prop_text_any"](participant_row, PARTICIPANT_PART_KEYS) or "").strip()


def _render_shared_part_filter(
    ctx,
    concert_id: str,
    participant_rows: list[dict],
    *,
    show_widget: bool,
) -> tuple[str, set[str], dict[str, str]]:
    """3タブ共通のパート絞り込み状態を返す。show_widget=True のときだけUIを描画。"""
    part_master_name_map = _load_part_master_name_map(ctx)
    part_by_player: dict[str, str] = {}
    for row in participant_rows:
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if not pids:
            continue
        pid = pids[0]
        part_name = _participant_part_name(ctx, row, part_master_name_map)
        if part_name:
            part_by_player[pid] = part_name

    all_parts = sorted({p for p in part_by_player.values() if p})
    all_player_ids = set(part_by_player.keys())
    if not all_parts:
        return ALL_PART_LABEL, all_player_ids, part_by_player

    opts = [ALL_PART_LABEL] + all_parts
    filter_key = f"assign_part_filter_{concert_id}"
    if show_widget:
        selected_part = st.selectbox(
            "パートを選択（全タブ共通）",
            opts,
            key=filter_key,
            help="ここで選択したパートは『希望入力』『アルゴリズム実行』『アサイン確定』で共通適用されます。",
        )
    else:
        selected_part = st.session_state.get(filter_key, ALL_PART_LABEL)
        if selected_part not in opts:
            selected_part = ALL_PART_LABEL
    if selected_part == ALL_PART_LABEL:
        return selected_part, all_player_ids, part_by_player
    selected_player_ids = {pid for pid, pname in part_by_player.items() if pname == selected_part}
    return selected_part, selected_player_ids, part_by_player


def _partdef_part_name(ctx, partdef_row: dict, part_master_name_map: dict[str, str]) -> str:
    rel_ids = ctx["extract_relation_ids_any"](partdef_row, PARTDEF_PART_REL_KEYS)
    for rid in rel_ids:
        if rid in part_master_name_map:
            return part_master_name_map[rid]
    return ""


def _partdef_matches_selected_part(ctx, partdef_row: dict, selected_part: str, part_master_name_map: dict[str, str]) -> bool:
    if not selected_part or selected_part == ALL_PART_LABEL:
        return True
    return _partdef_part_name(ctx, partdef_row, part_master_name_map) == selected_part


def _state_token(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(text or "")).strip("_") or "all"


def _partdef_label_for_song(ctx, part_row: dict, song_name: str, inst_name_map: dict[str, str]) -> str:
    iids = ctx["extract_relation_ids_any"](part_row, PARTDEF_INST_REL_KEYS)
    iid = iids[0] if iids else ""
    iname = inst_name_map.get(iid, iid) if iid else ""
    note = ctx["extract_prop_text_any"](part_row, PARTDEF_NOTE_KEYS) or ""
    pname = ctx["extract_prop_text_any"](part_row, PARTDEF_NAME_KEYS) or iname or "Part"
    pname = _strip_song_prefix(pname, song_name)
    return f"{pname}（{note}）" if note else pname


def _build_partdefs_by_song_for_selected_part(ctx, songs: list, selected_part: str) -> dict[str, list[dict]]:
    part_master_name_map = _load_part_master_name_map(ctx)
    out: dict[str, list[dict]] = {}
    for song in songs:
        sid = song.get("id", "")
        rows = [
            r for r in _load_song_instruments(ctx, sid)
            if _partdef_matches_selected_part(ctx, r, selected_part, part_master_name_map)
        ]
        if rows:
            out[sid] = rows
    return out


def _calc_counts_from_percentages(total_players: int, percentages: list[int]) -> list[int]:
    if total_players <= 0 or not percentages:
        return [0 for _ in percentages]
    raws = [max(0.0, total_players * (p / 100.0)) for p in percentages]
    bases = [int(math.floor(x)) for x in raws]
    target_total = min(total_players, int(round(sum(raws))))
    remain = max(target_total - sum(bases), 0)
    fracs = sorted(
        [(raws[i] - bases[i], i) for i in range(len(percentages))],
        key=lambda x: x[0],
        reverse=True,
    )
    counts = list(bases)
    for _, i in fracs:
        if remain <= 0:
            break
        counts[i] += 1
        remain -= 1
    return counts


def _render_part_distribution_controls(
    ctx,
    concert_id: str,
    songs: list,
    selected_part: str,
    selected_player_count: int,
) -> tuple[dict[tuple[str, str], int], bool]:
    """アルゴリズム実行タブ用のパート配分UIを描画し、required_count上書き値を返す。"""
    if selected_part == ALL_PART_LABEL:
        st.info("配分指定はパートを選択したときのみ利用できます。")
        return {}, False

    by_song = _build_partdefs_by_song_for_selected_part(ctx, songs, selected_part)
    if not by_song:
        st.warning(f"パート「{selected_part}」に一致する PART_DEFINITION がありません。")
        return {}, True

    token = _state_token(selected_part)
    mode_key = f"assign_dist_mode_{concert_id}_{token}"
    mode = st.radio(
        "配分指定",
        ["人数指定", "割合指定"],
        horizontal=True,
        key=mode_key,
        help="この配分は今回の候補案計算にのみ使用します（Notion未保存）。",
    )
    st.caption(f"対象人数（{selected_part}）: {selected_player_count}人")

    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}
    overrides: dict[tuple[str, str], int] = {}
    has_error = False

    for song in sorted(songs, key=lambda x: _song_name(x, ctx)):
        sid = song.get("id", "")
        part_rows = by_song.get(sid, [])
        if not part_rows:
            continue
        sname = _song_name(song, ctx)
        with st.expander(f"配分設定: {sname}", expanded=False):
            if mode == "人数指定":
                used = 0
                for idx, part in enumerate(part_rows):
                    part_id = part.get("id", "")
                    label = _partdef_label_for_song(ctx, part, sname, inst_name_map)
                    key = f"assign_dist_abs_{concert_id}_{token}_{sid}_{part_id}"
                    max_this = max(selected_player_count - used, 0)
                    if key not in st.session_state:
                        base_qty = ctx["extract_prop_text_any"](part, ["必要台数", "必要人数", "台数", "人数"]) or "0"
                        try:
                            st.session_state[key] = max(0, min(int(float(base_qty)), max_this))
                        except Exception:
                            st.session_state[key] = 0
                    if st.session_state[key] > max_this:
                        st.session_state[key] = max_this
                    val = int(st.number_input(
                        f"{label} の人数",
                        min_value=0,
                        max_value=max_this,
                        step=1,
                        key=key,
                    ))
                    used += val
                    overrides[(sid, part_id)] = val
                st.caption(f"降り番: {max(selected_player_count - used, 0)}人")
            else:
                percentages: list[int] = []
                part_ids: list[str] = []
                for part in part_rows:
                    part_id = part.get("id", "")
                    label = _partdef_label_for_song(ctx, part, sname, inst_name_map)
                    key = f"assign_dist_pct_{concert_id}_{token}_{sid}_{part_id}"
                    if key not in st.session_state:
                        st.session_state[key] = 0
                    pct = int(st.number_input(
                        f"{label} の割合(%)",
                        min_value=0,
                        max_value=100,
                        step=5,
                        key=key,
                    ))
                    percentages.append(pct)
                    part_ids.append(part_id)
                total_pct = sum(percentages)
                if total_pct > 100:
                    st.error("割合の合計が100%を超えています。")
                    has_error = True
                    continue
                counts = _calc_counts_from_percentages(selected_player_count, percentages)
                for part_id, cnt in zip(part_ids, counts):
                    overrides[(sid, part_id)] = cnt
                st.caption(
                    f"割合合計: {total_pct}% / "
                    f"配分人数: {sum(counts)}人 / 降り番: {max(selected_player_count - sum(counts), 0)}人"
                )

    return overrides, has_error


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

    注意:
    - Notion APIから返る archived / in_trash 行は既存回答として扱わない
    - 希望順位が空の行も既存回答として扱わない
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
                # Notion上で削除済み（archived / ゴミ箱）は除外
                if r.get("archived") or r.get("in_trash"):
                    continue
                pids = ctx["extract_relation_ids"](r, player_rel) if player_rel else []
                if not (pids and pids[0] in participant_ids):
                    continue
                # 希望順位が空のレコードは既存回答として扱わない
                priority_str = (ctx["extract_prop_text_any"](r, PREF_PRIORITY_KEYS) or "").strip()
                if not priority_str:
                    continue
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


def _get_pref_editor_version(concert_id: str, player_id: str) -> int:
    return int(st.session_state.get(f"pref_editor_version_{concert_id}_{player_id}", 0))


def _bump_pref_editor_version(concert_id: str, player_id: str) -> None:
    key = f"pref_editor_version_{concert_id}_{player_id}"
    st.session_state[key] = int(st.session_state.get(key, 0)) + 1


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

def _build_solver_input(
    ctx,
    concert_id: str,
    songs: list,
    players: list,
    selected_part: str = ALL_PART_LABEL,
    required_count_overrides: dict[tuple[str, str], int] | None = None,
):
    """
    NotionのDBからsolve_all()に渡すPrefs/Requirementsを構築する。
    assign_solver.pyのPref/Requirementデータクラスを直接使わず、
    dictで渡してsolve_all内で変換する形にする。
    """
    from concert.services.assign_solver import Pref, Requirement

    # 楽器マスタ
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}
    part_master_name_map = _load_part_master_name_map(ctx)

    # Requirements: パート定義DBから構築
    requirements: list[Requirement] = []
    for song in songs:
        sid = song.get("id", "")
        sname = _song_name(song, ctx)
        part_rows = _load_song_instruments(ctx, sid)
        for part in part_rows:
            if not _partdef_matches_selected_part(ctx, part, selected_part, part_master_name_map):
                continue
            iids = ctx["extract_relation_ids_any"](part, PARTDEF_INST_REL_KEYS)
            iid   = iids[0] if iids else ""
            iname = inst_name_map.get(iid, iid) if iid else ""
            qty_str = ctx["extract_prop_text_any"](part, ["必要台数", "必要人数", "台数", "人数"])
            try:
                qty = max(int(float(qty_str)), 1) if qty_str else 1
            except ValueError:
                qty = 1
            if required_count_overrides and (sid, part.get("id", "")) in required_count_overrides:
                qty = max(int(required_count_overrides[(sid, part.get("id", ""))]), 0)
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
            if not _partdef_matches_selected_part(ctx, part, selected_part, part_master_name_map):
                continue
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


def _compute_preference_completion(ctx: dict, concert_id: str) -> tuple[bool, int, int]:
    """
    希望入力の全件完了判定。
    参加者 ×（演奏会内の全パート定義）が埋まっている場合のみ True。
    """
    participants = _load_participants(ctx, concert_id)
    songs = _load_songs(ctx, concert_id)
    if not participants or not songs:
        return False, 0, 0

    partdef_ids: set[str] = set()
    for s in songs:
        sid = s.get("id", "")
        for pd in _load_song_instruments(ctx, sid):
            pdid = pd.get("id", "")
            if pdid:
                partdef_ids.add(pdid)
    if not partdef_ids:
        return False, 0, 0

    participant_ids: set[str] = set()
    player_to_participant: dict[str, str] = {}
    for row in participants:
        rid = row.get("id", "")
        if rid:
            participant_ids.add(rid)
        pids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        if rid and pids:
            player_to_participant[pids[0]] = rid

    target_pairs: set[tuple[str, str]] = {(pid, part_id) for pid in participant_ids for part_id in partdef_ids}
    pref_rows = _load_player_instruments(ctx, concert_id)
    answered_pairs: set[tuple[str, str]] = set()
    for r in pref_rows:
        player_ids = ctx["extract_relation_ids_any"](r, PREF_PLAYER_REL_KEYS)
        part_ids = ctx["extract_relation_ids_any"](r, PREF_PART_REL_KEYS)
        if not (player_ids and part_ids):
            continue
        part_id = part_ids[0]
        if part_id not in partdef_ids:
            continue
        raw_pid = player_ids[0]
        participant_id = raw_pid if raw_pid in participant_ids else player_to_participant.get(raw_pid, "")
        if not participant_id:
            continue
        answered_pairs.add((participant_id, part_id))

    missing_count = max(0, len(target_pairs) - len(answered_pairs))
    return len(target_pairs) > 0 and missing_count == 0, len(target_pairs), missing_count


def _compute_assignment_completion(ctx: dict, concert_id: str) -> tuple[bool, int, list[str]]:
    """
    アサイン確定可否判定。
    演奏会内の全パート定義に対して、担当フラグONの割当が1件以上あることを要件とする。
    """
    songs = _load_songs(ctx, concert_id)
    if not songs:
        return False, 0, []
    song_id_set = {s.get("id", "") for s in songs if s.get("id", "")}

    partdef_rows = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    required_part_ids: set[str] = set()
    partdef_label: dict[str, str] = {}
    for pd in partdef_rows:
        pdid = pd.get("id", "")
        if not pdid:
            continue
        pd_song_ids = set(ctx["extract_relation_ids_any"](pd, PARTDEF_SONG_REL_KEYS))
        if not song_id_set.intersection(pd_song_ids):
            continue
        required_part_ids.add(pdid)
        partdef_label[pdid] = (
            (ctx["extract_prop_text_any"](pd, PARTDEF_DISPLAY_NAME_KEYS) or "").strip()
            or (ctx["extract_prop_text_any"](pd, PARTDEF_NAME_KEYS) or "").strip()
            or pdid
        )

    if not required_part_ids:
        return False, 0, []

    db_id = ctx.get("CONCERT_DB_CONCERT_ASSIGNMENT", "")
    if not db_id:
        return False, len(required_part_ids), sorted(partdef_label.values())[:10]
    rows = ctx["query_all"](db_id, None)
    assigned_part_ids: set[str] = set()
    for r in rows:
        c_ids = ctx["extract_relation_ids_any"](r, ASSIGNMENT_CONCERT_REL_KEYS)
        if concert_id and c_ids and concert_id not in c_ids:
            continue
        if ctx["extract_prop_text_any"](r, ASSIGNMENT_FLAG_KEYS) != "True":
            continue
        p_ids = ctx["extract_relation_ids_any"](r, ASSIGNMENT_PARTDEF_REL_KEYS)
        if p_ids and p_ids[0] in required_part_ids:
            assigned_part_ids.add(p_ids[0])

    missing_ids = sorted(required_part_ids - assigned_part_ids)
    missing_labels = [partdef_label.get(pid, pid) for pid in missing_ids]
    return len(missing_ids) == 0, len(required_part_ids), missing_labels


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

    # パートフィルタ（3タブ共通）
    selected_part, selected_player_ids, _ = _render_shared_part_filter(
        ctx, concert_id, participant_rows, show_widget=True
    )
    if selected_player_ids:
        players = [p for p in players if p.get("id", "") in selected_player_ids]
    if not players:
        st.info(f"「{selected_part}」の参加者が見つかりません。")
        return

    # 楽器マスタ
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}
    part_master_name_map = _load_part_master_name_map(ctx)

    # パート定義→演奏曲の逆引きマップを構築
    # PREFERENCEには演奏曲リレーションがないため、パート定義IDから演奏曲IDを引く
    partdef_to_song: dict[str, str] = {}
    for song in songs:
        sid = song.get("id", "")
        for part in _load_song_instruments(ctx, sid):
            if not _partdef_matches_selected_part(ctx, part, selected_part, part_master_name_map):
                continue
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
        req_rows = [
            r for r in _load_song_instruments(ctx, sid)
            if _partdef_matches_selected_part(ctx, r, selected_part, part_master_name_map)
        ]
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
    player_id = player_opts.get(selected_player_name, "")
    if col_r.button("🔄", key="pref_refresh", help="パート定義・希望データを再読み込み"):
        for k in list(st.session_state.keys()):
            if k.startswith("si_list_") or k.startswith("pi_list_") or k.startswith("pref_editor_"):
                st.session_state.pop(k, None)
        if player_id:
            _bump_pref_editor_version(concert_id, player_id)
        st.rerun()
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
        si_rows = [
            r for r in _load_song_instruments(ctx, sid)
            if _partdef_matches_selected_part(ctx, r, selected_part, part_master_name_map)
        ]
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
            status = "🟢" if has_record else "🔴"

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
    pref_editor_version = _get_pref_editor_version(concert_id, player_id)
    edited_pref = st.data_editor(
        df_pref,
        num_rows="fixed",
        use_container_width=True,
        key=f"pref_editor_{player_id}_{concert_id}_{pref_editor_version}",
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

        pref_complete, pref_total, pref_missing = _compute_preference_completion(ctx, concert_id)
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_PREFERENCE_KEYS, pref_complete, selected_concert)
        if fail_count == 0:
            st.success(f"✅ {ok_count}件を保存しました。")
            if pref_complete:
                st.caption(f"希望入力確定条件を満たしました（{pref_total} / {pref_total}）。")
            else:
                st.caption(f"希望入力の未入力が残っています（未入力 {pref_missing} / 全{pref_total}）。")
            st.cache_data.clear()
            st.session_state.pop(f"pi_list_{concert_id}", None)
            _bump_pref_editor_version(concert_id, player_id)
        else:
            st.warning(f"⚠️ {ok_count}件成功、{fail_count}件失敗。")
            st.cache_data.clear()
            st.session_state.pop(f"pi_list_{concert_id}", None)
            _bump_pref_editor_version(concert_id, player_id)
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
    participant_rows = _load_participants(ctx, concert_id)
    selected_part, selected_player_ids, _ = _render_shared_part_filter(
        ctx, concert_id, participant_rows, show_widget=False
    )
    if selected_player_ids:
        players = [p for p in players if p.get("id", "") in selected_player_ids]
    if not players:
        st.info(f"「{selected_part}」に該当する奏者がいません。")
        return

    with st.expander("🎛 パート内配分（今回の計算のみ）", expanded=False):
        required_count_overrides, has_dist_error = _render_part_distribution_controls(
            ctx=ctx,
            concert_id=concert_id,
            songs=songs,
            selected_part=selected_part,
            selected_player_count=len(players),
        )
    if has_dist_error:
        st.warning("配分指定にエラーがあるため、修正後に候補案生成を実行してください。")
        return

    st.caption("ヒューリスティック解（高速）と厳密解（整数計画法）を同時に生成します。")

    if st.button("▶ 候補案を生成", type="primary", key=f"run_solver_{concert_id}"):
        # 再生成時は、候補タブでの手動変更状態を初期化する
        _clear_manual_assignment_state(concert_id)
        with st.spinner("候補案を生成中...（厳密解を含むため数秒かかります）"):
            try:
                from concert.services.assign_solver import (
                    greedy_solve, local_search, iterated_local_search,
                    _calc_stats, _build_absent_set,
                    _first_choice_rate, _total_score, score_assignment,
                    _min_player_score, _bring_count, _rest_std,
                    solve_exact,
                )
                absent   = _build_absent_set(ctx, concert_id)
                prefs, requirements = _build_solver_input(
                    ctx, concert_id, songs, players,
                    selected_part=selected_part,
                    required_count_overrides=required_count_overrides,
                )
                if not prefs:
                    st.warning("希望データがありません。先に希望入力を行ってください。")
                    return
                if not requirements:
                    st.warning("必要楽器が登録されていません。楽曲・楽器管理から登録してください。")
                    return

                # 参加者DB全員リスト（希望未提出も含む）をfallback候補に
                # ※ all_pidsより先に定義する必要がある
                _all_cast = participant_rows
                _part_to_pl = {}
                for _r in _all_cast:
                    _pids = ctx["extract_relation_ids_any"](_r, PARTICIPANT_PLAYER_REL_KEYS)
                    if _pids:
                        _pid = _pids[0]
                        if selected_player_ids and _pid not in selected_player_ids:
                            continue
                        # 打楽器パート（Perc系）のみを対象にする
                        _ppart = _participant_part_name(ctx, _r, _load_part_master_name_map(ctx))
                        if not _is_perc_part(_ppart):
                            continue
                        _pobj = next((p for p in players if p.get("id","") == _pid), None)
                        _pname = _player_name(_pobj, ctx) if _pobj else ""
                        # playersリストにない場合はPERFORMER DBから直接取得
                        if not _pname:
                            _all_pl = _load_players(ctx)
                            _pobj2 = next((p for p in _all_pl if p.get("id","") == _pid), None)
                            _pname = _player_name(_pobj2, ctx) if _pobj2 else _pid
                        _part_to_pl[_pid] = _pname
                _all_participants = sorted(_part_to_pl.items(), key=lambda x: x[1])
                st.session_state[f"assign_all_participants_{concert_id}"] = _all_participants

                # 公平性・降り番評価はCONCERT_CASTの打楽器奏者のみを対象に
                # （CONCERT_CASTに入っていない奏者は除外）
                all_pids = sorted({pid for pid, _ in _all_participants})
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

                # ── ヒューリスティック解（ILS）────────────────────
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
                h_results = []
                for label, fn in variants:
                    sol = iterated_local_search(
                        base, pref_map, fn,
                        absent_players=absent,
                        all_player_ids=all_pids,
                        n_restart=15, max_iter_per_restart=200,
                    )
                    _ls_ret = local_search(sol, pref_map, fn, max_iter=50,
                                           absent_players=absent, all_player_ids=all_pids,
                                           verbose=True)
                    sol, _ls_log = _ls_ret if isinstance(_ls_ret, tuple) else (_ls_ret, {})
                    stats = _calc_stats(sol, pref_map, all_pids)
                    stats["_ls_log"] = _ls_log
                    _assignments_fixed = []
                    for _a in sol:
                        _d = _a.__dict__.copy()
                        if not _d.get("player_name") or _d["player_name"] == _d["player_id"]:
                            _d["player_name"] = _name_map.get(_d["player_id"], _d["player_id"])
                        _assignments_fixed.append(_d)
                    h_results.append({
                        "label":       label,
                        "assignments": _assignments_fixed,
                        "stats":       stats,
                        "pref_map":    {str(k): v.__dict__ for k, v in pref_map.items()},
                    })
                st.session_state[f"assign_result_heuristic_{concert_id}"] = h_results

                # ── 厳密解（MILP）────────────────────────────────
                e_results = solve_exact(
                    prefs, requirements, absent,
                    all_player_ids=all_pids,
                    time_limit_sec=60.0,
                )
                _name_map_e = {p.get("id",""): _player_name(p, ctx) for p in players}
                for _pid, _pname in _all_participants:
                    if _pid not in _name_map_e or not _name_map_e[_pid]:
                        _name_map_e[_pid] = _pname
                for _r in e_results:
                    for _a in _r["assignments"]:
                        if not _a.get("player_name") or _a["player_name"] == _a["player_id"]:
                            _a["player_name"] = _name_map_e.get(_a["player_id"], _a["player_id"])
                    _r["pref_map"] = {str(k): v.__dict__ for k, v in pref_map.items()}
                if not e_results:
                    st.warning("⚠️ 厳密解の計算に失敗しました。ヒューリスティック解のみ表示します。")
                st.session_state[f"assign_result_exact_{concert_id}"] = e_results

                # 候補生成の世代を進め、手動編集状態をこの世代に同期させる
                st.session_state[f"assign_generation_{concert_id}"] = int(
                    st.session_state.get(f"assign_generation_{concert_id}", 0) or 0
                ) + 1

                # 表示用はヒューリスティックをデフォルトに
                st.success("✅ 候補案を生成しました。")
            except Exception as e:
                import traceback
                st.error(f"❌ 実行エラー：{e}")
                st.code(traceback.format_exc())
                return

    _h_res_sw = st.session_state.get(f"assign_result_heuristic_{concert_id}", [])
    _e_res_sw = st.session_state.get(f"assign_result_exact_{concert_id}", [])

    if not _h_res_sw and not _e_res_sw:
        return

    st.divider()

    # 表示切り替え（両方ある場合のみ）
    if _h_res_sw and _e_res_sw:
        _view_mode = st.radio(
            "表示する解",
            ["ヒューリスティック解（高速）", "厳密解（整数計画法）"],
            horizontal=True,
            key=f"view_mode_{concert_id}",
        )
        results = _e_res_sw if "厳密解" in _view_mode else _h_res_sw
    elif _h_res_sw:
        results = _h_res_sw
    else:
        results = _e_res_sw

    # 生成結果の健全性チェック（同一曲で同一奏者の重複割当は不可）
    for _r in results:
        _dup = _find_song_player_duplicates(_r.get("assignments", []))
        if _dup:
            st.error(f"{_r.get('label', '候補')} に重複割当が含まれています。再生成または手動修正してください。")

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
                # 乖離率の比較サマリ
                st.markdown("**📊 ヒューリスティック vs 厳密解 比較**")
                from concert.services.verify_results import verify as _vf
                _cmp_cols = st.columns(len(_h_results))
                for ci, (hr, er) in enumerate(zip(_h_results, _e_results)):
                    hv = _vf(hr["assignments"], hr["pref_map"], _v_pids)
                    ev = _vf(er["assignments"], er["pref_map"], _v_pids)
                    opt = ev["total_score"]
                    cur = hv["total_score"]
                    gap = opt - cur
                    rate = (cur / opt * 100) if opt > 0 else 100.0
                    label_short = hr["label"].replace("候補", "").split("：")[0]
                    _cmp_cols[ci].metric(
                        label_short,
                        f"{rate:.1f}%",
                        f"差 {gap:+.1f}点",
                        delta_color="inverse" if gap > 0 else "normal"
                    )
                st.caption("最適解比率：厳密解を100%としたときのヒューリスティック解の総スコア比率。")
                st.divider()
            _show_verify("🎯 厳密解", _e_results, "#5A1A7C")
        except Exception as _ve:
            st.warning(f"検証エラー: {_ve}")

    # PDFダウンロードボタン（3種類）
    col_title, col_sel = st.columns([4, 3])
    col_title.subheader("候補案比較")
    try:
        from concert.services.report import generate_assign_report
        from concert.services.convert_utils import render_report_output
        concert_name = ctx.get("SELECTED_CONCERT_NAME", "演奏会")
        _h_res = st.session_state.get(f"assign_result_heuristic_{concert_id}", [])
        _e_res = st.session_state.get(f"assign_result_exact_{concert_id}", [])

        # どのPDFを出力するか選択
        pdf_opts = []
        if _h_res:          pdf_opts.append("⚡ ヒューリスティック解")
        if _e_res:          pdf_opts.append("🎯 厳密解")
        if _h_res and _e_res: pdf_opts.append("📊 比較（両解を並べて表示）")

        if pdf_opts:
            sel_pdf = col_sel.radio(
                "出力する候補案", pdf_opts, horizontal=True,
                key=f"assign_pdf_sel_{concert_id}",
            )
            if st.button("📄 候補案を出力", key=f"assign_pdf_gen_{concert_id}",
                         use_container_width=True, type="primary"):
                with st.spinner("PDF生成中..."):
                    if "ヒューリスティック" in sel_pdf:
                        _pdf = generate_assign_report(concert_name, _h_res, songs, players, ctx)
                        _fname = f"アサイン_ヒューリスティック_{concert_name}"
                    elif "厳密解" in sel_pdf:
                        _pdf = generate_assign_report(concert_name, _e_res, songs, players, ctx)
                        _fname = f"アサイン_厳密解_{concert_name}"
                    else:
                        _pdf = generate_assign_report(
                            concert_name, _h_res, songs, players, ctx,
                            compare_results=_e_res, compare_label="厳密解",
                        )
                        _fname = f"アサイン比較_{concert_name}"
                    st.session_state[f"assign_pdf_bytes_{concert_id}"] = _pdf
                    st.session_state[f"assign_pdf_fname_{concert_id}"] = _fname

        _assign_pdf = st.session_state.get(f"assign_pdf_bytes_{concert_id}")
        if _assign_pdf:
            render_report_output(
                _assign_pdf,
                filename=st.session_state.get(f"assign_pdf_fname_{concert_id}", "アサイン候補案"),
                label="アサイン候補案",
                key_prefix=f"assign_pdf_{concert_id}",
            )
    except Exception as e:
        st.caption(f"PDF生成エラー: {e}")

    # サマリーカード（verify()ベース）
    from concert.services.verify_results import verify as _verify_ui
    def _vui(r):
        return _verify_ui(r["assignments"], r["pref_map"])

    cols = st.columns(len(results))
    for col, r in zip(cols, results):
        v = _vui(r)
        col.metric(r["label"].split("：")[1] if "：" in r["label"] else r["label"],
                   f"{v['total_score']:.1f}点",
                   f"第1希望率 {v['first_choice_rate']*100:.0f}%")

    st.divider()

    # ── グラフ表示（希望充足・担当数分布）────────────────────
    try:
        from concert.services.report import make_stacked_bar, make_dist_bar
        _gcol1, _gcol2 = st.columns(2)
        with _gcol1:
            st.markdown("**希望充足の内訳**")
            st.image(make_stacked_bar(results), use_container_width=True)
        with _gcol2:
            st.markdown("**担当曲数の分布**")
            st.image(make_dist_bar(results), use_container_width=True)
    except Exception as _ge:
        st.caption(f"グラフ生成エラー: {_ge}")

    st.divider()

    # 各候補の詳細
    tabs = st.tabs([r["label"] for r in results])
    song_name_map = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_order    = [s.get("id") for s in sorted(songs, key=lambda x: _song_name(x, ctx))]

    from concert.services.score_constants import CANDIDATE_DESC as _CAND_DESC_RAW
    # UI では long 説明を使用
    CANDIDATE_DESC = {k: v["long"] for k, v in _CAND_DESC_RAW.items()}

    for tab, result in zip(tabs, results):
        with tab:
            desc = CANDIDATE_DESC.get(result["label"], "")
            if desc:
                st.caption(desc)
            _ap = st.session_state.get(f"assign_all_participants_{concert_id}", [])
            _player_label_map = {pid: pname for pid, pname in _ap if pid}
            for _a in result["assignments"]:
                _pid = _a.get("player_id", "")
                _pname = _a.get("player_name", "") or _pid
                if _pid and _pid not in _player_label_map:
                    _player_label_map[_pid] = _pname
            _manual_assignments = _get_manual_assignments(concert_id, result["label"], result["assignments"])
            _manual_assignments = _render_manual_assignment_editor(
                concert_id=concert_id,
                result_label=result["label"],
                song_order=song_order,
                song_name_map=song_name_map,
                base_assignments=result["assignments"],
                player_label_map=_player_label_map,
            )
            display_assignments = _manual_assignments if _manual_assignments else result["assignments"]
            _vm = _vui(result)
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("総スコア",   f"{_vm['total_score']:.1f}点")
            m2.metric("第1希望率",  f"{_vm['first_choice_rate']*100:.1f}%")
            m3.metric("最低スコア", f"{_vm['min_player_score']:.1f}点")
            m4.metric("補完件数",   f"{_vm['supplemental_count']}件")

            # 曲ごとの割当（HTML表示）
            by_song: dict[str, list] = defaultdict(list)
            for a in display_assignments:
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

            # 表示対象はCONCERT_CASTの打楽器奏者のみ（_all_participants）
            _valid_pids = {pid for pid, _ in _ap}
            for _pid, _pname in _ap:
                if _pid and _pname:
                    player_names[_pid] = _pname

            # 割当済みスコアと曲セットを集計
            assigned_songs_per_player: dict[str, set] = defaultdict(set)
            for a in display_assignments:
                pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                pref = result["pref_map"].get(pk)
                sc   = ({1:3.0,2:2.0,3:1.0}.get(pref["priority"],0.0)
                        if pref and pref["priority"] > 0 else 0.5)
                player_scores[a["player_id"]] += sc
                if a["source"] in ("fallback", "swap"):
                    player_fb[a["player_id"]] += 1
                if a["player_id"] in _valid_pids:
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

            # pref_mapからの名前補完はCONCERT_CAST内の人のみ
            for pk_str, pref in result["pref_map"].items():
                pid = pref["player_id"]
                if pref["priority"] > 0 and pid in _valid_pids and pid not in player_names:
                    player_names[pid] = pref["player_name"]

            st.markdown(_render_player_score_html(
                player_scores, player_fb, player_names, player_unassigned),
                unsafe_allow_html=True)

            # 採用ボタン
            st.divider()
            _changes = _collect_assignment_changes(result["assignments"], display_assignments)
            _dup_map = _find_song_player_duplicates(display_assignments)
            if _dup_map:
                _dup_lines: list[str] = []
                for _sid, _names in _dup_map.items():
                    _dup_lines.append(f"{song_name_map.get(_sid, _sid)}: {', '.join(_names)}")
                st.error("同一曲で同一奏者の重複割当があります。採用前に解消してください。")
                for _line in _dup_lines:
                    st.markdown(f"- {_line}")
            if _changes:
                st.warning(f"手動変更 {len(_changes)}件（採用時は下記の内容で書き込みます）")
                for c in _changes[:30]:
                    st.markdown(
                        f"- {song_name_map.get(c.get('song_id', ''), c.get('song_id', ''))} / "
                        f"{c.get('part_name', '—')}: {c.get('from_player', '—')} → {c.get('to_player', '—')}"
                    )
                if len(_changes) > 30:
                    st.caption(f"…ほか {len(_changes) - 30}件")
            else:
                st.caption("手動変更なし（候補案そのままで採用されます）")
            if st.button(f"✅ この案を採用してNotionに書き込む",
                         key=f"adopt_{result['label']}", type="primary",
                         disabled=bool(_dup_map)):
                _write_assignments_to_notion(ctx, display_assignments, result["pref_map"])


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
    """採用した割当をCONCERT_ASSIGNMENTに書き込む。
    レコードの一意キー：演奏会 + 奏者 + パート定義
    """
    db_id    = ctx.get("CONCERT_DB_CONCERT_ASSIGNMENT", "")
    if not db_id:
        st.error("CONCERT_ASSIGNMENT DBが未設定です。secrets.tomlに CONCERT_DB_CONCERT_ASSIGNMENT を追加してください。")
        return
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("CONCERT_ASSIGNMENT DBのプロパティ取得に失敗しました。")
        return

    concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()

    # 書き込み前にこの演奏会の既存レコードをアーカイブ（全リセット）
    if concert_id:
        existing = ctx["query_all"](db_id)
        with st.spinner("既存のアサインをリセット中..."):
            for r in existing:
                c_ids = ctx["extract_relation_ids_any"](r, ASSIGNMENT_CONCERT_REL_KEYS)
                if concert_id not in (c_ids or []):
                    continue
                ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{r.get('id','')}",
                                   json={"archived": True})

    ok = fail = 0
    with st.spinner("Notionに書き込み中..."):
        for a in assignments:
            player_id = a["player_id"]
            part_id   = a["part_id"]
            song_id   = a.get("song_id", "")
            inst_id   = a.get("instrument_id", "")

            props: dict = {}
            ctx["put_key_any"](props, type_map, ASSIGNMENT_KEY_KEYS,
                               concert_id, player_id, part_id, prefix="assign")
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_CONCERT_REL_KEYS, concert_id)
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_PLAYER_REL_KEYS,  player_id)
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_PARTDEF_REL_KEYS, part_id)
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_SONG_REL_KEYS,    song_id)
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_INST_REL_KEYS,    inst_id)
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_FLAG_KEYS,        True)
            ctx["put_prop_any"](props, type_map, ASSIGNMENT_NOTE_KEYS,
                                f"{a.get('player_name','')} × {a.get('song_name','')} × {a.get('part_name','')}")
            res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                     json={"parent": {"database_id": db_id}, "properties": props})
            if res and res.status_code == 200:
                ok += 1
            else:
                fail += 1

    if concert_id:
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_PLAN_KEYS, fail == 0 and ok > 0)
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_ASSIGN_KEYS, False)
    # query_all のキャッシュを明示的に無効化して、直後のタブ遷移でも最新を表示する
    st.cache_data.clear()
    if fail == 0:
        st.success(f"✅ {ok}件のアサインを書き込みました。")
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

    # ── HARMONIA_CONCERTのフラグ状態を取得 ──────────────────────
    _hc_rows = ctx["query_all"](ctx.get("CONCERT_DB_HARMONIA_CONCERT", ""), None) if ctx.get("CONCERT_DB_HARMONIA_CONCERT") else []
    _hc_row = next(
        (r for r in _hc_rows if concert_id in ctx["extract_relation_ids_any"](r, HARMONIA_CONCERT_CONCERT_REL_KEYS)),
        None
    )
    _proposal_on = _hc_row and ctx["extract_prop_text_any"](_hc_row, HARMONIA_CONCERT_PLAN_KEYS) == "True"
    _assign_on   = _hc_row and ctx["extract_prop_text_any"](_hc_row, HARMONIA_CONCERT_ASSIGN_KEYS) == "True"

    # ── 現在の状態表示 ──────────────────────────────────────────
    if _assign_on:
        _status_label = "✅ アサイン確定済"
        _status_color = "#4caf50"
    elif _proposal_on:
        _status_label = "🟡 案提示中"
        _status_color = "#ffa726"
    else:
        _status_label = "⚪ 案未提示"
        _status_color = "#888"

    st.markdown(
        f"<div style='border:2px solid {_status_color};border-radius:8px;padding:8px 14px;margin-bottom:8px'>"        f"<b>現在の状態：</b> {_status_label}</div>",
        unsafe_allow_html=True,
    )

    # ── 確定前プレビュー（差分/不足確認）────────────────────────
    _assign_db = ctx.get("CONCERT_DB_CONCERT_ASSIGNMENT", "")
    _preview_rows = ctx["query_all"](_assign_db, None) if _assign_db else []
    _assigned_for_concert = []
    for _r in _preview_rows:
        _cids = ctx["extract_relation_ids_any"](_r, ASSIGNMENT_CONCERT_REL_KEYS)
        if concert_id and _cids and concert_id not in _cids:
            continue
        if ctx["extract_prop_text_any"](_r, ASSIGNMENT_FLAG_KEYS) == "True":
            _assigned_for_concert.append(_r)
    _by_song_count: dict[str, int] = defaultdict(int)
    _unique_players: set[str] = set()
    for _r in _assigned_for_concert:
        _sids = ctx["extract_relation_ids_any"](_r, ASSIGNMENT_SONG_REL_KEYS)
        _pids = ctx["extract_relation_ids_any"](_r, ASSIGNMENT_PLAYER_REL_KEYS)
        if _sids:
            _by_song_count[_sids[0]] += 1
        if _pids:
            _unique_players.add(_pids[0])
    _assign_ok, _req_total, _missing_labels = _compute_assignment_completion(ctx, concert_id)
    with st.expander("🧪 確定前プレビュー", expanded=False):
        st.caption(
            f"担当レコード: {len(_assigned_for_concert)} 件 / 担当者数: {len(_unique_players)} 人 / "
            f"未割当パート定義: {len(_missing_labels)} 件"
        )
        if _by_song_count:
            _song_map = {s.get("id", ""): _song_name(s, ctx) for s in _load_songs(ctx, concert_id)}
            _rows = [{"演奏曲": _song_map.get(_sid, _sid), "担当件数": _cnt} for _sid, _cnt in sorted(_by_song_count.items(), key=lambda x: x[0])]
            st.dataframe(_rows, use_container_width=True, hide_index=True)
        if _missing_labels:
            st.caption(f"未割当（先頭15件）: {' / '.join(_missing_labels[:15])}")
        if st.button("🔄 プレビューを更新", key="assign_preview_refresh", use_container_width=True):
            st.rerun()

    # ── フラグ操作ボタン ────────────────────────────────────────
    _col1, _col2 = st.columns(2)

    # 案を提示する
    if _col1.button(
        "📢 案を提示する",
        key="btn_propose",
        use_container_width=True,
        disabled=bool(_proposal_on or _assign_on),
    ):
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_PLAN_KEYS, True, concert_name)
        st.cache_data.clear()
        _clear_assign_cache()
        st.rerun()

    # 案の提示を取り消す
    if _col2.button(
        "↩ 案の提示を取り消す",
        key="btn_unpropose",
        use_container_width=True,
        disabled=bool(not _proposal_on or _assign_on),
    ):
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_PLAN_KEYS, False, concert_name)
        st.cache_data.clear()
        _clear_assign_cache()
        st.rerun()

    _col3, _col4 = st.columns(2)

    # アサインを確定する
    if _col3.button(
        "✅ アサインを確定する",
        key="btn_assign_confirm",
        type="primary",
        use_container_width=True,
        disabled=bool(not _proposal_on or _assign_on),
    ):
        assign_complete, required_count, missing_labels = _compute_assignment_completion(ctx, concert_id)
        if not assign_complete:
            st.error(f"未割当のパート定義があります（未割当 {len(missing_labels)} / 全{required_count}）。")
            if missing_labels:
                st.caption(f"未割当（先頭10件）: {' / '.join(missing_labels[:10])}")
        else:
            _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_ASSIGN_KEYS, True, concert_name)
            st.cache_data.clear()
            _clear_assign_cache()
            st.rerun()

    # アサインをやり直す
    if _col4.button(
        "↩ アサインをやり直す",
        key="btn_assign_reset",
        use_container_width=True,
        type="secondary",
    ):
        with st.spinner("リセット中..."):
            _reset_rows = [r for r in ctx["query_all"](ctx.get("CONCERT_DB_CONCERT_ASSIGNMENT",""), None)
                           if concert_id in ctx["extract_relation_ids_any"](r, ASSIGNMENT_CONCERT_REL_KEYS)]
            _ok = _fail = 0
            for r in _reset_rows:
                res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{r.get('id','')}",
                                          json={"archived": True})
                if res and res.status_code == 200: _ok += 1
                else: _fail += 1
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_PLAN_KEYS, False, concert_name)
        _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_ASSIGN_KEYS, False, concert_name)
        st.success(f"✅ {_ok}件リセットしました。")
        st.cache_data.clear()
        _clear_assign_cache()
        st.rerun()

    st.divider()

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
    participant_rows = _load_participants(ctx, concert_id)
    selected_part, selected_player_ids, _ = _render_shared_part_filter(
        ctx, concert_id, participant_rows, show_widget=False
    )
    if selected_player_ids:
        players = [p for p in players if p.get("id", "") in selected_player_ids]
    if not players:
        st.info(f"「{selected_part}」に該当する奏者がいません。")
        return

    inst_rows     = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}
    inst_opts     = {_instrument_name(r, ctx): r.get("id","") for r in sorted(inst_rows, key=lambda x: _instrument_name(x, ctx))}

    player_name_map = {p.get("id"): _player_name(p, ctx) for p in players}
    player_opts     = {_player_name(p, ctx): p.get("id","") for p in sorted(players, key=lambda x: _player_name(x, ctx))}
    song_name_map   = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_id_set     = {s.get("id") for s in songs}

    db_id = ctx.get("CONCERT_DB_CONCERT_ASSIGNMENT", "")
    if not db_id:
        st.warning("CONCERT_ASSIGNMENT DBが未設定です。")
        return
    t_map   = ctx["get_prop_types"](db_id)
    ext_any = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    # この演奏会のアサインレコードを取得
    all_rows = ctx["query_all"](db_id)
    assigned_rows = []
    for r in all_rows:
        c_ids = ext_rel(r, ASSIGNMENT_CONCERT_REL_KEYS)
        if concert_id and c_ids and concert_id not in c_ids:
            continue
        p_ids = ext_rel(r, ASSIGNMENT_PLAYER_REL_KEYS)
        if selected_player_ids and (not p_ids or p_ids[0] not in selected_player_ids):
            continue
        if ext_any(r, ASSIGNMENT_FLAG_KEYS) == "True":
            assigned_rows.append(r)

    if not assigned_rows:
        st.info("まだ担当が確定していません。「アルゴリズム実行」タブから割当を実行してください。")
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
        s_ids = ext_rel(r, ASSIGNMENT_SONG_REL_KEYS)
        matched_sid = s_ids[0] if s_ids and s_ids[0] in song_id_set else ""

        # フォールバック: パート定義リレーション経由
        if not matched_sid:
            pt_ids = ext_rel(r, ASSIGNMENT_PARTDEF_REL_KEYS)
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

    # 演奏曲フィルタ
    _sorted_songs = sorted(songs, key=lambda x: _song_name(x, ctx))
    _song_filter_opts = {"（全曲）": ""} | {_song_name(s, ctx): s.get("id", "") for s in _sorted_songs}
    _selected_song_label = st.selectbox(
        "演奏曲を絞り込む",
        list(_song_filter_opts.keys()),
        key="result_song_filter",
    )
    _selected_song_id = _song_filter_opts.get(_selected_song_label, "")

    for song in _sorted_songs:
        sid   = song.get("id","")
        if _selected_song_id and sid != _selected_song_id:
            continue
        sname = _song_name(song, ctx)
        rows  = by_song.get(sid, [])
        with st.expander(f"**{sname}**　{len(rows)}パート", expanded=True):
            if not rows:
                st.caption("割当なし")
                continue
            for r in rows:
                rid   = r.get("id","")
                p_ids = ext_rel(r, ASSIGNMENT_PLAYER_REL_KEYS)
                i_ids = ext_rel(r, ASSIGNMENT_INST_REL_KEYS)
                cur_pname = player_name_map.get(p_ids[0], "不明") if p_ids else "不明"
                cur_iname = inst_name_map.get(i_ids[0], "不明") if i_ids else "不明"
                note      = ext_any(r, ASSIGNMENT_NOTE_KEYS) or ""

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
                        ctx["put_prop_any"](props, t_map, ASSIGNMENT_PLAYER_REL_KEYS, new_pid)
                        ctx["put_prop_any"](props, t_map, ASSIGNMENT_INST_REL_KEYS,   new_iid)
                        ctx["put_prop_any"](props, t_map, ASSIGNMENT_NOTE_KEYS,       new_note)
                        res = ctx["api_request"]("patch",
                                                  f"https://api.notion.com/v1/pages/{rid}",
                                                  json={"properties": props})
                        if res and res.status_code == 200:
                            st.success("✅ 更新しました。")
                            _clear_assign_cache()
                            st.rerun()
                        else:
                            st.error("❌ 更新に失敗しました。")




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
