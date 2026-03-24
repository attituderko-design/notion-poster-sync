"""
concert.services.assign_solver
アサイン検討ロジック（貪欲法 + 局所探索）。

目的:
- 演奏会単位で「奏者×曲×パート」の候補割当を複数案生成
- 希望入力DB / パート定義DB / 練習出欠DB を参照
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import pstdev


@dataclass
class Pref:
    player_id: str
    player_name: str
    song_id: str
    song_name: str
    part_id: str
    part_name: str
    instrument_id: str
    instrument_name: str
    priority: int  # 1..3, 0=降り番希望, -1=絶対NG
    can_bring: bool


@dataclass
class Requirement:
    song_id: str
    song_name: str
    part_id: str
    part_name: str
    instrument_id: str
    instrument_name: str
    required_count: int


@dataclass
class Assignment:
    player_id: str
    player_name: str
    song_id: str
    song_name: str
    part_id: str
    part_name: str
    instrument_id: str
    instrument_name: str
    source: str  # "preference" / "fallback"
    tied: bool = False  # 同点タイブレーク発生フラグ
    tied_candidates: list = None  # 同点の候補者名リスト


SCORE_MAP = {1: 3.0, 2: 2.0, 3: 1.0, 0: 0.0}






def _priority_to_int(v: str) -> int:
    s = (v or "").strip()
    if not s:
        return 0
    if "絶対" in s and "NG" in s:
        return -1
    if "降り番" in s:
        return 0
    if "第1" in s:
        return 1
    if "第2" in s:
        return 2
    if "第3" in s:
        return 3
    try:
        n = int(s)
        if n in (1, 2, 3):
            return n
    except Exception:
        pass
    return 0


def _extract_rel_name_cache(ctx: dict, db_id: str, ids: set[str]) -> dict[str, str]:
    if not ids:
        return {}
    rows = ctx["query_all"](db_id)
    out = {}
    for r in rows:
        rid = r.get("id", "")
        if rid in ids:
            out[rid] = ctx["extract_title"](r) or rid
    return out


def _participant_to_player_map(ctx: dict) -> dict[str, str]:
    """演奏会参加者DB(page_id) -> 奏者DB(page_id) の逆引きマップ。"""
    out: dict[str, str] = {}
    rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"])
    for r in rows:
        part_id = r.get("id", "")
        if not part_id:
            continue
        pids = ctx["extract_relation_ids_any"](r, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            out[part_id] = pids[0]
    return out


def _build_absent_set(ctx: dict, concert_id: str) -> set[str]:
    practice_db = ctx["CONCERT_DB_PRACTICE"]
    attendance_db = ctx["CONCERT_DB_ATTENDANCE"]
    p_types = ctx["get_prop_types"](practice_db)
    p_rel = ctx["find_prop_name"](p_types, PRACTICE_CONCERT_REL_KEYS)
    if not p_rel:
        return set()
    p_rows = ctx["query_all"](practice_db, {"filter": {"property": p_rel, "relation": {"contains": concert_id}}})
    practice_ids = {r.get("id", "") for r in p_rows if r.get("id")}
    if not practice_ids:
        return set()

    a_rows = ctx["query_all"](attendance_db)
    absent = set()
    participant_map = _participant_to_player_map(ctx)
    for r in a_rows:
        pids = ctx["extract_relation_ids_any"](r, ATT_PRACTICE_REL_KEYS)
        if not pids or pids[0] not in practice_ids:
            continue
        status = ctx["extract_prop_text_any"](r, ATT_STATUS_KEYS)
        if "×" not in status:
            continue
        p_rel_ids = ctx["extract_relation_ids_any"](r, ATT_PLAYER_REL_KEYS)
        if p_rel_ids:
            pid = p_rel_ids[0]
            absent.add(participant_map.get(pid, pid))
    return absent


def _load_requirements(ctx: dict, concert_id: str) -> list[Requirement]:
    rows = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"])
    t_map = ctx["get_prop_types"](ctx["CONCERT_DB_PART_DEFINITION"])
    concert_rel_key = ctx["find_prop_name"](t_map, PART_CONCERT_REL_KEYS)
    out: list[Requirement] = []

    song_ids = set()
    inst_ids = set()
    tmp = []
    for r in rows:
        if concert_rel_key:
            cids = ctx["extract_relation_ids_any"](r, [concert_rel_key] if concert_rel_key else SONG_CONCERT_REL_KEYS)
            if concert_id not in cids:
                continue
        sid = (ctx["extract_relation_ids_any"](r, PART_SONG_REL_KEYS) or [""])[0]
        iid = (ctx["extract_relation_ids_any"](r, PART_INST_REL_KEYS) or [""])[0]
        if not sid:
            continue
        song_ids.add(sid)
        if iid:
            inst_ids.add(iid)
        cnt_text = ctx["extract_prop_text_any"](r, PART_COUNT_KEYS) or "1"
        try:
            cnt = max(int(float(cnt_text)), 1)
        except Exception:
            cnt = 1
        pname = ctx["extract_prop_text_any"](r, PART_NAME_KEYS) or ctx["extract_title"](r)
        tmp.append((sid, iid, pname, cnt, r.get("id", "")))

    song_name_map = _extract_rel_name_cache(ctx, ctx["CONCERT_DB_SONG"], song_ids)
    inst_name_map = _extract_rel_name_cache(ctx, ctx["CONCERT_DB_INSTRUMENT"], inst_ids)

    for sid, iid, pname, cnt, pid in tmp:
        sname = song_name_map.get(sid, sid)
        iname = inst_name_map.get(iid, iid) if iid else ""
        out.append(
            Requirement(
                song_id=sid,
                song_name=sname,
                part_id=pid,
                part_name=pname or iname or pid,
                instrument_id=iid,
                instrument_name=iname,
                required_count=cnt,
            )
        )
    return out


def _load_preferences(ctx: dict, concert_id: str) -> list[Pref]:
    rows = ctx["query_all"](ctx["CONCERT_DB_PREFERENCE"])
    t_map = ctx["get_prop_types"](ctx["CONCERT_DB_PREFERENCE"])
    concert_rel_key = ctx["find_prop_name"](t_map, PREF_CONCERT_REL_KEYS)

    player_ids = set()
    song_ids = set()
    part_ids = set()
    inst_ids = set()
    raw = []
    participant_map = _participant_to_player_map(ctx)
    for r in rows:
        if concert_rel_key:
            cids = ctx["extract_relation_ids_any"](r, [concert_rel_key] if concert_rel_key else SONG_CONCERT_REL_KEYS)
            if concert_id not in cids:
                continue
        raw_pid = (ctx["extract_relation_ids_any"](r, PREF_PLAYER_REL_KEYS) or [""])[0]
        pid = participant_map.get(raw_pid, raw_pid)
        sid = (ctx["extract_relation_ids_any"](r, PREF_SONG_REL_KEYS) or [""])[0]
        part_id = (ctx["extract_relation_ids_any"](r, PREF_PART_REL_KEYS) or [""])[0]
        iid = (ctx["extract_relation_ids_any"](r, PREF_INSTR_REL_KEYS) or [""])[0]
        if not (pid and sid and part_id):
            continue
        pr = _priority_to_int(ctx["extract_prop_text_any"](r, PREF_PRIORITY_KEYS))
        can_bring = ctx["extract_prop_text_any"](r, PREF_CAN_BRING_KEYS) == "True"
        player_ids.add(pid)
        song_ids.add(sid)
        part_ids.add(part_id)
        if iid:
            inst_ids.add(iid)
        raw.append((pid, sid, part_id, iid, pr, can_bring))

    player_map = _extract_rel_name_cache(ctx, ctx["CONCERT_DB_PLAYER"], player_ids)
    song_map = _extract_rel_name_cache(ctx, ctx["CONCERT_DB_SONG"], song_ids)
    part_map = _extract_rel_name_cache(ctx, ctx["CONCERT_DB_PART_DEFINITION"], part_ids)
    inst_map = _extract_rel_name_cache(ctx, ctx["CONCERT_DB_INSTRUMENT"], inst_ids)

    out = []
    for pid, sid, partid, iid, pr, can_bring in raw:
        out.append(
            Pref(
                player_id=pid,
                player_name=player_map.get(pid, pid),
                song_id=sid,
                song_name=song_map.get(sid, sid),
                part_id=partid,
                part_name=part_map.get(partid, partid),
                instrument_id=iid,
                instrument_name=inst_map.get(iid, iid) if iid else "",
                priority=pr,
                can_bring=can_bring,
            )
        )
    return out


def score_assignment(a: Assignment, pref_map: dict[tuple[str, str, str], Pref]) -> float:
    k = (a.player_id, a.song_id, a.part_id)
    p = pref_map.get(k)
    if not p:
        # 希望データ自体が存在しない = フォールバック割当
        return 0.5
    if p.priority == -1:
        return -9999.0
    if p.priority == 0:
        # 降り番希望 → 降り番 = 0点（割当がある場合はフォールバック扱いで0.5）
        return 0.5 if a.source in ("fallback", "swap") else 0.0
    base = SCORE_MAP.get(p.priority, 0.5)
    return base


def is_feasible_assign(
    req: Requirement,
    player_id: str,
    absent_players: set[str],
    assigned_song_players: set[tuple[str, str]],
    ng_map: set[tuple[str, str, str]],
) -> bool:
    if player_id in absent_players:
        return False
    # 1奏者は1曲で1パート
    if (req.song_id, player_id) in assigned_song_players:
        return False
    if (player_id, req.song_id, req.part_id) in ng_map:
        return False
    return True


def greedy_solve(
    prefs: list[Pref],
    requirements: list[Requirement],
    absent_players: set[str],
) -> list[Assignment]:
    by_req_key: dict[tuple[str, str], list[Pref]] = {}
    ng_map = {(p.player_id, p.song_id, p.part_id) for p in prefs if p.priority == -1}

    # 奏者ごとの有効希望数（priority > 0 のもの）をカウント
    # 希望数が少ない = 集中投票しているほど同点時に優先される
    from collections import Counter
    pref_count: Counter = Counter(
        p.player_id for p in prefs if p.priority > 0
    )

    for p in prefs:
        by_req_key.setdefault((p.song_id, p.part_id), []).append(p)
    for k in by_req_key:
        by_req_key[k].sort(
            key=lambda x: (
                SCORE_MAP.get(x.priority, 0),   # 第1キー：高スコア優先（降順）
                -pref_count[x.player_id],        # 第2キー：希望数が少ない人優先（昇順→負値で降順に）
            ),
            reverse=True,
        )

    assigned: list[Assignment] = []
    assigned_song_players: set[tuple[str, str]] = set()
    all_players = sorted({p.player_id: p.player_name for p in prefs}.items(), key=lambda x: x[1])

    for req in requirements:
        slots = max(req.required_count, 1)
        cands = by_req_key.get((req.song_id, req.part_id), [])

        # 実行可能な候補を順番に収集し、同点検出のために先読みする
        feasible: list[Pref] = []
        for pref in cands:
            if not is_feasible_assign(req, pref.player_id, absent_players, assigned_song_players, ng_map):
                continue
            feasible.append(pref)

        slot_idx = 0
        while slot_idx < slots and slot_idx < len(feasible):
            pref = feasible[slot_idx]
            # 次の候補と同点かどうか判定
            # ソートキー：(スコア, -希望数) が同じなら同点
            def sort_key(p):
                return (SCORE_MAP.get(p.priority, 0), -pref_count[p.player_id])
            tied = False
            tied_names = []
            next_idx = slot_idx + 1
            # まだ割り当てられていない次の候補が同点かチェック
            for ni in range(next_idx, len(feasible)):
                np = feasible[ni]
                if sort_key(np) == sort_key(pref) and pref.priority > 0:
                    tied = True
                    tied_names = [p.player_name for p in feasible[slot_idx:]
                                  if sort_key(p) == sort_key(pref) and p.priority > 0]
                    break

            assigned.append(
                Assignment(
                    player_id=pref.player_id,
                    player_name=pref.player_name,
                    song_id=req.song_id,
                    song_name=req.song_name,
                    part_id=req.part_id,
                    part_name=req.part_name,
                    instrument_id=req.instrument_id,
                    instrument_name=req.instrument_name,
                    source="preference",
                    tied=tied,
                    tied_candidates=tied_names if tied else [],
                )
            )
            assigned_song_players.add((req.song_id, pref.player_id))
            slot_idx += 1
        slots -= slot_idx

        # 希望不足分を補完
        if slots > 0:
            for pid, pname in all_players:
                if slots <= 0:
                    break
                if not is_feasible_assign(req, pid, absent_players, assigned_song_players, ng_map):
                    continue
                assigned.append(
                    Assignment(
                        player_id=pid,
                        player_name=pname,
                        song_id=req.song_id,
                        song_name=req.song_name,
                        part_id=req.part_id,
                        part_name=req.part_name,
                        instrument_id=req.instrument_id,
                        instrument_name=req.instrument_name,
                        source="fallback",
                    )
                )
                assigned_song_players.add((req.song_id, pid))
                slots -= 1
    return assigned


def _total_score(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref]) -> float:
    return sum(max(score_assignment(a, pref_map), 0) for a in solution)


def _first_choice_rate(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref]) -> float:
    if not solution:
        return 0.0
    n = 0
    for a in solution:
        p = pref_map.get((a.player_id, a.song_id, a.part_id))
        if p and p.priority == 1:
            n += 1
    return n / len(solution)


def _min_player_score(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref]) -> float:
    by_player: dict[str, int] = {}
    for a in solution:
        by_player.setdefault(a.player_id, 0)
        by_player[a.player_id] += max(score_assignment(a, pref_map), 0)
    return min(by_player.values()) if by_player else 0


def _bring_count(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref]) -> int:
    n = 0
    for a in solution:
        p = pref_map.get((a.player_id, a.song_id, a.part_id))
        if p and p.can_bring:
            n += 1
    return n


def _unassigned_penalty(
    solution: list[Assignment],
    pref_map: dict[tuple[str, str, str], Pref],
    all_player_ids: list[str],
) -> float:
    """
    希望を出した（priority > 0）のに割り当てられなかったパートにペナルティを付与。
    乗りたかったのに乗れなかった人が不公平にならないよう公平性重視の候補Cで使用。
    ペナルティ = 未割当の希望数 × 1.0点（奏者のスコアから差し引く概念）
    """
    assigned_keys: set[tuple[str, str, str]] = {
        (a.player_id, a.song_id, a.part_id) for a in solution
    }
    # 希望を出したが割り当てられなかった件数を奏者ごとに集計
    unassigned: dict[str, int] = {pid: 0 for pid in all_player_ids}
    for (pid, sid, part_id), p in pref_map.items():
        if p.priority > 0 and (pid, sid, part_id) not in assigned_keys:
            unassigned[pid] = unassigned.get(pid, 0) + 1
    # ペナルティ合計（全奏者の未割当希望数の合計）
    return float(sum(unassigned.values()))


def _rest_std(solution: list[Assignment], all_player_ids: list[str]) -> float:
    # 簡易版: 割当件数の標準偏差を逆指標として利用
    c = {pid: 0 for pid in all_player_ids}
    for a in solution:
        c[a.player_id] = c.get(a.player_id, 0) + 1
    arr = list(c.values())
    return pstdev(arr) if len(arr) > 1 else 0.0


def local_search(
    solution: list[Assignment],
    pref_map: dict[tuple[str, str, str], Pref],
    objective_fn,
    max_iter: int = 200,
) -> list[Assignment]:
    # 2人のパートを入れ替えるシンプル近傍探索
    cur = list(solution)
    cur_score = objective_fn(cur)
    improved = True
    it = 0
    while improved and it < max_iter:
        improved = False
        it += 1
        n = len(cur)
        for i in range(n):
            for j in range(i + 1, n):
                a = cur[i]
                b = cur[j]
                # 同一曲内の交換に限定（自然な運用）
                if a.song_id != b.song_id:
                    continue
                na = Assignment(
                    player_id=b.player_id,
                    player_name=b.player_name,
                    song_id=a.song_id,
                    song_name=a.song_name,
                    part_id=a.part_id,
                    part_name=a.part_name,
                    instrument_id=a.instrument_id,
                    instrument_name=a.instrument_name,
                    source="swap",
                )
                nb = Assignment(
                    player_id=a.player_id,
                    player_name=a.player_name,
                    song_id=b.song_id,
                    song_name=b.song_name,
                    part_id=b.part_id,
                    part_name=b.part_name,
                    instrument_id=b.instrument_id,
                    instrument_name=b.instrument_name,
                    source="swap",
                )
                trial = list(cur)
                trial[i], trial[j] = na, nb

                # 同一奏者×同一曲の重複割当は不可
                seen = set()
                bad = False
                for x in trial:
                    k = (x.song_id, x.player_id)
                    if k in seen:
                        bad = True
                        break
                    seen.add(k)
                    p = pref_map.get((x.player_id, x.song_id, x.part_id))
                    if p and p.priority == -1:
                        bad = True
                        break
                if bad:
                    continue

                sc = objective_fn(trial)
                if sc > cur_score:
                    cur, cur_score = trial, sc
                    improved = True
                    break
            if improved:
                break
    return cur


def _calc_stats(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref], all_player_ids: list[str]) -> dict:
    return {
        "first_choice_rate": round(_first_choice_rate(solution, pref_map), 4),
        "total_score": _total_score(solution, pref_map),
        "min_score": _min_player_score(solution, pref_map),
        "rental_count": max(len(solution) - _bring_count(solution, pref_map), 0),
        "rest_std": round(_rest_std(solution, all_player_ids), 4),
        "unassigned_penalty": _unassigned_penalty(solution, pref_map, all_player_ids),
    }


def solve_all(ctx: dict, concert_id: str) -> list[dict]:
    prefs = _load_preferences(ctx, concert_id)
    reqs = _load_requirements(ctx, concert_id)
    absent = _build_absent_set(ctx, concert_id)

    if not prefs or not reqs:
        return []

    pref_map = {(p.player_id, p.song_id, p.part_id): p for p in prefs}
    base = greedy_solve(prefs, reqs, absent)
    all_player_ids = sorted({p.player_id for p in prefs})

    def obj_a(sol):  # 第1希望率最大
        return _first_choice_rate(sol, pref_map) * 10000 + _total_score(sol, pref_map)

    def obj_b(sol):  # 総スコア最大
        return _total_score(sol, pref_map)

    def obj_c(sol):  # 公平性（未割当ペナルティ込み）
        penalty = _unassigned_penalty(sol, pref_map, all_player_ids)
        return _min_player_score(sol, pref_map) * 1000 + _total_score(sol, pref_map) - penalty * 10

    def obj_d(sol):  # レンタル最小
        return _bring_count(sol, pref_map) * 1000 + _total_score(sol, pref_map)

    def obj_e(sol):  # 降り番均等（簡易: 割当分散最小）
        return int((1000 - (_rest_std(sol, all_player_ids) * 100)) * 100) + _total_score(sol, pref_map)

    variants = [
        ("候補A：第1希望率最大", obj_a),
        ("候補B：総スコア最大", obj_b),
        ("候補C：公平性重視", obj_c),
        ("候補D：レンタル最小", obj_d),
        ("候補E：降り番均等", obj_e),
    ]

    out = []
    for label, fn in variants:
        sol = local_search(base, pref_map, fn, max_iter=250)
        out.append(
            {
                "label": label,
                "assignments": [a.__dict__ for a in sol],
                "stats": _calc_stats(sol, pref_map, all_player_ids),
            }
        )
    return out
