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


SCORE_MAP = {1: 3.0, 2: 2.0, 3: 1.0, 0: 0.0}

PREF_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者", "演奏会参加者"]
PREF_SONG_REL_KEYS = ["楽曲", "演奏曲", "FK楽曲", "作品楽章", "作品マスタ"]
PREF_PART_REL_KEYS = ["パート", "パート定義", "FKパート"]
PREF_INSTR_REL_KEYS = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PREF_PRIORITY_KEYS = ["希望順位", "優先度", "希望", "希望区分"]
PREF_CAN_BRING_KEYS = ["持参可", "持参可フラグ", "持参"]
PREF_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]

PART_SONG_REL_KEYS = ["楽曲", "演奏曲", "FK楽曲", "作品楽章", "作品マスタ"]
PART_INST_REL_KEYS = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PART_NAME_KEYS = ["パート名", "名称", "タイトル", "表示名"]
PART_COUNT_KEYS = ["必要人数", "必要台数", "台数", "人数"]
PART_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]

ATT_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者", "演奏会参加者"]
ATT_STATUS_KEYS = ["参加可否", "出欠", "参加状況"]
ATT_PRACTICE_REL_KEYS = ["練習", "FK練習", "演奏会"]
PARTICIPANT_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者", "演奏会参加者"]

PRACTICE_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]


def _priority_to_int(v: str) -> int:
    s = (v or "").strip()
    if not s:
        return 0
    # NG判定：「絶対NG」「NG」「不可」「無理」等をすべて-1に
    s_lower = s.lower()
    if any(kw in s for kw in ("NG", "絶対NG", "不可", "無理")) or s_lower in ("ng",):
        return -1
    if "降り番" in s or "希望なし" in s:
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
    """
    本番当日の出欠が×の奏者のみをabsentに入れる。
    練習欠席だけではアサイン対象外にしない。
    本番当日レコードが存在しない場合はabsentなしとして扱う。
    """
    practice_db = ctx["CONCERT_DB_PRACTICE"]
    attendance_db = ctx["CONCERT_DB_ATTENDANCE"]
    p_types = ctx["get_prop_types"](practice_db)
    p_rel = ctx["find_prop_name"](p_types, PRACTICE_CONCERT_REL_KEYS)
    if not p_rel:
        return set()
    p_rows = ctx["query_all"](practice_db,
                              {"filter": {"property": p_rel, "relation": {"contains": concert_id}}})

    # 本番当日レコードのIDだけ取得
    concert_day_ids: set[str] = set()
    for r in p_rows:
        day_flag = (ctx["extract_prop_text_any"](r, ["本番日", "演奏会当日フラグ", "本番フラグ"]) or "").lower()
        if day_flag in ("true", "1", "yes", "はい", "○"):
            rid = r.get("id", "")
            if rid:
                concert_day_ids.add(rid)

    # 本番当日レコードがなければabsentなし
    if not concert_day_ids:
        return set()

    a_rows = ctx["query_all"](attendance_db)
    absent: set[str] = set()
    participant_map = _participant_to_player_map(ctx)
    for r in a_rows:
        pids = ctx["extract_relation_ids_any"](r, ATT_PRACTICE_REL_KEYS)
        if not pids or pids[0] not in concert_day_ids:
            continue
        status = ctx["extract_prop_text_any"](r, ATT_STATUS_KEYS) or ""
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
            cids = ctx["extract_relation_ids"](r, concert_rel_key)
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
            cids = ctx["extract_relation_ids"](r, concert_rel_key)
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
        _cb_raw = ctx["extract_prop_text_any"](r, PREF_CAN_BRING_KEYS) or ""
        can_bring = _cb_raw.lower() in ("true", "1", "yes", "はい", "○", "true")
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
    all_participants: list[tuple[str, str]] | None = None,
) -> list[Assignment]:
    """
    all_participants: [(player_id, player_name), ...] 参加者全員リスト。
    希望未提出の参加者もfallback候補に含めるために使用。
    """
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
    # fallback候補：希望を提出した奏者のみ（未提出者は除外）
    _pref_players = {p.player_id: p.player_name for p in prefs}
    all_players = sorted(_pref_players.items(), key=lambda x: x[1])

    for req in requirements:
        slots = max(req.required_count, 1)
        cands = by_req_key.get((req.song_id, req.part_id), [])
        for pref in cands:
            if slots <= 0:
                break
            if not is_feasible_assign(req, pref.player_id, absent_players, assigned_song_players, ng_map):
                continue
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
                )
            )
            assigned_song_players.add((req.song_id, pref.player_id))
            slots -= 1

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


def _min_player_score(
    solution: list[Assignment],
    pref_map: dict[tuple[str, str, str], Pref],
    all_player_ids: list[str] | None = None,
) -> float:
    """全奏者（割当ゼロ含む）を対象にした最低スコア。"""
    by_player: dict[str, float] = {pid: 0.0 for pid in (all_player_ids or [])}
    for a in solution:
        by_player.setdefault(a.player_id, 0.0)
        by_player[a.player_id] += max(score_assignment(a, pref_map), 0)
    return min(by_player.values()) if by_player else 0.0


def _bring_count(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref]) -> int:
    n = 0
    for a in solution:
        p = pref_map.get((a.player_id, a.song_id, a.part_id))
        if p and p.can_bring:
            n += 1
    return n


def _rest_std(solution: list[Assignment], all_player_ids: list[str]) -> float:
    # 簡易版: 割当件数の標準偏差を逆指標として利用
    c = {pid: 0 for pid in all_player_ids}
    for a in solution:
        c[a.player_id] = c.get(a.player_id, 0) + 1
    arr = list(c.values())
    return pstdev(arr) if len(arr) > 1 else 0.0


def _is_valid(trial: list[Assignment], pref_map: dict,
              absent_players: set[str] | None = None) -> bool:
    """割当リストの重複・NG違反・欠席者チェック"""
    seen: set = set()
    for x in trial:
        # 欠席者チェック
        if absent_players and x.player_id in absent_players:
            return False
        k = (x.song_id, x.player_id)
        if k in seen:
            return False
        seen.add(k)
        p = pref_map.get((x.player_id, x.song_id, x.part_id))
        if p and p.priority == -1:
            return False
    return True


def local_search(
    solution: list[Assignment],
    pref_map: dict[tuple[str, str, str], Pref],
    objective_fn,
    max_iter: int = 250,
    absent_players: set[str] | None = None,
    all_player_ids: list[str] | None = None,
) -> list[Assignment]:
    """
    局所探索：3種類の近傍移動を試みる。
    1. 同一曲内スワップ（奏者AとBのパートを入れ替え）
    2. 同一曲内差し替え（降り番奏者→割当中奏者を置換）
    3. 曲またぎ交換：曲Aの奏者Xと曲Bの奏者Y（別曲）のパートを交換
    """
    absent_players = absent_players or set()
    cur = list(solution)
    cur_score = objective_fn(cur)

    # 差し替え候補：希望提出者のみ（未提出者は近傍探索でも投入しない）
    _pref_pids = sorted({p.player_id for p in pref_map.values()})
    all_pids = _pref_pids

    improved = True
    it = 0
    while improved and it < max_iter:
        improved = False
        it += 1
        n = len(cur)

        # ── 近傍1: 同一曲内スワップ ──────────────────────────
        for i in range(n):
            for j in range(i + 1, n):
                a, b = cur[i], cur[j]
                if a.song_id != b.song_id:
                    continue
                na = Assignment(b.player_id, b.player_name, a.song_id, a.song_name,
                                a.part_id, a.part_name, a.instrument_id, a.instrument_name, "swap")
                nb = Assignment(a.player_id, a.player_name, b.song_id, b.song_name,
                                b.part_id, b.part_name, b.instrument_id, b.instrument_name, "swap")
                trial = list(cur)
                trial[i], trial[j] = na, nb
                if not _is_valid(trial, pref_map, absent_players):
                    continue
                sc = objective_fn(trial)
                if sc > cur_score:
                    cur, cur_score = trial, sc
                    improved = True
                    break
            if improved:
                break
        if improved:
            continue

        # ── 近傍2: 降り番奏者→割当奏者の置き換え ────────────
        # 割当済み奏者と未割当（降り番）奏者を曲ごとに特定
        assigned_by_song: dict[str, set] = {}
        for a in cur:
            assigned_by_song.setdefault(a.song_id, set()).add(a.player_id)

        all_songs = {a.song_id for a in cur}
        for i, a in enumerate(cur):
            # この割当を別の奏者に差し替える
            absent_pids = [pid for pid in all_pids
                           if pid not in assigned_by_song.get(a.song_id, set())]
            for new_pid in absent_pids:
                # new_pidがこのパートにNGでないか確認
                p = pref_map.get((new_pid, a.song_id, a.part_id))
                if p and p.priority == -1:
                    continue
                # new_pidの名前を取得
                new_name = next(
                    (pref.player_name for pref in pref_map.values()
                     if pref.player_id == new_pid), new_pid
                )
                na = Assignment(new_pid, new_name, a.song_id, a.song_name,
                                a.part_id, a.part_name, a.instrument_id, a.instrument_name, "swap")
                trial = list(cur)
                trial[i] = na
                if not _is_valid(trial, pref_map, absent_players):
                    continue
                sc = objective_fn(trial)
                if sc > cur_score:
                    cur, cur_score = trial, sc
                    improved = True
                    break
            if improved:
                break
        if improved:
            continue

        # ── 近傍3: 曲またぎ交換 ────────────────────────────────
        # 異なる曲に割り当てられた奏者AとBのスロットを交換する。
        # 曲αのパートPをやっているAと、曲βのパートQをやっているBを交換：
        # → AがβのQ、BがαのPを担当する。
        # これにより、各曲単独では改善できない公平性・降り番均等を改善できる。
        if not improved:
            n2 = len(cur)
            for i in range(n2):
                if improved: break
                for j in range(i + 1, n2):
                    a, b = cur[i], cur[j]
                    # 同一曲は近傍1で処理済みなのでスキップ
                    if a.song_id == b.song_id:
                        continue
                    # AをbのパートへA, BをaのパートへB（曲またぎ）
                    na = Assignment(a.player_id, a.player_name,
                                    b.song_id, b.song_name,
                                    b.part_id, b.part_name,
                                    b.instrument_id, b.instrument_name, "swap")
                    nb = Assignment(b.player_id, b.player_name,
                                    a.song_id, a.song_name,
                                    a.part_id, a.part_name,
                                    a.instrument_id, a.instrument_name, "swap")
                    trial = list(cur)
                    trial[i], trial[j] = na, nb
                    if not _is_valid(trial, pref_map, absent_players):
                        continue
                    sc = objective_fn(trial)
                    if sc > cur_score:
                        cur, cur_score = trial, sc
                        improved = True
                        break

    return cur


def _calc_stats(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref], all_player_ids: list[str]) -> dict:
    return {
        "first_choice_rate": round(_first_choice_rate(solution, pref_map), 4),
        "total_score": _total_score(solution, pref_map),
        "min_score": _min_player_score(solution, pref_map, all_player_ids),  # 全員版
        "rental_count": max(len(solution) - _bring_count(solution, pref_map), 0),
        "rest_std": round(_rest_std(solution, all_player_ids), 4),
    }


def solve_all(ctx: dict, concert_id: str) -> list[dict]:
    prefs = _load_preferences(ctx, concert_id)
    reqs = _load_requirements(ctx, concert_id)
    absent = _build_absent_set(ctx, concert_id)

    if not prefs or not reqs:
        return []

    # 参加者DB全員（希望未提出も含む）をfallback候補に
    all_participants: list[tuple[str, str]] = []
    try:
        part_rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"])
        p_to_pl = _participant_to_player_map(ctx)
        pl_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
        pl_name = {r.get("id",""): ctx["extract_title"](r) or "" for r in pl_rows}
        for pid in p_to_pl.values():
            if pid:
                all_participants.append((pid, pl_name.get(pid, pid)))
        all_participants = sorted(set(all_participants), key=lambda x: x[1])
    except Exception:
        all_participants = []

    pref_map = {(p.player_id, p.song_id, p.part_id): p for p in prefs}
    base = greedy_solve(prefs, reqs, absent, all_participants)

    # 全員（参加者DB + 希望提出者）のIDリスト
    _all_pid_set = {p.player_id for p in prefs}
    for pid, _ in all_participants:
        _all_pid_set.add(pid)
    all_player_ids = sorted(_all_pid_set)

    def obj_a(sol):  # 第1希望率最大
        return _first_choice_rate(sol, pref_map) * 10000 + _total_score(sol, pref_map)

    def obj_b(sol):  # 総スコア最大
        return _total_score(sol, pref_map)

    def obj_c(sol):  # 公平性（全員の最低スコアを最大化）
        return _min_player_score(sol, pref_map, all_player_ids) * 1000 + _total_score(sol, pref_map)

    def obj_d(sol):  # 降り番均等（割当分散最小）
        return int((1000 - (_rest_std(sol, all_player_ids) * 100)) * 100) + _total_score(sol, pref_map)

    variants = [
        ("候補A：第1希望率最大", obj_a),
        ("候補B：総スコア最大",  obj_b),
        ("候補C：公平性重視",    obj_c),
        ("候補D：降り番均等",    obj_d),
    ]

    out = []
    for label, fn in variants:
        sol = local_search(base, pref_map, fn, max_iter=250,
                          absent_players=absent, all_player_ids=all_player_ids)
        out.append(
            {
                "label": label,
                "assignments": [a.__dict__ for a in sol],
                "stats": _calc_stats(sol, pref_map, all_player_ids),
            }
        )
    return out
