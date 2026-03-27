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
from concert.services.score_constants import SCORE_MAP, SUPPLEMENTAL_SCORE, NG_PRIORITY


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
        return SUPPLEMENTAL_SCORE
    if p.priority == NG_PRIORITY:
        return -9999.0
    if p.priority == 0:
        # 降り番希望 → 降り番 = 0点（割当がある場合はフォールバック扱いで0.5）
        return SUPPLEMENTAL_SCORE if a.source in ("fallback", "swap") else 0.0
    base = SCORE_MAP.get(p.priority, SUPPLEMENTAL_SCORE)
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
    shuffle_seed: int | None = None,
) -> list[Assignment]:
    """
    all_participants: 現在は未使用（将来の拡張用に引数として残す）。
    fallback候補は prefs 由来の希望提出者のみ。
    shuffle_seed: Noneなら決定的（デフォルト）、整数ならその乱数でreqの走査順をシャッフル。
    """
    by_req_key: dict[tuple[str, str], list[Pref]] = {}
    ng_map = {(p.player_id, p.song_id, p.part_id) for p in prefs if p.priority == NG_PRIORITY}

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
        if p and p.priority == NG_PRIORITY:
            return False
    return True


def _perturb(
    solution: list[Assignment],
    pref_map: dict,
    absent_players: set[str],
    n_swap: int = 3,
    seed: int = 0,
) -> list[Assignment]:
    """
    局所最適から脱出するための揺さぶり操作（perturbation）。
    n_swap件のアサインをランダムに入れ替えて初期解の多様性を確保する。
    """
    import random as _rnd
    rng = _rnd.Random(seed)
    cur = list(solution)
    n = len(cur)
    if n < 2:
        return cur

    attempts = 0
    swapped = 0
    while swapped < n_swap and attempts < n_swap * 10:
        attempts += 1
        i, j = rng.sample(range(n), 2)
        a, b = cur[i], cur[j]
        if a.song_id == b.song_id:
            # 同一曲内スワップ
            na = Assignment(b.player_id, b.player_name, a.song_id, a.song_name,
                            a.part_id, a.part_name, a.instrument_id, a.instrument_name, "swap")
            nb = Assignment(a.player_id, a.player_name, b.song_id, b.song_name,
                            b.part_id, b.part_name, b.instrument_id, b.instrument_name, "swap")
            trial = list(cur); trial[i], trial[j] = na, nb
        else:
            # 曲またぎスワップ
            na = Assignment(a.player_id, a.player_name, b.song_id, b.song_name,
                            b.part_id, b.part_name, b.instrument_id, b.instrument_name, "swap")
            nb = Assignment(b.player_id, b.player_name, a.song_id, a.song_name,
                            a.part_id, a.part_name, a.instrument_id, a.instrument_name, "swap")
            trial = list(cur); trial[i], trial[j] = na, nb

        # 欠席・NG違反は無視（揺さぶりなので制約を一部緩める）
        # ただし同一奏者×同一曲の重複だけは弾く
        seen: set = set()
        bad = False
        for x in trial:
            k = (x.song_id, x.player_id)
            if k in seen: bad = True; break
            seen.add(k)
            p = pref_map.get((x.player_id, x.song_id, x.part_id))
            if p and p.priority == NG_PRIORITY: bad = True; break
            if x.player_id in absent_players: bad = True; break
        if not bad:
            cur = trial
            swapped += 1
    return cur


def local_search(
    solution: list[Assignment],
    pref_map: dict[tuple[str, str, str], Pref],
    objective_fn,
    max_iter: int = 250,
    absent_players: set[str] | None = None,
    all_player_ids: list[str] | None = None,
    verbose: bool = False,
) -> list[Assignment]:
    """
    局所探索：4種類の近傍移動を試みる。
    1. 同一曲内スワップ（奏者AとBのパートを入れ替え）
    2. 同一曲内差し替え（降り番奏者→割当中奏者を置換）
    3. 曲またぎ交換：曲Aの奏者Xと曲Bの奏者Y（別曲）のパートを交換
    4. スコア底上げ差し替え（第1希望以外の割当をより高スコアの未割当奏者で置換）
    verbose=True の場合、改善ログを返す（戻り値がtupleになる）。

    限界: 1件ずつの近傍移動のため、複数件の同時変更が必要な改善は検出できない。
    厳密解との差が生じる場合は iterated_local_search で揺さぶりを試みる。
    それでも残る差は構造的なもので、局所探索の範囲では解消できない。
    """
    absent_players = absent_players or set()
    cur = list(solution)
    cur_score = objective_fn(cur)

    # 差し替え候補：希望提出者のみ（未提出者は近傍探索でも投入しない）
    _pref_pids = sorted({p.player_id for p in pref_map.values()})
    all_pids = _pref_pids

    improved = True
    it = 0
    improve_log: list[dict] = []   # verbose用改善ログ
    n1_count = n2_count = n3_count = 0  # 近傍タイプ別採用回数
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
                if p and p.priority == NG_PRIORITY:
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
                    if verbose:
                        improve_log.append({"iter": it, "type": "n2_replace", "delta": sc - cur_score})
                        n2_count += 1
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
                        if verbose:
                            improve_log.append({"iter": it, "type": "n3_crosssong", "delta": sc - cur_score})
                            n3_count += 1
                        cur, cur_score = trial, sc
                        improved = True
                        break

        # ── 近傍4: スコア底上げ差し替え ──────────────────────────
        # 第1希望以外の割当を、同曲でより高スコアの未割当奏者に置換する
        if not improved:
            _nm4 = {pref.player_id: pref.player_name for pref in pref_map.values()}
            abs_by_song4: dict[str, set] = {}
            for a in cur:
                abs_by_song4.setdefault(a.song_id, set()).add(a.player_id)
            for i, a in enumerate(cur):
                if improved: break
                sc_a = score_assignment(a, pref_map)
                if sc_a >= 3.0:
                    continue
                absent_here4 = [pid for pid in all_pids
                                if pid not in abs_by_song4.get(a.song_id, set())]
                for new_pid in absent_here4:
                    p_new = pref_map.get((new_pid, a.song_id, a.part_id))
                    if p_new and p_new.priority == NG_PRIORITY:
                        continue
                    sc_new = SCORE_MAP.get(p_new.priority, 0.5) if (p_new and p_new.priority > 0) else 0.5
                    if sc_new <= sc_a:
                        continue
                    na = Assignment(new_pid, _nm4.get(new_pid, new_pid),
                                    a.song_id, a.song_name,
                                    a.part_id, a.part_name,
                                    a.instrument_id, a.instrument_name, "swap")
                    trial = list(cur)
                    trial[i] = na
                    if not _is_valid(trial, pref_map, absent_players):
                        continue
                    sc = objective_fn(trial)
                    if sc > cur_score:
                        if verbose:
                            improve_log.append({"iter": it, "type": "n4_uplift", "delta": sc - cur_score})
                        cur, cur_score = trial, sc
                        improved = True
                        break

    if verbose:
        n4_count = sum(1 for e in improve_log if e.get("type") == "n4_uplift")
        summary = {
            "total_iterations":    it,
            "total_improvements":  n1_count + n2_count + n3_count + n4_count,
            "n1_swap_count":       n1_count,
            "n2_replace_count":    n2_count,
            "n3_crosssong_count":  n3_count,
            "n4_uplift_count":     n4_count,
            "final_objective":     cur_score,
            "improve_log":         improve_log,
        }
        return cur, summary
    return cur


def _calc_stats(solution: list[Assignment], pref_map: dict[tuple[str, str, str], Pref], all_player_ids: list[str]) -> dict:
    return {
        "first_choice_rate": round(_first_choice_rate(solution, pref_map), 4),
        "total_score": _total_score(solution, pref_map),
        "min_score": _min_player_score(solution, pref_map, all_player_ids),  # 全員版
        "rental_count": max(len(solution) - _bring_count(solution, pref_map), 0),
        "rest_std": round(_rest_std(solution, all_player_ids), 4),
    }


def iterated_local_search(
    solution: list[Assignment],
    pref_map: dict,
    objective_fn,
    absent_players: set[str],
    all_player_ids: list[str] | None = None,
    n_restart: int = 15,
    max_iter_per_restart: int = 200,
    n_perturb: int = 3,
) -> list[Assignment]:
    """
    反復局所探索（Iterated Local Search）。
    局所最適に陥ったら揺さぶりをかけて再探索する。
    n_restart: 揺さぶりの回数
    n_perturb: 1回の揺さぶりで入れ替えるアサイン数
    """
    best = list(solution)
    best_score = objective_fn(best)

    cur = best
    for restart in range(n_restart):
        # 揺さぶり
        perturbed = _perturb(cur, pref_map, absent_players,
                             n_swap=n_perturb, seed=restart)
        # 局所探索
        improved = local_search(perturbed, pref_map, objective_fn,
                                max_iter=max_iter_per_restart,
                                absent_players=absent_players,
                                all_player_ids=all_player_ids)
        # tupleで返ってきた場合（verbose=True時）の対処
        if isinstance(improved, tuple):
            improved = improved[0]
        sc = objective_fn(improved)
        if sc > best_score:
            best = improved
            best_score = sc
        # 現在解を更新（広域探索のためにacceptする）
        cur = improved

    return best


def solve_all(ctx: dict, concert_id: str) -> list[dict]:
    prefs = _load_preferences(ctx, concert_id)
    reqs = _load_requirements(ctx, concert_id)
    absent = _build_absent_set(ctx, concert_id)

    if not prefs or not reqs:
        return []

    # fallback候補は希望提出者のみ（greedy_solve内で prefs から構築）
    pref_map = {(p.player_id, p.song_id, p.part_id): p for p in prefs}

    # 複数初期解を生成して最良（総スコア最大）を採用
    def _total_sc(sol):
        return sum(max(score_assignment(a, pref_map), 0) for a in sol)
    base = greedy_solve(prefs, reqs, absent)
    best_base_score = _total_sc(base)
    for _seed in range(1, 8):  # 7通りの走査順で試す
        _cand = greedy_solve(prefs, reqs, absent, shuffle_seed=_seed)
        _cand_sc = _total_sc(_cand)
        if _cand_sc > best_base_score:
            base = _cand
            best_base_score = _cand_sc

    # 評価対象：希望提出者のみ（solve_allは参加者DBにアクセスしない）
    all_player_ids = sorted({p.player_id for p in prefs})

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
        sol = iterated_local_search(base, pref_map, fn,
                                    absent_players=absent,
                                    all_player_ids=all_player_ids,
                                    n_restart=15, max_iter_per_restart=200)
        out.append(
            {
                "label": label,
                "assignments": [a.__dict__ for a in sol],
                "stats": _calc_stats(sol, pref_map, all_player_ids),
            }
        )
    return out


# ============================================================
# 厳密解法（scipy.optimize.milp による整数計画法）
# ============================================================

def _build_var_index(
    player_ids: list[str],
    song_ids: list[str],
    part_ids_by_song: dict[str, list[str]],
) -> tuple[dict[tuple[str, str, str], int], int]:
    """変数インデックス (player_id, song_id, part_id) → int を構築。"""
    var_index: dict[tuple[str, str, str], int] = {}
    idx = 0
    for p in player_ids:
        for s in song_ids:
            for t in part_ids_by_song.get(s, []):
                var_index[(p, s, t)] = idx
                idx += 1
    return var_index, idx


def _base_constraints(
    var_index: dict,
    n_vars: int,
    player_ids: list[str],
    song_ids: list[str],
    part_ids_by_song: dict[str, list[str]],
    req_map: dict[tuple[str, str], int],
    absent_players: set[str],
    ng_set: set[tuple[str, str, str]],
) -> tuple[list, list, list, "np.ndarray"]:
    """
    全候補共通の基本制約を構築。
    戻り値: (A_rows, b_lo, b_hi, ub)
    """
    import numpy as np
    ub = np.ones(n_vars)
    for (p, s, t), vi in var_index.items():
        if p in absent_players or (p, s, t) in ng_set:
            ub[vi] = 0.0

    A_rows, b_lo, b_hi = [], [], []

    # C1: 各(s,t)の必要数
    for (s, t), rc in req_map.items():
        row = np.zeros(n_vars)
        for p in player_ids:
            vi = var_index.get((p, s, t))
            if vi is not None:
                row[vi] = 1.0
        A_rows.append(row)
        b_lo.append(float(rc))
        b_hi.append(float(rc))

    # C2: 1奏者1曲1パート
    for p in player_ids:
        for s in song_ids:
            pts = part_ids_by_song.get(s, [])
            if len(pts) <= 1:
                continue
            row = np.zeros(n_vars)
            for t in pts:
                vi = var_index.get((p, s, t))
                if vi is not None:
                    row[vi] = 1.0
            A_rows.append(row)
            b_lo.append(0.0)
            b_hi.append(1.0)

    return A_rows, b_lo, b_hi, ub


def _run_milp(c, A_rows, b_lo, b_hi, ub_arr, n_int_vars: int, time_limit: float):
    """共通のmilp呼び出し。"""
    import numpy as np
    from scipy.optimize import milp, LinearConstraint, Bounds
    n_vars = len(c)
    integrality = np.zeros(n_vars)
    integrality[:n_int_vars] = 1
    bounds = Bounds(lb=np.zeros(n_vars), ub=ub_arr)
    constraints = LinearConstraint(np.array(A_rows), lb=b_lo, ub=b_hi)
    return milp(c=c, constraints=constraints, integrality=integrality,
                bounds=bounds, options={"time_limit": time_limit, "disp": False})


def _extract_assignments(
    x,
    var_index: dict,
    pref_map: dict,
    req_name_map: dict,
    player_name_map: dict,
) -> list[Assignment]:
    """milp解からAssignmentリストを生成。"""
    result = []
    for (p, s, t), vi in var_index.items():
        if x[vi] > 0.5:
            sname, pname, iid, iname = req_name_map.get((s, t), (s, t, "", ""))
            pref = pref_map.get((p, s, t))
            result.append(Assignment(
                player_id=p,
                player_name=player_name_map.get(p, p),
                song_id=s,
                song_name=sname,
                part_id=t,
                part_name=pname,
                instrument_id=iid,
                instrument_name=iname,
                source="exact",
            ))
    return result


def solve_exact(
    prefs: list[Pref],
    requirements: list[Requirement],
    absent_players: set[str],
    all_player_ids: list[str] | None = None,
    time_limit_sec: float = 30.0,
) -> list[dict]:
    """
    scipy.optimize.milp による厳密解法（整数計画法）。
    候補A〜Dを厳密に最適化して返す。

    候補A: 総スコア最大化（Σ score*x を最大化）
    候補B: 公平性（max min_p y_p、補助変数mで線形化）
    候補C: 降り番均等（min range of c_p = c_max - c_min）
    候補D: 第1希望率最大化（第1希望割当数を最大化）

    制約（全候補共通）:
      Σ_p x[p,s,t] = req[s,t]        （必要数充足）
      Σ_t x[p,s,t] ≤ 1              （1奏者1曲1パート）
      x[p,s,t] = 0 if p ∈ absent   （欠席除外）
      x[p,s,t] = 0 if NG           （NG除外）
      x[p,s,t] ∈ {0, 1}
    """
    import numpy as np

    if not prefs or not requirements:
        return []

    # fallback候補：希望提出者のみ（希望未提出者は割当対象外）
    player_ids  = sorted({p.player_id for p in prefs})
    song_ids    = sorted({r.song_id for r in requirements})
    part_ids_by_song: dict[str, list[str]] = {}
    for r in requirements:
        part_ids_by_song.setdefault(r.song_id, []).append(r.part_id)

    pref_map        = {(p.player_id, p.song_id, p.part_id): p for p in prefs}
    ng_set          = {(p.player_id, p.song_id, p.part_id) for p in prefs if p.priority == NG_PRIORITY}
    req_map         = {(r.song_id, r.part_id): r.required_count for r in requirements}
    req_name_map    = {(r.song_id, r.part_id): (r.song_name, r.part_name, r.instrument_id, r.instrument_name)
                      for r in requirements}
    player_name_map = {p.player_id: p.player_name for p in prefs}

    # スコアマップ（希望なし・fallbackは0.5）
    def _sc(p: str, s: str, t: str) -> float:
        pref = pref_map.get((p, s, t))
        if pref is None:
            return SUPPLEMENTAL_SCORE
        if pref.priority <= 0:
            return 0.0
        return SCORE_MAP.get(pref.priority, SUPPLEMENTAL_SCORE)

    var_index, n_x = _build_var_index(player_ids, song_ids, part_ids_by_song)
    if n_x == 0:
        return []

    A_base, blo_base, bhi_base, ub_base = _base_constraints(
        var_index, n_x, player_ids, song_ids, part_ids_by_song,
        req_map, absent_players, ng_set,
    )

    # 全候補で使う評価用 all_player_ids
    _eval_pids = sorted(set(all_player_ids or []) | set(player_ids))

    results = []

    # ── 候補A: 第1希望率最大 + 同率なら総スコア最大（高速Aと完全一致）
    # 高速モード: _first_choice_rate×10000 + _total_score
    # 第1希望割当 = 10000点、それ以外はスコアそのまま（最大3点）
    # 10000 >> 3 なので単一目的関数として正しく優先順位を反映できる
    first_choice_set = {(p.player_id, p.song_id, p.part_id) for p in prefs if p.priority == 1}
    n_first = sum(1 for k in first_choice_set if k in var_index)  # 第1希望変数数（正規化用）
    def _sc_a(p, s, t):
        if (p, s, t) in first_choice_set:
            return 10000.0 + _sc(p, s, t)  # 第1希望 = 最優先 + スコア
        return _sc(p, s, t)                 # それ以外はスコアのみ
    c_a = np.array([-_sc_a(p, s, t) for (p, s, t) in var_index])
    r_a = _run_milp(c_a, A_base, blo_base, bhi_base, ub_base, n_x, time_limit_sec)
    if r_a.success:
        sol_a = _extract_assignments(r_a.x, var_index, pref_map, req_name_map, player_name_map)
        results.append({
            "label": "候補A：第1希望率最大（厳密解）",
            "assignments": [a.__dict__ for a in sol_a],
            "stats": _calc_stats(sol_a, pref_map, _eval_pids),
        })

    # ── 候補B: 総スコア最大化（高速モードのBと対応）──────────
    c_b_score = np.array([-_sc(p, s, t) for (p, s, t) in var_index])
    r_b_score = _run_milp(c_b_score, A_base, blo_base, bhi_base, ub_base, n_x, time_limit_sec)
    if r_b_score.success:
        sol_b_score = _extract_assignments(r_b_score.x, var_index, pref_map, req_name_map, player_name_map)
        results.append({
            "label": "候補B：総スコア最大（厳密解）",
            "assignments": [a.__dict__ for a in sol_b_score],
            "stats": _calc_stats(sol_b_score, pref_map, _eval_pids),
        })

    # ── 候補C: 公平性（max min_p y_p）（高速モードのCと対応）──
    # 変数: [x(n_x), m(1)]  目的: min -m
    n_b = n_x + 1; m_idx = n_x
    c_b = np.zeros(n_b); c_b[m_idx] = -1.0
    ub_b = np.append(ub_base, 1e6)
    A_b = [np.append(row, 0.0) for row in A_base]
    blo_b = list(blo_base); bhi_b = list(bhi_base)
    # m ≤ y_p: -Σ score*x + m ≤ 0
    for p in player_ids:
        row = np.zeros(n_b)
        for s in song_ids:
            for t in part_ids_by_song.get(s, []):
                vi = var_index.get((p, s, t))
                if vi is not None:
                    row[vi] = -_sc(p, s, t)
        row[m_idx] = 1.0
        A_b.append(row); blo_b.append(-1e9); bhi_b.append(0.0)
    r_b = _run_milp(c_b, A_b, blo_b, bhi_b, ub_b, n_x, time_limit_sec)
    if r_b.success:
        sol_b = _extract_assignments(r_b.x[:n_x], var_index, pref_map, req_name_map, player_name_map)
        results.append({
            "label": "候補C：公平性重視（厳密解）",
            "assignments": [a.__dict__ for a in sol_b],
            "stats": _calc_stats(sol_b, pref_map, _eval_pids),
        })

    # ── 候補D: 降り番均等（min c_max - c_min）────────────────
    # 変数: [x(n_x), c_max(1), c_min(1)]  目的: min c_max - c_min
    n_c = n_x + 2; cmax_i = n_x; cmin_i = n_x + 1
    c_c = np.zeros(n_c); c_c[cmax_i] = 1.0; c_c[cmin_i] = -1.0
    n_songs = float(len(song_ids))
    ub_c = np.append(ub_base, [n_songs, n_songs])
    A_c = [np.append(row, [0.0, 0.0]) for row in A_base]
    blo_c = list(blo_base); bhi_c = list(bhi_base)
    for p in player_ids:
        row_max = np.zeros(n_c); row_min = np.zeros(n_c)
        for s in song_ids:
            for t in part_ids_by_song.get(s, []):
                vi = var_index.get((p, s, t))
                if vi is not None:
                    row_max[vi] = -1.0; row_min[vi] = 1.0
        row_max[cmax_i] = 1.0; row_min[cmin_i] = -1.0
        A_c.append(row_max); blo_c.append(0.0); bhi_c.append(1e9)
        A_c.append(row_min); blo_c.append(0.0); bhi_c.append(1e9)
    r_c = _run_milp(c_c, A_c, blo_c, bhi_c, ub_c, n_x, time_limit_sec)
    if r_c.success:
        sol_c = _extract_assignments(r_c.x[:n_x], var_index, pref_map, req_name_map, player_name_map)
        results.append({
            "label": "候補D：降り番均等（厳密解）",
            "assignments": [a.__dict__ for a in sol_c],
            "stats": _calc_stats(sol_c, pref_map, _eval_pids),
        })

    return results
