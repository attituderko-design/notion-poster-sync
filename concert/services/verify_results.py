"""
concert/services/verify_results.py
ヒューリスティック解と厳密解を同じ基準で再採点する検証モジュール。
report.pyのstats表示とは独立して動作する。

設計方針:
- 入出力はdict（assignments list + pref_map dict）のまま維持する
- 他モジュールへの影響ゼロで内部ロジックだけ改善する
- pref_mapのキー形式を明示的に扱い、暗黙の前提を排除する
"""
from __future__ import annotations

# スコア定数（assign_solver.pyのSCORE_MAPと同じ値）
_SCORE_MAP: dict[int, float] = {1: 3.0, 2: 2.0, 3: 1.0}

# pref_mapのキーを生成する唯一の関数
# str((pid, sid, part_id)) 形式に統一
def _pref_key(player_id: str, song_id: str, part_id: str) -> str:
    return str((player_id, song_id, part_id))


def _pref_score(pref: dict | None, source: str) -> float:
    """
    1件のassignmentのスコアを計算する。
    pref: pref_map から取得した希望データ（Noneなら希望なし）
    source: assignment の source フィールド
    """
    if pref is None:
        # 希望データなし → 補完割当なら0.5、それ以外は0.0
        return 0.5 if source in ("fallback", "swap", "exact") else 0.0
    prio = pref.get("priority", 0)
    if prio > 0:
        return _SCORE_MAP.get(prio, 0.0)
    # priority == 0: 降り番希望 → 0.0
    # priority == -1: NG（割当されてはいけないが念のため）
    return 0.0


def _is_supplemental(pref: dict | None, source: str) -> bool:
    """
    補完割当かどうかを判定する。
    「希望データがない or 希望なし(priority<=0)」かつ割当あり = 補完。
    ILSのswapでも希望がある場合は補完としない。
    """
    prio = pref.get("priority", None) if pref else None
    return (prio is None or prio <= 0) and source in ("fallback", "swap", "exact")


def verify(
    assignments: list[dict],
    pref_map: dict,
    all_player_ids: list[str] | None = None,
) -> dict:
    """
    assignmentsとpref_mapを受け取り、共通基準で再採点する。

    Parameters
    ----------
    assignments : list[dict]
        Assignment.__dict__のリスト。
        必須フィールド: player_id, song_id, part_id, source
    pref_map : dict
        str((player_id, song_id, part_id)) → {"priority": int, "player_id": str, ...}
        キーは _pref_key() で生成した文字列であること。
    all_player_ids : list[str] | None
        評価対象奏者IDリスト。Noneならassignmentsに出てきた奏者のみ。

    Returns
    -------
    dict with keys:
        total_score, first_choice_count, first_choice_rate,
        min_player_score, assignment_count_by_player,
        score_by_player, supplemental_count, total_assignments
    """
    assigned_pids = {a["player_id"] for a in assignments}
    eval_pids     = set(all_player_ids) if all_player_ids else assigned_pids

    by_player_score: dict[str, float] = {pid: 0.0 for pid in eval_pids}
    by_player_count: dict[str, int]   = {pid: 0   for pid in eval_pids}
    total_score        = 0.0
    first_choice_count = 0
    supplemental_count = 0  # 補完割当件数（旧fallback_count + swap_countの統合）

    for a in assignments:
        pid    = a["player_id"]
        source = a.get("source", "")
        pk     = _pref_key(pid, a["song_id"], a["part_id"])
        pref   = pref_map.get(pk)
        sc     = _pref_score(pref, source)

        total_score += sc
        by_player_score[pid] = by_player_score.get(pid, 0.0) + sc
        by_player_count[pid] = by_player_count.get(pid, 0) + 1

        prio = pref.get("priority", 0) if pref else None
        if prio == 1:
            first_choice_count += 1
        if _is_supplemental(pref, source):
            supplemental_count += 1

    n_slots = len(assignments)
    first_choice_rate = (first_choice_count / n_slots) if n_slots > 0 else 0.0
    min_player_score  = min(by_player_score.values()) if by_player_score else 0.0

    return {
        "total_score":               round(total_score, 2),
        "first_choice_count":        first_choice_count,
        "first_choice_rate":         round(first_choice_rate, 4),
        "min_player_score":          round(min_player_score, 2),
        "assignment_count_by_player": dict(by_player_count),
        "score_by_player":           {pid: round(sc, 2) for pid, sc in by_player_score.items()},
        "supplemental_count":        supplemental_count,
        # 後方互換性のためのエイリアス（report.py等が参照している）
        "fallback_count":            supplemental_count,
        "swap_count":                0,
        "total_assignments":         n_slots,
    }


def compare(
    heuristic_results: list[dict],
    exact_results: list[dict],
    pref_map: dict | None = None,  # 非推奨・後方互換のみ
    all_player_ids: list[str] | None = None,
) -> list[dict]:
    """
    ヒューリスティック解と厳密解を同じ基準で再採点して比較する。
    各resultのpref_mapを使うので、pref_map引数は不要（後方互換のために残す）。
    """
    out = []
    for hr, er in zip(heuristic_results, exact_results):
        # 各resultが持つpref_mapを優先使用
        pm_h = hr.get("pref_map") or pref_map or {}
        pm_e = er.get("pref_map") or pref_map or {}
        hv = verify(hr["assignments"], pm_h, all_player_ids)
        ev = verify(er["assignments"], pm_e, all_player_ids)
        out.append({
            "label_heuristic": hr["label"],
            "label_exact":     er["label"],
            "heuristic":       hv,
            "exact":           ev,
            "diff": {
                "total_score":        round(ev["total_score"] - hv["total_score"], 2),
                "first_choice_count": ev["first_choice_count"] - hv["first_choice_count"],
                "min_player_score":   round(ev["min_player_score"] - hv["min_player_score"], 2),
            },
        })
    return out


def format_compare(comparisons: list[dict]) -> str:
    """比較結果を人間が読みやすいテキスト形式で返す。"""
    lines = []
    for c in comparisons:
        lines.append(f"\n{'='*60}")
        lines.append(f"  H: {c['label_heuristic']}")
        lines.append(f"  E: {c['label_exact']}")
        lines.append(f"{'─'*60}")
        h, e, d = c["heuristic"], c["exact"], c["diff"]
        rows = [
            ("総スコア",        f"{h['total_score']:.2f}", f"{e['total_score']:.2f}", f"{d['total_score']:+.2f}"),
            ("第1希望本数",     str(h['first_choice_count']), str(e['first_choice_count']), f"{d['first_choice_count']:+d}"),
            ("第1希望率",       f"{h['first_choice_rate']:.4f}", f"{e['first_choice_rate']:.4f}", "—"),
            ("最低スコア",      f"{h['min_player_score']:.2f}", f"{e['min_player_score']:.2f}", f"{d['min_player_score']:+.2f}"),
            ("割当総数",        str(h['total_assignments']), str(e['total_assignments']), "—"),
            ("補完件数",        str(h['supplemental_count']), str(e['supplemental_count']), "—"),
        ]
        lines.append(f"  {'指標':<22} {'ヒューリスティック':>12} {'厳密解':>10} {'差':>8}")
        lines.append(f"  {'─'*54}")
        for label, hval, eval_, diff in rows:
            lines.append(f"  {label:<22} {hval:>12} {eval_:>10} {diff:>8}")
        lines.append(f"  {'─'*54}")
        lines.append("  奏者別割当数:")
        all_pids = sorted(
            set(h["assignment_count_by_player"]) | set(e["assignment_count_by_player"])
        )
        for pid in all_pids:
            hc = h["assignment_count_by_player"].get(pid, 0)
            ec = e["assignment_count_by_player"].get(pid, 0)
            hs = h["score_by_player"].get(pid, 0.0)
            es = e["score_by_player"].get(pid, 0.0)
            lines.append(f"    {pid[:32]:<32} H:{hc}曲/{hs:.1f}点  E:{ec}曲/{es:.1f}点")
    return "\n".join(lines)
