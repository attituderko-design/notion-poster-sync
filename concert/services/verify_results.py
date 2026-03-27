"""
concert/services/verify_results.py
ヒューリスティック解と厳密解を同じ基準で再採点する検証モジュール。
report.pyのstats表示とは独立して動作する。
"""
from __future__ import annotations
from collections import defaultdict


def _pref_score(pref: dict | None, source: str) -> float:
    """1件のassignmentのスコアを計算。pref_mapと同じ基準。"""
    if pref and pref.get("priority", 0) > 0:
        return {1: 3.0, 2: 2.0, 3: 1.0}.get(pref["priority"], 0.0)
    elif pref and pref.get("priority", 0) == 0:
        return 0.0   # 降り番希望
    elif source in ("fallback", "swap", "exact"):
        return 0.5   # 補完割当
    return 0.0


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
        Assignment.__dict__のリスト（player_id, song_id, part_id, source を含む）
    pref_map : dict
        str((player_id, song_id, part_id)) → {"priority": int, "player_id": str, ...}
    all_player_ids : list[str] | None
        評価対象奏者IDリスト。Noneならassignmentsに出てきた奏者のみ。

    Returns
    -------
    dict
        total_score, first_choice_count, first_choice_rate,
        min_player_score, assignment_count_by_player,
        fallback_count, swap_count
    """
    # 評価対象奏者を固定
    assigned_pids = {a["player_id"] for a in assignments}
    eval_pids = set(all_player_ids) if all_player_ids else assigned_pids

    by_player_score:  dict[str, float] = {pid: 0.0 for pid in eval_pids}
    by_player_count:  dict[str, int]   = {pid: 0   for pid in eval_pids}
    total_score       = 0.0
    first_choice_count = 0
    fallback_count    = 0
    swap_count        = 0

    for a in assignments:
        pid    = a["player_id"]
        pk     = str((pid, a["song_id"], a["part_id"]))
        pref   = pref_map.get(pk)
        source = a.get("source", "")
        sc     = _pref_score(pref, source)

        total_score += sc
        by_player_score[pid] = by_player_score.get(pid, 0.0) + sc
        by_player_count[pid] = by_player_count.get(pid, 0) + 1

        if pref and pref.get("priority") == 1:
            first_choice_count += 1
        if source == "fallback":
            fallback_count += 1
        elif source == "swap":
            swap_count += 1

    n_slots = len(assignments)
    first_choice_rate = (first_choice_count / n_slots) if n_slots > 0 else 0.0
    min_player_score  = min(by_player_score.values()) if by_player_score else 0.0

    return {
        "total_score":             round(total_score, 2),
        "first_choice_count":      first_choice_count,
        "first_choice_rate":       round(first_choice_rate, 4),
        "min_player_score":        round(min_player_score, 2),
        "assignment_count_by_player": dict(by_player_count),
        "score_by_player":         {pid: round(sc, 2) for pid, sc in by_player_score.items()},
        "fallback_count":          fallback_count,
        "swap_count":              swap_count,
        "total_assignments":       n_slots,
    }


def compare(
    heuristic_results: list[dict],
    exact_results: list[dict],
    pref_map: dict,
    all_player_ids: list[str] | None = None,
) -> list[dict]:
    """
    ヒューリスティック解と厳密解を同じ基準で再採点して比較する。

    Returns
    -------
    list[dict]
        候補ごとの比較結果
    """
    out = []
    for hr, er in zip(heuristic_results, exact_results):
        hv = verify(hr["assignments"], pref_map, all_player_ids)
        ev = verify(er["assignments"], pref_map, all_player_ids)
        out.append({
            "label_heuristic": hr["label"],
            "label_exact":     er["label"],
            "heuristic":       hv,
            "exact":           ev,
            "diff": {
                "total_score":        round(ev["total_score"] - hv["total_score"], 2),
                "first_choice_count": ev["first_choice_count"] - hv["first_choice_count"],
                "min_player_score":   round(ev["min_player_score"] - hv["min_player_score"], 2),
            }
        })
    return out


def format_compare(comparisons: list[dict]) -> str:
    """比較結果を人間が読みやすい形式で返す。"""
    lines = []
    for c in comparisons:
        lines.append(f"\n{'='*60}")
        lines.append(f"  H: {c['label_heuristic']}")
        lines.append(f"  E: {c['label_exact']}")
        lines.append(f"{'─'*60}")
        h, e = c["heuristic"], c["exact"]
        d = c["diff"]
        lines.append(f"  {'指標':<22} {'ヒューリスティック':>10} {'厳密解':>10} {'差':>8}")
        lines.append(f"  {'─'*52}")
        lines.append(f"  {'総スコア':<22} {h['total_score']:>10.2f} {e['total_score']:>10.2f} {d['total_score']:>+8.2f}")
        lines.append(f"  {'第1希望本数':<22} {h['first_choice_count']:>10} {e['first_choice_count']:>10} {d['first_choice_count']:>+8}")
        lines.append(f"  {'第1希望率':<22} {h['first_choice_rate']:>10.4f} {e['first_choice_rate']:>10.4f}")
        lines.append(f"  {'最低スコア（公平性）':<22} {h['min_player_score']:>10.2f} {e['min_player_score']:>10.2f} {d['min_player_score']:>+8.2f}")
        lines.append(f"  {'割当総数':<22} {h['total_assignments']:>10} {e['total_assignments']:>10}")
        lines.append(f"  {'FB件数':<22} {h['fallback_count']:>10} {e['fallback_count']:>10}")
        lines.append(f"  {'swap件数':<22} {h['swap_count']:>10} {e['swap_count']:>10}")
        lines.append(f"  {'─'*52}")
        lines.append("  奏者別割当数:")
        all_pids = sorted(set(h["assignment_count_by_player"]) | set(e["assignment_count_by_player"]))
        for pid in all_pids:
            hc = h["assignment_count_by_player"].get(pid, 0)
            ec = e["assignment_count_by_player"].get(pid, 0)
            hs = h["score_by_player"].get(pid, 0.0)
            es = e["score_by_player"].get(pid, 0.0)
            lines.append(f"    {pid[:32]:<32} H:{hc}曲/{hs:.1f}点  E:{ec}曲/{es:.1f}点")
    return "\n".join(lines)
