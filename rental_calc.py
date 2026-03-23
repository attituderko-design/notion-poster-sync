"""
concert.services.rental_calc
レンタル必要楽器の逆算ロジック。

算出式（練習日 or 演奏会当日ごと）:
  必要楽器（SongInstrument の必要台数合計）
  - その日参加する奏者が持参可能な楽器台数
  = レンタル必要台数（0 以下は 0 扱い）
"""
from collections import defaultdict


def calc_rental_requirements(
    ctx: dict,
    practice_id: str,
) -> list[dict]:
    """
    1 つの練習日（または演奏会当日）について、
    レンタルが必要な楽器種別と台数を返す。

    Returns:
        [
            {
                "instrument_id":   str,
                "instrument_name": str,
                "required":        int,   # 曲別必要台数合計
                "bring_available": int,   # 参加奏者が持参可能な台数
                "rental_needed":   int,   # レンタル必要台数（>=0）
            },
            ...
        ]
    """
    query_all        = ctx["query_all"]
    extract_rel_ids  = ctx["extract_relation_ids"]
    extract_text     = ctx["extract_prop_text"]

    DB_CONCERT           = ctx["CONCERT_DB_CONCERT"]
    DB_PRACTICE          = ctx["CONCERT_DB_PRACTICE"]
    DB_SONG              = ctx["CONCERT_DB_SONG"]
    DB_INSTRUMENT        = ctx["CONCERT_DB_INSTRUMENT"]
    DB_SONG_INSTRUMENT   = ctx["CONCERT_DB_SONG_INSTRUMENT"]
    DB_PLAYER            = ctx["CONCERT_DB_PLAYER"]
    DB_ATTENDANCE        = ctx["CONCERT_DB_ATTENDANCE"]
    DB_PLAYER_INSTRUMENT = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]

    # ── 1. この練習日に参加する奏者 ID を取得 ──────────────
    attendance_rows = query_all(
        DB_ATTENDANCE,
        {"filter": {"property": "練習", "relation": {"contains": practice_id}}},
    )
    attending_player_ids: set[str] = set()
    for row in attendance_rows:
        # 参加可否が "○" または "△" の奏者のみカウント
        status = extract_text(row, "参加可否")
        if status in ("○", "△"):
            for pid in extract_rel_ids(row, "奏者"):
                attending_player_ids.add(pid)

    # ── 2. この練習日に関連する演奏会を特定 ──────────────────
    practice_rows = query_all(
        DB_PRACTICE,
        {"filter": {"property": "練習名", "title": {"is_not_empty": True}}},
    )
    concert_id = ""
    for row in practice_rows:
        if row.get("id") == practice_id:
            ids = extract_rel_ids(row, "演奏会")
            concert_id = ids[0] if ids else ""
            break

    # ── 3. 演奏会に紐づく楽曲一覧を取得 ──────────────────────
    song_ids: set[str] = set()
    if concert_id:
        song_rows = query_all(
            DB_SONG,
            {"filter": {"property": "演奏会", "relation": {"contains": concert_id}}},
        )
        for row in song_rows:
            song_ids.add(row.get("id", ""))

    # ── 4. SongInstrument から必要台数を集計 ─────────────────
    # instrument_id → 必要台数合計
    required_map: dict[str, int] = defaultdict(int)
    instrument_name_map: dict[str, str] = {}

    si_rows = query_all(DB_SONG_INSTRUMENT, None)
    for row in si_rows:
        s_ids = extract_rel_ids(row, "楽曲")
        if not s_ids or s_ids[0] not in song_ids:
            continue
        i_ids = extract_rel_ids(row, "楽器種別")
        if not i_ids:
            continue
        inst_id = i_ids[0]
        qty_str = extract_text(row, "必要台数")
        try:
            qty = int(float(qty_str)) if qty_str else 1
        except ValueError:
            qty = 1
        required_map[inst_id] += qty

    # ── 5. 参加奏者の持参可能楽器を集計 ──────────────────────
    # instrument_id → 持参可能人数（= 台数として扱う）
    bring_map: dict[str, int] = defaultdict(int)

    if attending_player_ids:
        pi_rows = query_all(DB_PLAYER_INSTRUMENT, None)
        for row in pi_rows:
            p_ids = extract_rel_ids(row, "奏者")
            if not p_ids or p_ids[0] not in attending_player_ids:
                continue
            # 持参可フラグが True のもののみ
            bring_flag = extract_text(row, "持参可フラグ")
            if bring_flag != "True":
                continue
            i_ids = extract_rel_ids(row, "楽器種別")
            if not i_ids:
                continue
            bring_map[i_ids[0]] += 1

    # ── 6. 楽器名を取得 ──────────────────────────────────────
    all_inst_ids = set(required_map.keys()) | set(bring_map.keys())
    if all_inst_ids:
        inst_rows = query_all(DB_INSTRUMENT, None)
        for row in inst_rows:
            rid = row.get("id", "")
            if rid in all_inst_ids:
                instrument_name_map[rid] = extract_text(row, "楽器名") or rid

    # ── 7. 差分を計算してリスト化 ─────────────────────────────
    results = []
    for inst_id, required in required_map.items():
        bring = bring_map.get(inst_id, 0)
        rental = max(0, required - bring)
        results.append({
            "instrument_id":   inst_id,
            "instrument_name": instrument_name_map.get(inst_id, inst_id),
            "required":        required,
            "bring_available": bring,
            "rental_needed":   rental,
        })

    # 楽器名でソート
    results.sort(key=lambda x: x["instrument_name"])
    return results


def calc_rental_for_all_practices(ctx: dict, concert_id: str) -> dict[str, list[dict]]:
    """
    演奏会に紐づく全練習日のレンタル試算をまとめて返す。

    Returns:
        { practice_id: [rental_requirement, ...], ... }
    """
    query_all       = ctx["query_all"]
    extract_rel_ids = ctx["extract_relation_ids"]
    extract_text    = ctx["extract_prop_text"]

    DB_PRACTICE = ctx["CONCERT_DB_PRACTICE"]

    practice_rows = query_all(
        DB_PRACTICE,
        {"filter": {"property": "演奏会", "relation": {"contains": concert_id}}},
    )

    result: dict[str, list[dict]] = {}
    for row in practice_rows:
        pid = row.get("id", "")
        if not pid:
            continue
        practice_name = extract_text(row, "練習名")
        reqs = calc_rental_requirements(ctx, pid)
        result[pid] = {
            "name":         practice_name,
            "requirements": reqs,
        }
    return result
