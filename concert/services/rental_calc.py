"""
concert.services.rental_calc
レンタル必要楽器の逆算ロジック。

算出式（練習日 or 演奏会当日ごと）:
  必要楽器（PART_DEFINITION の必要楽器 × 必要台数合計）
  - その日参加する奏者が持参可能な楽器台数
  = レンタル必要台数（0 以下は 0 扱い）
"""
from collections import defaultdict
from concert.services.keys import (
    ATT_STATUS_KEYS, ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS,
    PRACTICE_CONCERT_REL_KEYS, PRACTICE_SONG_REL_KEYS,
    PRACTICE_PERCUSSION_OFF_KEYS,
    PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS,
    PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_BRING_KEYS, PI_CONCERT_REL_KEYS,
    INSTRUMENT_NAME_KEYS,
    SONG_CONCERT_REL_KEYS,
)


def calc_rental_requirements(
    ctx: dict,
    practice_id: str,
) -> list[dict]:
    """
    1 つの練習日（または演奏会当日）について、
    レンタルが必要な楽器種別と台数を返す。
    """
    query_all    = ctx["query_all"]
    ext_rel      = ctx["extract_relation_ids_any"]
    ext_text     = ctx["extract_prop_text_any"]
    find_prop    = ctx["find_prop_name"]
    get_types    = ctx["get_prop_types"]

    DB_PRACTICE          = ctx["CONCERT_DB_PRACTICE"]
    DB_SONG              = ctx["CONCERT_DB_SONG"]
    DB_INSTRUMENT        = ctx["CONCERT_DB_INSTRUMENT"]
    DB_PART_DEFINITION   = ctx["CONCERT_DB_PART_DEFINITION"]
    DB_ATTENDANCE        = ctx["CONCERT_DB_ATTENDANCE"]
    DB_PLAYER_INSTRUMENT = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]

    # ── 1. この練習日に参加する奏者IDを取得 ──────────────────
    att_type_map = get_types(DB_ATTENDANCE)
    att_practice_rel = find_prop(att_type_map, ATT_PRACTICE_REL_KEYS)
    attendance_rows = query_all(
        DB_ATTENDANCE,
        {"filter": {"property": att_practice_rel, "relation": {"contains": practice_id}}}
        if att_practice_rel else None,
    )
    attending_player_ids: set[str] = set()
    for row in attendance_rows:
        status = ext_text(row, ATT_STATUS_KEYS)
        if status in ("○", "△"):
            for pid in ext_rel(row, ATT_PLAYER_REL_KEYS):
                attending_player_ids.add(pid)

    # ── 2. この練習日の情報を取得（演奏曲・打楽器休みフラグ）──
    practice_type_map   = get_types(DB_PRACTICE)
    prac_concert_rel    = find_prop(practice_type_map, PRACTICE_CONCERT_REL_KEYS)
    prac_song_rel       = find_prop(practice_type_map, PRACTICE_SONG_REL_KEYS)
    prac_perc_off_rel   = find_prop(practice_type_map, PRACTICE_PERCUSSION_OFF_KEYS)

    practice_row = None
    practice_rows = query_all(DB_PRACTICE, None)
    for row in practice_rows:
        if row.get("id") == practice_id:
            practice_row = row
            break

    concert_id = ""
    if practice_row:
        ids = ext_rel(practice_row, PRACTICE_CONCERT_REL_KEYS)
        concert_id = ids[0] if ids else ""

    # 打楽器休みフラグ確認（Trueなら問答無用で必要台数0）
    is_percussion_off = False
    if practice_row:
        flag = ext_text(practice_row, PRACTICE_PERCUSSION_OFF_KEYS)
        is_percussion_off = flag in ("True", "true", "1", "✓", "はい", "yes")

    if is_percussion_off:
        return [{"percussion_off": True}]  # 打楽器休みフラグ

    # ── 3. この練習日に演奏する曲を取得 ──────────────────────
    # PRACTICEの「演奏曲」リレーションを優先、なければ演奏会の全曲
    song_ids: set[str] = set()
    if practice_row and prac_song_rel:
        # 練習日に紐づく演奏曲が設定されている場合はそれを使う
        practice_song_ids = ext_rel(practice_row, PRACTICE_SONG_REL_KEYS)
        song_ids = set(practice_song_ids)

    if not song_ids and concert_id:
        # 未設定の場合は演奏会の全曲にフォールバック
        song_type_map = get_types(DB_SONG)
        song_conc_rel = find_prop(song_type_map, SONG_CONCERT_REL_KEYS)
        song_rows = query_all(
            DB_SONG,
            {"filter": {"property": song_conc_rel, "relation": {"contains": concert_id}}}
            if song_conc_rel else None,
        )
        for row in song_rows:
            song_ids.add(row.get("id", ""))

    # ── 4. PART_DEFINITIONから必要楽器と台数を集計 ───────────
    # 考え方：同じ楽器が複数曲に登場しても、
    #         曲をまたいで同時演奏はしないので1台で足りる。
    #         1つの曲の中で同じ楽器が複数パートに登場する場合のみ複数台必要。
    # → 楽器ごとに「1曲あたりの最大必要台数」を求め、全曲の最大値を取る。
    #
    # instrument_id → 必要台数（全曲を通じた最大同時使用台数）
    required_map: dict[str, int] = defaultdict(int)
    instrument_name_map: dict[str, str] = {}

    if song_ids:
        pd_rows = query_all(DB_PART_DEFINITION, None)

        # 曲ごと×楽器ごとのパート定義件数を集計
        # song_id → instrument_id → 台数
        per_song_inst: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for row in pd_rows:
            s_ids = ext_rel(row, PARTDEF_SONG_REL_KEYS)
            if not s_ids or s_ids[0] not in song_ids:
                continue
            i_ids = ext_rel(row, PARTDEF_INST_REL_KEYS)
            if not i_ids:
                continue
            sid = s_ids[0]
            # 1パート定義に複数楽器が紐づく場合は各楽器1台として扱う
            for inst_id in i_ids:
                per_song_inst[sid][inst_id] += 1

        # 楽器ごとに全曲を通じた最大台数を取る
        all_inst_ids_in_partdef: set[str] = set()
        for sid, inst_map in per_song_inst.items():
            for inst_id, qty in inst_map.items():
                all_inst_ids_in_partdef.add(inst_id)
                if qty > required_map[inst_id]:
                    required_map[inst_id] = qty

    # ── 5. PLAYER_INSTRUMENTから担当・持参情報を取得 ──────────
    bring_map: dict[str, int] = defaultdict(int)

    # アサイン確定済みかどうかを確認
    # 担当フラグTrueのレコードが1件でもあればアサイン確定済みと判断
    pi_rows = query_all(DB_PLAYER_INSTRUMENT, None) if attending_player_ids else []
    concert_pi_rows = []
    for row in pi_rows:
        if concert_id:
            c_ids = ext_rel(row, PI_CONCERT_REL_KEYS)
            if c_ids and concert_id not in c_ids:
                continue
        concert_pi_rows.append(row)

    is_assigned = any(
        ext_text(r, PI_ASSIGN_KEYS) == "True"
        for r in concert_pi_rows
    )

    if is_assigned:
        # アサイン確定後：担当フラグTrueの出席者がいない楽器はrequired_mapから除外
        # ※持参可フラグはアサイン前後問わず出席者全員で計算（持参は担当と無関係）
        assigned_attending: dict[str, int] = defaultdict(int)  # 担当かつ出席

        for row in concert_pi_rows:
            if ext_text(row, PI_ASSIGN_KEYS) != "True":
                continue
            p_ids = ext_rel(row, PI_PLAYER_REL_KEYS)
            if not p_ids or p_ids[0] not in attending_player_ids:
                continue  # 担当者が欠席 → スキップ
            i_ids = ext_rel(row, PI_INST_REL_KEYS)
            if not i_ids:
                continue
            inst_id = i_ids[0]
            if inst_id not in required_map:
                continue
            assigned_attending[inst_id] += 1

        # 担当出席者がいない楽器はrequired_mapから除外（演奏しない）
        for inst_id in list(required_map.keys()):
            if assigned_attending[inst_id] == 0:
                del required_map[inst_id]

    # 持参可能台数の集計（アサイン前後共通・出席者全員の持参可フラグで計算）
    for row in concert_pi_rows:
        p_ids = ext_rel(row, PI_PLAYER_REL_KEYS)
        if not p_ids or p_ids[0] not in attending_player_ids:
            continue
        if ext_text(row, PI_BRING_KEYS) != "True":
            continue
        i_ids = ext_rel(row, PI_INST_REL_KEYS)
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
                instrument_name_map[rid] = ext_text(row, INSTRUMENT_NAME_KEYS) or rid

    # ── 7. 差分を計算してリスト化 ─────────────────────────────
    results = []
    for inst_id, required in required_map.items():
        bring  = bring_map.get(inst_id, 0)
        rental = max(0, required - bring)
        results.append({
            "instrument_id":   inst_id,
            "instrument_name": instrument_name_map.get(inst_id, inst_id),
            "required":        required,
            "bring_available": bring,
            "rental_needed":   rental,
        })

    results.sort(key=lambda x: x["instrument_name"])
    return results


def calc_rental_for_all_practices(ctx: dict, concert_id: str) -> dict:
    """演奏会に紐づく全練習日のレンタル試算をまとめて返す。"""
    query_all    = ctx["query_all"]
    ext_text     = ctx["extract_prop_text_any"]
    find_prop    = ctx["find_prop_name"]
    get_types    = ctx["get_prop_types"]

    DB_PRACTICE = ctx["CONCERT_DB_PRACTICE"]
    practice_type_map = get_types(DB_PRACTICE)
    prac_concert_rel  = find_prop(practice_type_map, ["演奏会", "出演", "FK演奏会"])

    practice_rows = query_all(
        DB_PRACTICE,
        {"filter": {"property": prac_concert_rel, "relation": {"contains": concert_id}}}
        if prac_concert_rel else None,
    )

    result: dict = {}
    for row in practice_rows:
        pid = row.get("id", "")
        if not pid:
            continue
        practice_name = ext_text(row, ["練習名", "タイトル", "PK練習名"])
        reqs = calc_rental_requirements(ctx, pid)
        result[pid] = {
            "name":         practice_name,
            "requirements": reqs,
        }
    return result
