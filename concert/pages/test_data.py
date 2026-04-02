"""
concert/pages/test_data.py
テストデータの一括投入・一括削除機能

2パターン：
  [TEST] 軽量版 - 各パート1〜2名（計約20名）日常テスト用
  [DEMO] フル版 - 2管編成55名、サンプルデータ提示用
"""
import uuid
import streamlit as st
from datetime import date, timedelta
from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS, CONCERT_VENUE_KEYS, CONCERT_ADDRESS_KEYS,
    CONCERT_CONDUCTOR_KEYS, CONCERT_SOLOIST_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_CONCERT_REL_KEYS, PRACTICE_DATE_KEYS,
    PRACTICE_VENUE_KEYS, PRACTICE_ADDRESS_KEYS, PRACTICE_CONCERT_DAY_KEYS,
    PRACTICE_SONG_REL_KEYS,
    SONG_NAME_KEYS, SONG_CONCERT_REL_KEYS, SONG_COMPOSER_KEYS,
    SONG_ALL_MOVEMENTS_KEYS, SONG_MOVEMENT_REL_KEYS,
    INSTRUMENT_NAME_KEYS, INSTRUMENT_CATEGORY_KEYS, INSTRUMENT_KEY_KEYS,
    PARTDEF_KEY_KEYS, PARTDEF_RECORD_KEYS, PARTDEF_CONCERT_REL_KEYS,
    PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS, PARTDEF_NAME_KEYS,
    PARTDEF_DISPLAY_NAME_KEYS, PARTDEF_PART_REL_KEYS,
    PARTICIPANT_RECORD_KEYS, PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_REL_KEYS, PARTICIPANT_ROLE_KEYS, PARTICIPANT_ROLE_OPS_KEYS,
    PARTICIPANT_FEE_KEYS, PARTICIPANT_SYSTEM_ROLE_KEYS,
    PARTMASTER_NAME_KEYS, PARTMASTER_TYPE_KEYS,
    PLAYER_NAME_KEYS, PLAYER_HN_KEYS, PLAYER_EMAIL_KEYS, PLAYER_PHONE_KEYS,
    ATTENDANCE_KEY_KEYS, ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS, ATT_STATUS_KEYS,
    PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_CONCERT_REL_KEYS, PI_OWN_COUNT_KEYS,
    PI_PARTICIPANT_REL_KEYS, PI_BRING_ASSIGN_KEYS, PI_BRING_COUNT_KEYS, PI_PRACTICE_REL_KEYS,
    ASSIGN_KEY_KEYS,
    PREFERENCE_KEY_KEYS, PREF_PLAYER_REL_KEYS, PREF_PART_REL_KEYS, PREF_PRIORITY_KEYS,
    RENTAL_RECORD_KEYS, RENTAL_PRACTICE_REL_KEYS, RENTAL_INST_REL_KEYS,
    RENTAL_ITEM_NAME_KEYS, RENTAL_VENDOR_KEYS, RENTAL_QTY_KEYS,
    RENTAL_UNIT_PRICE_KEYS, RENTAL_CONFIRMED_KEYS, RENTAL_COST_TYPE_KEYS,
    RENTAL_KEY_KEYS,
    SCHEDULE_KEY_KEYS, SCHEDULE_PRACTICE_REL_KEYS, SCHEDULE_START_KEYS,
    SCHEDULE_END_KEYS, SCHEDULE_TYPE_KEYS, SCHEDULE_CONTENT_KEYS, SCHEDULE_ORDER_KEYS,
    SCHEDULE_SONG_REL_KEYS,
    CONCERT_SONG_KEY_KEYS, CONCERT_SONG_CONCERT_REL_KEYS, CONCERT_SONG_SONG_REL_KEYS,
    CONCERT_SONG_ORDER_KEYS, CONCERT_SONG_DONE_KEYS,
    CONCERT_INST_KEY_KEYS, CONCERT_INST_CONCERT_REL_KEYS, CONCERT_INST_SONG_REL_KEYS,
    CONCERT_INST_INST_REL_KEYS, CONCERT_INST_COUNT_KEYS,
    HARMONIA_CONCERT_KEY_KEYS, HARMONIA_CONCERT_CONCERT_REL_KEYS,
    HARMONIA_CONCERT_MANAGED_KEYS, HARMONIA_CONCERT_SONG_INFO_KEYS,
    HARMONIA_CONCERT_PRACTICE_INFO_KEYS, HARMONIA_CONCERT_PRACTICE_DATE_KEYS,
    HARMONIA_CONCERT_REQUIRED_INST_KEYS, HARMONIA_CONCERT_PARTDEF_KEYS,
    HARMONIA_CONCERT_PLAYER_INFO_KEYS, HARMONIA_CONCERT_ATTENDANCE_KEYS,
    HARMONIA_CONCERT_PREFERENCE_KEYS, HARMONIA_CONCERT_INVITE_CODE_KEYS,
    MOVEMENT_KEY_KEYS, MOVEMENT_NAME_KEYS, MOVEMENT_NO_KEYS,
    MOVEMENT_ORDER_KEYS, MOVEMENT_ROMAN_KEYS,
)

ATLAS_SCORE_REL_KEYS         = ["演奏曲"]
ATLAS_SCORE_HISTORY_REL_KEYS = ["出演履歴"]
ATLAS_CREATOR_KEYS           = ["クリエイター"]

# ── パート構成定義 ───────────────────────────────────────────
# (part_def_name, inst_category, inst_name, part_master_name, system_role, n_test, n_demo)
# part_master_name は PART_MASTER DBの実際の値に合わせる
# part_def_name はPART_DEFINITIONの表示パート名（席番・番号付き）
PART_ROSTER = [
    ("Fl.1",   "管楽器", "Flute",       "Flute",       "Player",  1, 2),
    ("Fl.2",   "管楽器", "Flute",       "Flute",       "Player",  1, 2),
    ("Ob.1",   "管楽器", "Oboe",        "Oboe",        "Player",  1, 2),
    ("Ob.2",   "管楽器", "Oboe",        "Oboe",        "Player",  1, 2),
    ("Cl.1",   "管楽器", "Clarinet",    "Clarinet",    "Player",  1, 2),
    ("Cl.2",   "管楽器", "Clarinet",    "Clarinet",    "Player",  1, 2),
    ("Fg.1",   "管楽器", "Fagotto",     "Faggot",      "Player",  1, 2),
    ("Fg.2",   "管楽器", "Fagotto",     "Faggot",      "Player",  1, 2),
    ("Hr.1",   "管楽器", "Horn",        "Horn",        "Player",  1, 4),
    ("Hr.2",   "管楽器", "Horn",        "Horn",        "Player",  1, 4),
    ("Hr.3",   "管楽器", "Horn",        "Horn",        "Player",  0, 4),
    ("Hr.4",   "管楽器", "Horn",        "Horn",        "Player",  0, 4),
    ("Tp.1",   "管楽器", "Trumpet",     "Trumpet",     "Player",  1, 2),
    ("Tp.2",   "管楽器", "Trumpet",     "Trumpet",     "Player",  1, 2),
    ("Tb.1",   "管楽器", "Trombone",    "Trombone",    "Player",  1, 3),
    ("Tb.2",   "管楽器", "Trombone",    "Trombone",    "Player",  1, 3),
    ("Tb.3",   "管楽器", "Trombone",    "Trombone",    "Player",  0, 3),
    ("Tuba",   "管楽器", "Tuba",        "Tuba",        "Player",  1, 1),
    ("Timp.",  "打楽器", "Timpani",     "Percussion",  "Leader",  1, 1),
    ("Perc.1", "打楽器", "Percussion",  "Percussion",  "Player",  1, 1),
    ("Perc.2", "打楽器", "Percussion",  "Percussion",  "Player",  0, 1),
    ("Perc.3", "打楽器", "Percussion",  "Percussion",  "Player",  0, 1),
    ("Vn1",    "弦楽器", "Violin",      "Violin",      "Manager", 1,12),
    ("Vn2",    "弦楽器", "Violin",      "Violin",      "Leader",  1,10),
    ("Va",     "弦楽器", "Viola",       "Viola",       "Player",  1, 8),
    ("Vc",     "弦楽器", "Violoncello", "Violoncello", "Player",  1, 8),
    ("Cb",     "弦楽器", "Contrabass",  "Contrabass",  "Player",  1, 4),
]

# 役職_運営のサンプル割り当て（パート別先頭奏者向け）
ROLE_OPS_BY_PART = {
    "Vn1":  "代表",
    "Vn2":  "副代表",
    "Timp.":"会計",
    "Fl.1": "広報",
}

# CONCERT_INSTRUMENTに登録するテスト用楽器（演奏曲ごとに必要な楽器）
# (inst_name, qty) ─ 曲Aと曲Bで同じ楽器セットを使用
CONCERT_INST_ITEMS = [
    ("Timpani",        1),
    ("Snare Drum",     1),
    ("Bass Drum",      1),
    ("Crash Cymbals",  1),
    ("Triangle",       1),
    ("Tambourine",     1),
]

# RENTALに登録するサンプル（Timpaniのみレンタル確定）
RENTAL_ITEMS = [
    ("Timpani", "テスト楽器店", "Timpani 23inch×26inch 一式", 1, 25000, True,  "楽器レンタル"),
    ("Snare Drum","テスト楽器店","Snare Drum 一式",            1,  8000, False, "楽器レンタル"),
]


def _clear_cache():
    prefixes = (
        "practice_list_", "concert_list", "song_list_", "partdef_list_",
        "pi_list_", "attendance_list_", "participant_list_", "instrument_list",
        "schedule_list_", "expense_list_", "cast_list_", "pi_master_",
        "si_list_", "pi_practice_", "concert_song_list_",
        "_movement_map_cache", "_song_display_name_cache", "_part_master_map_cache",
        "songs_concert_list", "concert_song_rows_",
    )
    for k in list(st.session_state.keys()):
        if any(k.startswith(p) for p in prefixes):
            st.session_state.pop(k, None)
    st.cache_data.clear()


def _archive(ctx, pid: str) -> bool:
    r = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{pid}",
                            json={"archived": True})
    return r is not None and r.status_code == 200


def _create(ctx, db_id: str, props: dict) -> str:
    r = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                            json={"parent": {"database_id": db_id}, "properties": props})
    return r.json().get("id", "") if r and r.status_code == 200 else ""


def _p(ctx, db_id): return ctx["get_prop_types"](db_id) or {}
def _put(ctx, props, t, keys, value): ctx["put_prop_any"](props, t, keys, value)


# ============================================================
# 投入
# ============================================================

def _seed_all(ctx, pfx: str, is_demo: bool) -> dict:
    summary = {}
    created_ids: list[str] = []
    n_idx = 6 if is_demo else 5

    def track(pid: str) -> str:
        if pid: created_ids.append(pid)
        return pid

    # ── 1. 既存INSTRUMENTマスタを名前→IDで引く ───────────────
    inst_db = ctx["CONCERT_DB_INSTRUMENT"]
    all_insts = ctx["query_all"](inst_db, None)
    inst_name_to_id: dict[str, str] = {
        ctx["extract_prop_text_any"](r, INSTRUMENT_NAME_KEYS): r.get("id", "")
        for r in all_insts
        if ctx["extract_prop_text_any"](r, INSTRUMENT_NAME_KEYS)
    }
    # 不足分は新規作成
    ti = _p(ctx, inst_db)
    needed = set(row[2] for row in PART_ROSTER) | \
             set(item[0] for item in CONCERT_INST_ITEMS) | \
             set(r[0] for r in RENTAL_ITEMS)
    new_inst_count = 0
    for iname in needed:
        if iname not in inst_name_to_id:
            cat = next((row[1] for row in PART_ROSTER if row[2] == iname), "打楽器")
            props = {}
            ctx["put_key_any"](props, ti, INSTRUMENT_KEY_KEYS, iname, prefix="inst")
            _put(ctx, props, ti, INSTRUMENT_NAME_KEYS,     iname)
            _put(ctx, props, ti, INSTRUMENT_CATEGORY_KEYS, cat)
            iid = track(_create(ctx, inst_db, props))
            if iid:
                inst_name_to_id[iname] = iid
                new_inst_count += 1
    if new_inst_count:
        summary["INSTRUMENT（新規作成）"] = new_inst_count

    # ── 2. PERFORMER ─────────────────────────────────────────
    player_db = ctx["CONCERT_DB_PLAYER"]
    tp = _p(ctx, player_db)
    cast_plan: list[tuple[str, str, str]] = []  # (part_def_name, player_id, system_role)
    player_count = 0
    player_no = 1
    for part_def, _, _, pm_name, sys_role, n_test, n_demo in PART_ROSTER:
        n = n_demo if is_demo else n_test
        for seat in range(n):
            props = {}
            _put(ctx, props, tp, PLAYER_NAME_KEYS,  f"{pfx} 奏者{player_no:02d}")
            _put(ctx, props, tp, PLAYER_HN_KEYS,    f"Player{player_no:02d}")
            _put(ctx, props, tp, PLAYER_EMAIL_KEYS,
                 f"test.player{player_no:02d}@harmonia.example.com")
            pid = track(_create(ctx, player_db, props))
            if pid:
                cast_plan.append((part_def, pid, sys_role if seat == 0 else "Player"))
                player_count += 1
            player_no += 1
    summary["PERFORMER"] = player_count

    # ── 3. CONCERT（ATLAS） ───────────────────────────────────
    concert_db = ctx["CONCERT_DB_CONCERT"]
    tc = _p(ctx, concert_db)
    props = {}
    _put(ctx, props, tc, CONCERT_NAME_KEYS,      f"{pfx} テスト演奏会")
    _put(ctx, props, tc, CONCERT_VENUE_KEYS,     "いずみホール")
    _put(ctx, props, tc, CONCERT_ADDRESS_KEYS,   "大阪府大阪市中央区城見1丁目4-70")
    _put(ctx, props, tc, CONCERT_CONDUCTOR_KEYS, "テスト指揮者 太郎")
    _put(ctx, props, tc, CONCERT_SOLOIST_KEYS,   "テストソリスト 花子（Vn）")
    media_key = ctx["find_prop_name"](tc, ["媒体", "Media"])
    if media_key:
        mt = tc.get(media_key, "")
        if mt == "multi_select": props[media_key] = {"multi_select": [{"name": "出演"}]}
        elif mt == "select":     props[media_key] = {"select": {"name": "出演"}}
    concert_date = date.today() + timedelta(days=90)
    dt_key = ctx["find_prop_name"](tc, CONCERT_DATE_KEYS)
    if dt_key:
        props[dt_key] = {"date": {"start": concert_date.isoformat() + "T14:00:00+09:00"}}
    concert_id = track(_create(ctx, concert_db, props))
    summary["CONCERT"] = 1 if concert_id else 0
    if not concert_id:
        st.session_state[f"test_created_ids_{pfx}"] = created_ids
        return summary

    # ── 4. MOVEMENT（楽章） ───────────────────────────────────
    mv_db = ctx.get("CONCERT_DB_MOVEMENT", "")
    mv_ids: list[str] = []
    if mv_db:
        tm = _p(ctx, mv_db)
        for no, roman, mv_name in [
            (1, "I",   "Allegro vivace"),
            (2, "II",  "Andante con moto"),
            (3, "III", "Scherzo: Allegro"),
            (4, "IV",  "Finale: Allegro"),
        ]:
            props = {}
            _put(ctx, props, tm, MOVEMENT_KEY_KEYS,  f"{pfx}_symphony_mv{no}")
            _put(ctx, props, tm, MOVEMENT_NAME_KEYS,  mv_name)
            _put(ctx, props, tm, MOVEMENT_NO_KEYS,    no)
            _put(ctx, props, tm, MOVEMENT_ORDER_KEYS, no)
            _put(ctx, props, tm, MOVEMENT_ROMAN_KEYS, roman)
            mid = track(_create(ctx, mv_db, props))
            if mid: mv_ids.append(mid)
        summary["MOVEMENT"] = len(mv_ids)

    # ── 5. SONG（ATLAS + APOLLO） ─────────────────────────────
    # 曲A: テスト序曲（全楽章フラグON）
    # 曲B: テスト交響曲（第4楽章のみ、MOVEMENTリレーションあり）
    song_defs = [
        ("テスト序曲",   "Test, Composer A.", True,  None),
        ("テスト交響曲", "Test, Composer B.", False, mv_ids[3] if len(mv_ids) >= 4 else None),
    ]
    song_ids: list[str] = []                      # APOLLO IDs
    atlas_song_id_list: list[tuple[str, str]] = [] # [(atlas_sid, apollo_sid), ...]
    apollo_db = ctx["CONCERT_DB_SONG"]
    ta = _p(ctx, apollo_db)
    for sname, composer, all_mvmt, mv_id in song_defs:
        # ATLASに演奏曲ページを作成
        props = {}
        _put(ctx, props, tc, CONCERT_NAME_KEYS,  f"{pfx} {sname}")
        _put(ctx, props, tc, ATLAS_CREATOR_KEYS, composer)
        if media_key:
            mt = tc.get(media_key, "")
            if mt == "multi_select": props[media_key] = {"multi_select": [{"name": "演奏曲"}]}
            elif mt == "select":     props[media_key] = {"select": {"name": "演奏曲"}}
        _put(ctx, props, tc, ATLAS_SCORE_HISTORY_REL_KEYS, concert_id)
        atlas_sid = track(_create(ctx, concert_db, props))

        # APOLLOにも作成
        apollo_sid = ""
        if atlas_sid and ta:
            apollo_props = {}
            _put(ctx, apollo_props, ta, SONG_NAME_KEYS,          f"{pfx} {sname}")
            _put(ctx, apollo_props, ta, SONG_COMPOSER_KEYS,      composer)
            _put(ctx, apollo_props, ta, SONG_CONCERT_REL_KEYS,   concert_id)
            _put(ctx, apollo_props, ta, SONG_ALL_MOVEMENTS_KEYS, all_mvmt)
            ctx["put_prop_any"](apollo_props, ta, ["演奏曲", "FK演奏曲"], atlas_sid)
            if mv_id and not all_mvmt:
                ctx["put_prop_any"](apollo_props, ta, SONG_MOVEMENT_REL_KEYS, mv_id)
            apollo_sid = track(_create(ctx, apollo_db, apollo_props))
            if apollo_sid:
                song_ids.append(apollo_sid)
        if atlas_sid and apollo_sid:
            atlas_song_id_list.append((atlas_sid, apollo_sid))

    # ATLASの演奏会に「演奏曲」リレーションをセット
    if atlas_song_id_list:
        upd = {}
        _put(ctx, upd, tc, ATLAS_SCORE_REL_KEYS,
             [atlas_sid for atlas_sid, _ in atlas_song_id_list])
        ctx["api_request"]("patch",
            f"https://api.notion.com/v1/pages/{concert_id}",
            json={"properties": upd})
    summary["SONG"] = len(song_ids)

    # ── 6. CONCERT_SONG（ATLASのIDで登録） ───────────────────
    cs_db = ctx.get("CONCERT_DB_CONCERT_SONG", "")
    cs_ids: list[str] = []  # CONCERT_SONG IDs（SCHEDULEのリレーションに使用）
    if cs_db and atlas_song_id_list:
        tcs = _p(ctx, cs_db)
        for idx, (atlas_sid, apollo_sid) in enumerate(atlas_song_id_list, start=1):
            props = {}
            ctx["put_key_any"](props, tcs, CONCERT_SONG_KEY_KEYS,
                               concert_id, atlas_sid, prefix="concert_song")
            _put(ctx, props, tcs, CONCERT_SONG_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, tcs, CONCERT_SONG_SONG_REL_KEYS,    atlas_sid)
            _put(ctx, props, tcs, CONCERT_SONG_ORDER_KEYS,       idx)
            _put(ctx, props, tcs, CONCERT_SONG_DONE_KEYS,        True)
            csid = track(_create(ctx, cs_db, props))
            if csid: cs_ids.append(csid)
    summary["CONCERT_SONG"] = len(cs_ids)

    # ── 7. CONCERT_INSTRUMENT（演奏会必要楽器） ───────────────
    ci_db = ctx.get("CONCERT_DB_CONCERT_INSTRUMENT", "")
    ci_count = 0
    if ci_db and cs_ids:
        tci = _p(ctx, ci_db)
        # 全曲×全楽器で登録
        for csid in cs_ids:
            for iname, qty in CONCERT_INST_ITEMS:
                iid = inst_name_to_id.get(iname, "")
                if not iid: continue
                props = {}
                ctx["put_key_any"](props, tci, CONCERT_INST_KEY_KEYS,
                                   concert_id, csid, iid, prefix="ci")
                _put(ctx, props, tci, CONCERT_INST_CONCERT_REL_KEYS, concert_id)
                _put(ctx, props, tci, CONCERT_INST_SONG_REL_KEYS,    csid)
                _put(ctx, props, tci, CONCERT_INST_INST_REL_KEYS,    iid)
                _put(ctx, props, tci, CONCERT_INST_COUNT_KEYS,       qty)
                if track(_create(ctx, ci_db, props)): ci_count += 1
    summary["CONCERT_INSTRUMENT"] = ci_count

    # ── 8. PRACTICE（3回＋本番日） ───────────────────────────
    prac_db = ctx["CONCERT_DB_PRACTICE"]
    tpr = _p(ctx, prac_db)
    practice_ids: list[str] = []
    venues = [
        ("ザ・シンフォニーホール",     "大阪府大阪市北区大淀南2丁目3-3"),
        ("豊中市立文化芸術センター",   "大阪府豊中市曽根東町3丁目7-2"),
        ("吹田市文化会館 メイシアター","大阪府吹田市泉町2丁目29-1"),
    ]
    times = ["T10:00:00+09:00", "T13:00:00+09:00", "T09:30:00+09:00"]
    base  = date.today() + timedelta(days=21)
    for i in range(3):
        props = {}
        _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{pfx} 第{i+1}回練習")
        _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tpr, PRACTICE_VENUE_KEYS,       venues[i][0])
        _put(ctx, props, tpr, PRACTICE_ADDRESS_KEYS,     venues[i][1])
        # 演奏曲リレーション（APOLLO IDsを設定）
        if song_ids:
            _put(ctx, props, tpr, PRACTICE_SONG_REL_KEYS, song_ids)
        d = base + timedelta(weeks=i * 2)
        dt_k = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
        if dt_k: props[dt_k] = {"date": {"start": d.isoformat() + times[i]}}
        pr_id = track(_create(ctx, prac_db, props))
        if pr_id: practice_ids.append(pr_id)

    # 本番日
    props = {}
    _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{pfx} 本番当日")
    _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
    _put(ctx, props, tpr, PRACTICE_CONCERT_DAY_KEYS, True)
    _put(ctx, props, tpr, PRACTICE_VENUE_KEYS,       "いずみホール")
    _put(ctx, props, tpr, PRACTICE_ADDRESS_KEYS,     "大阪府大阪市中央区城見1丁目4-70")
    if song_ids:
        _put(ctx, props, tpr, PRACTICE_SONG_REL_KEYS, song_ids)
    dt_k4 = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
    if dt_k4:
        props[dt_k4] = {"date": {"start": concert_date.isoformat() + "T14:00:00+09:00"}}
    concert_day_id = track(_create(ctx, prac_db, props))
    if concert_day_id: practice_ids.append(concert_day_id)
    summary["PRACTICE"] = len(practice_ids)

    # ── 9. PART_MASTERをname→IDで引く ────────────────────────
    pm_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    pm_name_to_id: dict[str, str] = {
        ctx["extract_prop_text_any"](r, PARTMASTER_NAME_KEYS): r.get("id", "")
        for r in pm_rows
    }

    # ── 10. PART_DEFINITION ──────────────────────────────────
    pd_db = ctx["CONCERT_DB_PART_DEFINITION"]
    tpd = _p(ctx, pd_db)
    # (part_def_name, song_idx) → partdef_id
    partdef_map: dict[tuple[str, int], str] = {}
    pd_count = 0
    # APOLLOのIDから曲名を引くマップ
    apollo_name_map = {
        apollo_sid: sname
        for (sname, _, _, _), (_, apollo_sid) in zip(song_defs, atlas_song_id_list)
    } if atlas_song_id_list else {}

    for song_idx, apollo_sid in enumerate(song_ids):
        sname = apollo_name_map.get(apollo_sid, f"曲{song_idx+1}")
        for part_def, _, inst_name, pm_name, _, n_test, n_demo in PART_ROSTER:
            n = n_demo if is_demo else n_test
            if n == 0: continue
            iid   = inst_name_to_id.get(inst_name, "")
            pm_id = pm_name_to_id.get(pm_name, "")
            if not iid: continue
            record_title = f"{pfx} {sname} / {part_def}"
            props = {}
            _put(ctx, props, tpd, PARTDEF_RECORD_KEYS,       record_title)
            _put(ctx, props, tpd, PARTDEF_NAME_KEYS,         record_title)
            _put(ctx, props, tpd, PARTDEF_DISPLAY_NAME_KEYS, part_def)
            _put(ctx, props, tpd, PARTDEF_CONCERT_REL_KEYS,  concert_id)
            _put(ctx, props, tpd, PARTDEF_SONG_REL_KEYS,     apollo_sid)
            _put(ctx, props, tpd, PARTDEF_INST_REL_KEYS,     iid)
            if pm_id:
                _put(ctx, props, tpd, PARTDEF_PART_REL_KEYS, pm_id)
            ctx["put_key_any"](props, tpd, PARTDEF_KEY_KEYS,
                               concert_id, apollo_sid, part_def, prefix="part")
            pd_id = track(_create(ctx, pd_db, props))
            if pd_id:
                partdef_map[(part_def, song_idx)] = pd_id
                pd_count += 1
    summary["PART_DEFINITION"] = pd_count

    # ── 11. CONCERT_CAST ─────────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    tcast = _p(ctx, cast_db)
    cast_ids: list[str] = []
    player_to_cast: dict[str, str] = {}
    for part_def, player_id, sys_role in cast_plan:
        pm_name = next((row[3] for row in PART_ROSTER if row[0] == part_def), "")
        pm_id   = pm_name_to_id.get(pm_name, "")
        props = {}
        ctx["put_key_any"](props, tcast, PARTICIPANT_RECORD_KEYS,
                           concert_id, player_id, prefix="participant")
        _put(ctx, props, tcast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tcast, PARTICIPANT_PLAYER_REL_KEYS,  player_id)
        if pm_id:
            _put(ctx, props, tcast, PARTICIPANT_PART_REL_KEYS, pm_id)
        # 役職_音楽：ロールに応じて設定
        role_music = {
            "Manager": "コンサートマスター",
            "Leader":  "パートリーダー",
            "Player":  "プレイヤー",
        }.get(sys_role, "プレイヤー")
        _put(ctx, props, tcast, PARTICIPANT_ROLE_KEYS,        role_music)
        # 役職_運営：役職持ちは個別設定、それ以外は「団員」
        role_ops = ROLE_OPS_BY_PART.get(part_def, "") if sys_role != "Player" else ""
        _put(ctx, props, tcast, PARTICIPANT_ROLE_OPS_KEYS,    role_ops if role_ops else "団員")
        _put(ctx, props, tcast, PARTICIPANT_SYSTEM_ROLE_KEYS, sys_role)
        _put(ctx, props, tcast, PARTICIPANT_FEE_KEYS,         5000)
        cast_id = track(_create(ctx, cast_db, props))
        if cast_id:
            cast_ids.append(cast_id)
            player_to_cast[player_id] = cast_id
    summary["CONCERT_CAST"] = len(cast_ids)

    # ── 12. ATTENDANCE ───────────────────────────────────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    tatt = _p(ctx, att_db)
    pr_rel  = ctx["find_prop_name"](tatt, ATT_PRACTICE_REL_KEYS)
    pl_rel  = ctx["find_prop_name"](tatt, ATT_PLAYER_REL_KEYS)
    st_key  = ctx["find_prop_name"](tatt, ATT_STATUS_KEYS)
    att_count = 0
    status_pattern = ["○","○","○","△","×","○","○","△","○","○"]
    for pr_idx, pr_id in enumerate(practice_ids):
        is_concert_day = (pr_id == concert_day_id)
        for i, (_, player_id, _) in enumerate(cast_plan):
            cast_id = player_to_cast.get(player_id, "")
            if not cast_id: continue
            if is_concert_day:
                status = "○"
            elif pr_idx == 2 and i % 5 == 0:
                continue  # 第3回は一部未回答（進行中状態の再現）
            else:
                status = status_pattern[i % len(status_pattern)]
            props = {}
            ctx["put_key_any"](props, tatt, ATTENDANCE_KEY_KEYS,
                               cast_id, pr_id, prefix="att")
            if pr_rel: ctx["put_prop"](props, tatt, pr_rel, pr_id)
            if pl_rel: ctx["put_prop"](props, tatt, pl_rel, cast_id)
            if st_key: ctx["put_prop"](props, tatt, st_key, status)
            if track(_create(ctx, att_db, props)): att_count += 1
    summary["ATTENDANCE"] = att_count

    # ── 13. PLAYER_INSTRUMENT（Perc奏者の所有楽器） ──────────
    pi_db = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    tpi   = _p(ctx, pi_db)
    perc_parts = {row[0] for row in PART_ROSTER if row[1] == "打楽器"}
    perc_inst_names = list(dict.fromkeys(
        row[2] for row in PART_ROSTER if row[1] == "打楽器"
    ))
    pi_count = 0
    for part_def, player_id, _ in cast_plan:
        if part_def not in perc_parts: continue
        cast_id = player_to_cast.get(player_id, "")
        for iname in perc_inst_names:
            iid = inst_name_to_id.get(iname, "")
            if not iid: continue
            props = {}
            # PKはassign_key形式
            ctx["put_key_any"](props, tpi, ASSIGN_KEY_KEYS,
                               player_id, iid, prefix="pi")
            _put(ctx, props, tpi, PI_CONCERT_REL_KEYS,      concert_id)
            _put(ctx, props, tpi, PI_PLAYER_REL_KEYS,       player_id)
            if cast_id:
                _put(ctx, props, tpi, PI_PARTICIPANT_REL_KEYS, cast_id)
            _put(ctx, props, tpi, PI_INST_REL_KEYS,         iid)
            _put(ctx, props, tpi, PI_OWN_COUNT_KEYS,        1)
            if track(_create(ctx, pi_db, props)): pi_count += 1
    summary["PLAYER_INSTRUMENT"] = pi_count

    # ── 14. PREFERENCE（Perc奏者のパート希望） ───────────────
    pref_db   = ctx["CONCERT_DB_PREFERENCE"]
    tpref     = _p(ctx, pref_db)
    pref_count = 0
    prio_cycle = ["第1希望", "第2希望", "第3希望", "希望なし/降り番でも可"]
    # Percパートのパート定義IDを収集
    perc_pd_ids: list[str] = []
    for (part_def, song_idx), pd_id in partdef_map.items():
        if part_def in perc_parts:
            perc_pd_ids.append(pd_id)

    for part_def, player_id, _ in cast_plan:
        if part_def not in perc_parts: continue
        cast_id = player_to_cast.get(player_id, "")
        if not cast_id: continue
        for j, pd_id in enumerate(perc_pd_ids):
            priority = prio_cycle[j % len(prio_cycle)]
            props = {}
            ctx["put_key_any"](props, tpref, PREFERENCE_KEY_KEYS,
                               cast_id, pd_id, prefix="pref")
            # PREF_PLAYER_REL_KEYS → CONCERT_CASTへのリレーション
            _put(ctx, props, tpref, PREF_PLAYER_REL_KEYS, cast_id)
            _put(ctx, props, tpref, PREF_PART_REL_KEYS,   pd_id)
            _put(ctx, props, tpref, PREF_PRIORITY_KEYS,   priority)
            if track(_create(ctx, pref_db, props)): pref_count += 1
    summary["PREFERENCE"] = pref_count

    # ── 15. RENTAL ───────────────────────────────────────────
    rent_db = ctx.get("CONCERT_DB_RENTAL", "")
    rent_count = 0
    if rent_db and practice_ids:
        trent = _p(ctx, rent_db)
        pr_id_r = practice_ids[0]
        for iname, vendor, item_name, qty, unit_price, confirmed, cost_type in RENTAL_ITEMS:
            iid = inst_name_to_id.get(iname, "")
            props = {}
            # PKはrental_{practice_id}_{instrument_id}_{uuid8}形式
            uid = str(uuid.uuid4())[:8]
            ctx["put_key_any"](props, trent, RENTAL_KEY_KEYS,
                               pr_id_r, iid or cost_type, uid, prefix="rental")
            _put(ctx, props, trent, RENTAL_PRACTICE_REL_KEYS, pr_id_r)
            if iid:
                _put(ctx, props, trent, RENTAL_INST_REL_KEYS, iid)
            _put(ctx, props, trent, RENTAL_RECORD_KEYS,
                 f"{item_name} × 第1回練習 / {vendor}")
            _put(ctx, props, trent, RENTAL_ITEM_NAME_KEYS,  item_name)
            _put(ctx, props, trent, RENTAL_VENDOR_KEYS,     vendor)
            _put(ctx, props, trent, RENTAL_QTY_KEYS,        qty)
            _put(ctx, props, trent, RENTAL_UNIT_PRICE_KEYS, unit_price)
            _put(ctx, props, trent, RENTAL_CONFIRMED_KEYS,  confirmed)
            _put(ctx, props, trent, RENTAL_COST_TYPE_KEYS,  cost_type)
            if track(_create(ctx, rent_db, props)): rent_count += 1
    summary["RENTAL"] = rent_count

    # ── 16. SCHEDULE（第1回練習） ─────────────────────────────
    sched_db = ctx.get("CONCERT_DB_SCHEDULE", "")
    sched_count = 0
    if sched_db and practice_ids and song_ids:
        tsched = _p(ctx, sched_db)
        pr_id_s = practice_ids[0]
        items = [
            (1, "搬入", "09:00", "10:00", "楽器搬入",  None),
            (2, "練習", "10:00", "12:00", "午前練習",  song_ids[0]),
            (3, "休憩", "12:00", "13:00", "昼休憩",    None),
            (4, "練習", "13:00", "17:00", "午後練習",  song_ids[1] if len(song_ids) > 1 else song_ids[0]),
            (5, "搬出", "17:00", "18:00", "楽器搬出",  None),
        ]
        for order, stype, start, end, content, apollo_sid in items:
            props = {}
            ctx["put_key_any"](props, tsched, SCHEDULE_KEY_KEYS,
                               pr_id_s, start, prefix="sched")
            _put(ctx, props, tsched, SCHEDULE_PRACTICE_REL_KEYS, pr_id_s)
            _put(ctx, props, tsched, SCHEDULE_TYPE_KEYS,          stype)
            _put(ctx, props, tsched, SCHEDULE_START_KEYS,         start)
            _put(ctx, props, tsched, SCHEDULE_END_KEYS,           end)
            _put(ctx, props, tsched, SCHEDULE_CONTENT_KEYS,       content)
            _put(ctx, props, tsched, SCHEDULE_ORDER_KEYS,         order)
            # 練習コマにAPOLLOリレーションを設定
            if apollo_sid:
                _put(ctx, props, tsched, SCHEDULE_SONG_REL_KEYS, apollo_sid)
            if track(_create(ctx, sched_db, props)): sched_count += 1
    summary["SCHEDULE"] = sched_count

    # ── 17. HARMONIA_CONCERT ─────────────────────────────────
    hc_db = ctx.get("CONCERT_DB_HARMONIA_CONCERT", "")
    if hc_db:
        import random, string as _string
        invite_code = "".join(
            random.choices(_string.ascii_uppercase + _string.digits, k=8))
        thc = _p(ctx, hc_db)
        props = {}
        _put(ctx, props, thc, HARMONIA_CONCERT_KEY_KEYS,
             f"harmonia_{concert_id[:8]}_{pfx}")
        _put(ctx, props, thc, HARMONIA_CONCERT_CONCERT_REL_KEYS,   concert_id)
        _put(ctx, props, thc, HARMONIA_CONCERT_MANAGED_KEYS,       True)
        _put(ctx, props, thc, HARMONIA_CONCERT_SONG_INFO_KEYS,     True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PRACTICE_INFO_KEYS, True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PRACTICE_DATE_KEYS, True)
        _put(ctx, props, thc, HARMONIA_CONCERT_REQUIRED_INST_KEYS, True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PARTDEF_KEYS,       True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PLAYER_INFO_KEYS,   True)
        _put(ctx, props, thc, HARMONIA_CONCERT_ATTENDANCE_KEYS,    False)
        _put(ctx, props, thc, HARMONIA_CONCERT_PREFERENCE_KEYS,    False)
        _put(ctx, props, thc, HARMONIA_CONCERT_INVITE_CODE_KEYS,   invite_code)
        hc_res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                    json={"parent": {"database_id": hc_db},
                                          "properties": props})
        hc_id = hc_res.json().get("id", "") if hc_res and hc_res.status_code == 200 else ""
        if hc_id:
            track(hc_id)
            summary["HARMONIA_CONCERT"] = 1
            summary["招待コード"] = invite_code
        else:
            err = hc_res.json() if hc_res else "No response"
            summary["HARMONIA_CONCERT"] = 0
            summary["HARMONIA_CONCERT_ERROR"] = str(err)[:200]

    # 作成IDを保存
    existing = st.session_state.get(f"test_created_ids_{pfx}", [])
    st.session_state[f"test_created_ids_{pfx}"] = existing + created_ids
    summary["作成総件数"] = len(created_ids)
    _clear_cache()
    return summary


# ============================================================
# 削除
# ============================================================

def _delete_all(ctx, pfx: str) -> dict:
    summary = {}
    created_ids = st.session_state.get(f"test_created_ids_{pfx}", [])
    if created_ids:
        count = sum(1 for pid in created_ids if _archive(ctx, pid))
        st.session_state.pop(f"test_created_ids_{pfx}", None)
        summary["削除件数"] = count
        _clear_cache()
        return summary

    st.warning("session_stateが消えているため、プレフィックス検索で削除します。")
    test_concert_ids: set[str] = set()
    for r in ctx["query_all"](ctx.get("CONCERT_DB_CONCERT", ""), None):
        n = ctx["extract_prop_text_any"](r, CONCERT_NAME_KEYS) or ""
        if n.startswith(pfx): test_concert_ids.add(r.get("id", ""))

    if test_concert_ids:
        for db_key, rel_keys in [
            ("CONCERT_DB_PARTICIPANT",     ["演奏会", "FK演奏会"]),
            ("CONCERT_DB_PRACTICE",        ["演奏会", "FK演奏会"]),
            ("CONCERT_DB_CONCERT_SONG",    ["演奏会", "FK演奏会"]),
            ("CONCERT_DB_CONCERT_INSTRUMENT", ["演奏会", "FK演奏会"]),
            ("CONCERT_DB_HARMONIA_CONCERT",["演奏会", "FK演奏会"]),
            ("CONCERT_DB_PLAYER_INSTRUMENT",["演奏会", "FK演奏会"]),
        ]:
            db = ctx.get(db_key, "")
            if not db: continue
            cnt = 0
            for r in ctx["query_all"](db, None):
                if any(cid in ctx["extract_relation_ids_any"](r, rel_keys)
                       for cid in test_concert_ids):
                    if _archive(ctx, r.get("id", "")): cnt += 1
            if cnt: summary[db_key.replace("CONCERT_DB_", "")] = cnt

        # 練習IDを使って削除
        test_practice_ids: set[str] = set()
        for r in ctx["query_all"](ctx.get("CONCERT_DB_PRACTICE", ""), None):
            if any(cid in ctx["extract_relation_ids_any"](r, ["演奏会", "FK演奏会"])
                   for cid in test_concert_ids):
                test_practice_ids.add(r.get("id", ""))

        for db_key, rel_keys in [
            ("CONCERT_DB_ATTENDANCE",["練習", "FK練習"]),
            ("CONCERT_DB_RENTAL",    ["練習", "FK練習"]),
            ("CONCERT_DB_SCHEDULE",  ["練習", "FK練習"]),
        ]:
            db = ctx.get(db_key, "")
            if not db: continue
            cnt = 0
            for r in ctx["query_all"](db, None):
                if any(pid in ctx["extract_relation_ids_any"](r, rel_keys)
                       for pid in test_practice_ids):
                    if _archive(ctx, r.get("id", "")): cnt += 1
            if cnt: summary[db_key.replace("CONCERT_DB_", "")] = cnt

        # PREFERENCE（CONCERT_CAST経由）
        test_cast_ids: set[str] = set()
        for r in ctx["query_all"](ctx.get("CONCERT_DB_PARTICIPANT", ""), None):
            if any(cid in ctx["extract_relation_ids_any"](r, ["演奏会", "FK演奏会"])
                   for cid in test_concert_ids):
                test_cast_ids.add(r.get("id", ""))
        pref_cnt = 0
        for r in ctx["query_all"](ctx.get("CONCERT_DB_PREFERENCE", ""), None):
            if any(cid in ctx["extract_relation_ids_any"](r, PREF_PLAYER_REL_KEYS)
                   for cid in test_cast_ids):
                if _archive(ctx, r.get("id", "")): pref_cnt += 1
        if pref_cnt: summary["PREFERENCE"] = pref_cnt

        # PART_DEFINITION（演奏会リレーション経由）
        pd_cnt = 0
        for r in ctx["query_all"](ctx.get("CONCERT_DB_PART_DEFINITION", ""), None):
            if any(cid in ctx["extract_relation_ids_any"](r, ["演奏会", "FK演奏会"])
                   for cid in test_concert_ids):
                if _archive(ctx, r.get("id", "")): pd_cnt += 1
        if pd_cnt: summary["PART_DEFINITION"] = pd_cnt

        # MOVEMENT（KeyフィールドにPFXが含まれるもの）
        mv_cnt = 0
        mv_db = ctx.get("CONCERT_DB_MOVEMENT", "")
        if mv_db:
            for r in ctx["query_all"](mv_db, None):
                k = ctx["extract_prop_text_any"](r, MOVEMENT_KEY_KEYS) or \
                    ctx["extract_title"](r) or ""
                if pfx in k and _archive(ctx, r.get("id", "")): mv_cnt += 1
        if mv_cnt: summary["MOVEMENT"] = mv_cnt

    # プレフィックスで直接検索できるDB
    for db_key, keys in [
        ("CONCERT_DB_CONCERT",   CONCERT_NAME_KEYS),
        ("CONCERT_DB_SONG",      SONG_NAME_KEYS),
        ("CONCERT_DB_PRACTICE",  PRACTICE_NAME_KEYS),
        ("CONCERT_DB_PLAYER",    PLAYER_NAME_KEYS),
        ("CONCERT_DB_MOVEMENT",  MOVEMENT_NAME_KEYS),
    ]:
        db = ctx.get(db_key, "")
        if not db: continue
        cnt = 0
        for r in ctx["query_all"](db, None):
            n = ctx["extract_prop_text_any"](r, keys) or ctx["extract_title"](r) or ""
            if pfx in n and _archive(ctx, r.get("id", "")): cnt += 1
        if cnt: summary[db_key.replace("CONCERT_DB_", "")] = cnt

    _clear_cache()
    return summary


# ============================================================
# メイン
# ============================================================

def render(ctx: dict):
    st.caption("演奏会未選択でも利用できます。")
    st.warning("⚠️ この画面はテスト・開発用です。本番運用時は使用しないでください。")
    st.divider()
    st.subheader("📦 HARMONIAテストデータ")

    tab_test, tab_demo = st.tabs(["🔬 [TEST] 軽量版", "🎭 [DEMO] フル版（2管編成）"])

    for tab, pfx, is_demo, desc in [
        (tab_test, "[TEST]", False,
         "各パート1〜2名（計約20名）。日常のテスト・デバッグ用。"),
        (tab_demo, "[DEMO]", True,
         "2管編成フル人数（計55名）。サンプルデータ提示・デモ用。"),
    ]:
        with tab:
            st.caption(desc)
            created = st.session_state.get(f"test_created_ids_{pfx}", [])
            if created:
                st.info(f"投入済み: {len(created)}件（削除可能）")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("🚀 投入", type="primary",
                             use_container_width=True,
                             key=f"seed_{pfx}",
                             disabled=bool(created)):
                    with st.spinner("投入中... しばらくお待ちください"):
                        result = _seed_all(ctx, pfx, is_demo)
                    st.success("✅ 投入完了")
                    for k, v in result.items():
                        st.caption(f"  {k}: {v}")

            with col2:
                confirm = st.checkbox("削除対象を確認しました",
                                      key=f"confirm_{pfx}")
                if st.button("🗑️ 削除", type="secondary",
                             use_container_width=True,
                             key=f"delete_{pfx}",
                             disabled=not confirm):
                    with st.spinner("削除中..."):
                        result = _delete_all(ctx, pfx)
                    st.success("✅ 削除完了") if result else st.info("削除対象なし")
                    for k, v in result.items():
                        st.caption(f"  {k}: {v}")
