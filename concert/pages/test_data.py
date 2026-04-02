"""
concert/pages/test_data.py
テストデータの一括投入・一括削除機能

2パターン：
  [TEST] 軽量版 - 各パート1〜2名（計約20名）日常テスト用
  [DEMO] フル版 - 2管編成55名、サンプルデータ提示用
"""
import streamlit as st
from datetime import date, timedelta
from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS, CONCERT_VENUE_KEYS, CONCERT_ADDRESS_KEYS,
    CONCERT_CONDUCTOR_KEYS, CONCERT_SOLOIST_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_CONCERT_REL_KEYS, PRACTICE_DATE_KEYS,
    PRACTICE_VENUE_KEYS, PRACTICE_ADDRESS_KEYS, PRACTICE_CONCERT_DAY_KEYS,
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
    EXPENSE_KEY_KEYS, EXPENSE_CONCERT_REL_KEYS, EXPENSE_TYPE_KEYS,
    EXPENSE_CONTENT_KEYS, EXPENSE_AMOUNT_KEYS, EXPENSE_CONFIRMED_KEYS,
    RENTAL_RECORD_KEYS, RENTAL_PRACTICE_REL_KEYS, RENTAL_INST_REL_KEYS,
    RENTAL_ITEM_NAME_KEYS, RENTAL_VENDOR_KEYS, RENTAL_QTY_KEYS,
    RENTAL_UNIT_PRICE_KEYS, RENTAL_CONFIRMED_KEYS,
    SCHEDULE_KEY_KEYS, SCHEDULE_PRACTICE_REL_KEYS, SCHEDULE_START_KEYS,
    SCHEDULE_END_KEYS, SCHEDULE_TYPE_KEYS, SCHEDULE_CONTENT_KEYS, SCHEDULE_ORDER_KEYS,
    CONCERT_SONG_KEY_KEYS, CONCERT_SONG_CONCERT_REL_KEYS, CONCERT_SONG_SONG_REL_KEYS,
    CONCERT_SONG_ORDER_KEYS, CONCERT_SONG_DONE_KEYS,
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
# (part_name, inst_category, inst_name, part_master_name, system_role, n_test, n_demo)
PART_ROSTER = [
    ("Fl.1",   "管楽器", "Flute",       "Fl",   "Player", 1, 2),
    ("Fl.2",   "管楽器", "Flute",       "Fl",   "Player", 1, 2),
    ("Ob.1",   "管楽器", "Oboe",        "Ob",   "Player", 1, 2),
    ("Ob.2",   "管楽器", "Oboe",        "Ob",   "Player", 1, 2),
    ("Cl.1",   "管楽器", "Clarinet",    "Cl",   "Player", 1, 2),
    ("Cl.2",   "管楽器", "Clarinet",    "Cl",   "Player", 1, 2),
    ("Fg.1",   "管楽器", "Fagotto",     "Fg",   "Player", 1, 2),
    ("Fg.2",   "管楽器", "Fagotto",     "Fg",   "Player", 1, 2),
    ("Hr.1",   "管楽器", "Horn",        "Hr",   "Player", 1, 4),
    ("Hr.2",   "管楽器", "Horn",        "Hr",   "Player", 1, 4),
    ("Hr.3",   "管楽器", "Horn",        "Hr",   "Player", 0, 4),
    ("Hr.4",   "管楽器", "Horn",        "Hr",   "Player", 0, 4),
    ("Tp.1",   "管楽器", "Trumpet",     "Tp",   "Player", 1, 2),
    ("Tp.2",   "管楽器", "Trumpet",     "Tp",   "Player", 1, 2),
    ("Tb.1",   "管楽器", "Trombone",    "Tb",   "Player", 1, 3),
    ("Tb.2",   "管楽器", "Trombone",    "Tb",   "Player", 1, 3),
    ("Tb.3",   "管楽器", "Trombone",    "Tb",   "Player", 0, 3),
    ("Tuba",   "管楽器", "Tuba",        "Tuba", "Player", 1, 1),
    ("Timp.",  "打楽器", "Timpani",     "Perc", "Leader", 1, 1),
    ("Perc.1", "打楽器", "Percussion",  "Perc", "Player", 1, 1),
    ("Perc.2", "打楽器", "Percussion",  "Perc", "Player", 0, 1),
    ("Perc.3", "打楽器", "Percussion",  "Perc", "Player", 0, 1),
    ("Vn1",    "弦楽器", "Violin",      "Vn1",  "Manager",1,12),
    ("Vn2",    "弦楽器", "Violin",      "Vn2",  "Leader", 1,10),
    ("Va",     "弦楽器", "Viola",       "Va",   "Player", 1, 8),
    ("Vc",     "弦楽器", "Violoncello", "Vc",   "Player", 1, 8),
    ("Cb",     "弦楽器", "Contrabass",  "Cb",   "Player", 1, 4),
]


def _clear_cache():
    prefixes = ("practice_list_","concert_list","song_list_","partdef_list_",
                "pi_list_","attendance_list_","participant_list_","instrument_list",
                "schedule_list_","expense_list_","cast_list_","pi_master_",
                "si_list_","pi_practice_","concert_song_list_",
                "_movement_map_cache","_song_display_name_cache","_part_master_map_cache")
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
    return r.json().get("id","") if r and r.status_code == 200 else ""


def _p(ctx, db_id): return ctx["get_prop_types"](db_id) or {}
def _put(ctx, props, t, keys, value): ctx["put_prop_any"](props, t, keys, value)


def _roman(n: int) -> str:
    mapping = [(4,"IV"),(1,"I"),(9,"IX") if n>8 else (0,"")]
    table = [(1000,"M"),(900,"CM"),(500,"D"),(400,"CD"),(100,"C"),(90,"XC"),
             (50,"L"),(40,"XL"),(10,"X"),(9,"IX"),(5,"V"),(4,"IV"),(1,"I")]
    result = ""
    for val, sym in table:
        while n >= val:
            result += sym; n -= val
    return result


# ============================================================
# 投入
# ============================================================

def _seed_all(ctx, pfx: str, is_demo: bool) -> dict:
    summary = {}
    created_ids: list[str] = []
    n_idx = 6 if is_demo else 5  # PART_ROSTERの人数列

    def track(pid: str) -> str:
        if pid: created_ids.append(pid)
        return pid

    # ── 1. INSTRUMENT ────────────────────────────────────────
    inst_db = ctx["CONCERT_DB_INSTRUMENT"]
    ti = _p(ctx, inst_db)
    # 既存楽器を再利用、なければ作成
    existing_insts = ctx["query_all"](inst_db, None)
    existing_inst_map: dict[str, str] = {}  # inst_name → id
    for r in existing_insts:
        n = ctx["extract_prop_text_any"](r, INSTRUMENT_NAME_KEYS) or ""
        if n: existing_inst_map[n] = r.get("id","")

    inst_name_to_id: dict[str, str] = {}
    inst_names_needed = list(dict.fromkeys(row[2] for row in PART_ROSTER))
    inst_count = 0
    for iname in inst_names_needed:
        clean = iname.replace(f"{pfx} ","")
        if clean in existing_inst_map:
            inst_name_to_id[iname] = existing_inst_map[clean]
        else:
            cat = next((row[1] for row in PART_ROSTER if row[2]==iname), "その他")
            props = {}
            ctx["put_key_any"](props, ti, INSTRUMENT_KEY_KEYS, iname, prefix="inst")
            _put(ctx, props, ti, INSTRUMENT_NAME_KEYS,     iname)
            _put(ctx, props, ti, INSTRUMENT_CATEGORY_KEYS, cat)
            iid = track(_create(ctx, inst_db, props))
            if iid:
                inst_name_to_id[iname] = iid
                inst_count += 1
    if inst_count: summary["INSTRUMENT（新規）"] = inst_count

    # ── 2. PERFORMER ─────────────────────────────────────────
    player_db = ctx["CONCERT_DB_PLAYER"]
    tp = _p(ctx, player_db)
    # part_name → [(player_id, system_role), ...]
    cast_plan: list[tuple[str,str,str]] = []  # (part_name, player_id, system_role)
    player_count = 0
    player_no = 1
    for part_name, _, _, pm_name, sys_role, n_test, n_demo in PART_ROSTER:
        n = n_demo if is_demo else n_test
        for seat in range(n):
            label = f"{pfx} 奏者{player_no:02d}（{part_name}）"
            props = {}
            _put(ctx, props, tp, PLAYER_NAME_KEYS,  label)
            _put(ctx, props, tp, PLAYER_HN_KEYS,    f"{part_name}-{seat+1}")
            _put(ctx, props, tp, PLAYER_EMAIL_KEYS,
                 f"test.player{player_no:02d}@harmonia.example.com")
            pid = track(_create(ctx, player_db, props))
            if pid:
                cast_plan.append((part_name, pid, sys_role if seat == 0 else "Player"))
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
    media_key = ctx["find_prop_name"](tc, ["媒体","Media"])
    if media_key:
        mt = tc.get(media_key,"")
        if mt == "multi_select": props[media_key] = {"multi_select":[{"name":"出演"}]}
        elif mt == "select":     props[media_key] = {"select":{"name":"出演"}}
    dt_key = ctx["find_prop_name"](tc, CONCERT_DATE_KEYS)
    concert_date = date.today() + timedelta(days=90)
    if dt_key:
        props[dt_key] = {"date":{"start": concert_date.isoformat() + "T14:00:00+09:00"}}
    concert_id = track(_create(ctx, concert_db, props))
    summary["CONCERT"] = 1 if concert_id else 0
    if not concert_id:
        st.session_state[f"test_created_ids_{pfx}"] = created_ids
        return summary

    # ── 4. MOVEMENT（楽章）───────────────────────────────────
    # 「テスト交響曲」用の楽章を4つ作成
    mv_db = ctx.get("CONCERT_DB_MOVEMENT","")
    mv_ids: list[str] = []
    if mv_db:
        tm = _p(ctx, mv_db)
        movements = [
            (1, "I",   "Allegro vivace"),
            (2, "II",  "Andante con moto"),
            (3, "III", "Scherzo: Allegro"),
            (4, "IV",  "Finale: Allegro"),
        ]
        for no, roman, mv_name in movements:
            props = {}
            ctx["put_key_any"](props, tm, MOVEMENT_KEY_KEYS,
                               f"{pfx}_symphony_mv{no}", prefix="mv")
            _put(ctx, props, tm, MOVEMENT_NAME_KEYS,  mv_name)
            _put(ctx, props, tm, MOVEMENT_NO_KEYS,    no)
            _put(ctx, props, tm, MOVEMENT_ORDER_KEYS, no)
            _put(ctx, props, tm, MOVEMENT_ROMAN_KEYS, roman)
            mid = track(_create(ctx, mv_db, props))
            if mid: mv_ids.append(mid)
        summary["MOVEMENT"] = len(mv_ids)

    # ── 5. SONG（ATLAS内の演奏曲ページ） ─────────────────────
    # 曲A: テスト序曲（全楽章フラグON、楽章なし）
    # 曲B: テスト交響曲 第4楽章（全楽章フラグOFF、MVリレーションあり）
    song_defs = [
        ("テスト序曲",        "Test, Composer A.", True,  None),
        ("テスト交響曲",      "Test, Composer B.", False, mv_ids[3] if len(mv_ids)>=4 else None),
    ]
    song_ids: list[str] = []
    apollo_db = ctx["CONCERT_DB_SONG"]
    ta = _p(ctx, apollo_db)
    for sname, composer, all_mvmt, mv_id in song_defs:
        # まずATLASに「演奏曲」媒体のレコードを作成
        props = {}
        _put(ctx, props, tc, CONCERT_NAME_KEYS,      f"{pfx} {sname}")
        _put(ctx, props, tc, ATLAS_CREATOR_KEYS,     composer)
        if media_key:
            mt = tc.get(media_key,"")
            if mt == "multi_select": props[media_key] = {"multi_select":[{"name":"演奏曲"}]}
            elif mt == "select":     props[media_key] = {"select":{"name":"演奏曲"}}
        _put(ctx, props, tc, ATLAS_SCORE_HISTORY_REL_KEYS, concert_id)
        atlas_sid = track(_create(ctx, concert_db, props))

        # APOLLOにも作成してATLASとリレーション
        if atlas_sid and ta:
            apollo_props = {}
            _put(ctx, apollo_props, ta, SONG_NAME_KEYS,         f"{pfx} {sname}")
            _put(ctx, apollo_props, ta, SONG_COMPOSER_KEYS,     composer)
            _put(ctx, apollo_props, ta, SONG_CONCERT_REL_KEYS,  concert_id)
            _put(ctx, apollo_props, ta, SONG_ALL_MOVEMENTS_KEYS, all_mvmt)
            # ATLASへのリレーション（演奏曲フィールド）
            ctx["put_prop_any"](apollo_props, ta, ["演奏曲","FK演奏曲"], atlas_sid)
            if mv_id and not all_mvmt:
                ctx["put_prop_any"](apollo_props, ta, SONG_MOVEMENT_REL_KEYS, mv_id)
            apollo_sid = track(_create(ctx, apollo_db, apollo_props))
            if apollo_sid:
                song_ids.append(apollo_sid)

    # 親演奏会側に演奏曲リレーションをセット
    if song_ids:
        upd = {}; _put(ctx, upd, tc, ATLAS_SCORE_REL_KEYS,
                       [s for s in song_ids])
        ctx["api_request"]("patch",
            f"https://api.notion.com/v1/pages/{concert_id}",
            json={"properties": upd})
    summary["SONG"] = len(song_ids)

    # ── 6. CONCERT_SONG ──────────────────────────────────────
    cs_db = ctx.get("CONCERT_DB_CONCERT_SONG","")
    cs_count = 0
    cs_ids_for_atlas: list[str] = []  # ATLAS songのIDをCONCERT_SONGに入れる
    # ATLAS側のsong IDを取得（APOLLOのatlas_sidリスト）
    atlas_song_ids_for_cs = []
    for sname, _, _, _ in song_defs:
        # ATLASから作ったIDを探す（直前のループで作成した順と対応）
        # track済みのcreated_idsから逆引き（少し簡略化してsong_idsと同数と仮定）
        pass
    # 実際はATLASのIDをtrack順から取得するより、CONCERT_SONGにはAPOLLO IDを直接入れる方針に
    if cs_db and song_ids:
        tcs = _p(ctx, cs_db)
        for idx, sid in enumerate(song_ids, start=1):
            props = {}
            ctx["put_key_any"](props, tcs, CONCERT_SONG_KEY_KEYS,
                               concert_id, sid, prefix="concert_song")
            _put(ctx, props, tcs, CONCERT_SONG_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, tcs, CONCERT_SONG_SONG_REL_KEYS,    sid)
            _put(ctx, props, tcs, CONCERT_SONG_ORDER_KEYS,       idx)
            _put(ctx, props, tcs, CONCERT_SONG_DONE_KEYS,        True)
            if track(_create(ctx, cs_db, props)): cs_count += 1
    summary["CONCERT_SONG"] = cs_count

    # ── 7. PRACTICE（3回＋本番日） ───────────────────────────
    prac_db = ctx["CONCERT_DB_PRACTICE"]
    tpr = _p(ctx, prac_db)
    practice_ids: list[str] = []
    venues = [
        ("ザ・シンフォニーホール",     "大阪府大阪市北区大淀南2丁目3-3"),
        ("豊中市立文化芸術センター",   "大阪府豊中市曽根東町3丁目7-2"),
        ("吹田市文化会館 メイシアター","大阪府吹田市泉町2丁目29-1"),
    ]
    times = ["T10:00:00+09:00","T13:00:00+09:00","T09:30:00+09:00"]
    base  = date.today() + timedelta(days=21)
    for i in range(3):
        props = {}
        _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{pfx} 第{i+1}回練習")
        _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tpr, PRACTICE_VENUE_KEYS,       venues[i][0])
        _put(ctx, props, tpr, PRACTICE_ADDRESS_KEYS,     venues[i][1])
        d = base + timedelta(weeks=i*2)
        dt_k = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
        if dt_k: props[dt_k] = {"date":{"start": d.isoformat()+times[i]}}
        pr_id = track(_create(ctx, prac_db, props))
        if pr_id: practice_ids.append(pr_id)

    # 本番日
    props = {}
    _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{pfx} 本番当日")
    _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
    _put(ctx, props, tpr, PRACTICE_CONCERT_DAY_KEYS, True)
    _put(ctx, props, tpr, PRACTICE_VENUE_KEYS,       "いずみホール")
    _put(ctx, props, tpr, PRACTICE_ADDRESS_KEYS,     "大阪府大阪市中央区城見1丁目4-70")
    dt_k4 = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
    if dt_k4:
        props[dt_k4] = {"date":{"start": concert_date.isoformat()+"T14:00:00+09:00"}}
    concert_day_id = track(_create(ctx, prac_db, props))
    if concert_day_id: practice_ids.append(concert_day_id)
    summary["PRACTICE"] = len(practice_ids)

    # ── 8. PART_MASTER参照 ───────────────────────────────────
    pm_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    pm_name_to_id: dict[str, str] = {
        ctx["extract_prop_text_any"](r, PARTMASTER_NAME_KEYS): r.get("id","")
        for r in pm_rows
    }

    # ── 9. INSTRUMENT名→ID逆引き ─────────────────────────────
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_name_map: dict[str, str] = {}
    for r in inst_rows:
        n = ctx["extract_prop_text_any"](r, INSTRUMENT_NAME_KEYS) or ""
        clean = n.replace(f"{pfx} ","")
        inst_name_map[clean] = r.get("id","")

    # ── 10. PART_DEFINITION ──────────────────────────────────
    pd_db = ctx["CONCERT_DB_PART_DEFINITION"]
    tpd = _p(ctx, pd_db)
    # part_name → [partdef_id per song]
    part_pd_map: dict[str, list[str]] = {}
    pd_count = 0
    for sid in song_ids:
        sname = ctx["extract_prop_text_any"](
            next((r for r in ctx["query_all"](ctx["CONCERT_DB_SONG"],None)
                  if r.get("id")==sid), {}), SONG_NAME_KEYS) or sid[:8]
        for part_name, _, inst_name, pm_name, _, n_test, n_demo in PART_ROSTER:
            n = n_demo if is_demo else n_test
            if n == 0: continue
            iid = inst_name_map.get(inst_name,"")
            pm_id = pm_name_to_id.get(pm_name,"")
            if not iid: continue
            clean_sname = sname.replace(f"{pfx} ","")
            record_title = f"{pfx} {clean_sname} / {part_name}"
            props = {}
            _put(ctx, props, tpd, PARTDEF_RECORD_KEYS,       record_title)
            _put(ctx, props, tpd, PARTDEF_NAME_KEYS,         record_title)
            _put(ctx, props, tpd, PARTDEF_DISPLAY_NAME_KEYS, part_name)
            _put(ctx, props, tpd, PARTDEF_CONCERT_REL_KEYS,  concert_id)
            _put(ctx, props, tpd, PARTDEF_SONG_REL_KEYS,     sid)
            _put(ctx, props, tpd, PARTDEF_INST_REL_KEYS,     iid)
            if pm_id:
                _put(ctx, props, tpd, PARTDEF_PART_REL_KEYS, pm_id)
            ctx["put_key_any"](props, tpd, PARTDEF_KEY_KEYS,
                               concert_id, sid, part_name, prefix="part")
            pd_id = track(_create(ctx, pd_db, props))
            if pd_id:
                part_pd_map.setdefault(part_name, []).append(pd_id)
                pd_count += 1
    summary["PART_DEFINITION"] = pd_count

    # ── 11. CONCERT_CAST ─────────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    tcast = _p(ctx, cast_db)
    cast_ids: list[str] = []
    player_to_cast: dict[str, str] = {}
    for part_name, player_id, sys_role in cast_plan:
        pm_id = pm_name_to_id.get(
            next((row[3] for row in PART_ROSTER if row[0]==part_name), ""), "")
        props = {}
        ctx["put_key_any"](props, tcast, PARTICIPANT_RECORD_KEYS,
                           concert_id, player_id, prefix="participant")
        _put(ctx, props, tcast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tcast, PARTICIPANT_PLAYER_REL_KEYS,  player_id)
        if pm_id: _put(ctx, props, tcast, PARTICIPANT_PART_REL_KEYS, pm_id)
        _put(ctx, props, tcast, PARTICIPANT_ROLE_KEYS,         "プレイヤー")
        _put(ctx, props, tcast, PARTICIPANT_SYSTEM_ROLE_KEYS,  sys_role)
        _put(ctx, props, tcast, PARTICIPANT_FEE_KEYS,          5000)
        cast_id = track(_create(ctx, cast_db, props))
        if cast_id:
            cast_ids.append(cast_id)
            player_to_cast[player_id] = cast_id
    summary["CONCERT_CAST"] = len(cast_ids)

    # ── 12. ATTENDANCE ───────────────────────────────────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    tatt = _p(ctx, att_db)
    pr_rel = ctx["find_prop_name"](tatt, ATT_PRACTICE_REL_KEYS)
    pl_rel = ctx["find_prop_name"](tatt, ATT_PLAYER_REL_KEYS)
    st_key = ctx["find_prop_name"](tatt, ATT_STATUS_KEYS)
    att_count = 0
    # 第1・2回は回答済み、第3回は未回答者あり（準備進行中の状態）
    for pr_idx, pr_id in enumerate(practice_ids):
        is_concert_day = (pr_id == concert_day_id)
        for i, (_, player_id, _) in enumerate(cast_plan):
            cast_id = player_to_cast.get(player_id,"")
            if not cast_id: continue
            if is_concert_day:
                status = "○"
            elif pr_idx == 2 and i % 5 == 0:
                continue  # 第3回は一部未回答（空のまま）
            else:
                status = ["○","○","○","△","×","○","○","△","○","○"][i % 10]
            props = {}
            ctx["put_key_any"](props, tatt, ATTENDANCE_KEY_KEYS,
                               cast_id, pr_id, prefix="att")
            if pr_rel: ctx["put_prop"](props, tatt, pr_rel, pr_id)
            if pl_rel: ctx["put_prop"](props, tatt, pl_rel, cast_id)
            if st_key: ctx["put_prop"](props, tatt, st_key, status)
            if track(_create(ctx, att_db, props)): att_count += 1
    summary["ATTENDANCE"] = att_count

    # ── 13. PREFERENCE（Percパートのみ） ─────────────────────
    pref_db = ctx["CONCERT_DB_PREFERENCE"]
    tpref = _p(ctx, pref_db)
    perc_parts = [row[0] for row in PART_ROSTER if row[1]=="打楽器"]
    pref_count = 0
    prio_opts = ["第1希望","第2希望","第3希望","希望なし/降り番でも可"]
    for part_name, player_id, _ in cast_plan:
        if part_name not in perc_parts: continue
        cast_id = player_to_cast.get(player_id,"")
        if not cast_id: continue
        pd_ids_for_player = []
        for pn, pd_list in part_pd_map.items():
            if pn in perc_parts:
                pd_ids_for_player.extend(pd_list)
        for j, pd_id in enumerate(pd_ids_for_player):
            priority = prio_opts[j % len(prio_opts)]
            props = {}
            ctx["put_key_any"](props, tpref, PREFERENCE_KEY_KEYS,
                               cast_id, pd_id, prefix="pref")
            _put(ctx, props, tpref, PREF_PLAYER_REL_KEYS, cast_id)
            _put(ctx, props, tpref, PREF_PART_REL_KEYS,   pd_id)
            _put(ctx, props, tpref, PREF_PRIORITY_KEYS,   priority)
            if track(_create(ctx, pref_db, props)): pref_count += 1
    summary["PREFERENCE"] = pref_count

    # ── 14. PLAYER_INSTRUMENT（Perc所有楽器） ────────────────
    pi_db = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    tpi = _p(ctx, pi_db)
    perc_inst_ids = [inst_name_map.get(row[2],"")
                     for row in PART_ROSTER if row[1]=="打楽器" and inst_name_map.get(row[2])]
    perc_inst_ids = list(dict.fromkeys(filter(None, perc_inst_ids)))
    pi_count = 0
    for part_name, player_id, _ in cast_plan:
        if part_name not in perc_parts: continue
        cast_id = player_to_cast.get(player_id,"")
        for iid in perc_inst_ids:
            props = {}
            ctx["put_key_any"](props, tpi, ASSIGN_KEY_KEYS, player_id, iid, prefix="pi")
            _put(ctx, props, tpi, PI_CONCERT_REL_KEYS,     concert_id)
            _put(ctx, props, tpi, PI_PLAYER_REL_KEYS,      player_id)
            if cast_id: _put(ctx, props, tpi, PI_PARTICIPANT_REL_KEYS, cast_id)
            _put(ctx, props, tpi, PI_INST_REL_KEYS,        iid)
            _put(ctx, props, tpi, PI_OWN_COUNT_KEYS,       1)
            if track(_create(ctx, pi_db, props)): pi_count += 1
    summary["PLAYER_INSTRUMENT"] = pi_count

    # ── 15. RENTAL ───────────────────────────────────────────
    rent_db = ctx.get("CONCERT_DB_RENTAL","")
    rent_count = 0
    if rent_db and practice_ids and perc_inst_ids:
        trent = _p(ctx, rent_db)
        for iid in perc_inst_ids[:2]:
            iname = next((n for n,i in inst_name_map.items() if i==iid),"楽器")
            props = {}
            _put(ctx, props, trent, RENTAL_RECORD_KEYS,       f"{pfx} rental_{iname}")
            _put(ctx, props, trent, RENTAL_PRACTICE_REL_KEYS, practice_ids[0])
            _put(ctx, props, trent, RENTAL_INST_REL_KEYS,     iid)
            _put(ctx, props, trent, RENTAL_ITEM_NAME_KEYS,    iname)
            _put(ctx, props, trent, RENTAL_VENDOR_KEYS,       "テスト楽器店")
            _put(ctx, props, trent, RENTAL_QTY_KEYS,          1)
            _put(ctx, props, trent, RENTAL_UNIT_PRICE_KEYS,   15000)
            _put(ctx, props, trent, RENTAL_CONFIRMED_KEYS,    True)
            if track(_create(ctx, rent_db, props)): rent_count += 1
    summary["RENTAL"] = rent_count

    # ── 16. SCHEDULE（第1回練習） ─────────────────────────────
    sched_db = ctx.get("CONCERT_DB_SCHEDULE","")
    sched_count = 0
    if sched_db and practice_ids:
        tsched = _p(ctx, sched_db)
        items = [
            (1,"搬入","09:00","10:00","楽器搬入"),
            (2,"練習","10:00","12:00","午前練習"),
            (3,"休憩","12:00","13:00","昼休憩"),
            (4,"練習","13:00","17:00","午後練習"),
            (5,"搬出","17:00","18:00","楽器搬出"),
        ]
        for order, stype, start, end, content in items:
            props = {}
            ctx["put_key_any"](props, tsched, SCHEDULE_KEY_KEYS,
                               practice_ids[0], start, prefix="sched")
            _put(ctx, props, tsched, SCHEDULE_PRACTICE_REL_KEYS, practice_ids[0])
            _put(ctx, props, tsched, SCHEDULE_TYPE_KEYS,         stype)
            _put(ctx, props, tsched, SCHEDULE_START_KEYS,        start)
            _put(ctx, props, tsched, SCHEDULE_END_KEYS,          end)
            _put(ctx, props, tsched, SCHEDULE_CONTENT_KEYS,      content)
            _put(ctx, props, tsched, SCHEDULE_ORDER_KEYS,        order)
            if track(_create(ctx, sched_db, props)): sched_count += 1
    summary["SCHEDULE"] = sched_count

    # ── 17. CONCERT_EXPENSE ──────────────────────────────────
    exp_db = ctx.get("CONCERT_DB_CONCERT_EXPENSE","")
    exp_count = 0
    if exp_db:
        texp = _p(ctx, exp_db)
        for type_, content, amount, confirmed in [
            ("会場費","いずみホール使用料",120000,True),
            ("楽器レンタル","ティンパニレンタル",35000,True),
            ("印刷物・プログラム","プログラム印刷",18000,False),
        ]:
            props = {}
            _put(ctx, props, texp, EXPENSE_KEY_KEYS,         f"{pfx} {type_}")
            _put(ctx, props, texp, EXPENSE_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, texp, EXPENSE_TYPE_KEYS,        type_)
            _put(ctx, props, texp, EXPENSE_CONTENT_KEYS,     content)
            _put(ctx, props, texp, EXPENSE_AMOUNT_KEYS,      amount)
            _put(ctx, props, texp, EXPENSE_CONFIRMED_KEYS,   confirmed)
            if track(_create(ctx, exp_db, props)): exp_count += 1
    summary["CONCERT_EXPENSE"] = exp_count

    # ── 18. HARMONIA_CONCERT（管理開始済み・進行中状態） ─────
    hc_db = ctx.get("CONCERT_DB_HARMONIA_CONCERT","")
    if hc_db:
        thc = _p(ctx, hc_db)
        import random, string
        invite_code = "".join(random.choices(string.ascii_uppercase+string.digits, k=8))
        props = {}
        ctx["put_key_any"](props, thc, HARMONIA_CONCERT_KEY_KEYS,
                           concert_id, pfx, prefix="harmonia")
        _put(ctx, props, thc, HARMONIA_CONCERT_CONCERT_REL_KEYS,    concert_id)
        _put(ctx, props, thc, HARMONIA_CONCERT_MANAGED_KEYS,        True)
        _put(ctx, props, thc, HARMONIA_CONCERT_SONG_INFO_KEYS,      True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PRACTICE_INFO_KEYS,  True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PRACTICE_DATE_KEYS,  True)
        _put(ctx, props, thc, HARMONIA_CONCERT_REQUIRED_INST_KEYS,  True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PARTDEF_KEYS,        True)
        _put(ctx, props, thc, HARMONIA_CONCERT_PLAYER_INFO_KEYS,    True)
        # 出欠・希望入力は途中（進行中状態）
        _put(ctx, props, thc, HARMONIA_CONCERT_ATTENDANCE_KEYS,     False)
        _put(ctx, props, thc, HARMONIA_CONCERT_PREFERENCE_KEYS,     False)
        _put(ctx, props, thc, HARMONIA_CONCERT_INVITE_CODE_KEYS,    invite_code)
        hc_id = track(_create(ctx, hc_db, props))
        summary["HARMONIA_CONCERT"] = 1 if hc_id else 0
        if hc_id:
            summary["招待コード"] = invite_code

    # 作成IDをsession_stateに保存
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

    # フォールバック：プレフィックス検索
    st.warning("session_stateが消えているため、プレフィックス検索で削除します。")
    test_concert_ids: set[str] = set()
    for r in ctx["query_all"](ctx.get("CONCERT_DB_CONCERT",""), None):
        n = ctx["extract_prop_text_any"](r, CONCERT_NAME_KEYS) or ""
        if n.startswith(pfx): test_concert_ids.add(r.get("id",""))

    if test_concert_ids:
        for db_key, rel_keys in [
            ("CONCERT_DB_PARTICIPANT",    ["出演","演奏会","FK演奏会"]),
            ("CONCERT_DB_PRACTICE",       ["演奏会","FK演奏会"]),
            ("CONCERT_DB_CONCERT_SONG",   ["演奏会","FK演奏会"]),
            ("CONCERT_DB_CONCERT_EXPENSE",["演奏会","FK演奏会"]),
            ("CONCERT_DB_HARMONIA_CONCERT",["演奏会","FK演奏会"]),
            ("CONCERT_DB_PLAYER_INSTRUMENT",["演奏会","FK演奏会"]),
        ]:
            db = ctx.get(db_key,"")
            if not db: continue
            cnt = 0
            for r in ctx["query_all"](db, None):
                if any(cid in ctx["extract_relation_ids_any"](r, rel_keys)
                       for cid in test_concert_ids):
                    if _archive(ctx, r.get("id","")): cnt += 1
            if cnt: summary[db_key.replace("CONCERT_DB_","")] = cnt

    # プレフィックスで直接検索できるDB
    for db_key, keys in [
        ("CONCERT_DB_CONCERT",    CONCERT_NAME_KEYS),
        ("CONCERT_DB_SONG",       SONG_NAME_KEYS),
        ("CONCERT_DB_PRACTICE",   PRACTICE_NAME_KEYS),
        ("CONCERT_DB_PLAYER",     PLAYER_NAME_KEYS),
        ("CONCERT_DB_INSTRUMENT", INSTRUMENT_NAME_KEYS),
        ("CONCERT_DB_MOVEMENT",   MOVEMENT_NAME_KEYS),
    ]:
        db = ctx.get(db_key,"")
        if not db: continue
        cnt = 0
        for r in ctx["query_all"](db, None):
            n = ctx["extract_prop_text_any"](r, keys) or ctx["extract_title"](r) or ""
            if pfx in n and _archive(ctx, r.get("id","")): cnt += 1
        if cnt: summary[db_key.replace("CONCERT_DB_","")] = cnt

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
                if st.button(f"🚀 投入", type="primary",
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
                if st.button(f"🗑️ 削除", type="secondary",
                             use_container_width=True,
                             key=f"delete_{pfx}",
                             disabled=not confirm):
                    with st.spinner("削除中..."):
                        result = _delete_all(ctx, pfx)
                    st.success("✅ 削除完了") if result else st.info("削除対象なし")
                    for k, v in result.items():
                        st.caption(f"  {k}: {v}")
