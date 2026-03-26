"""
concert/pages/test_data.py
テストデータの一括投入・一括削除機能
"""
import streamlit as st
from datetime import date, timedelta
from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS, CONCERT_VENUE_KEYS, CONCERT_ADDRESS_KEYS,
    CONCERT_CONDUCTOR_KEYS, CONCERT_SOLOIST_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_CONCERT_REL_KEYS, PRACTICE_DATE_KEYS,
    PRACTICE_VENUE_KEYS, PRACTICE_ADDRESS_KEYS,
    PRACTICE_SONG_REL_KEYS,
    SONG_NAME_KEYS, SONG_CONCERT_REL_KEYS, SONG_COMPOSER_KEYS,
    INSTRUMENT_NAME_KEYS,
    PARTDEF_KEY_KEYS, PARTDEF_RECORD_KEYS, PARTDEF_CONCERT_REL_KEYS,
    PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS, PARTDEF_NAME_KEYS,
    PARTICIPANT_RECORD_KEYS, PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_KEYS, PARTICIPANT_ROLE_KEYS, PARTICIPANT_FEE_KEYS,
    PLAYER_NAME_KEYS,
    ATTENDANCE_KEY_KEYS, ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS, ATT_STATUS_KEYS,
    PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_CONCERT_REL_KEYS, PI_OWN_COUNT_KEYS,
    PREFERENCE_KEY_KEYS, PREF_PLAYER_REL_KEYS, PREF_PART_REL_KEYS, PREF_PRIORITY_KEYS,
    EXPENSE_KEY_KEYS, EXPENSE_CONCERT_REL_KEYS, EXPENSE_TYPE_KEYS,
    EXPENSE_CONTENT_KEYS, EXPENSE_AMOUNT_KEYS, EXPENSE_CONFIRMED_KEYS,
    PLAYER_HN_KEYS, PLAYER_EMAIL_KEYS, PLAYER_PHONE_KEYS, PLAYER_LINE_KEYS,
    PARTICIPANT_ROLE_OPS_KEYS,
    PRACTICE_CONCERT_DAY_KEYS,
    PI_BRING_ASSIGN_KEYS, PI_BRING_COUNT_KEYS, PI_PRACTICE_REL_KEYS,
    RENTAL_RECORD_KEYS, RENTAL_PRACTICE_REL_KEYS, RENTAL_INST_REL_KEYS,
    RENTAL_ITEM_NAME_KEYS, RENTAL_VENDOR_KEYS, RENTAL_QTY_KEYS,
    RENTAL_UNIT_PRICE_KEYS, RENTAL_CONFIRMED_KEYS, RENTAL_NOTE_KEYS,
    SCHEDULE_KEY_KEYS, SCHEDULE_PRACTICE_REL_KEYS, SCHEDULE_START_KEYS,
    SCHEDULE_END_KEYS, SCHEDULE_TYPE_KEYS, SCHEDULE_CONTENT_KEYS,
    SCHEDULE_ORDER_KEYS,
)

TEST_PREFIX = "[TEST]"


def _clear_cache():
    """HARMONIAの全セッションキャッシュをクリアする。"""
    cache_prefixes = (
        "practice_list_", "concert_list", "song_list_", "partdef_list_",
        "pi_list_", "attendance_list_", "participant_list_", "instrument_list",
        "schedule_list_", "expense_list_", "cast_list_", "pi_master_",
        "si_list_", "pi_practice_",
    )
    for k in list(st.session_state.keys()):
        if any(k.startswith(p) for p in cache_prefixes):
            st.session_state.pop(k, None)
    st.cache_data.clear()


def _archive(ctx, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"archived": True})
    return res is not None and res.status_code == 200


def _create(ctx, db_id: str, props: dict) -> str:
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    if res and res.status_code == 200:
        return res.json().get("id", "")
    return ""


def _p(ctx, db_id):
    return ctx["get_prop_types"](db_id) or {}


def _put(ctx, props, t, keys, value):
    ctx["put_prop_any"](props, t, keys, value)


# ============================================================
# 投入
# ============================================================

def _seed_all(ctx) -> dict:
    summary = {}
    created_ids: list[str] = []

    def track(page_id: str) -> str:
        if page_id:
            created_ids.append(page_id)
        return page_id

    # ── 1. PERFORMER ──────────────────────────────────────
    player_db = ctx["CONCERT_DB_PLAYER"]
    tp = _p(ctx, player_db)
    player_ids = []
    player_data = [
        # Percパート奏者（5名）
        ("テスト奏者A", "Perc-A", "090-0001-0001", "test_line_a"),
        ("テスト奏者B", "Perc-B", "090-0001-0002", "test_line_b"),
        ("テスト奏者C", "Perc-C", "090-0001-0003", "test_line_c"),
        ("テスト奏者D", "Perc-D", "090-0001-0004", ""),
        ("テスト奏者E", "Perc-E", "",              ""),
        # 他パート奏者（3名）
        ("テスト奏者F", "Vn1-F", "090-0001-0006", ""),
        ("テスト奏者G", "Vn2-G", "",              ""),
        ("テスト奏者H", "Va-H",  "",              ""),
    ]
    for name, hn, phone, line_id in player_data:
        props = {}
        _put(ctx, props, tp, PLAYER_NAME_KEYS,  f"{TEST_PREFIX} {name}")
        _put(ctx, props, tp, PLAYER_HN_KEYS,    hn)
        _put(ctx, props, tp, PLAYER_EMAIL_KEYS, f"test_{name.lower().replace(' ','')}@example.com")
        _put(ctx, props, tp, PLAYER_PHONE_KEYS, phone)
        _put(ctx, props, tp, PLAYER_LINE_KEYS,  line_id)
        pid = track(_create(ctx, player_db, props))
        if pid:
            player_ids.append(pid)
    summary["PERFORMER"] = len(player_ids)

    # ── 2. INSTRUMENT ─────────────────────────────────────
    inst_db = ctx["CONCERT_DB_INSTRUMENT"]
    ti = _p(ctx, inst_db)
    instrument_ids = []
    for name in ["Timpani", "Snare Drum", "Marimba"]:
        props = {}
        _put(ctx, props, ti, INSTRUMENT_NAME_KEYS, f"{TEST_PREFIX} {name}")
        iid = track(_create(ctx, inst_db, props))
        if iid:
            instrument_ids.append(iid)
    summary["INSTRUMENT"] = len(instrument_ids)

    # ── 3. CONCERT ────────────────────────────────────────
    concert_db = ctx["CONCERT_DB_CONCERT"]
    tc = _p(ctx, concert_db)
    props = {}
    _put(ctx, props, tc, CONCERT_NAME_KEYS, f"{TEST_PREFIX} テスト演奏会")
    media_key = ctx["find_prop_name"](tc, ["媒体", "Media"])
    if media_key:
        mtype = tc.get(media_key, "")
        if mtype == "multi_select":
            props[media_key] = {"multi_select": [{"name": "出演"}]}
        elif mtype == "select":
            props[media_key] = {"select": {"name": "出演"}}
    dt_key = ctx["find_prop_name"](tc, CONCERT_DATE_KEYS)
    if dt_key:
        props[dt_key] = {"date": {"start": "2099-12-31"}}
    concert_id = track(_create(ctx, concert_db, props))
    summary["CONCERT"] = 1 if concert_id else 0
    if not concert_id:
        st.session_state["test_created_ids"] = created_ids
        return summary

    # ── 4. SONG ───────────────────────────────────────────
    song_db = ctx["CONCERT_DB_SONG"]
    ts = _p(ctx, song_db)
    song_ids = []
    song_composers = {"テスト曲α": "テスト太郎（作曲）", "テスト曲β": "テスト次郎（作曲）"}
    for name in ["テスト曲α", "テスト曲β"]:
        props = {}
        _put(ctx, props, ts, SONG_NAME_KEYS,        f"{TEST_PREFIX} {name}")
        _put(ctx, props, ts, SONG_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, ts, SONG_COMPOSER_KEYS,    song_composers[name])
        sid = track(_create(ctx, song_db, props))
        if sid:
            song_ids.append(sid)
    summary["SONG"] = len(song_ids)

    # ── 5. PRACTICE ───────────────────────────────────────
    practice_db = ctx["CONCERT_DB_PRACTICE"]
    tpr = _p(ctx, practice_db)
    practice_ids = []
    venues   = [
        "ザ・シンフォニーホール",
        "豊中市立文化芸術センター",
        "吹田市文化会館 メイシアター",
    ]
    addresses = [
        "大阪府大阪市北区大淀南2丁目3-3",
        "大阪府豊中市曽根東町3丁目7-2",
        "大阪府吹田市泉町2丁目29-1",
    ]
    times = ["T10:00:00+09:00", "T13:00:00+09:00", "T09:30:00+09:00"]
    base = date(2099, 10, 1)
    for i in range(3):
        props = {}
        _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{TEST_PREFIX} 第{i+1}回練習")
        _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tpr, PRACTICE_VENUE_KEYS,       venues[i])
        _put(ctx, props, tpr, PRACTICE_ADDRESS_KEYS,     addresses[i])
        if song_ids:
            _put(ctx, props, tpr, PRACTICE_SONG_REL_KEYS, song_ids)
        dt_key2 = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
        if dt_key2:
            d = base + timedelta(weeks=i*2)
            props[dt_key2] = {"date": {"start": d.isoformat() + times[i]}}
        pr_id = track(_create(ctx, practice_db, props))
        if pr_id:
            practice_ids.append(pr_id)
    summary["PRACTICE"] = len(practice_ids)

    # ── 6. PART_DEFINITION ────────────────────────────────
    partdef_db = ctx["CONCERT_DB_PART_DEFINITION"]
    tpd = _p(ctx, partdef_db)
    partdef_ids = []
    part_names = ["Part1 Timp.", "Part2 S.D.", "Part3 Mar."]
    # 曲名マップ
    song_name_map = {s.get("id",""): ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or ""
                     for s in ctx["query_all"](ctx["CONCERT_DB_SONG"], None)}
    inst_name_map_pd = {i.get("id",""): ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ""
                        for i in ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)}
    part_no = 0
    for sid in song_ids:
        sname = song_name_map.get(sid, sid[:8])
        for pname, iid in zip(part_names, instrument_ids):
            iname = inst_name_map_pd.get(iid, iid[:8])
            props = {}
            # songs.pyと同じ形式でタイトル設定
            _put(ctx, props, tpd, PARTDEF_RECORD_KEYS,      f"{TEST_PREFIX} {sname} / {pname} / {iname}")
            _put(ctx, props, tpd, PARTDEF_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, tpd, PARTDEF_SONG_REL_KEYS,    sid)
            _put(ctx, props, tpd, PARTDEF_INST_REL_KEYS,    iid)
            _put(ctx, props, tpd, PARTDEF_NAME_KEYS,        f"{TEST_PREFIX} {pname}")
            ctx["put_key_any"](props, tpd, PARTDEF_KEY_KEYS,
                               concert_id, sid, part_no, pname, iid, prefix="part")
            part_no += 1
            pd_id = track(_create(ctx, partdef_db, props))
            if pd_id:
                partdef_ids.append(pd_id)
    summary["PART_DEFINITION"] = len(partdef_ids)

    # ── 7. CONCERT_CAST ───────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    tcast = _p(ctx, cast_db)
    cast_ids = []
    parts = ["Perc", "Perc", "Perc", "Perc", "Perc", "Vn1", "Vn2", "Va"]
    fees  = [5000, 5000, 5000, 5000, 5000, 5000, 5000, 0]
    roles_ops = ["", "", "", "会計", "広報", "", "", ""]
    for i, pid in enumerate(player_ids):
        props = {}
        ctx["put_key_any"](props, tcast, PARTICIPANT_RECORD_KEYS,
                           concert_id, pid, prefix="participant")
        _put(ctx, props, tcast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tcast, PARTICIPANT_PLAYER_REL_KEYS,  pid)
        _put(ctx, props, tcast, PARTICIPANT_PART_KEYS,        parts[i])
        _put(ctx, props, tcast, PARTICIPANT_ROLE_KEYS,        "プレイヤー")
        _put(ctx, props, tcast, PARTICIPANT_ROLE_OPS_KEYS,    roles_ops[i] if i < len(roles_ops) else "")
        _put(ctx, props, tcast, PARTICIPANT_FEE_KEYS,         fees[i])
        cid = track(_create(ctx, cast_db, props))
        if cid:
            cast_ids.append(cid)
    summary["CONCERT_CAST"] = len(cast_ids)

    # ── 8. ATTENDANCE → ステップ12（本番当日作成）後に実施 ───

    # ── 9. PLAYER_INSTRUMENT ──────────────────────────────
    pi_db = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    tpi = _p(ctx, pi_db)
    pi_count = 0
    for pid in player_ids[:5]:  # Perc奏者5名分
        for iid in instrument_ids:
            props = {}
            ctx["put_key_any"](props, tpi, ["record_key", "タイトル", "PK名称"],
                               pid, iid, prefix="assign")
            _put(ctx, props, tpi, PI_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, tpi, PI_PLAYER_REL_KEYS,  pid)
            _put(ctx, props, tpi, PI_INST_REL_KEYS,    iid)
            _put(ctx, props, tpi, PI_OWN_COUNT_KEYS,   1)
            pi_id = track(_create(ctx, pi_db, props))
            if pi_id:
                pi_count += 1
    summary["PLAYER_INSTRUMENT"] = pi_count

    # ── 10. PREFERENCE ────────────────────────────────────
    # Percパート奏者（cast_ids[:2]=奏者A・B）の全パート定義に希望を登録
    # 希望分布：各奏者が異なる第1希望を持つようにデザイン
    #
    # 奏者A: 曲α Part1=第1希望, Part2=希望なし, Part3=第2希望
    #        曲β Part1=第2希望, Part2=希望なし, Part3=第1希望
    # 奏者B: 曲α Part1=希望なし, Part2=第1希望, Part3=第2希望
    #        曲β Part1=第1希望, Part2=第2希望, Part3=希望なし
    # partdef_ids = [α_P1, α_P2, α_P3, β_P1, β_P2, β_P3]（2曲×3パート）
    pref_db = ctx["CONCERT_DB_PREFERENCE"]
    tpref = _p(ctx, pref_db)
    pref_count = 0
    # 全5奏者×全6パート定義（αP1,αP2,αP3,βP1,βP2,βP3）
    # 競合を意図的に作り、アルゴリズムの動きが分かるよう設計
    NA = "希望なし/降り番でも可"
    pref_matrix = {
        0: ["第1希望", NA,       "第2希望", "第3希望", NA,       "第1希望"],  # 奏者A
        1: [NA,       "第1希望", "第2希望", "第1希望", "第2希望", NA      ],  # 奏者B
        2: ["第2希望", "第1希望", NA,       NA,       "第1希望", "第2希望"],  # 奏者C
        3: ["第1希望", "第2希望", "第1希望", NA,       "第3希望", NA      ],  # 奏者D
        4: ["第3希望", NA,       "第1希望", "第2希望", NA,       "第1希望"],  # 奏者E
    }
    for i, (pid, cast_id) in enumerate(zip(player_ids[:5], cast_ids[:5])):  # Perc奏者5名のみ
        for j, pd_id in enumerate(partdef_ids[:6]):
            priority = pref_matrix[i][j] if j < len(pref_matrix[i]) else NA
            props = {}
            ctx["put_key_any"](props, tpref, PREFERENCE_KEY_KEYS,
                               cast_id, pd_id, prefix="pref")
            _put(ctx, props, tpref, PREF_PLAYER_REL_KEYS, cast_id)
            _put(ctx, props, tpref, PREF_PART_REL_KEYS,   pd_id)
            _put(ctx, props, tpref, PREF_PRIORITY_KEYS,   priority)
            pref_id = track(_create(ctx, pref_db, props))
            if pref_id:
                pref_count += 1
    summary["PREFERENCE"] = pref_count

    # ── 11. CONCERT_EXPENSE ───────────────────────────────
    exp_db = ctx.get("CONCERT_DB_CONCERT_EXPENSE", "")
    exp_count = 0
    if exp_db:
        texp = _p(ctx, exp_db)
        items = [("会場費", "テスト会場", 30000, True),
                 ("楽器レンタル", "テストレンタル", 15000, False),
                 ("印刷物・プログラム", "テストプログラム", 8000, True)]
        for type_, content, amount, confirmed in items:
            props = {}
            _put(ctx, props, texp, EXPENSE_KEY_KEYS,         f"{TEST_PREFIX} {type_}/{content}")
            _put(ctx, props, texp, EXPENSE_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, texp, EXPENSE_TYPE_KEYS,        type_)
            _put(ctx, props, texp, EXPENSE_CONTENT_KEYS,     content)
            _put(ctx, props, texp, EXPENSE_AMOUNT_KEYS,      amount)
            _put(ctx, props, texp, EXPENSE_CONFIRMED_KEYS,   confirmed)
            eid = track(_create(ctx, exp_db, props))
            if eid:
                exp_count += 1
    summary["CONCERT_EXPENSE"] = exp_count

    # ── 12. 本番日練習（PRACTICE追加）────────────────────────
    props = {}
    _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{TEST_PREFIX} 本番当日")
    _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
    _put(ctx, props, tpr, PRACTICE_CONCERT_DAY_KEYS, True)
    _put(ctx, props, tpr, PRACTICE_VENUE_KEYS,       "いずみホール")
    _put(ctx, props, tpr, PRACTICE_ADDRESS_KEYS,     "大阪府大阪市中央区城見1丁目4-70")
    dt_key3 = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
    if dt_key3:
        props[dt_key3] = {"date": {"start": "2099-12-31T10:00:00+09:00"}}
    concert_day_id = track(_create(ctx, practice_db, props))
    if concert_day_id:
        practice_ids.append(concert_day_id)
    summary["PRACTICE（本番日）"] = 1 if concert_day_id else 0

    # ── 8→12移動: ATTENDANCE（本番当日含む全練習日） ──────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    tatt = _p(ctx, att_db)
    att_count = 0
    statuses = ["○", "○", "△", "×", "○", "○", "△", "○"]
    # この時点でpractice_idsに本番当日IDが追加済み
    # concert_day_idも本番当日のIDとして使える
    practice_rel_key = ctx["find_prop_name"](tatt, ATT_PRACTICE_REL_KEYS)
    if not practice_rel_key:
        for k, t in (tatt or {}).items():
            if t == "relation" and any(kw in str(k).lower() for kw in ["練習", "practice"]):
                practice_rel_key = k; break
    player_rel_key = ctx["find_prop_name"](tatt, ATT_PLAYER_REL_KEYS)
    if not player_rel_key:
        for k, t in (tatt or {}).items():
            if t == "relation" and k != practice_rel_key and any(
                kw in str(k).lower() for kw in ["奏者", "participant", "player", "出演"]):
                player_rel_key = k; break
    status_key = ctx["find_prop_name"](tatt, ATT_STATUS_KEYS)

    for pr_id in practice_ids:  # 本番当日を含む全練習日
        for i, (pid, cast_id) in enumerate(zip(player_ids, cast_ids)):
            status = "○" if pr_id == concert_day_id else statuses[i % len(statuses)]
            props = {}
            ctx["put_key_any"](props, tatt, ATTENDANCE_KEY_KEYS,
                               cast_id, pr_id, prefix="att")
            if practice_rel_key:
                ctx["put_prop"](props, tatt, practice_rel_key, pr_id)
            if player_rel_key:
                ctx["put_prop"](props, tatt, player_rel_key, cast_id)
            if status_key:
                ctx["put_prop"](props, tatt, status_key, status)
            att_id = track(_create(ctx, att_db, props))
            if att_id:
                att_count += 1
    summary["ATTENDANCE"] = att_count

    # ── 13. 持参担当（PLAYER_INSTRUMENT追加）─────────────────
    bring_count = 0
    if practice_ids and player_ids and instrument_ids:
        for pr_id in practice_ids[:2]:  # 最初の2練習日
            pid = player_ids[0]
            iid = instrument_ids[0]
            props = {}
            _put(ctx, props, tpi, ["record_key", "タイトル", "PK名称"],
                 f"{TEST_PREFIX} bring_{pr_id[:6]}_{pid[:6]}")
            _put(ctx, props, tpi, PI_CONCERT_REL_KEYS,   concert_id)
            _put(ctx, props, tpi, PI_PLAYER_REL_KEYS,    pid)
            _put(ctx, props, tpi, PI_INST_REL_KEYS,      iid)
            _put(ctx, props, tpi, PI_BRING_ASSIGN_KEYS,  True)
            _put(ctx, props, tpi, PI_BRING_COUNT_KEYS,   1)
            _put(ctx, props, tpi, PI_PRACTICE_REL_KEYS,  pr_id)
            bid = track(_create(ctx, pi_db, props))
            if bid:
                bring_count += 1
    summary["持参担当"] = bring_count

    # ── 14. RENTAL（レンタル見積）─────────────────────────────
    rental_db = ctx.get("CONCERT_DB_RENTAL", "")
    rental_count = 0
    if rental_db and practice_ids and instrument_ids:
        trent = _p(ctx, rental_db)
        rental_items = [
            ("楽器レンタル", "テスト楽器店", instrument_ids[0], "Timpani 23inch", 1, 15000, True),
            ("楽器レンタル", "テスト楽器店", instrument_ids[1], "Snare Drum", 2, 5000, False),
        ]
        for cost_type, vendor, iid, item_name, qty, unit_price, confirmed in rental_items:
            props = {}
            _put(ctx, props, trent, RENTAL_RECORD_KEYS,       f"{TEST_PREFIX} rental_{iid[:6]}")
            _put(ctx, props, trent, RENTAL_PRACTICE_REL_KEYS, practice_ids[0])
            _put(ctx, props, trent, RENTAL_INST_REL_KEYS,     iid)
            _put(ctx, props, trent, RENTAL_ITEM_NAME_KEYS,    item_name)
            _put(ctx, props, trent, RENTAL_VENDOR_KEYS,       vendor)
            _put(ctx, props, trent, RENTAL_QTY_KEYS,          qty)
            _put(ctx, props, trent, RENTAL_UNIT_PRICE_KEYS,   unit_price)
            _put(ctx, props, trent, RENTAL_CONFIRMED_KEYS,    confirmed)
            rid2 = track(_create(ctx, rental_db, props))
            if rid2:
                rental_count += 1
    summary["RENTAL"] = rental_count

    # ── 15. SCHEDULE（タイムスケジュール）────────────────────
    sched_db = ctx.get("CONCERT_DB_SCHEDULE", "")
    sched_count = 0
    if sched_db and practice_ids:
        tsched = _p(ctx, sched_db)
        sched_items = [
            (1, "搬入", "09:00", "10:00", "楽器搬入"),
            (2, "練習", "10:00", "12:00", "午前練習"),
            (3, "休憩", "12:00", "13:00", "昼休憩"),
            (4, "練習", "13:00", "17:00", "午後練習"),
            (5, "搬出", "17:00", "18:00", "楽器搬出"),
        ]
        for order, stype, start, end, content in sched_items:
            props = {}
            _put(ctx, props, tsched, SCHEDULE_KEY_KEYS,          f"{TEST_PREFIX} sched_{start}")
            _put(ctx, props, tsched, SCHEDULE_PRACTICE_REL_KEYS, practice_ids[0])
            _put(ctx, props, tsched, SCHEDULE_TYPE_KEYS,         stype)
            _put(ctx, props, tsched, SCHEDULE_START_KEYS,        start)
            _put(ctx, props, tsched, SCHEDULE_END_KEYS,          end)
            _put(ctx, props, tsched, SCHEDULE_CONTENT_KEYS,      content)
            _put(ctx, props, tsched, SCHEDULE_ORDER_KEYS,        order)
            sid2 = track(_create(ctx, sched_db, props))
            if sid2:
                sched_count += 1
    summary["SCHEDULE"] = sched_count

    # 作成IDをsession_stateに保存（削除時に使用）
    existing = st.session_state.get("test_created_ids", [])
    st.session_state["test_created_ids"] = existing + created_ids
    summary["作成総件数"] = len(created_ids)

    # 全キャッシュをクリアして即時反映
    _clear_cache()

    return summary


# ============================================================
# 削除
# ============================================================

def _delete_all_test_data(ctx) -> dict:
    summary = {}

    # 方法1: session_stateに記録されたIDを直接アーカイブ
    created_ids = st.session_state.get("test_created_ids", [])
    if created_ids:
        count = sum(1 for pid in created_ids if _archive(ctx, pid))
        st.session_state.pop("test_created_ids", None)
        summary["削除件数（ID直接指定）"] = count
        _clear_cache()
        return summary

    # 方法2: フォールバック（session_stateが消えた場合）[TEST]プレフィックスで全DB検索
    st.warning("session_stateが消えているため、プレフィックス検索で削除します。時間がかかる場合があります。")
    db_map = [
        ("CONCERT_DB_ATTENDANCE",        ["record_key", "タイトル", "PK"]),
        ("CONCERT_DB_PREFERENCE",        ["record_key", "タイトル", "PK"]),
        ("CONCERT_DB_PLAYER_INSTRUMENT", ["record_key", "タイトル", "PK名称"]),
        ("CONCERT_DB_CONCERT_EXPENSE",   EXPENSE_KEY_KEYS),
        ("CONCERT_DB_PARTICIPANT",       PARTICIPANT_RECORD_KEYS),
        ("CONCERT_DB_PART_DEFINITION",   PARTDEF_NAME_KEYS),
        ("CONCERT_DB_PRACTICE",          PRACTICE_NAME_KEYS),
        ("CONCERT_DB_SONG",              SONG_NAME_KEYS),
        ("CONCERT_DB_CONCERT",           CONCERT_NAME_KEYS),
        ("CONCERT_DB_INSTRUMENT",        INSTRUMENT_NAME_KEYS),
        ("CONCERT_DB_PLAYER",            PLAYER_NAME_KEYS),
    ]
    for db_key, title_keys in db_map:
        db_id = ctx.get(db_key, "")
        if not db_id:
            continue
        rows = ctx["query_all"](db_id, None)
        count = 0
        for r in rows:
            name = (ctx["extract_prop_text_any"](r, title_keys) or
                    ctx["extract_title"](r) or "")
            if name.startswith(TEST_PREFIX):
                if _archive(ctx, r.get("id", "")):
                    count += 1
        if count > 0:
            summary[db_key.replace("CONCERT_DB_", "")] = count
    return summary


# ============================================================
# メイン
# ============================================================

def render(ctx: dict):
    st.header("🧪 テストデータ管理")
    st.warning("⚠️ この画面はテスト・開発用です。本番運用時は使用しないでください。")

    created_ids = st.session_state.get("test_created_ids", [])
    if created_ids:
        st.info(f"投入済みテストデータ: {len(created_ids)}件（削除可能）")

    st.markdown(f"""
**投入されるデータ（全レコードに `{TEST_PREFIX}` プレフィックス付与）：**

| DB | 件数 |
|---|---|
| PERFORMER | 8名（Perc×5・Vn1/Vn2/Va×各1） |
| INSTRUMENT | 3種 |
| CONCERT | 1件（2099-12-31） |
| SONG | 2曲 |
| PRACTICE | 3回 |
| PART_DEFINITION | 6件（2曲×3パート） |
| CONCERT_CAST | 8件 |
| ATTENDANCE | 32件（4回×8名） |
| PLAYER_INSTRUMENT | 15件（所有：Perc5名×3楽器）+ 2件（持参担当） |
| PREFERENCE | 30件（Perc5名×6パート定義） |
| CONCERT_EXPENSE | 3件 |
| RENTAL | 2件 |
| SCHEDULE | 5件（第1回練習のタイムスケジュール） |
| PRACTICE（本番日） | 1件 |
""")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📥 テストデータ投入")
        if st.button("🚀 テストデータを一括投入", type="primary",
                     use_container_width=True, key="seed_btn"):
            with st.spinner("投入中... しばらくお待ちください"):
                summary = _seed_all(ctx)
            st.success("✅ 投入完了")
            for k, v in summary.items():
                st.caption(f"  {k}: {v}件")

    with col2:
        st.subheader("🗑️ テストデータ削除")
        if created_ids:
            st.caption(f"投入済み{len(created_ids)}件を削除します。")
        else:
            st.caption(f"`{TEST_PREFIX}` プレフィックスで全DB検索して削除します。")
        confirm = st.checkbox("削除対象を確認しました", key="delete_confirm")
        if st.button("🗑️ テストデータを一括削除", type="secondary",
                     use_container_width=True, key="delete_btn",
                     disabled=not confirm):
            with st.spinner("削除中..."):
                summary = _delete_all_test_data(ctx)
            if summary:
                st.success("✅ 削除完了")
                for k, v in summary.items():
                    st.caption(f"  {k}: {v}件")
            else:
                st.info("削除対象のテストデータが見つかりませんでした。")
