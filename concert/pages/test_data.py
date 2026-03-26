"""
concert/pages/test_data.py
テストデータの一括投入・一括削除機能
"""
import streamlit as st
from datetime import date, timedelta
from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_CONCERT_REL_KEYS, PRACTICE_DATE_KEYS,
    PRACTICE_SONG_REL_KEYS,
    SONG_NAME_KEYS, SONG_CONCERT_REL_KEYS,
    INSTRUMENT_NAME_KEYS,
    PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS, PARTDEF_NAME_KEYS,
    PARTICIPANT_RECORD_KEYS, PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_KEYS, PARTICIPANT_ROLE_KEYS, PARTICIPANT_FEE_KEYS,
    PLAYER_NAME_KEYS,
    ATT_RECORD_KEYS, ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS, ATT_STATUS_KEYS,
    PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_CONCERT_REL_KEYS, PI_OWN_COUNT_KEYS,
    PREF_PLAYER_REL_KEYS, PREF_PART_REL_KEYS, PREF_PRIORITY_KEYS,
    EXPENSE_KEY_KEYS, EXPENSE_CONCERT_REL_KEYS, EXPENSE_TYPE_KEYS,
    EXPENSE_CONTENT_KEYS, EXPENSE_AMOUNT_KEYS, EXPENSE_CONFIRMED_KEYS,
    PLAYER_HN_KEYS, PLAYER_PHONE_KEYS, PLAYER_LINE_KEYS,
    PARTICIPANT_ROLE_OPS_KEYS,
    PRACTICE_CONCERT_DAY_KEYS,
    PI_BRING_ASSIGN_KEYS, PI_BRING_COUNT_KEYS, PI_PRACTICE_REL_KEYS,
    RENTAL_RECORD_KEYS, RENTAL_PRACTICE_REL_KEYS, RENTAL_INST_REL_KEYS,
    RENTAL_ITEM_NAME_KEYS, RENTAL_VENDOR_KEYS, RENTAL_QTY_KEYS,
    RENTAL_UNIT_PRICE_KEYS, RENTAL_CONFIRMED_KEYS, RENTAL_NOTE_KEYS,
    SCHEDULE_KEY_KEYS, SCHEDULE_PRACTICE_REL_KEYS, SCHEDULE_START_KEYS,
    SCHEDULE_END_KEYS, SCHEDULE_TYPE_KEYS, SCHEDULE_CONTENT_KEYS,
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
        ("テスト奏者A", "A", "090-0001-0001", "test_line_a"),
        ("テスト奏者B", "B", "090-0001-0002", "test_line_b"),
        ("テスト奏者C", "C", "090-0001-0003", ""),
        ("テスト奏者D", "D", "", ""),
        ("テスト奏者E", "E", "", ""),
    ]
    for name, hn, phone, line_id in player_data:
        props = {}
        _put(ctx, props, tp, PLAYER_NAME_KEYS,  f"{TEST_PREFIX} {name}")
        _put(ctx, props, tp, PLAYER_HN_KEYS,    hn)
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
    for name in ["テスト曲α", "テスト曲β"]:
        props = {}
        _put(ctx, props, ts, SONG_NAME_KEYS,        f"{TEST_PREFIX} {name}")
        _put(ctx, props, ts, SONG_CONCERT_REL_KEYS, concert_id)
        sid = track(_create(ctx, song_db, props))
        if sid:
            song_ids.append(sid)
    summary["SONG"] = len(song_ids)

    # ── 5. PRACTICE ───────────────────────────────────────
    practice_db = ctx["CONCERT_DB_PRACTICE"]
    tpr = _p(ctx, practice_db)
    practice_ids = []
    base = date(2099, 10, 1)
    for i in range(3):
        props = {}
        _put(ctx, props, tpr, PRACTICE_NAME_KEYS,        f"{TEST_PREFIX} 第{i+1}回練習")
        _put(ctx, props, tpr, PRACTICE_CONCERT_REL_KEYS, concert_id)
        if song_ids:
            _put(ctx, props, tpr, PRACTICE_SONG_REL_KEYS, song_ids)
        dt_key2 = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
        if dt_key2:
            d = base + timedelta(weeks=i*2)
            props[dt_key2] = {"date": {"start": d.isoformat()}}
        pr_id = track(_create(ctx, practice_db, props))
        if pr_id:
            practice_ids.append(pr_id)
    summary["PRACTICE"] = len(practice_ids)

    # ── 6. PART_DEFINITION ────────────────────────────────
    partdef_db = ctx["CONCERT_DB_PART_DEFINITION"]
    tpd = _p(ctx, partdef_db)
    partdef_ids = []
    part_names = ["Part1 Timp.", "Part2 S.D.", "Part3 Mar."]
    for sid in song_ids:
        for pname, iid in zip(part_names, instrument_ids):
            props = {}
            _put(ctx, props, tpd, PARTDEF_NAME_KEYS,     f"{TEST_PREFIX} {pname}")
            _put(ctx, props, tpd, PARTDEF_SONG_REL_KEYS, sid)
            _put(ctx, props, tpd, PARTDEF_INST_REL_KEYS, iid)
            pd_id = track(_create(ctx, partdef_db, props))
            if pd_id:
                partdef_ids.append(pd_id)
    summary["PART_DEFINITION"] = len(partdef_ids)

    # ── 7. CONCERT_CAST ───────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    tcast = _p(ctx, cast_db)
    cast_ids = []
    parts = ["Perc", "Perc", "Vn1", "Vn2", "Va"]
    fees  = [5000, 5000, 5000, 5000, 0]
    for i, pid in enumerate(player_ids):
        props = {}
        ctx["put_key_any"](props, tcast, PARTICIPANT_RECORD_KEYS,
                           concert_id, pid, prefix="participant")
        _put(ctx, props, tcast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
        _put(ctx, props, tcast, PARTICIPANT_PLAYER_REL_KEYS,  pid)
        _put(ctx, props, tcast, PARTICIPANT_PART_KEYS,        parts[i])
        _put(ctx, props, tcast, PARTICIPANT_ROLE_KEYS,        "プレイヤー")
        _put(ctx, props, tcast, PARTICIPANT_ROLE_OPS_KEYS,    "" if i < 3 else "会計" if i == 3 else "広報")
        _put(ctx, props, tcast, PARTICIPANT_FEE_KEYS,         fees[i])
        cid = track(_create(ctx, cast_db, props))
        if cid:
            cast_ids.append(cid)
    summary["CONCERT_CAST"] = len(cast_ids)

    # ── 8. ATTENDANCE ─────────────────────────────────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    tatt = _p(ctx, att_db)
    att_count = 0
    statuses = ["○", "○", "△", "×", "○"]
    # 本番当日も○で追加
    all_pr_rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    concert_day_id = next(
        (p.get("id","") for p in all_pr_rows
         if concert_id in ctx["extract_relation_ids_any"](p, PRACTICE_CONCERT_REL_KEYS)
         and ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS) == "True"),
        None
    )
    all_prac_ids = practice_ids + ([concert_day_id] if concert_day_id else [])
    for pr_id in all_prac_ids:
        for i, (pid, cast_id) in enumerate(zip(player_ids, cast_ids)):
            status = "○" if pr_id == concert_day_id else statuses[i % len(statuses)]
            props = {}
            ctx["put_key_any"](props, tatt, ATT_RECORD_KEYS,
                               pr_id, cast_id, prefix="att")
            _put(ctx, props, tatt, ATT_PRACTICE_REL_KEYS, pr_id)
            _put(ctx, props, tatt, ATT_PLAYER_REL_KEYS,   cast_id)
            _put(ctx, props, tatt, ATT_STATUS_KEYS,        status)
            att_id = track(_create(ctx, att_db, props))
            if att_id:
                att_count += 1
    summary["ATTENDANCE"] = att_count

    # ── 9. PLAYER_INSTRUMENT ──────────────────────────────
    pi_db = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    tpi = _p(ctx, pi_db)
    pi_count = 0
    for pid in player_ids[:2]:
        for iid in instrument_ids:
            props = {}
            _put(ctx, props, tpi, ["record_key", "タイトル", "PK名称"],
                 f"{TEST_PREFIX} pi_{pid[:6]}_{iid[:6]}")
            _put(ctx, props, tpi, PI_CONCERT_REL_KEYS, concert_id)
            _put(ctx, props, tpi, PI_PLAYER_REL_KEYS,  pid)
            _put(ctx, props, tpi, PI_INST_REL_KEYS,    iid)
            _put(ctx, props, tpi, PI_OWN_COUNT_KEYS,   1)
            pi_id = track(_create(ctx, pi_db, props))
            if pi_id:
                pi_count += 1
    summary["PLAYER_INSTRUMENT"] = pi_count

    # ── 10. PREFERENCE ────────────────────────────────────
    pref_db = ctx["CONCERT_DB_PREFERENCE"]
    tpref = _p(ctx, pref_db)
    pref_count = 0
    priorities = ["第1希望", "第2希望", "希望なし/降り番でも可"]
    for i, pid in enumerate(player_ids[:2]):
        for j, pd_id in enumerate(partdef_ids[:3]):
            props = {}
            _put(ctx, props, tpref, ["record_key", "タイトル", "PK"],
                 f"{TEST_PREFIX} pref_{pid[:6]}_{pd_id[:6]}")
            _put(ctx, props, tpref, PREF_PLAYER_REL_KEYS, pid)
            _put(ctx, props, tpref, PREF_PART_REL_KEYS,   pd_id)
            _put(ctx, props, tpref, PREF_PRIORITY_KEYS,   priorities[j % len(priorities)])
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
    dt_key3 = ctx["find_prop_name"](tpr, PRACTICE_DATE_KEYS)
    if dt_key3:
        props[dt_key3] = {"date": {"start": "2099-12-31"}}
    concert_day_id = track(_create(ctx, practice_db, props))
    if concert_day_id:
        practice_ids.append(concert_day_id)
    summary["PRACTICE（本番日）"] = 1 if concert_day_id else 0

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
            ("搬入",   "09:00", "10:00", "楽器搬入"),
            ("練習",   "10:00", "12:00", "午前練習"),
            ("休憩",   "12:00", "13:00", "昼休憩"),
            ("練習",   "13:00", "17:00", "午後練習"),
            ("搬出",   "17:00", "18:00", "楽器搬出"),
        ]
        for stype, start, end, content in sched_items:
            props = {}
            _put(ctx, props, tsched, SCHEDULE_KEY_KEYS,         f"{TEST_PREFIX} sched_{start}")
            _put(ctx, props, tsched, SCHEDULE_PRACTICE_REL_KEYS, practice_ids[0])
            _put(ctx, props, tsched, SCHEDULE_TYPE_KEYS,         stype)
            _put(ctx, props, tsched, SCHEDULE_START_KEYS,        start)
            _put(ctx, props, tsched, SCHEDULE_END_KEYS,          end)
            _put(ctx, props, tsched, SCHEDULE_CONTENT_KEYS,      content)
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
| PERFORMER | 5名 |
| INSTRUMENT | 3種 |
| CONCERT | 1件（2099-12-31） |
| SONG | 2曲 |
| PRACTICE | 3回 |
| PART_DEFINITION | 6件 |
| CONCERT_CAST | 5件 |
| ATTENDANCE | 15件 |
| PLAYER_INSTRUMENT | 6件（所有）+ 2件（持参担当） |
| PREFERENCE | 6件 |
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
