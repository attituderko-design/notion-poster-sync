"""
concert/pages/test_data.py
テストデータの一括投入・一括削除機能
"""
import streamlit as st
from datetime import date, timedelta

TEST_PREFIX = "[TEST]"

# テスト対象の全DB定義（db_key, title_keys）
TEST_DB_MAP = [
    ("CONCERT_DB_CONCERT",          ["名称", "演奏会名", "タイトル", "PK名称"]),
    ("CONCERT_DB_PRACTICE",         ["練習名", "名称", "タイトル"]),
    ("CONCERT_DB_SONG",             ["曲名", "タイトル", "Song"]),
    ("CONCERT_DB_INSTRUMENT",       ["楽器名", "名称", "タイトル"]),
    ("CONCERT_DB_PART_DEFINITION",  ["パート名", "名称", "タイトル"]),
    ("CONCERT_DB_PARTICIPANT",      ["record_key", "タイトル", "PK"]),
    ("CONCERT_DB_ATTENDANCE",       ["record_key", "タイトル", "PK"]),
    ("CONCERT_DB_PLAYER_INSTRUMENT",["record_key", "タイトル", "PK名称"]),
    ("CONCERT_DB_PREFERENCE",       ["record_key", "タイトル", "PK"]),
    ("CONCERT_DB_CONCERT_EXPENSE",  ["expense_key", "タイトル"]),
    ("CONCERT_DB_PLAYER",           ["氏名", "名前", "タイトル"]),
]


def _archive_page(ctx, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"archived": True})
    return res is not None and res.status_code == 200


def _create(ctx, db_id: str, props: dict) -> str:
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    if res and res.status_code == 200:
        return res.json().get("id", "")
    return ""


def _put(ctx, props, db_id, keys, value):
    t = ctx["get_prop_types"](db_id)
    if t:
        ctx["put_prop_any"](props, t, keys, value)


# ============================================================
# 投入関数
# ============================================================

def _seed_all(ctx) -> dict:
    """テストデータを全DB に一括投入する。作成したIDのサマリを返す。"""
    summary = {}
    ext = ctx["extract_prop_text_any"]

    def t(db_key): return ctx["get_prop_types"](ctx.get(db_key, "")) or {}

    # ── 1. PERFORMER（奏者） ───────────────────────────────
    player_db = ctx["CONCERT_DB_PLAYER"]
    player_ids = []
    test_players = [
        ("テスト奏者A", "test_a@example.com"),
        ("テスト奏者B", "test_b@example.com"),
        ("テスト奏者C", "test_c@example.com"),
        ("テスト奏者D", "test_d@example.com"),
        ("テスト奏者E", "test_e@example.com"),
    ]
    for pname, email in test_players:
        props = {}
        tp = t("CONCERT_DB_PLAYER")
        ctx["put_prop_any"](props, tp, ["氏名", "名前", "タイトル"], f"{TEST_PREFIX} {pname}")
        ctx["put_prop_any"](props, tp, ["メールアドレス", "Email"], email)
        pid = _create(ctx, player_db, props)
        if pid:
            player_ids.append(pid)
    summary["PERFORMER"] = len(player_ids)

    # ── 2. INSTRUMENT（楽器） ─────────────────────────────
    inst_db = ctx["CONCERT_DB_INSTRUMENT"]
    instrument_ids = []
    test_instruments = ["Timpani", "Snare Drum", "Marimba"]
    for iname in test_instruments:
        props = {}
        ti = t("CONCERT_DB_INSTRUMENT")
        ctx["put_prop_any"](props, ti, ["楽器名", "名称", "タイトル"], f"{TEST_PREFIX} {iname}")
        iid = _create(ctx, inst_db, props)
        if iid:
            instrument_ids.append(iid)
    summary["INSTRUMENT"] = len(instrument_ids)

    # ── 3. CONCERT（演奏会） ──────────────────────────────
    concert_db = ctx["CONCERT_DB_CONCERT"]
    props = {}
    tc = t("CONCERT_DB_CONCERT")
    ctx["put_prop_any"](props, tc, ["名称", "演奏会名", "タイトル", "PK名称"], f"{TEST_PREFIX} テスト演奏会")
    # 媒体フィールドはselect/multi_select両対応
    media_key = ctx["find_prop_name"](tc, ["媒体", "Media"])
    if media_key:
        media_type = tc.get(media_key, "")
        if media_type == "multi_select":
            props[media_key] = {"multi_select": [{"name": "出演"}]}
        elif media_type == "select":
            props[media_key] = {"select": {"name": "出演"}}
    dt_key = ctx["find_prop_name"](tc, ["日時", "日付", "出演日"])
    if dt_key:
        props[dt_key] = {"date": {"start": "2099-12-31"}}
    concert_id = _create(ctx, concert_db, props)
    summary["CONCERT"] = 1 if concert_id else 0
    if not concert_id:
        return summary

    # ── 4. SONG（楽曲） ───────────────────────────────────
    song_db = ctx["CONCERT_DB_SONG"]
    song_ids = []
    for sname in ["テスト曲α", "テスト曲β"]:
        props = {}
        ts = t("CONCERT_DB_SONG")
        ctx["put_prop_any"](props, ts, ["曲名", "タイトル", "Song"], f"{TEST_PREFIX} {sname}")
        ctx["put_prop_any"](props, ts, ["演奏会", "FK演奏会"], concert_id)
        sid = _create(ctx, song_db, props)
        if sid:
            song_ids.append(sid)
    summary["SONG"] = len(song_ids)

    # ── 5. PRACTICE（練習日） ─────────────────────────────
    practice_db = ctx["CONCERT_DB_PRACTICE"]
    practice_ids = []
    base = date(2099, 10, 1)
    for i in range(3):
        props = {}
        tp2 = t("CONCERT_DB_PRACTICE")
        ctx["put_prop_any"](props, tp2, ["練習名", "名称", "タイトル"], f"{TEST_PREFIX} 第{i+1}回練習")
        ctx["put_prop_any"](props, tp2, ["演奏会", "FK演奏会", "Concert"], concert_id)
        if song_ids:
            ctx["put_prop_any"](props, tp2, ["演奏曲", "曲", "Songs"], song_ids)
        dt_key2 = ctx["find_prop_name"](tp2, ["日時", "日付", "Date"])
        if dt_key2:
            d = base + timedelta(weeks=i*2)
            props[dt_key2] = {"date": {"start": d.isoformat()}}
        pr_id = _create(ctx, practice_db, props)
        if pr_id:
            practice_ids.append(pr_id)
    summary["PRACTICE"] = len(practice_ids)

    # ── 6. PART_DEFINITION（パート定義） ──────────────────
    partdef_db = ctx["CONCERT_DB_PART_DEFINITION"]
    partdef_ids = []
    part_defs = [("Part1 Timp.", instrument_ids[0] if instrument_ids else ""),
                 ("Part2 S.D.",  instrument_ids[1] if len(instrument_ids)>1 else ""),
                 ("Part3 Mar.",  instrument_ids[2] if len(instrument_ids)>2 else "")]
    for sid in song_ids:
        for pname, iid in part_defs:
            if not iid:
                continue
            props = {}
            tpd = t("CONCERT_DB_PART_DEFINITION")
            ctx["put_prop_any"](props, tpd, ["パート名", "名称", "タイトル"], f"{TEST_PREFIX} {pname}")
            ctx["put_prop_any"](props, tpd, ["曲", "FK曲", "Song"], sid)
            ctx["put_prop_any"](props, tpd, ["楽器種別", "FK楽器種別", "Instrument"], iid)
            pd_id = _create(ctx, partdef_db, props)
            if pd_id:
                partdef_ids.append(pd_id)
    summary["PART_DEFINITION"] = len(partdef_ids)

    # ── 7. CONCERT_CAST（参加者） ─────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    cast_ids = []
    parts = ["Perc", "Perc", "Vn1", "Vn2", "Va"]
    roles = ["プレイヤー", "プレイヤー", "トップ", "プレイヤー", "プレイヤー"]
    fees  = [5000, 5000, 5000, 5000, 0]  # 最後はエキストラ想定で0
    for i, pid in enumerate(player_ids):
        props = {}
        tcast = t("CONCERT_DB_PARTICIPANT")
        ctx["put_prop_any"](props, tcast, ["record_key", "タイトル", "PK"],
                            f"{TEST_PREFIX} cast_{i+1}")
        ctx["put_prop_any"](props, tcast, ["演奏会", "FK演奏会"], concert_id)
        ctx["put_prop_any"](props, tcast, ["奏者", "FK奏者", "Player"], pid)
        ctx["put_prop_any"](props, tcast, ["パート", "Part"], parts[i % len(parts)])
        ctx["put_prop_any"](props, tcast, ["役職", "Role"], roles[i % len(roles)])
        ctx["put_prop_any"](props, tcast, ["参加費", "Fee"], fees[i % len(fees)])
        cid = _create(ctx, cast_db, props)
        if cid:
            cast_ids.append(cid)
    summary["CONCERT_CAST"] = len(cast_ids)

    # ── 8. ATTENDANCE（出欠） ─────────────────────────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    att_count = 0
    statuses = ["○", "○", "△", "×", "○"]
    for pr_id in practice_ids:
        for i, pid in enumerate(player_ids):
            props = {}
            tatt = t("CONCERT_DB_ATTENDANCE")
            ctx["put_prop_any"](props, tatt, ["record_key", "タイトル", "PK"],
                                f"{TEST_PREFIX} att_{pr_id[:6]}_{pid[:6]}")
            ctx["put_prop_any"](props, tatt, ["練習", "FK練習", "Practice"], pr_id)
            ctx["put_prop_any"](props, tatt, ["奏者", "FK奏者", "Player"], pid)
            ctx["put_prop_any"](props, tatt, ["参加可否", "出欠", "Status"],
                                statuses[i % len(statuses)])
            att_id = _create(ctx, att_db, props)
            if att_id:
                att_count += 1
    summary["ATTENDANCE"] = att_count

    # ── 9. PLAYER_INSTRUMENT（所有楽器） ──────────────────
    pi_db = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    pi_count = 0
    for i, pid in enumerate(player_ids[:2]):  # PercパートのみPI登録
        for iid in instrument_ids:
            props = {}
            tpi = t("CONCERT_DB_PLAYER_INSTRUMENT")
            ctx["put_prop_any"](props, tpi, ["record_key", "タイトル", "PK名称"],
                                f"{TEST_PREFIX} pi_{pid[:6]}_{iid[:6]}")
            ctx["put_prop_any"](props, tpi, ["演奏会", "FK演奏会", "Concert"], concert_id)
            ctx["put_prop_any"](props, tpi, ["奏者", "FK奏者", "Player"], pid)
            ctx["put_prop_any"](props, tpi, ["楽器種別", "FK楽器種別", "Instrument"], iid)
            ctx["put_prop_any"](props, tpi, ["所有台数", "OwnCount"], 1)
            pi_id = _create(ctx, pi_db, props)
            if pi_id:
                pi_count += 1
    summary["PLAYER_INSTRUMENT"] = pi_count

    # ── 10. PREFERENCE（希望入力） ────────────────────────
    pref_db = ctx["CONCERT_DB_PREFERENCE"]
    pref_count = 0
    priorities = ["第1希望", "第2希望", "希望なし/降り番でも可"]
    for i, pid in enumerate(player_ids[:2]):  # Perc奏者のみ
        for j, pd_id in enumerate(partdef_ids[:3]):
            props = {}
            tpref = t("CONCERT_DB_PREFERENCE")
            ctx["put_prop_any"](props, tpref, ["record_key", "タイトル", "PK"],
                                f"{TEST_PREFIX} pref_{pid[:6]}_{pd_id[:6]}")
            ctx["put_prop_any"](props, tpref, ["演奏会", "FK演奏会"], concert_id)
            ctx["put_prop_any"](props, tpref, ["奏者", "FK奏者", "Player"], pid)
            ctx["put_prop_any"](props, tpref, ["パート定義", "FK パート定義", "PartDef"], pd_id)
            ctx["put_prop_any"](props, tpref, ["希望順位", "Priority"],
                                priorities[j % len(priorities)])
            pref_id = _create(ctx, pref_db, props)
            if pref_id:
                pref_count += 1
    summary["PREFERENCE"] = pref_count

    # ── 11. CONCERT_EXPENSE（経費） ───────────────────────
    exp_db = ctx.get("CONCERT_DB_CONCERT_EXPENSE", "")
    exp_count = 0
    if exp_db:
        items = [("会場費", "テスト会場", 30000, True),
                 ("楽器レンタル", "テストレンタル", 15000, False),
                 ("印刷物・プログラム", "テストプログラム", 8000, True)]
        for type_, content, amount, confirmed in items:
            props = {}
            texp = t("CONCERT_DB_CONCERT_EXPENSE")
            ctx["put_prop_any"](props, texp, ["expense_key", "タイトル"],
                                f"{TEST_PREFIX} {type_}/{content}")
            ctx["put_prop_any"](props, texp, ["演奏会", "FK演奏会"], concert_id)
            ctx["put_prop_any"](props, texp, ["種別", "Type"], type_)
            ctx["put_prop_any"](props, texp, ["内容", "Content"], content)
            ctx["put_prop_any"](props, texp, ["金額", "Amount"], amount)
            ctx["put_prop_any"](props, texp, ["確定", "Confirmed"], confirmed)
            eid = _create(ctx, exp_db, props)
            if eid:
                exp_count += 1
    summary["CONCERT_EXPENSE"] = exp_count

    return summary


# ============================================================
# 削除関数
# ============================================================

def _delete_all_test_data(ctx) -> dict:
    """[TEST]プレフィックスのレコードを全DBからアーカイブする。"""
    summary = {}
    for db_key, title_keys in TEST_DB_MAP:
        db_id = ctx.get(db_key, "")
        if not db_id:
            continue
        rows = ctx["query_all"](db_id, None)
        count = 0
        for r in rows:
            name = (ctx["extract_prop_text_any"](r, title_keys) or
                    ctx["extract_title"](r) or "")
            if name.startswith(TEST_PREFIX):
                if _archive_page(ctx, r.get("id", "")):
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

    st.markdown("""
**投入されるデータ：**
- PERFORMER × 5名（テスト奏者A〜E）
- INSTRUMENT × 3種（Timpani・Snare Drum・Marimba）
- CONCERT × 1件（2099-12-31）
- SONG × 2曲
- PRACTICE × 3回
- PART_DEFINITION × 6件（2曲×3パート）
- CONCERT_CAST × 5件（Perc×2・その他×3）
- ATTENDANCE × 15件（3練習×5人）
- PLAYER_INSTRUMENT × 6件（Perc奏者×3楽器）
- PREFERENCE × 6件（Perc奏者×3パート）
- CONCERT_EXPENSE × 3件

全レコードに `{prefix}` プレフィックスを付与します。削除時はこのプレフィックスで識別します。
""".format(prefix=TEST_PREFIX))

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
        st.caption(f"`{TEST_PREFIX}` で始まる全レコードをアーカイブします。")
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
