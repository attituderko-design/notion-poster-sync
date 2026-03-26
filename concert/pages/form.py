"""
concert/pages/form.py
奏者向け入力フォーム（演奏会単位・単一URL）
"""
import streamlit as st
import hashlib
import requests as _requests

# ============================================================
# トークン生成・検証
# ============================================================

_TOKEN_SECRET = "harmonia_form_2024"

def make_form_token(concert_id: str) -> str:
    """演奏会IDから12文字のトークンを生成。"""
    h = hashlib.sha256(f"{_TOKEN_SECRET}:{concert_id}".encode()).hexdigest()
    return h[:12]

def verify_form_token(token: str, concert_id: str) -> bool:
    return hmac.compare_digest(token, make_form_token(concert_id))

import hmac

# ============================================================
# TinyURL短縮
# ============================================================

def shorten_url(long_url: str) -> str:
    """TinyURL APIでURLを短縮する（APIキー不要）。"""
    try:
        res = _requests.get(
            "https://tinyurl.com/api-create.php",
            params={"url": long_url},
            timeout=5,
        )
        if res.status_code == 200 and res.text.startswith("http"):
            return res.text.strip()
    except Exception:
        pass
    return long_url


# ============================================================
# フォームUI
# ============================================================

def _get_select_options_direct(ctx, db_id: str, field_keys: list) -> list[str]:
    """Notionのselectフィールドのオプションを取得。"""
    try:
        t = ctx["get_prop_types"](db_id)
        if not t:
            return []
        field_name = ctx["find_prop_name"](t, field_keys)
        if not field_name:
            return []
        res = ctx["api_request"]("get", f"https://api.notion.com/v1/databases/{db_id}")
        if not res or res.status_code != 200:
            return []
        props = res.json().get("properties", {})
        opts  = props.get(field_name, {}).get("select", {}).get("options", [])
        return [o["name"] for o in opts if o.get("name")]
    except Exception:
        return []


def _find_player(ctx, name: str) -> dict | None:
    """PERFORMERマスタから氏名で検索。"""
    from concert.services.keys import PLAYER_NAME_KEYS
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    name_stripped = name.strip()
    for p in players:
        pname = ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or ""
        if pname.strip() == name_stripped:
            return p
    return None


def _create_player_form(ctx, name: str, hn: str = "") -> str:
    """PERFORMERに新規奏者を作成してIDを返す。"""
    from concert.services.keys import PLAYER_NAME_KEYS, PLAYER_HN_KEYS
    db_id = ctx["CONCERT_DB_PLAYER"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return ""
    props: dict = {}
    ctx["put_prop_any"](props, t, PLAYER_NAME_KEYS, name.strip())
    if hn:
        ctx["put_prop_any"](props, t, PLAYER_HN_KEYS, hn.strip())
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    if res and res.status_code == 200:
        return res.json().get("id", "")
    return ""


def _upsert_attendance_form(ctx, player_id: str, practice_id: str,
                             practice_name: str, status: str) -> bool:
    """出欠を登録（既存があれば更新）。"""
    from concert.services.keys import (
        ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS, ATT_STATUS_KEYS,
    )
    db_id = ctx["CONCERT_DB_ATTENDANCE"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    # 既存レコード検索
    all_att = ctx["query_all"](db_id, None)
    existing_id = ""
    for r in all_att:
        pl_ids = ctx["extract_relation_ids_any"](r, ATT_PLAYER_REL_KEYS)
        pr_ids = ctx["extract_relation_ids_any"](r, ATT_PRACTICE_REL_KEYS)
        if player_id in pl_ids and practice_id in pr_ids:
            existing_id = r.get("id", "")
            break
    props: dict = {}
    ctx["put_prop_any"](props, t, ["record_key", "タイトル", "PK"],
                        f"{practice_name} × 出欠")
    ctx["put_prop_any"](props, t, ATT_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, ATT_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, t, ATT_STATUS_KEYS, status)
    if existing_id:
        res = ctx["api_request"]("patch",
            f"https://api.notion.com/v1/pages/{existing_id}",
            json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _upsert_preference_form(ctx, player_id: str, part_def_id: str,
                             concert_id: str, priority: str) -> bool:
    """パート希望を登録（既存があれば更新）。"""
    from concert.services.keys import (
        PREF_PLAYER_REL_KEYS, PREF_PART_REL_KEYS, PREF_PRIORITY_KEYS,
    )
    db_id = ctx["CONCERT_DB_PREFERENCE"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    all_pref = ctx["query_all"](db_id, None)
    existing_id = ""
    for r in all_pref:
        pl_ids = ctx["extract_relation_ids_any"](r, PREF_PLAYER_REL_KEYS)
        pd_ids = ctx["extract_relation_ids_any"](r, PREF_PART_REL_KEYS)
        if player_id in pl_ids and part_def_id in pd_ids:
            existing_id = r.get("id", "")
            break
    props: dict = {}
    ctx["put_prop_any"](props, t, ["record_key", "タイトル", "PK"],
                        f"希望_{player_id[:6]}_{part_def_id[:6]}")
    ctx["put_prop_any"](props, t, PREF_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, PREF_PART_REL_KEYS, part_def_id)
    ctx["put_prop_any"](props, t, PREF_PRIORITY_KEYS, priority)
    if existing_id:
        res = ctx["api_request"]("patch",
            f"https://api.notion.com/v1/pages/{existing_id}",
            json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _upsert_own_instrument_form(ctx, player_id: str, concert_id: str,
                                 instrument_id: str, instrument_name: str,
                                 own_count: int) -> bool:
    """所有楽器を登録（既存があれば更新）。"""
    from concert.services.keys import (
        PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_CONCERT_REL_KEYS,
        PI_OWN_COUNT_KEYS,
    )
    db_id = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    all_pi = ctx["query_all"](db_id, None)
    existing_id = ""
    for r in all_pi:
        pl_ids = ctx["extract_relation_ids_any"](r, PI_PLAYER_REL_KEYS)
        i_ids  = ctx["extract_relation_ids_any"](r, PI_INST_REL_KEYS)
        c_ids  = ctx["extract_relation_ids_any"](r, PI_CONCERT_REL_KEYS)
        if player_id in pl_ids and instrument_id in i_ids and concert_id in c_ids:
            existing_id = r.get("id", "")
            break
    props: dict = {}
    ctx["put_prop_any"](props, t, ["record_key", "タイトル", "PK名称"],
                        f"{instrument_name} 所有_{player_id[:6]}")
    ctx["put_prop_any"](props, t, PI_PLAYER_REL_KEYS,  player_id)
    ctx["put_prop_any"](props, t, PI_INST_REL_KEYS,    instrument_id)
    ctx["put_prop_any"](props, t, PI_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PI_OWN_COUNT_KEYS,   own_count)
    if existing_id:
        res = ctx["api_request"]("patch",
            f"https://api.notion.com/v1/pages/{existing_id}",
            json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# フォームのステップ管理
# ============================================================

def render_form(ctx: dict, concert_id: str):
    """奏者向けフォームを表示。"""
    from concert.services.keys import (
        CONCERT_NAME_KEYS, CONCERT_DATE_KEYS,
        PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS,
        PRACTICE_CONCERT_REL_KEYS, PRACTICE_CONCERT_DAY_KEYS,
        PARTICIPANT_PART_KEYS, PARTICIPANT_ROLE_KEYS,
        PARTDEF_NAME_KEYS, PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS,
        SONG_NAME_KEYS, INSTRUMENT_NAME_KEYS,
        PLAYER_NAME_KEYS, PLAYER_HN_KEYS,
        PREF_PRIORITY_KEYS,
    )

    PRIORITY_OPTIONS = ["第1希望", "第2希望", "第3希望", "希望なし/降り番でも可", "NG"]
    ATT_OPTIONS      = ["○", "△", "×"]
    OTHER_PART       = "一覧にない（別途入力）"

    # ── 演奏会情報取得 ──────────────────────────────────────
    concert_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
    concert = next((r for r in concert_rows if r.get("id") == concert_id), None)
    if not concert:
        st.error("演奏会が見つかりません。URLを確認してください。")
        return

    c_name = ctx["extract_prop_text_any"](concert, CONCERT_NAME_KEYS) or "演奏会"
    c_date = ctx["extract_prop_text_any"](concert, CONCERT_DATE_KEYS) or ""
    c_date_disp = c_date[:10] if c_date else "日時未設定"

    st.title(f"🎵 {c_name}")
    st.caption(f"本番日：{c_date_disp}")
    st.divider()

    # 練習一覧（本番当日除く、日付順）
    all_practices = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    practices = sorted(
        [p for p in all_practices
         if concert_id in ctx["extract_relation_ids_any"](p, PRACTICE_CONCERT_REL_KEYS)
         and ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS) != "True"],
        key=lambda p: ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or "9999"
    )

    # パート選択肢（CONCERT_CASTから動的取得）
    part_opts_raw = _get_select_options_direct(ctx, ctx["CONCERT_DB_PARTICIPANT"],
                                                PARTICIPANT_PART_KEYS)
    part_opts = part_opts_raw + [OTHER_PART] if part_opts_raw else [OTHER_PART]

    # ── ステップ管理 ─────────────────────────────────────────
    step = st.session_state.get("form_step", 1)

    # ── STEP 1: 氏名入力 ─────────────────────────────────────
    if step == 1:
        st.subheader("Step 1 / 氏名を入力してください")
        with st.form("form_step1"):
            name    = st.text_input("氏名 *", placeholder="例：山田 太郎")
            hn      = st.text_input("H.N.（任意）", placeholder="例：Yuta")
            part_sel = st.selectbox("担当パート *", part_opts)
            part_other = ""
            if part_sel == OTHER_PART:
                part_other = st.text_input("パートを入力してください",
                                           placeholder="例：Sax / Tu など")
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            if not name.strip():
                st.error("氏名は必須です。")
                return
            actual_part = part_other.strip() if part_sel == OTHER_PART else part_sel
            if not actual_part:
                st.error("パートを入力してください。")
                return

            with st.spinner("確認中..."):
                existing = _find_player(ctx, name.strip())
                if existing:
                    player_id = existing.get("id", "")
                    st.session_state["form_is_new_player"] = False
                else:
                    player_id = _create_player_form(ctx, name.strip(), hn.strip())
                    st.session_state["form_is_new_player"] = True

            if not player_id:
                st.error("奏者情報の登録に失敗しました。もう一度お試しください。")
                return

            st.session_state["form_player_id"]   = player_id
            st.session_state["form_player_name"]  = name.strip()
            st.session_state["form_player_hn"]    = hn.strip()
            st.session_state["form_player_part"]  = actual_part
            st.session_state["form_step"]         = 2
            st.rerun()

    # ── STEP 2: 出欠入力 ─────────────────────────────────────
    elif step == 2:
        player_name = st.session_state.get("form_player_name", "")
        part        = st.session_state.get("form_player_part", "")
        is_new      = st.session_state.get("form_is_new_player", False)

        st.subheader("Step 2 / 練習出欠を入力してください")
        if is_new:
            st.success(f"✅ {player_name} さんを新規登録しました。")
        else:
            st.info(f"👤 {player_name} さん（既存）")
        st.caption(f"担当パート：{part}")

        if not practices:
            st.warning("練習日がまだ登録されていません。")
            st.session_state["form_att"] = {}
        else:
            with st.form("form_step2"):
                att: dict[str, str] = {}
                for p in practices:
                    pr_id   = p.get("id", "")
                    pr_name = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or pr_id
                    pr_date = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or ""
                    pr_date_disp = pr_date[:10] if pr_date else "日時未設定"
                    label = f"{pr_name}（{pr_date_disp}）"
                    val = st.radio(label, ATT_OPTIONS, index=1, horizontal=True,
                                   key=f"att_{pr_id}")
                    att[pr_id] = val

                submitted = st.form_submit_button("次へ →", type="primary",
                                                  use_container_width=True)
            if submitted:
                st.session_state["form_att"] = att
                is_perc = part.lower() in ("perc", "percussion", "打楽器")
                st.session_state["form_step"] = 3 if is_perc else 5
                st.rerun()

    # ── STEP 3: パート希望（Percのみ） ───────────────────────
    elif step == 3:
        st.subheader("Step 3 / パート希望を入力してください")
        st.caption("各パートに対して希望順位を選択してください。")

        # 曲→パート定義を取得
        all_songs    = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
        concert_songs = [s for s in all_songs
                         if concert_id in ctx["extract_relation_ids_any"](
                             s, ["演奏会", "FK演奏会", "Concert"])]
        all_partdefs = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
        song_ids = {s.get("id","") for s in concert_songs}
        song_name_map = {s.get("id",""): ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or ""
                         for s in concert_songs}

        partdefs = [p for p in all_partdefs
                    if any(sid in ctx["extract_relation_ids_any"](p, PARTDEF_SONG_REL_KEYS)
                           for sid in song_ids)]

        if not partdefs:
            st.info("パート定義が登録されていません。スキップします。")
            st.session_state["form_pref"] = {}
            st.session_state["form_step"] = 4
            st.rerun()
            return

        with st.form("form_step3"):
            pref: dict[str, str] = {}
            for pd in partdefs:
                pd_id   = pd.get("id", "")
                pd_name = ctx["extract_prop_text_any"](pd, PARTDEF_NAME_KEYS) or pd_id
                song_ids_rel = ctx["extract_relation_ids_any"](pd, PARTDEF_SONG_REL_KEYS)
                sname = song_name_map.get(song_ids_rel[0], "") if song_ids_rel else ""
                label = f"【{sname}】{pd_name}" if sname else pd_name
                val = st.selectbox(label, PRIORITY_OPTIONS, index=3, key=f"pref_{pd_id}")
                pref[pd_id] = val

            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_pref"] = pref
            st.session_state["form_step"] = 4
            st.rerun()

    # ── STEP 4: 所有楽器（Percのみ） ─────────────────────────
    elif step == 4:
        st.subheader("Step 4 / 所有楽器を入力してください")
        st.caption("所有台数が0の楽器は入力不要です。")

        all_insts   = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
        all_partdefs = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
        all_songs    = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
        concert_songs = [s for s in all_songs
                         if concert_id in ctx["extract_relation_ids_any"](
                             s, ["演奏会", "FK演奏会", "Concert"])]
        song_ids = {s.get("id","") for s in concert_songs}
        partdefs = [p for p in all_partdefs
                    if any(sid in ctx["extract_relation_ids_any"](p, PARTDEF_SONG_REL_KEYS)
                           for sid in song_ids)]
        required_inst_ids: set[str] = set()
        for pd in partdefs:
            required_inst_ids.update(ctx["extract_relation_ids_any"](pd, PARTDEF_INST_REL_KEYS))

        inst_name_map = {i.get("id",""): ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ""
                         for i in all_insts}
        required_insts = [(iid, inst_name_map.get(iid, iid))
                          for iid in required_inst_ids if iid]
        required_insts.sort(key=lambda x: x[1])

        if not required_insts:
            st.info("必要楽器の情報がありません。スキップします。")
            st.session_state["form_own"] = {}
            st.session_state["form_step"] = 5
            st.rerun()
            return

        with st.form("form_step4"):
            own: dict[str, int] = {}
            for iid, iname in required_insts:
                val = st.number_input(f"{iname}　所有台数",
                                      min_value=0, max_value=10, step=1,
                                      value=0, key=f"own_{iid}")
                own[iid] = int(val)
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_own"] = own
            st.session_state["form_step"] = 5
            st.rerun()

    # ── STEP 5: 確認・送信 ───────────────────────────────────
    elif step == 5:
        player_id   = st.session_state.get("form_player_id", "")
        player_name = st.session_state.get("form_player_name", "")
        part        = st.session_state.get("form_player_part", "")
        att         = st.session_state.get("form_att", {})
        pref        = st.session_state.get("form_pref", {})
        own         = st.session_state.get("form_own", {})

        st.subheader("Step 5 / 確認して送信")
        st.markdown(f"**氏名：** {player_name}　　**パート：** {part}")

        if att:
            st.markdown("**出欠：**")
            all_practices = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
            prac_map = {p.get("id",""): ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or p.get("id","")
                        for p in all_practices}
            for pr_id, status in att.items():
                st.caption(f"　{prac_map.get(pr_id, pr_id)}：{status}")

        if pref:
            st.markdown("**パート希望：**")
            all_partdefs = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
            pd_map = {p.get("id",""): ctx["extract_prop_text_any"](p, PARTDEF_NAME_KEYS) or p.get("id","")
                      for p in all_partdefs}
            for pd_id, priority in pref.items():
                st.caption(f"　{pd_map.get(pd_id, pd_id)}：{priority}")

        if own:
            st.markdown("**所有楽器：**")
            all_insts = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
            inst_map  = {i.get("id",""): ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ""
                         for i in all_insts}
            for iid, cnt in own.items():
                if cnt > 0:
                    st.caption(f"　{inst_map.get(iid, iid)}：{cnt}台")

        col1, col2 = st.columns(2)
        if col1.button("← 修正する", use_container_width=True):
            is_perc = part.lower() in ("perc", "percussion", "打楽器")
            st.session_state["form_step"] = 2
            st.rerun()

        if col2.button("✅ 送信する", type="primary", use_container_width=True):
            errors = []
            with st.spinner("送信中..."):
                # 出欠登録
                from concert.services.keys import PRACTICE_NAME_KEYS as PNK
                all_prac = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
                prac_name_map = {p.get("id",""): ctx["extract_prop_text_any"](p, PNK) or ""
                                 for p in all_prac}
                for pr_id, status in att.items():
                    ok = _upsert_attendance_form(ctx, player_id, pr_id,
                                                  prac_name_map.get(pr_id, ""), status)
                    if not ok:
                        errors.append(f"出欠登録失敗：{prac_name_map.get(pr_id, pr_id)}")

                # パート希望登録
                for pd_id, priority in pref.items():
                    ok = _upsert_preference_form(ctx, player_id, pd_id,
                                                  concert_id, priority)
                    if not ok:
                        errors.append(f"希望登録失敗：{pd_id[:8]}")

                # 所有楽器登録
                all_insts = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
                inst_name_map2 = {i.get("id",""): ctx["extract_prop_text_any"](
                    i, INSTRUMENT_NAME_KEYS) or "" for i in all_insts}
                for iid, cnt in own.items():
                    if cnt > 0:
                        ok = _upsert_own_instrument_form(
                            ctx, player_id, concert_id, iid,
                            inst_name_map2.get(iid, iid), cnt)
                        if not ok:
                            errors.append(f"楽器登録失敗：{inst_name_map2.get(iid, iid)}")

            if errors:
                for e in errors:
                    st.error(e)
            else:
                st.session_state["form_step"] = 6
                st.rerun()

    # ── STEP 6: 完了 ─────────────────────────────────────────
    elif step == 6:
        player_name = st.session_state.get("form_player_name", "")
        st.balloons()
        st.success(f"✅ {player_name} さんの入力が完了しました。ありがとうございました！")
        st.info("このページを閉じて構いません。")
        if st.button("別の方の入力をする", use_container_width=True):
            for k in list(st.session_state.keys()):
                if k.startswith("form_"):
                    st.session_state.pop(k, None)
            st.rerun()


# ============================================================
# 管理者向け：URLとトークンの生成
# ============================================================

def render_url_generator(ctx: dict, concert_id: str, concert_name: str):
    """練習管理画面から呼び出すURL生成UI。"""
    token    = make_form_token(concert_id)
    base_url = st.secrets.get("FORM_BASE_URL", "https://artemis-cers.streamlit.app")
    long_url = f"{base_url}/?concert={token}&cid={concert_id}"

    st.markdown("### 📋 奏者フォームURL")
    st.code(long_url)

    col1, col2 = st.columns(2)
    if col1.button("🔗 TinyURLで短縮", key="shorten_url_btn", use_container_width=True):
        with st.spinner("短縮中..."):
            short = shorten_url(long_url)
        st.session_state["form_short_url"] = short
        st.rerun()

    short = st.session_state.get("form_short_url", "")
    if short and short != long_url:
        st.success("短縮URL：")
        st.code(short)
        col2.link_button("📋 開く", short, use_container_width=True)
    elif short == long_url:
        st.warning("短縮に失敗しました。長いURLをそのまま使用してください。")
