"""
concert/pages/form.py
奏者向け入力フォーム（URLトークンでアクセス制御）
"""
import streamlit as st
import hashlib, hmac
import requests as _requests

from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS, CONCERT_VENUE_KEYS,
    CONCERT_CONDUCTOR_KEYS, CONCERT_SOLOIST_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS, PRACTICE_VENUE_KEYS,
    PRACTICE_CONCERT_REL_KEYS, PRACTICE_CONCERT_DAY_KEYS,
    PARTICIPANT_PART_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_RECORD_KEYS,
    PARTDEF_NAME_KEYS, PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS,
    SONG_NAME_KEYS, SONG_CREATOR_KEYS, SONG_CONCERT_REL_KEYS,
    INSTRUMENT_NAME_KEYS,
    PLAYER_NAME_KEYS, PLAYER_HN_KEYS, PLAYER_EMAIL_KEYS,
    PLAYER_PHONE_KEYS, PLAYER_LINE_KEYS,
    ATTENDANCE_KEY_KEYS, ATT_RECORD_KEYS, ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS, ATT_STATUS_KEYS,
    PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_CONCERT_REL_KEYS, PI_OWN_COUNT_KEYS,
    PREF_PLAYER_REL_KEYS, PREF_PART_REL_KEYS, PREF_PRIORITY_KEYS,
    CONCERT_CONFIRMED_FEE_KEYS,
)

_TOKEN_SECRET = "harmonia_form_2024"
PRIORITY_OPTS = ["第1希望", "第2希望", "第3希望", "希望なし/降り番でも可", "NG"]
ATT_OPTS      = ["○", "△", "×"]
OTHER_PART    = "一覧にない（管理者に連絡）"
IS_PERC       = lambda p: (p or "").lower() in ("perc", "percussion", "打楽器")

# ── トークン ──────────────────────────────────────────────────

def make_form_token(concert_id: str) -> str:
    h = hashlib.sha256(f"{_TOKEN_SECRET}:{concert_id}".encode()).hexdigest()
    return h[:12]

def verify_form_token(token: str, concert_id: str) -> bool:
    return hmac.compare_digest(token, make_form_token(concert_id))

# ── TinyURL ───────────────────────────────────────────────────

def shorten_url(long_url: str) -> str:
    try:
        res = _requests.get("https://tinyurl.com/api-create.php",
                            params={"url": long_url}, timeout=5)
        if res.status_code == 200 and res.text.startswith("http"):
            return res.text.strip()
    except Exception:
        pass
    return long_url

# ── データ一括取得（初回のみ） ────────────────────────────────

def _load_form_data(ctx, concert_id: str):
    if st.session_state.get("form_data_loaded") == concert_id:
        return
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    concert_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
    concert = next((r for r in concert_rows if r.get("id") == concert_id), None)

    all_prac = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    # 練習回のみ（本番当日除く）
    practices = sorted(
        [p for p in all_prac
         if concert_id in ext_rel(p, PRACTICE_CONCERT_REL_KEYS)
         and ext(p, PRACTICE_CONCERT_DAY_KEYS) != "True"],
        key=lambda p: ext(p, PRACTICE_DATE_KEYS) or "9999"
    )
    # 本番当日レコード
    concert_day = next(
        (p for p in all_prac
         if concert_id in ext_rel(p, PRACTICE_CONCERT_REL_KEYS)
         and ext(p, PRACTICE_CONCERT_DAY_KEYS) == "True"),
        None
    )

    all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    songs = [s for s in all_songs
             if concert_id in ext_rel(s, SONG_CONCERT_REL_KEYS)]
    song_ids = {s.get("id","") for s in songs}

    all_pd = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    partdefs = [p for p in all_pd
                if any(sid in ext_rel(p, PARTDEF_SONG_REL_KEYS) for sid in song_ids)]

    instruments = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_map = {i.get("id",""): ext(i, INSTRUMENT_NAME_KEYS) or "" for i in instruments}

    req_inst_ids: set[str] = set()
    for pd in partdefs:
        req_inst_ids.update(ext_rel(pd, PARTDEF_INST_REL_KEYS))

    part_opts = _get_select_opts(ctx, ctx["CONCERT_DB_PARTICIPANT"], PARTICIPANT_PART_KEYS)

    st.session_state.update({
        "form_data_loaded": concert_id,
        "form_concert":     concert,
        "form_practices":   practices,
        "form_concert_day": concert_day,
        "form_songs":       songs,
        "form_partdefs":    partdefs,
        "form_inst_map":    inst_map,
        "form_req_insts":   sorted(req_inst_ids, key=lambda x: inst_map.get(x, x)),
        "form_part_opts":   part_opts + [OTHER_PART],
    })


def _get_select_opts(ctx, db_id: str, field_keys: list) -> list[str]:
    try:
        t = ctx["get_prop_types"](db_id)
        field_name = ctx["find_prop_name"](t, field_keys) if t else None
        if not field_name:
            return []
        res = ctx["api_request"]("get", f"https://api.notion.com/v1/databases/{db_id}")
        if not res or res.status_code != 200:
            return []
        opts = res.json().get("properties", {}).get(field_name, {}).get("select", {}).get("options", [])
        return [o["name"] for o in opts if o.get("name")]
    except Exception:
        return []

# ── 送信処理 ──────────────────────────────────────────────────

def _find_rel(type_map: dict, candidates: list, keywords: list,
              exclude: set | None = None) -> str:
    """relationフィールド名をfuzzy検索（players.pyの_find_relation_propと同等）。"""
    exclude = exclude or set()
    for k in candidates:
        if (type_map or {}).get(k) == "relation" and k not in exclude:
            return k
    for k, t in (type_map or {}).items():
        if t != "relation" or k in exclude:
            continue
        if any(kw.lower() in str(k).lower() for kw in keywords):
            return k
    for k, t in (type_map or {}).items():
        if t == "relation" and k not in exclude:
            return k
    return ""


def _submit_all(ctx, concert_id: str, concert_name: str,
                player_id: str, player_name: str,
                att: dict, pref: dict, own: dict) -> tuple[int, list[str], dict]:
    """全データをNotionに送信。(成功件数, エラーリスト, デバッグ情報)を返す。"""
    ext_rel  = ctx["extract_relation_ids_any"]
    ok_n     = 0
    errors: list[str] = []
    debug: dict = {}  # テスト用デバッグ情報

    # ── CONCERT_CAST ──────────────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    t_cast  = ctx["get_prop_types"](cast_db)
    cast_id = ""
    if t_cast:
        all_cast = ctx["query_all"](cast_db, None)
        for r in all_cast:
            pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
            cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
            if player_id in pids and concert_id in cids:
                cast_id = r.get("id", "")
                break
        if not cast_id:
            props: dict = {}
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_PLAYER_REL_KEYS,  player_id)
            ctx["put_key_any"](props, t_cast, PARTICIPANT_RECORD_KEYS,
                               concert_id, player_id, prefix="participant")
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_PART_KEYS,
                                st.session_state.get("form_player_part", ""))
            confirmed_fee = st.session_state.get(f"confirmed_fee_{concert_id}")
            if confirmed_fee is not None:
                ctx["put_prop_any"](props, t_cast, ["参加費", "Fee"], confirmed_fee)
            res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                     json={"parent": {"database_id": cast_db}, "properties": props})
            if res and res.status_code == 200:
                cast_id = res.json().get("id", "")
                ok_n += 1
                debug["cast_id"] = cast_id
            else:
                errors.append(f"CONCERT_CAST登録失敗 (status={getattr(res,'status_code','?')})")
        else:
            debug["cast_id"] = cast_id + "（既存）"
            # 既存レコードのパートが空なら書き込む
            existing_row = next((r for r in all_cast if r.get("id","") == cast_id), {})
            if not ctx["extract_prop_text_any"](existing_row, PARTICIPANT_PART_KEYS):
                props_p: dict = {}
                ctx["put_prop_any"](props_p, t_cast, PARTICIPANT_PART_KEYS,
                                    st.session_state.get("form_player_part", ""))
                ctx["api_request"]("patch",
                    f"https://api.notion.com/v1/pages/{cast_id}",
                    json={"properties": props_p})

    debug["player_id"] = player_id

    # ── ATTENDANCE（練習回 + 本番当日を○で自動登録） ──────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    t_att  = ctx["get_prop_types"](att_db)
    if t_att:
        all_att = ctx["query_all"](att_db, None)
        practices = st.session_state.get("form_practices", [])
        concert_day = st.session_state.get("form_concert_day")
        prac_name_map = {p.get("id",""): ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ""
                         for p in practices}
        if concert_day:
            prac_name_map[concert_day.get("id","")] = \
                ctx["extract_prop_text_any"](concert_day, PRACTICE_NAME_KEYS) or "本番当日"

        att_all = dict(att)
        if concert_day:
            att_all[concert_day.get("id","")] = "○"

        # 既存players.pyと同じロジック：_find_relation_propでフィールド名を特定
        practice_rel_key = _find_rel(t_att, ATT_PRACTICE_REL_KEYS, ["練習", "practice"])
        player_rel_key   = _find_rel(t_att, ATT_PLAYER_REL_KEYS,
                                     ["奏者", "出演者", "participant", "player"],
                                     exclude={practice_rel_key} if practice_rel_key else set())
        status_key = ctx["find_prop_name"](t_att, ATT_STATUS_KEYS)
        rel_target = cast_id if cast_id else player_id

        att_ids: list[str] = []
        for pr_id, status in att_all.items():
            existing_id = ""
            for r in all_att:
                pl = (r.get("properties",{}).get(player_rel_key,{}).get("relation",[]) or [])
                pr = (r.get("properties",{}).get(practice_rel_key,{}).get("relation",[]) or [])
                pl_ids = [x.get("id","") for x in pl]
                pr_ids = [x.get("id","") for x in pr]
                if rel_target in pl_ids and pr_id in pr_ids:
                    existing_id = r.get("id","")
                    break
            pname = prac_name_map.get(pr_id, "")
            props = {}
            # attendance_key
            ctx["put_key_any"](props, t_att, ATTENDANCE_KEY_KEYS,
                               rel_target, pr_id, prefix="att")
            # relationsをput_propで直接指定
            if player_rel_key:
                ctx["put_prop"](props, t_att, player_rel_key,   rel_target)
            if practice_rel_key:
                ctx["put_prop"](props, t_att, practice_rel_key, pr_id)
            if status_key:
                ctx["put_prop"](props, t_att, status_key, status)
            if existing_id:
                res = ctx["api_request"]("patch",
                    f"https://api.notion.com/v1/pages/{existing_id}",
                    json={"properties": props})
            else:
                res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                         json={"parent": {"database_id": att_db},
                                               "properties": props})
            if res and res.status_code == 200:
                ok_n += 1
                att_ids.append(res.json().get("id","") if not existing_id else existing_id)
            else:
                errors.append(f"出欠登録失敗：{pname} (status={getattr(res,'status_code','?')})")
        debug["att_ids"] = att_ids

    # ── パート希望 ────────────────────────────────────────────
    if pref:
        pref_db = ctx["CONCERT_DB_PREFERENCE"]
        t_pref  = ctx["get_prop_types"](pref_db)
        if t_pref:
            all_pref = ctx["query_all"](pref_db, None)
            for pd_id, priority in pref.items():
                if priority == "未回答":
                    continue
                existing_id = ""
                for r in all_pref:
                    pl = ext_rel(r, PREF_PLAYER_REL_KEYS)
                    pd = ext_rel(r, PREF_PART_REL_KEYS)
                    if player_id in pl and pd_id in pd:
                        existing_id = r.get("id", "")
                        break
                props = {}
                ctx["put_key_any"](props, t_pref, ["record_key", "タイトル", "PK"],
                                   player_id, pd_id, prefix="pref")
                ctx["put_prop_any"](props, t_pref, PREF_PLAYER_REL_KEYS, player_id)
                ctx["put_prop_any"](props, t_pref, PREF_PART_REL_KEYS,   pd_id)
                ctx["put_prop_any"](props, t_pref, PREF_PRIORITY_KEYS,   priority)
                if existing_id:
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{existing_id}",
                        json={"properties": props})
                else:
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": pref_db},
                                                   "properties": props})
                if res and res.status_code == 200:
                    ok_n += 1
                else:
                    errors.append(f"希望登録失敗：{pd_id[:8]} (status={getattr(res,'status_code','?')})")

    # ── 所有楽器 ──────────────────────────────────────────────
    if own:
        pi_db  = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
        t_pi   = ctx["get_prop_types"](pi_db)
        inst_map = st.session_state.get("form_inst_map", {})
        if t_pi:
            all_pi = ctx["query_all"](pi_db, None)
            for iid, cnt in own.items():
                if cnt == 0:
                    continue
                iname = inst_map.get(iid, iid)
                existing_id = ""
                for r in all_pi:
                    if (player_id in ext_rel(r, PI_PLAYER_REL_KEYS) and
                            iid in ext_rel(r, PI_INST_REL_KEYS) and
                            concert_id in ext_rel(r, PI_CONCERT_REL_KEYS)):
                        existing_id = r.get("id", "")
                        break
                props = {}
                ctx["put_key_any"](props, t_pi, ["record_key", "タイトル", "PK名称"],
                                   player_id, iid, prefix="assign")
                ctx["put_prop_any"](props, t_pi, PI_PLAYER_REL_KEYS,  player_id)
                ctx["put_prop_any"](props, t_pi, PI_INST_REL_KEYS,    iid)
                ctx["put_prop_any"](props, t_pi, PI_CONCERT_REL_KEYS, concert_id)
                ctx["put_prop_any"](props, t_pi, PI_OWN_COUNT_KEYS,   cnt)
                if existing_id:
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{existing_id}",
                        json={"properties": props})
                else:
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": pi_db},
                                                   "properties": props})
                if res and res.status_code == 200:
                    ok_n += 1
                else:
                    errors.append(f"楽器登録失敗：{iname} (status={getattr(res,'status_code','?')})")

    return ok_n, errors, debug

# ── プライバシーポリシー ──────────────────────────────────────

_PRIVACY_POLICY = """
## プライバシーポリシー

本フォームは **ArtéMis HARMONIA** が提供する演奏会運営支援フォームです。

### 収集する情報
- 氏名・ハンドルネーム
- メールアドレス・電話番号・LINE ID（任意）
- 練習出欠・パート希望・所有楽器情報

### 利用目的
収集した情報は演奏会の運営・調整（出欠管理・パートアサイン・楽器手配）のみに使用します。

### 第三者提供
収集した個人情報を本演奏会の運営目的以外に使用せず、第三者に提供しません。

### 管理者
ArtéMis HARMONIA開発者　喜田悠太
attituderko@gmail.com

### 開示・削除請求
上記メールアドレスにご連絡ください。

---
*本フォームへの入力・送信をもって上記に同意したものとみなします。*
"""

# ── フォームメイン ────────────────────────────────────────────

def render_form(ctx, concert_id: str):
    ext = ctx["extract_prop_text_any"]

    # 初回のみデータ一括取得
    if not st.session_state.get("form_data_loaded"):
        with st.spinner("読み込み中..."):
            _load_form_data(ctx, concert_id)

    concert   = st.session_state.get("form_concert")
    practices = st.session_state.get("form_practices", [])
    songs     = st.session_state.get("form_songs", [])
    partdefs  = st.session_state.get("form_partdefs", [])
    inst_map  = st.session_state.get("form_inst_map", {})
    req_insts = st.session_state.get("form_req_insts", [])
    part_opts = st.session_state.get("form_part_opts", [OTHER_PART])

    if not concert:
        st.error("演奏会が見つかりません。URLを確認してください。")
        return

    # ── 演奏会情報ヘッダー ────────────────────────────────────
    c_name      = ext(concert, CONCERT_NAME_KEYS) or "演奏会"
    c_date      = (ext(concert, CONCERT_DATE_KEYS) or "")[:10]
    c_venue     = ext(concert, CONCERT_VENUE_KEYS) or ""
    c_conductor = ext(concert, CONCERT_CONDUCTOR_KEYS) or ""
    c_soloist   = ext(concert, CONCERT_SOLOIST_KEYS) or ""

    st.title(f"🎵 {c_name}")
    if c_date:      st.caption(f"📅 本番日：{c_date}")
    if c_venue:     st.caption(f"📍 会場：{c_venue}")
    if c_conductor: st.caption(f"🎼 指揮：{c_conductor}")
    if c_soloist:   st.caption(f"🌟 ソリスト：{c_soloist}")
    if songs:
        st.caption("🎶 演奏曲目：" + "　/　".join(
            f"{ext(s, SONG_NAME_KEYS) or ''}（{ext(s, SONG_CREATOR_KEYS) or ''}）".strip("（）")
            for s in songs
        ))
    st.divider()

    step = st.session_state.get("form_step", 1)

    # ── STEP 0: プライバシーポリシー同意 ─────────────────────
    if step == 1 and not st.session_state.get("form_privacy_agreed"):
        st.subheader("はじめに")
        st.markdown(_PRIVACY_POLICY)
        if st.button("✅ 同意して入力を開始する", type="primary",
                     use_container_width=True, key="privacy_agree"):
            st.session_state["form_privacy_agreed"] = True
            st.rerun()
        return

    # ── STEP 1: 氏名・連絡先・パート ──────────────────────────
    if step == 1:
        st.subheader("Step 1 / 基本情報を入力してください")
        with st.form("step1"):
            name     = st.text_input("氏名 *", placeholder="例：山田太郎")
            hn       = st.text_input("H.N.（任意）", placeholder="例：酒席ティンパニ奏者")
            email    = st.text_input("メールアドレス（任意）", placeholder="例：yamada@example.com")
            phone    = st.text_input("電話番号（任意）", placeholder="例：09012345678")
            line_id  = st.text_input("LINE ID（任意）", placeholder="例：yamada_taro")
            st.divider()
            part_sel = st.selectbox("担当パート *", part_opts)
            part_other = ""
            if part_sel == OTHER_PART:
                part_other = st.text_input("パートを入力してください")
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            if not name.strip():
                st.error("氏名は必須です。")
                return
            actual_part = part_other.strip() if part_sel == OTHER_PART else part_sel
            if not actual_part or actual_part == OTHER_PART:
                st.error("パートを選択または入力してください。")
                return

            with st.spinner("確認中..."):
                players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
                existing = next(
                    (p for p in players
                     if (ext(p, PLAYER_NAME_KEYS) or "").strip() == name.strip()),
                    None
                )
                if existing:
                    player_id = existing.get("id", "")
                    # 連絡先の更新（入力があった場合のみ）
                    t_pl = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
                    if t_pl and any([hn.strip(), email.strip(), phone.strip(), line_id.strip()]):
                        upd: dict = {}
                        if hn.strip():      ctx["put_prop_any"](upd, t_pl, PLAYER_HN_KEYS,    hn.strip())
                        if email.strip():   ctx["put_prop_any"](upd, t_pl, PLAYER_EMAIL_KEYS, email.strip())
                        if phone.strip():   ctx["put_prop_any"](upd, t_pl, PLAYER_PHONE_KEYS, phone.strip())
                        if line_id.strip(): ctx["put_prop_any"](upd, t_pl, PLAYER_LINE_KEYS,  line_id.strip())
                        ctx["api_request"]("patch",
                            f"https://api.notion.com/v1/pages/{player_id}",
                            json={"properties": upd})
                    st.session_state["form_is_new"] = False
                else:
                    t_pl = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
                    props = {}
                    ctx["put_prop_any"](props, t_pl, PLAYER_NAME_KEYS, name.strip())
                    if hn.strip():      ctx["put_prop_any"](props, t_pl, PLAYER_HN_KEYS,    hn.strip())
                    if email.strip():   ctx["put_prop_any"](props, t_pl, PLAYER_EMAIL_KEYS, email.strip())
                    if phone.strip():   ctx["put_prop_any"](props, t_pl, PLAYER_PHONE_KEYS, phone.strip())
                    if line_id.strip(): ctx["put_prop_any"](props, t_pl, PLAYER_LINE_KEYS,  line_id.strip())
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": ctx["CONCERT_DB_PLAYER"]},
                                                   "properties": props})
                    player_id = res.json().get("id", "") if res and res.status_code == 200 else ""
                    st.session_state["form_is_new"] = True

            if not player_id:
                st.error("登録に失敗しました。もう一度お試しください。")
                return

            st.session_state.update({
                "form_player_id":   player_id,
                "form_player_name": name.strip(),
                "form_player_part": actual_part,
                "form_att":  {},
                "form_pref": {},
                "form_own":  {},
                "form_step": 2,
            })
            st.rerun()

    # ── STEP 2: 出欠 ──────────────────────────────────────────
    elif step == 2:
        pname  = st.session_state.get("form_player_name","")
        part   = st.session_state.get("form_player_part","")
        is_new = st.session_state.get("form_is_new", False)

        st.subheader("Step 2 / 練習出欠を入力してください")
        st.caption(f"👤 {pname}　　パート：{part}")
        if is_new:
            st.success("✅ 新規奏者として登録しました。")
        st.caption("※ 本番当日の出席は自動で登録されます。")

        if not practices:
            st.info("練習日が登録されていません。")
            st.session_state["form_step"] = 3 if IS_PERC(part) else 5
            if st.button("次へ →", type="primary", use_container_width=True):
                st.rerun()
            return

        with st.form("step2"):
            att: dict[str, str] = {}
            for p in practices:
                pr_id    = p.get("id","")
                pr_name  = ext(p, PRACTICE_NAME_KEYS) or pr_id
                pr_date  = ext(p, PRACTICE_DATE_KEYS) or ""
                pr_venue = ext(p, PRACTICE_VENUE_KEYS) or ""
                date_disp = pr_date[:10] if pr_date else "日時未設定"
                time_disp = pr_date[11:16] if len(pr_date) > 10 else ""
                label = f"**{pr_name}**　{date_disp}"
                if time_disp: label += f" {time_disp}"
                if pr_venue:  label += f"　📍 {pr_venue}"
                st.markdown(label)
                val = st.radio("　", ATT_OPTS, index=1, horizontal=True,
                               key=f"att_{pr_id}", label_visibility="collapsed")
                att[pr_id] = val
                st.divider()
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_att"]  = att
            st.session_state["form_step"] = 3 if IS_PERC(part) else 5
            st.rerun()

    # ── STEP 3: パート希望（Percのみ） ───────────────────────
    elif step == 3:
        st.subheader("Step 3 / パート希望を入力してください")
        st.caption("希望するパートに第1希望〜第3希望を入力してください。それ以外のパートは「希望なし/降り番でも可」を選択してください。")

        if not partdefs:
            st.info("パート定義がまだ登録されていません。スキップします。")
            st.session_state["form_pref"] = {}
            st.session_state["form_step"] = 4
            st.rerun()
            return

        # 曲ごとにグループ化
        song_name_map = {s.get("id",""): ext(s, SONG_NAME_KEYS) or "" for s in songs}
        from collections import defaultdict
        pd_by_song: dict[str, list] = defaultdict(list)
        for pd in partdefs:
            sids = ctx["extract_relation_ids_any"](pd, PARTDEF_SONG_REL_KEYS)
            sid = sids[0] if sids else "__none__"
            pd_by_song[sid].append(pd)

        with st.form("step3"):
            pref: dict[str, str] = {}
            for sid, pds in pd_by_song.items():
                sname = song_name_map.get(sid, "曲目未設定")
                st.markdown(f"**🎵 {sname}**")
                for pd in pds:
                    pd_id   = pd.get("id","")
                    pd_name = ext(pd, PARTDEF_NAME_KEYS) or pd_id
                    val = st.selectbox(pd_name, PRIORITY_OPTS, index=3, key=f"pref_{pd_id}")
                    pref[pd_id] = val
                st.divider()
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_pref"] = pref
            st.session_state["form_step"] = 4
            st.rerun()

    # ── STEP 4: 所有楽器（Percのみ） ─────────────────────────
    elif step == 4:
        st.subheader("Step 4 / 所有楽器の台数を入力してください")
        st.caption("所有していない楽器は 0 のままで構いません。")

        if not req_insts:
            st.info("必要楽器の情報がありません。スキップします。")
            st.session_state["form_own"]  = {}
            st.session_state["form_step"] = 5
            st.rerun()
            return

        with st.form("step4"):
            own: dict[str, int] = {}
            for iid in req_insts:
                iname = inst_map.get(iid, iid)
                val = st.number_input(iname, min_value=0, max_value=10,
                                      step=1, value=0, key=f"own_{iid}")
                own[iid] = int(val)
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_own"]  = own
            st.session_state["form_step"] = 5
            st.rerun()

    # ── STEP 5: 確認・送信 ───────────────────────────────────
    elif step == 5:
        player_id    = st.session_state.get("form_player_id","")
        player_name  = st.session_state.get("form_player_name","")
        part         = st.session_state.get("form_player_part","")
        att          = st.session_state.get("form_att",  {})
        pref         = st.session_state.get("form_pref", {})
        own          = st.session_state.get("form_own",  {})
        concert_name = ext(concert, CONCERT_NAME_KEYS) or ""

        st.subheader("Step 5 / 内容を確認して送信してください")
        st.markdown(f"**氏名：** {player_name}　　**パート：** {part}")

        if att:
            with st.expander("出欠", expanded=True):
                prac_map = {p.get("id",""): ext(p, PRACTICE_NAME_KEYS) or "" for p in practices}
                for pr_id, status in att.items():
                    st.write(f"{prac_map.get(pr_id, pr_id)}：**{status}**")
                st.caption("※ 本番当日は自動で○が登録されます。")

        if pref:
            with st.expander("パート希望", expanded=True):
                pd_map = {p.get("id",""): ext(p, PARTDEF_NAME_KEYS) or "" for p in partdefs}
                for pd_id, priority in pref.items():
                    st.write(f"{pd_map.get(pd_id, pd_id)}：**{priority}**")

        owned = {iid: cnt for iid, cnt in own.items() if cnt > 0}
        if owned:
            with st.expander("所有楽器", expanded=True):
                for iid, cnt in owned.items():
                    st.write(f"{inst_map.get(iid, iid)}：{cnt}台")

        col1, col2 = st.columns(2)
        if col1.button("← 修正する", use_container_width=True):
            st.session_state["form_step"] = 2
            st.rerun()

        if col2.button("✅ 送信する", type="primary", use_container_width=True):
            with st.spinner("送信中..."):
                ok_n, errors, debug = _submit_all(
                    ctx, concert_id, concert_name,
                    player_id, player_name, att, pref, own
                )
            st.session_state["form_submit_count"] = ok_n
            st.session_state["form_submit_errors"] = errors
            st.session_state["form_submit_debug"]  = debug
            st.session_state["form_step"] = 6
            st.rerun()

    # ── STEP 6: 完了 ─────────────────────────────────────────
    elif step == 6:
        player_name = st.session_state.get("form_player_name","")
        ok_n        = st.session_state.get("form_submit_count", 0)
        errors      = st.session_state.get("form_submit_errors", [])
        debug       = st.session_state.get("form_submit_debug", {})

        if errors:
            st.warning(f"⚠️ 一部の登録に失敗しました。")
            for e in errors:
                st.error(e)
        else:
            st.balloons()
            st.success(f"✅ 送信完了！ {ok_n}件のデータが登録されました。")
        st.markdown(f"**{player_name}** さん、ありがとうございました。")
        st.info("このページを閉じて構いません。")

        # テスト用デバッグ情報（URLにdebug=1がある場合のみ表示）
        if st.query_params.get("debug") == "1" and debug:
            with st.expander("🔧 デバッグ情報", expanded=True):
                for k, v in debug.items():
                    st.code(f"{k}: {v}")


# ── 管理者：URL生成UI ─────────────────────────────────────────

def _delete_form_test_data(ctx, concert_id: str) -> dict:
    """フォームから送信されたテストデータを一括削除。
    対象：指定演奏会に紐づくATTENDANCE・CONCERT_CAST・PREFERENCE・PLAYER_INSTRUMENT。
    PERFORMERは[TEST]プレフィックスのもののみ削除。
    """
    ext_rel = ctx["extract_relation_ids_any"]
    ext     = ctx["extract_prop_text_any"]
    summary: dict[str, int] = {}

    def archive(page_id: str) -> bool:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                                 json={"archived": True})
        return res is not None and res.status_code == 200

    # 1. この演奏会のCONCERT_CASTを取得（後続の削除に使用）
    all_cast = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    cast_rows = [r for r in all_cast
                 if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    cast_ids = {r.get("id","") for r in cast_rows}

    # 2. ATTENDANCE（cast_idまたはplayer_idでリレーション）
    all_att = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    att_count = 0
    for r in all_att:
        pl_ids = ext_rel(r, ATT_PLAYER_REL_KEYS)
        pr_ids = ext_rel(r, ATT_PRACTICE_REL_KEYS)
        # 練習がこの演奏会に紐づいているか確認
        all_pr = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
        pr_concert_map = {p.get("id",""): ext_rel(p, PRACTICE_CONCERT_REL_KEYS) for p in all_pr}
        if any(concert_id in pr_concert_map.get(pr_id, []) for pr_id in pr_ids):
            if archive(r.get("id","")):
                att_count += 1
    if att_count: summary["ATTENDANCE"] = att_count

    # 3. PREFERENCE（player_idで紐づく、この演奏会のパート定義に関連）
    all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    song_ids = {s.get("id","") for s in all_songs
                if concert_id in ext_rel(s, SONG_CONCERT_REL_KEYS)}
    all_pd = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    pd_ids = {p.get("id","") for p in all_pd
              if any(sid in ext_rel(p, PARTDEF_SONG_REL_KEYS) for sid in song_ids)}
    all_pref = ctx["query_all"](ctx["CONCERT_DB_PREFERENCE"], None)
    pref_count = 0
    for r in all_pref:
        if any(pd_id in ext_rel(r, PREF_PART_REL_KEYS) for pd_id in pd_ids):
            if archive(r.get("id","")): pref_count += 1
    if pref_count: summary["PREFERENCE"] = pref_count

    # 4. PLAYER_INSTRUMENT（この演奏会に紐づく）
    all_pi = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    pi_count = 0
    for r in all_pi:
        if concert_id in ext_rel(r, PI_CONCERT_REL_KEYS):
            if archive(r.get("id","")): pi_count += 1
    if pi_count: summary["PLAYER_INSTRUMENT"] = pi_count

    # 5. CONCERT_CAST
    cast_count = sum(1 for r in cast_rows if archive(r.get("id","")))
    if cast_count: summary["CONCERT_CAST"] = cast_count

    # 6. PERFORMER（[TEST]プレフィックスのみ）
    from concert.services.keys import PLAYER_NAME_KEYS as PNK
    all_pl = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    pl_count = 0
    for r in all_pl:
        if (ext(r, PNK) or "").startswith("[TEST]"):
            if archive(r.get("id","")): pl_count += 1
    if pl_count: summary["PERFORMER[TEST]"] = pl_count

    return summary


def render_url_generator(ctx: dict, concert_id: str, concert_name: str):
    if not concert_id:
        st.caption("演奏会を選択するとURLが生成されます。")
        return
    token    = make_form_token(concert_id)
    base_url = st.secrets.get("FORM_BASE_URL", "https://artemis-cers.streamlit.app")
    long_url = f"{base_url}/?concert={token}&cid={concert_id}"

    st.code(long_url, language=None)
    if st.button("🔗 TinyURLで短縮", key="shorten_url_btn", use_container_width=True):
        with st.spinner("短縮中..."):
            short = shorten_url(long_url)
        st.session_state["form_short_url"] = short
        st.rerun()

    short = st.session_state.get("form_short_url", "")
    if short:
        if short != long_url:
            st.success("短縮URL：")
            st.code(short, language=None)
            st.caption("テスト用（デバッグ情報表示）：")
            st.code(f"{long_url}&debug=1", language=None)
        else:
            st.warning("短縮に失敗しました。上のURLをそのまま使用してください。")

    st.divider()
    st.caption("🗑️ フォームテストデータ削除")
    st.caption("この演奏会に紐づくATTENDANCE・CONCERT_CAST・PREFERENCE・PLAYER_INSTRUMENTを削除します。PERFORMERは[TEST]プレフィックスのもののみ対象。")
    confirm = st.checkbox("削除対象を確認しました", key="form_test_delete_confirm")
    if st.button("🗑️ フォームテストデータを削除", type="secondary",
                 use_container_width=True, key="form_test_delete_btn",
                 disabled=not confirm):
        with st.spinner("削除中..."):
            summary = _delete_form_test_data(ctx, concert_id)
        if summary:
            st.success("✅ 削除完了")
            for k, v in summary.items():
                st.caption(f"  {k}: {v}件")
        else:
            st.info("削除対象が見つかりませんでした。")
