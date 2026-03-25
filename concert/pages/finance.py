"""
concert/pages/finance.py
演奏会の収支管理・振込管理・予算計算
"""
import streamlit as st
import pandas as pd
from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_CONFIRMED_FEE_KEYS,
    PARTICIPANT_RECORD_KEYS, PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_KEYS, PARTICIPANT_ROLE_KEYS,
    PARTICIPANT_FEE_KEYS, PARTICIPANT_PAID_KEYS,
    PLAYER_NAME_KEYS,
    EXPENSE_KEY_KEYS, EXPENSE_CONCERT_REL_KEYS, EXPENSE_TYPE_KEYS,
    EXPENSE_CONTENT_KEYS, EXPENSE_AMOUNT_KEYS, EXPENSE_CONFIRMED_KEYS,
    EXPENSE_NOTE_KEYS, EXPENSE_TYPE_OPTIONS,
)


# ============================================================
# キャッシュ
# ============================================================

def _clear_finance_cache(concert_id: str = ""):
    for k in list(st.session_state.keys()):
        if k.startswith("expense_list_") or k.startswith("cast_list_"):
            if not concert_id or concert_id in k:
                st.session_state.pop(k, None)


def _write_concert_fee(ctx, concert_id: str, fee: int) -> bool:
    """ATLASの演奏会レコードに確定参加費を書き込む。"""
    db_id = ctx["CONCERT_DB_CONCERT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    props: dict = {}
    ctx["put_prop_any"](props, t, CONCERT_CONFIRMED_FEE_KEYS, fee)
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{concert_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


def _read_concert_fee(ctx, concert_id: str) -> int:
    """ATLASの演奏会レコードから確定参加費を読み込む。"""
    db_id = ctx["CONCERT_DB_CONCERT"]
    res = ctx["api_request"]("get", f"https://api.notion.com/v1/pages/{concert_id}")
    if not res or res.status_code != 200:
        return 0
    props = res.json().get("properties", {})
    t = ctx["get_prop_types"](db_id)
    fee_key = ctx["find_prop_name"](t, CONCERT_CONFIRMED_FEE_KEYS) if t else None
    if not fee_key or fee_key not in props:
        return 0
    num = props[fee_key].get("number")
    try:
        return int(num) if num is not None else 0
    except Exception:
        return 0


def _load_expenses(ctx, concert_id: str) -> list[dict]:
    key = f"expense_list_{concert_id}"
    if key not in st.session_state:
        db_id = ctx.get("CONCERT_DB_CONCERT_EXPENSE", "")
        if not db_id:
            return []
        t   = ctx["get_prop_types"](db_id)
        rel = ctx["find_prop_name"](t, EXPENSE_CONCERT_REL_KEYS) if t else None
        f   = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


def _load_cast(ctx, concert_id: str) -> list[dict]:
    key = f"cast_list_{concert_id}"
    if key not in st.session_state:
        db_id = ctx["CONCERT_DB_PARTICIPANT"]
        t   = ctx["get_prop_types"](db_id)
        rel = ctx["find_prop_name"](t, PARTICIPANT_CONCERT_REL_KEYS) if t else None
        f   = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


# ============================================================
# CRUD
# ============================================================

def _upsert_expense(ctx, concert_id: str, concert_name: str,
                    type_: str, content: str, amount: int,
                    confirmed: bool, note: str,
                    existing_id: str = "") -> bool:
    db_id = ctx.get("CONCERT_DB_CONCERT_EXPENSE", "")
    if not db_id:
        st.error("経費DBのIDが未設定です。secrets.tomlに CONCERT_DB_CONCERT_EXPENSE を追加してください。")
        return False
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    props: dict = {}
    ctx["put_prop_any"](props, t, EXPENSE_KEY_KEYS,         f"{concert_name} / {type_} / {content}")
    ctx["put_prop_any"](props, t, EXPENSE_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, EXPENSE_TYPE_KEYS,        type_)
    ctx["put_prop_any"](props, t, EXPENSE_CONTENT_KEYS,     content)
    ctx["put_prop_any"](props, t, EXPENSE_AMOUNT_KEYS,      amount)
    ctx["put_prop_any"](props, t, EXPENSE_CONFIRMED_KEYS,   confirmed)
    ctx["put_prop_any"](props, t, EXPENSE_NOTE_KEYS,        note)
    if existing_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_cast_finance(ctx, page_id: str, part: str, role: str,
                         fee: int, paid: bool) -> bool:
    db_id = ctx["CONCERT_DB_PARTICIPANT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    props: dict = {}
    ctx["put_prop_any"](props, t, PARTICIPANT_PART_KEYS, part)
    ctx["put_prop_any"](props, t, PARTICIPANT_ROLE_KEYS, role)
    ctx["put_prop_any"](props, t, PARTICIPANT_FEE_KEYS,  fee)
    ctx["put_prop_any"](props, t, PARTICIPANT_PAID_KEYS, paid)
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# タブ：経費管理
# ============================================================

def _render_expense_tab(ctx, concert_id: str, concert_name: str):
    st.caption("演奏会の経費を登録・管理します。")

    col_h, col_r = st.columns([8, 1])
    col_h.markdown(f"**経費一覧**")
    if col_r.button("🔄", key="expense_refresh"):
        _clear_finance_cache(concert_id)
        st.rerun()

    expenses = _load_expenses(ctx, concert_id)
    ext = ctx["extract_prop_text_any"]

    # 既存データをDataFrameに
    exp_rows: list[dict] = []
    exp_meta: list[dict] = []
    for r in expenses:
        cur_type  = ext(r, EXPENSE_TYPE_KEYS)     or "その他"
        cur_cont  = ext(r, EXPENSE_CONTENT_KEYS)  or ""
        cur_amt_s = ext(r, EXPENSE_AMOUNT_KEYS)   or "0"
        cur_conf  = ext(r, EXPENSE_CONFIRMED_KEYS) == "True"
        cur_note  = ext(r, EXPENSE_NOTE_KEYS)     or ""
        try: cur_amt = int(float(cur_amt_s))
        except: cur_amt = 0
        exp_rows.append({
            "種別":   cur_type,
            "内容":   cur_cont,
            "金額":   cur_amt,
            "確定":   cur_conf,
            "備考":   cur_note,
        })
        exp_meta.append({
            "eid":      r.get("id", ""),
            "cur_type": cur_type, "cur_cont": cur_cont,
            "cur_amt":  cur_amt,  "cur_conf": cur_conf,
            "cur_note": cur_note,
        })

    # 空行1行（既存0件のとき）
    if not exp_rows:
        exp_rows.append({"種別": "その他", "内容": "", "金額": 0, "確定": False, "備考": ""})
        exp_meta.append({"eid": "", "cur_type": "", "cur_cont": "", "cur_amt": 0, "cur_conf": False, "cur_note": ""})

    editor_version = st.session_state.get("expense_editor_version", 0)
    df_exp = pd.DataFrame(exp_rows)
    edited_exp = st.data_editor(
        df_exp,
        num_rows="dynamic",
        use_container_width=True,
        key=f"expense_editor_{concert_id}_{editor_version}",
        column_config={
            "種別":   st.column_config.SelectboxColumn("種別", options=EXPENSE_TYPE_OPTIONS, required=True, default="その他"),
            "内容":   st.column_config.TextColumn("内容", max_chars=100),
            "金額":   st.column_config.NumberColumn("金額（円）", min_value=0, step=1000, default=0),
            "確定":   st.column_config.CheckboxColumn("確定", default=False),
            "備考":   st.column_config.TextColumn("備考", max_chars=100),
        },
    )

    # 小計表示
    try:
        total_all       = int(edited_exp["金額"].sum())
        total_confirmed = int(edited_exp[edited_exp["確定"] == True]["金額"].sum())
    except Exception:
        total_all = total_confirmed = 0

    c1, c2, c3 = st.columns(3)
    c1.metric("合計（全見積）",   f"¥{total_all:,}")
    c2.metric("確定済み合計",     f"¥{total_confirmed:,}")
    c3.metric("見積中",           f"¥{total_all - total_confirmed:,}")

    if st.button("💾 まとめて保存", type="primary", use_container_width=True, key="expense_save"):
        ok_n = ng_n = skip_n = 0
        with st.spinner("保存中..."):
            df_reset = edited_exp.reset_index(drop=True)
            for idx in range(len(df_reset)):
                row      = df_reset.iloc[idx]
                new_type = str(row.get("種別") or "その他").strip()
                new_cont = str(row.get("内容") or "").strip()
                new_amt  = int(row.get("金額") or 0)
                new_conf = bool(row.get("確定") or False)
                new_note = str(row.get("備考") or "").strip()

                if not new_cont and new_amt == 0:
                    skip_n += 1
                    continue

                eid = exp_meta[idx]["eid"] if idx < len(exp_meta) else ""
                ok  = _upsert_expense(ctx, concert_id, concert_name,
                                      new_type, new_cont, new_amt,
                                      new_conf, new_note, eid)
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1

        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        st.session_state["expense_editor_version"] = editor_version + 1
        _clear_finance_cache(concert_id)
        st.rerun()


# ============================================================
# タブ：予算計算機
# ============================================================

def _render_budget_tab(ctx, concert_id: str):
    st.caption("経費の積み上げから1人あたり参加費を試算します。")

    # ATLASの確定参加費を表示
    current_fee = _read_concert_fee(ctx, concert_id)
    if current_fee > 0:
        st.info(f"現在の確定参加費：**¥{current_fee:,}**　※一括設定ボタンで上書きされます")

    expenses  = _load_expenses(ctx, concert_id)
    cast_rows = _load_cast(ctx, concert_id)
    ext = ctx["extract_prop_text_any"]

    # 経費合計
    total_confirmed = 0
    total_estimate  = 0
    by_type: dict[str, int] = {}
    for r in expenses:
        amt_s = ext(r, EXPENSE_AMOUNT_KEYS) or "0"
        try: amt = int(float(amt_s))
        except: amt = 0
        conf = ext(r, EXPENSE_CONFIRMED_KEYS) == "True"
        type_ = ext(r, EXPENSE_TYPE_KEYS) or "その他"
        total_estimate += amt
        if conf:
            total_confirmed += amt
        by_type[type_] = by_type.get(type_, 0) + amt

    # 参加人数
    n_members = len(cast_rows)

    st.markdown("### 経費内訳")
    if by_type:
        df_type = pd.DataFrame([
            {"種別": k, "金額": v} for k, v in sorted(by_type.items(), key=lambda x: -x[1])
        ])
        st.dataframe(df_type, use_container_width=True, hide_index=True)
    else:
        st.info("経費が登録されていません。")

    st.divider()
    st.markdown("### 参加費試算")

    col1, col2 = st.columns(2)
    col1.metric("経費合計（全見積）", f"¥{total_estimate:,}")
    col1.metric("経費合計（確定済）", f"¥{total_confirmed:,}")
    col2.metric("参加予定人数", f"{n_members}人")

    st.markdown("**調整・試算**")
    extra = st.number_input("追加バッファ（円）", min_value=0, step=1000, value=0, key="budget_extra",
                             help="予備費や端数調整用")
    manual_members = st.number_input("試算人数（変更可）", min_value=1, value=max(n_members, 1), step=1,
                                      key="budget_members")
    round_unit = st.selectbox("端数処理（円単位）", [100, 500, 1000, 5000], index=2, key="budget_round")

    total_with_extra = total_estimate + extra
    per_person_raw   = total_with_extra / manual_members if manual_members > 0 else 0
    import math
    per_person = math.ceil(per_person_raw / round_unit) * round_unit

    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    c1.metric("総額（バッファ込）",   f"¥{total_with_extra:,}")
    c2.metric("1人あたり（切り上げ）", f"¥{per_person:,}")
    c3.metric("徴収総額",             f"¥{per_person * manual_members:,}")

    surplus = per_person * manual_members - total_with_extra
    st.caption(f"余剰：¥{surplus:,}（徴収総額 - 経費総額）")

    # 参加費を一括設定
    if st.button(f"💸 全員の参加費を ¥{per_person:,} に設定する",
                 key="budget_apply", use_container_width=True):
        ok_n = ng_n = 0
        with st.spinner("設定中..."):
            for r in cast_rows:
                rid  = r.get("id", "")
                part = ext(r, PARTICIPANT_PART_KEYS) or ""
                role = ext(r, PARTICIPANT_ROLE_KEYS) or ""
                paid = ext(r, PARTICIPANT_PAID_KEYS) == "True"
                ok   = _update_cast_finance(ctx, rid, part, role, per_person, paid)
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
        if ng_n == 0:
            # ATLASの確定参加費フィールドに書き込む
            _write_concert_fee(ctx, concert_id, per_person)
            # session_stateにも保存（新規参加者登録時の自動セット用）
            st.session_state[f"confirmed_fee_{concert_id}"] = per_person
            st.success(f"✅ {ok_n}人の参加費を ¥{per_person:,} に設定しました。新規参加者登録時も自動で ¥{per_person:,} が入力されます。")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        _clear_finance_cache(concert_id)
        st.rerun()


# ============================================================
# タブ：振込管理
# ============================================================

def _render_payment_tab(ctx, concert_id: str):
    st.caption("参加者の振込状況を管理します。")

    cast_rows = _load_cast(ctx, concert_id)
    if not cast_rows:
        st.info("参加者が登録されていません。先に『奏者・出欠』画面で参加者を登録してください。")
        return

    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    # 奏者名取得
    player_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {r.get("id", ""): ext(r, PLAYER_NAME_KEYS) or "" for r in player_rows}

    col_h, col_r = st.columns([8, 1])
    col_h.markdown("**振込状況一覧**")
    if col_r.button("🔄", key="payment_refresh"):
        _clear_finance_cache(concert_id)
        st.rerun()

    df_rows: list[dict] = []
    df_meta: list[dict] = []
    for r in sorted(cast_rows, key=lambda x: (
        ext(x, PARTICIPANT_PART_KEYS) or "",
        ext(x, PARTICIPANT_ROLE_KEYS) or "",
    )):
        rid   = r.get("id", "")
        pids  = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        pname = player_name_map.get(pids[0], "") if pids else ""
        part  = ext(r, PARTICIPANT_PART_KEYS) or ""
        role  = ext(r, PARTICIPANT_ROLE_KEYS) or ""
        fee_s = ext(r, PARTICIPANT_FEE_KEYS) or "0"
        paid  = ext(r, PARTICIPANT_PAID_KEYS) == "True"
        try: fee = int(float(fee_s))
        except: fee = 0

        df_rows.append({"氏名": pname, "パート": part, "役職": role,
                        "参加費": fee, "入金済": paid})
        df_meta.append({"rid": rid, "pname": pname,
                        "cur_part": part, "cur_role": role,
                        "cur_fee": fee, "cur_paid": paid})

    # 入金サマリ
    total_fee    = sum(m["cur_fee"] for m in df_meta)
    paid_fee     = sum(m["cur_fee"] for m in df_meta if m["cur_paid"])
    paid_count   = sum(1 for m in df_meta if m["cur_paid"])
    unpaid_count = len(df_meta) - paid_count

    c1, c2, c3 = st.columns(3)
    c1.metric("入金済",   f"{paid_count}人  ¥{paid_fee:,}")
    c2.metric("未入金",   f"{unpaid_count}人  ¥{total_fee - paid_fee:,}")
    c3.metric("合計",     f"{len(df_meta)}人  ¥{total_fee:,}")

    editor_version = st.session_state.get("payment_editor_version", 0)
    df_pay = pd.DataFrame(df_rows)
    edited_pay = st.data_editor(
        df_pay,
        num_rows="fixed",
        use_container_width=True,
        key=f"payment_editor_{concert_id}_{editor_version}",
        column_config={
            "氏名":   st.column_config.TextColumn("氏名", disabled=True),
            "パート": st.column_config.TextColumn("パート", disabled=True),
            "役職":   st.column_config.TextColumn("役職",  disabled=True),
            "参加費": st.column_config.NumberColumn("参加費（円）", min_value=0, step=100),
            "入金済": st.column_config.CheckboxColumn("入金済", default=False),
        },
    )

    if st.button("💾 まとめて保存", type="primary", use_container_width=True, key="payment_save"):
        ok_n = ng_n = skip_n = 0
        with st.spinner("保存中..."):
            df_reset = edited_pay.reset_index(drop=True)
            for idx, meta in enumerate(df_meta):
                if idx >= len(df_reset): break
                row      = df_reset.iloc[idx]
                new_fee  = int(row.get("参加費")  or 0)
                new_paid = bool(row.get("入金済") or False)
                if new_fee == meta["cur_fee"] and new_paid == meta["cur_paid"]:
                    skip_n += 1
                    continue
                ok = _update_cast_finance(ctx, meta["rid"],
                                          meta["cur_part"], meta["cur_role"],
                                          new_fee, new_paid)
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
        if ng_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        st.session_state["payment_editor_version"] = editor_version + 1
        _clear_finance_cache(concert_id)
        st.rerun()


# ============================================================
# メイン
# ============================================================

def _render_finance_pdf_tab(ctx: dict, concert_id: str, concert_name: str):
    st.caption("経費・参加費・振込状況をまとめた収支報告PDFを出力します。")

    col1, col2 = st.columns(2)
    col1.metric("経費登録",   f"{len(_load_expenses(ctx, concert_id))}件")
    cast = _load_cast(ctx, concert_id)
    paid = sum(1 for r in cast
               if ctx["extract_prop_text_any"](r, PARTICIPANT_PAID_KEYS) == "True")
    col2.metric("入金済",     f"{paid} / {len(cast)}人")

    if st.button("📊 収支報告PDFを出力", type="primary",
                 use_container_width=True, key="finance_pdf_btn"):
        with st.spinner("PDF生成中..."):
            try:
                from concert.services.finance_report import generate_finance_report
                pdf_bytes = generate_finance_report(ctx, concert_id)
                fname = f"収支報告_{concert_name or concert_id}.pdf"
                st.download_button(
                    label="⬇️ ダウンロード",
                    data=pdf_bytes,
                    file_name=fname,
                    mime="application/pdf",
                    key="finance_pdf_dl",
                )
            except Exception as e:
                st.error(f"PDF生成に失敗しました: {e}")


def render(ctx: dict):
    st.header("💰 収支・振込管理")

    concert_id   = (ctx.get("SELECTED_CONCERT_ID")   or "").strip()
    concert_name = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()
    if not concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    st.caption(f"対象演奏会: {concert_name or concert_id}")

    tab_expense, tab_budget, tab_payment, tab_pdf = st.tabs(["経費管理", "予算計算機", "振込管理", "収支報告PDF"])

    with tab_expense:
        _render_expense_tab(ctx, concert_id, concert_name)
    with tab_budget:
        _render_budget_tab(ctx, concert_id)
    with tab_payment:
        _render_payment_tab(ctx, concert_id)
    with tab_pdf:
        _render_finance_pdf_tab(ctx, concert_id, concert_name)
