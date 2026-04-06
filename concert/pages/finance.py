"""
concert/pages/finance.py
演奏会の収支管理・振込管理・予算計算
"""
import streamlit as st
import pandas as pd
import io
from datetime import date, timedelta
from concert.services.keys import (
    HARMONIA_CONCERT_KEY_KEYS, HARMONIA_CONCERT_CONCERT_REL_KEYS, HARMONIA_CONCERT_MANAGED_KEYS, HARMONIA_CONCERT_FINANCE_KEYS,
    CONCERT_NAME_KEYS, CONCERT_CONFIRMED_FEE_KEYS,
    PRACTICE_CONCERT_REL_KEYS,
    PARTICIPANT_RECORD_KEYS, PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_KEYS, PARTICIPANT_PART_REL_KEYS, PARTICIPANT_ROLE_KEYS, PARTICIPANT_ROLE_OPS_KEYS,
    PARTICIPANT_FEE_KEYS, PARTICIPANT_PAID_KEYS,
    PLAYER_NAME_KEYS,
    EXPENSE_KEY_KEYS, EXPENSE_CONCERT_REL_KEYS, EXPENSE_TYPE_KEYS,
    EXPENSE_CONTENT_KEYS, EXPENSE_AMOUNT_KEYS, EXPENSE_CONFIRMED_KEYS,
    EXPENSE_NOTE_KEYS, EXPENSE_TYPE_OPTIONS,
    RENTAL_PRACTICE_REL_KEYS, RENTAL_CONFIRMED_KEYS,
    BILLING_KEY_KEYS, BILLING_CONCERT_REL_KEYS, BILLING_DOC_TYPE_KEYS,
    BILLING_ISSUE_DATE_KEYS, BILLING_DUE_DATE_KEYS, BILLING_MEMBER_COUNT_KEYS,
    BILLING_PRACTICE_COUNT_KEYS, BILLING_OPTION_KEYS, BILLING_DISCOUNT_KEYS,
    BILLING_TAX_RATE_KEYS, BILLING_SUBTOTAL_KEYS, BILLING_TAX_KEYS,
    BILLING_TOTAL_KEYS, BILLING_MODE_KEYS, BILLING_NOTE_KEYS,
)
from concert.services.part_master_utils import load_part_master_map, build_player_part_map, part_id_from_name


def _normalize_page_id(v: str) -> str:
    return (v or "").replace("-", "").strip().lower()


def _find_prop_name_loose(ctx: dict, type_map: dict, candidates: list[str]) -> str:
    key = ctx["find_prop_name"](type_map, candidates)
    if key:
        return key
    norm_map = {str(k or "").replace(" ", "").replace("　", "").strip().lower(): k for k in (type_map or {}).keys()}
    for c in candidates:
        got = norm_map.get(str(c or "").replace(" ", "").replace("　", "").strip().lower())
        if got:
            return got
    return ""


# ============================================================
# キャッシュ
# ============================================================



def _load_harmonia_concert_row(ctx: dict, concert_id: str) -> dict:
    if not concert_id or not ctx.get("CONCERT_DB_HARMONIA_CONCERT"):
        return {}
    db_id = ctx["CONCERT_DB_HARMONIA_CONCERT"]
    t = ctx["get_prop_types"](db_id) or {}
    rel_key = _find_prop_name_loose(ctx, t, HARMONIA_CONCERT_CONCERT_REL_KEYS)
    target = _normalize_page_id(concert_id)
    rows = []
    if rel_key:
        rows = ctx["query_all"](db_id, {"filter": {"property": rel_key, "relation": {"contains": concert_id}}})
    if not rows:
        rows = ctx["query_all"](db_id)
    for r in rows:
        ids = ctx["extract_relation_ids_any"](r, [rel_key] if rel_key else HARMONIA_CONCERT_CONCERT_REL_KEYS)
        if any(_normalize_page_id(x) == target for x in ids):
            return r
    return {}


def _ensure_harmonia_concert_row(ctx: dict, concert_id: str, concert_name: str = "") -> tuple[dict, bool]:
    row = _load_harmonia_concert_row(ctx, concert_id)
    if row:
        return row, False
    db_id = ctx.get("CONCERT_DB_HARMONIA_CONCERT", "")
    if not db_id:
        return {}, False
    t = ctx["get_prop_types"](db_id) or {}
    props: dict = {}
    ctx["put_key_any"](props, t, HARMONIA_CONCERT_KEY_KEYS, concert_id, concert_name or concert_id, prefix="harmonia")
    ctx["put_prop_any"](props, t, HARMONIA_CONCERT_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, HARMONIA_CONCERT_MANAGED_KEYS, True)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    if res is not None and res.status_code == 200:
        return res.json() or {}, True
    return {}, False


def _set_harmonia_concert_checkbox(ctx: dict, concert_id: str, key_candidates: list[str], checked: bool, concert_name: str = "") -> bool:
    row, _ = _ensure_harmonia_concert_row(ctx, concert_id, concert_name)
    if not row:
        return False
    db_id = ctx.get("CONCERT_DB_HARMONIA_CONCERT", "")
    t = ctx["get_prop_types"](db_id) or {}
    flag_key = _find_prop_name_loose(ctx, t, key_candidates)
    if not flag_key:
        return False
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{row.get('id','')}", json={"properties": {flag_key: {"checkbox": bool(checked)}}})
    return res is not None and res.status_code == 200

def _clear_finance_cache(concert_id: str = ""):
    for k in list(st.session_state.keys()):
        if k.startswith(("expense_list_", "cast_list_", "billing_list_")):
            if not concert_id or concert_id in k:
                st.session_state.pop(k, None)
    st.cache_data.clear()  # Notionクエリキャッシュを無効化
    for _k in [k for k in st.session_state if k.startswith("harmonia_preloaded_")]:
        st.session_state.pop(_k, None)  # 次回ホームで再プリフェッチ


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


def _count_practices(ctx, concert_id: str) -> int:
    """対象演奏会に紐づく練習回数を返す。"""
    db_id = ctx.get("CONCERT_DB_PRACTICE", "")
    if not db_id:
        return 0
    t = ctx["get_prop_types"](db_id)
    rel = ctx["find_prop_name"](t, PRACTICE_CONCERT_REL_KEYS) if t else None
    if not rel:
        return 0
    rows = ctx["query_all"](db_id, {"filter": {"property": rel, "relation": {"contains": concert_id}}})
    return len(rows or [])


def _list_practice_ids(ctx, concert_id: str) -> list[str]:
    db_id = ctx.get("CONCERT_DB_PRACTICE", "")
    if not db_id:
        return []
    t = ctx["get_prop_types"](db_id)
    rel = ctx["find_prop_name"](t, PRACTICE_CONCERT_REL_KEYS) if t else None
    rows = ctx["query_all"](db_id, {"filter": {"property": rel, "relation": {"contains": concert_id}}}) if rel else []
    return [r.get("id", "") for r in (rows or []) if r.get("id", "")]


def _count_unconfirmed_rentals(ctx, concert_id: str) -> int:
    db_id = (ctx.get("CONCERT_DB_RENTAL") or "").strip()
    if not db_id:
        return 0
    practice_ids = set(_list_practice_ids(ctx, concert_id))
    rows = ctx["query_all"](db_id, None)
    count = 0
    for r in rows:
        rel_practice = set(ctx["extract_relation_ids_any"](r, RENTAL_PRACTICE_REL_KEYS))
        if practice_ids and not practice_ids.intersection(rel_practice):
            continue
        if ctx["extract_prop_text_any"](r, RENTAL_CONFIRMED_KEYS) != "True":
            count += 1
    return count


def _load_billing_rows(ctx, concert_id: str) -> list[dict]:
    db_id = (ctx.get("CONCERT_DB_BILLING") or "").strip()
    if not db_id:
        return []
    key = f"billing_list_{concert_id}"
    if key not in st.session_state:
        t = ctx["get_prop_types"](db_id)
        rel = ctx["find_prop_name"](t, BILLING_CONCERT_REL_KEYS) if t else None
        f = {"filter": {"property": rel, "relation": {"contains": concert_id}}} if rel else None
        st.session_state[key] = ctx["query_all"](db_id, f)
    return st.session_state.get(key, [])


def _clear_billing_cache(concert_id: str):
    st.session_state.pop(f"billing_list_{concert_id}", None)
    st.cache_data.clear()  # Notionクエリキャッシュを無効化
    for _k in [k for k in st.session_state if k.startswith("harmonia_preloaded_")]:
        st.session_state.pop(_k, None)  # 次回ホームで再プリフェッチ


def _save_billing_record(
    ctx,
    concert_id: str,
    doc_type: str,
    mode: str,
    issue_on: date,
    due_on: date,
    calc: dict,
    note: str = "",
) -> bool:
    db_id = (ctx.get("CONCERT_DB_BILLING") or "").strip()
    if not db_id:
        return False
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False

    # 同一キー（演奏会×書類種別）で上書き
    rows = _load_billing_rows(ctx, concert_id)
    ext = ctx["extract_prop_text_any"]
    target_id = ""
    for r in rows:
        if (ext(r, BILLING_DOC_TYPE_KEYS) or "").strip() == doc_type:
            target_id = r.get("id", "")
            break

    props: dict = {}
    ctx["put_key_any"](props, t, BILLING_KEY_KEYS, concert_id, doc_type, prefix="billing")
    ctx["put_prop_any"](props, t, BILLING_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, BILLING_DOC_TYPE_KEYS, doc_type)
    ctx["put_prop_any"](props, t, BILLING_MODE_KEYS, mode)
    ctx["put_prop_any"](props, t, BILLING_ISSUE_DATE_KEYS, issue_on.isoformat())
    ctx["put_prop_any"](props, t, BILLING_DUE_DATE_KEYS, due_on.isoformat())
    ctx["put_prop_any"](props, t, BILLING_MEMBER_COUNT_KEYS, int(calc.get("member_count", 0)))
    ctx["put_prop_any"](props, t, BILLING_PRACTICE_COUNT_KEYS, int(calc.get("practice_count", 0)))
    ctx["put_prop_any"](props, t, BILLING_OPTION_KEYS, int(calc.get("option_actual", 0)))
    ctx["put_prop_any"](props, t, BILLING_DISCOUNT_KEYS, int(calc.get("discount_applied", 0)))
    ctx["put_prop_any"](props, t, BILLING_TAX_RATE_KEYS, 10)
    ctx["put_prop_any"](props, t, BILLING_SUBTOTAL_KEYS, int(calc.get("subtotal", 0)))
    ctx["put_prop_any"](props, t, BILLING_TAX_KEYS, int(calc.get("tax", 0)))
    ctx["put_prop_any"](props, t, BILLING_TOTAL_KEYS, int(calc.get("total", 0)))
    ctx["put_prop_any"](props, t, BILLING_NOTE_KEYS, note)

    if target_id:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{target_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id}, "properties": props})
    ok = res is not None and res.status_code == 200
    if ok:
        _clear_billing_cache(concert_id)
    return ok


def _build_billing_pdf(doc_title: str, concert_name: str, issue_on: date, due_on: date, calc: dict) -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.pdfgen import canvas

    font_regular = "Helvetica"
    font_bold = "Helvetica-Bold"
    for f in [
        "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf",
        "C:/Windows/Fonts/msgothic.ttc",
    ]:
        try:
            pdfmetrics.registerFont(TTFont("JPRegular", f))
            pdfmetrics.registerFont(TTFont("JPBold", f))
            font_regular = "JPRegular"
            font_bold = "JPBold"
            break
        except Exception:
            pass

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4
    y = h - 18 * mm

    c.setFont(font_bold, 16)
    c.drawString(18 * mm, y, doc_title)
    y -= 10 * mm
    c.setFont(font_regular, 10)
    c.drawString(18 * mm, y, f"対象演奏会: {concert_name or '-'}")
    y -= 6 * mm
    c.drawString(18 * mm, y, f"発行日: {issue_on.isoformat()}    支払期限: {due_on.isoformat()}")
    y -= 10 * mm

    lines = [
        ("基本料", calc["base_fee"]),
        (f"参加者数×100円 ({calc['member_count']}人)", calc["participant_fee"]),
        (f"練習回数×800円 ({calc['practice_count']}回)", calc["practice_fee"]),
        ("オプション実費", calc["option_actual"]),
        ("税抜小計", calc["subtotal"]),
        ("出精値引き", -calc["discount_applied"]),
        ("消費税(10%)", calc["tax"]),
        ("税込合計", calc["total"]),
    ]

    for label, amount in lines:
        c.setFont(font_regular if label != "税込合計" else font_bold, 11)
        c.drawString(22 * mm, y, label)
        c.drawRightString(w - 18 * mm, y, f"¥{amount:,}")
        y -= 7 * mm

    c.showPage()
    c.save()
    return buf.getvalue()


def _build_receipt_pdf(
    concert_name: str,
    receipt_date: date,
    amount_total: int,
    addressee: str,
    item_label: str,
    payment_method: str,
    issuer_name: str,
) -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.pdfgen import canvas

    font_regular = "Helvetica"
    font_bold = "Helvetica-Bold"
    for f in [
        "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf",
        "C:/Windows/Fonts/msgothic.ttc",
    ]:
        try:
            pdfmetrics.registerFont(TTFont("JPRegular", f))
            pdfmetrics.registerFont(TTFont("JPBold", f))
            font_regular = "JPRegular"
            font_bold = "JPBold"
            break
        except Exception:
            pass

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4
    y = h - 20 * mm
    c.setFont(font_bold, 20)
    c.drawString(18 * mm, y, "領収書")
    y -= 10 * mm

    c.setFont(font_regular, 11)
    if addressee.strip():
        c.drawString(18 * mm, y, f"{addressee.strip()} 御中")
        y -= 8 * mm

    c.setFont(font_bold, 14)
    c.drawString(18 * mm, y, f"¥ {int(amount_total):,}")
    y -= 8 * mm
    c.setFont(font_regular, 10)
    c.drawString(18 * mm, y, f"但し、{item_label.strip() or '管理代行費として'}")
    y -= 8 * mm
    c.drawString(18 * mm, y, f"対象演奏会: {concert_name or '-'}")
    y -= 6 * mm
    c.drawString(18 * mm, y, f"受領日: {receipt_date.isoformat()}    受領方法: {payment_method}")
    y -= 10 * mm
    c.drawString(18 * mm, y, f"発行者: {issuer_name.strip() or 'ArtéMis HARMONIA'}")

    c.showPage()
    c.save()
    return buf.getvalue()


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
    # PKは共通ルール（put_key_any）で必ず投入する
    ctx["put_key_any"](props, t, EXPENSE_KEY_KEYS, concert_id, type_, content, prefix="expense")
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
                         fee: int, paid: bool, role_ops: str = "") -> bool:
    db_id = ctx["CONCERT_DB_PARTICIPANT"]
    t = ctx["get_prop_types"](db_id)
    if not t:
        return False
    props: dict = {}
    if part:
        pm_map_f = load_part_master_map(ctx)
        pm_id_f  = part_id_from_name(pm_map_f, part)
        if pm_id_f:
            ctx["put_prop_any"](props, t, PARTICIPANT_PART_REL_KEYS, pm_id_f)
    ctx["put_prop_any"](props, t, PARTICIPANT_ROLE_KEYS,     role)
    ctx["put_prop_any"](props, t, PARTICIPANT_ROLE_OPS_KEYS, role_ops)
    ctx["put_prop_any"](props, t, PARTICIPANT_FEE_KEYS,      fee)
    ctx["put_prop_any"](props, t, PARTICIPANT_PAID_KEYS,     paid)
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
    st.caption("想定参加費 × 試算人数で集まる金額を確認し、想定予算との差額を見ます。")

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
        try:
            amt = int(float(amt_s))
        except Exception:
            amt = 0
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
    col1.metric("想定予算（全見積）", f"¥{total_estimate:,}")
    col1.metric("想定予算（確定済）", f"¥{total_confirmed:,}")
    col2.metric("現在の奏者数", f"{n_members}人")

    st.markdown("**試算条件**")
    manual_members = st.number_input(
        "試算人数（変更可）",
        min_value=1,
        value=max(n_members, 1),
        step=1,
        key="budget_members",
    )
    assumed_fee_default = current_fee if current_fee > 0 else 10000
    assumed_fee = st.number_input(
        "想定参加費（円）",
        min_value=0,
        step=100,
        value=assumed_fee_default,
        key="budget_assumed_fee",
        help="この金額を全員から集める想定で試算します。",
    )

    collected_total = int(assumed_fee) * int(manual_members)
    diff_vs_budget = collected_total - total_estimate

    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    c1.metric("想定参加費", f"¥{int(assumed_fee):,}")
    c2.metric("集まる想定額", f"¥{collected_total:,}")
    c3.metric("想定予算との差額", f"¥{diff_vs_budget:,}")

    if diff_vs_budget >= 0:
        st.caption(f"想定予算に対して **+¥{diff_vs_budget:,}** です。")
    else:
        st.caption(f"想定予算に対して **-¥{abs(diff_vs_budget):,}** です。")

    # 参加費を一括設定
    if st.button(
        f"💸 全員の参加費を ¥{int(assumed_fee):,} に設定する",
        key="budget_apply",
        use_container_width=True,
    ):
        ok_n = ng_n = 0
        _pm_map_fb = load_part_master_map(ctx)
        with st.spinner("設定中..."):
            for r in cast_rows:
                rid  = r.get("id", "")
                _pm_ids_f = ctx["extract_relation_ids_any"](r, PARTICIPANT_PART_REL_KEYS)
                part = _pm_map_fb.get(_pm_ids_f[0], {}).get("name", "") if _pm_ids_f else ""
                role = ext(r, PARTICIPANT_ROLE_KEYS) or ""
                paid = ext(r, PARTICIPANT_PAID_KEYS) == "True"
                role_ops = ext(r, PARTICIPANT_ROLE_OPS_KEYS) or ""
                ok   = _update_cast_finance(ctx, rid, part, role, int(assumed_fee), paid, role_ops=role_ops)
                ok_n += 1 if ok else 0
                ng_n += 0 if ok else 1
        if ng_n == 0:
            _write_concert_fee(ctx, concert_id, int(assumed_fee))
            st.session_state[f"confirmed_fee_{concert_id}"] = int(assumed_fee)
            st.success(
                f"✅ {ok_n}人の参加費を ¥{int(assumed_fee):,} に設定しました。"
                f" 新規参加者登録時も自動で ¥{int(assumed_fee):,} が入力されます。"
            )
        else:
            st.warning(f"⚠️ {ok_n}件成功、{ng_n}件失敗")
        _clear_finance_cache(concert_id)
        st.rerun()


def _render_billing_tab(ctx, concert_id: str):
    st.caption("管理代行費の見積・請求・領収（収支計算とは分離）")
    st.info("このタブの金額は請求用です。演奏会の収支・参加費には反映しません。")
    st.caption("料金式: 基本料 5,000円 + 参加者数 × 100円 + 練習回数 × 800円 + オプション実費")
    if not (ctx.get("CONCERT_DB_BILLING") or "").strip():
        st.warning("ℹ️ 見積/請求/領収DB未設定のため、保存は無効です。secrets.toml に `CONCERT_DB_BILLING` を追加してください。")

    def _calc(member_count: int, practice_count: int, option_actual: int, discount: int) -> dict:
        base_fee = 5000
        participant_fee_unit = 100
        practice_fee_unit = 800
        subtotal = (
            base_fee
            + (int(member_count) * participant_fee_unit)
            + (int(practice_count) * practice_fee_unit)
            + int(option_actual)
        )
        discount_applied = min(int(discount), max(subtotal, 0))
        taxable = max(subtotal - discount_applied, 0)
        tax = int(round(taxable * 0.10))
        total = taxable + tax
        return {
            "base_fee": base_fee,
            "participant_fee": int(member_count) * participant_fee_unit,
            "practice_fee": int(practice_count) * practice_fee_unit,
            "option_actual": int(option_actual),
            "subtotal": subtotal,
            "discount_applied": discount_applied,
            "tax": tax,
            "total": total,
            "member_count": int(member_count),
            "practice_count": int(practice_count),
        }

    def _to_int(v, default=0):
        try:
            return int(float(v))
        except Exception:
            return int(default)

    linked_members = len(_load_cast(ctx, concert_id))
    linked_practices = _count_practices(ctx, concert_id)
    issue_default = date.today()
    due_default = issue_default + timedelta(days=14)
    ext = ctx["extract_prop_text_any"]
    billing_rows = _load_billing_rows(ctx, concert_id)

    def _find_doc_row(doc_type: str) -> dict | None:
        for r in billing_rows:
            if (ext(r, BILLING_DOC_TYPE_KEYS) or "").strip() == doc_type:
                return r
        return None

    est_saved = _find_doc_row("見積")
    inv_saved = _find_doc_row("請求")

    tab_est, tab_inv, tab_rec = st.tabs(["見積（自由入力）", "請求（実績連動）", "領収書（請求連動）"])

    with tab_est:
        st.markdown("#### 見積（自由入力）")
        c1, c2, c3 = st.columns(3)
        est_members_default = max(linked_members, 0)
        est_practices_default = max(linked_practices, 0)
        est_option_default = 0
        est_discount_default = 0
        est_issue_default = issue_default
        est_due_default = due_default
        if est_saved:
            est_members_default = _to_int(ext(est_saved, BILLING_MEMBER_COUNT_KEYS), est_members_default)
            est_practices_default = _to_int(ext(est_saved, BILLING_PRACTICE_COUNT_KEYS), est_practices_default)
            est_option_default = _to_int(ext(est_saved, BILLING_OPTION_KEYS), 0)
            est_discount_default = _to_int(ext(est_saved, BILLING_DISCOUNT_KEYS), 0)
            try:
                d1 = (ext(est_saved, BILLING_ISSUE_DATE_KEYS) or "")[:10]
                d2 = (ext(est_saved, BILLING_DUE_DATE_KEYS) or "")[:10]
                if d1:
                    y, m, d = map(int, d1.split("-")); est_issue_default = date(y, m, d)
                if d2:
                    y, m, d = map(int, d2.split("-")); est_due_default = date(y, m, d)
            except Exception:
                pass
        est_members = c1.number_input("見積参加者数", min_value=0, value=est_members_default, step=1, key="billing_est_members")
        est_practices = c2.number_input("見積練習回数", min_value=0, value=est_practices_default, step=1, key="billing_est_practices")
        c3.number_input("税率(%)", min_value=10, max_value=10, value=10, step=1, key="billing_est_tax", disabled=True)
        est_option = st.number_input("オプション実費（円）", min_value=0, step=1000, value=est_option_default, key="billing_est_option")
        est_discount = st.number_input("出精値引き（円）", min_value=0, step=1000, value=est_discount_default, key="billing_est_discount")
        est_issue = st.date_input("発行日", value=est_issue_default, key="billing_est_issue")
        est_due = st.date_input("支払期限", value=est_due_default, key="billing_est_due")

        est_calc = _calc(est_members, est_practices, est_option, est_discount)
        e1, e2, e3, e4 = st.columns(4)
        e1.metric("税抜小計", f"¥{est_calc['subtotal']:,}")
        e2.metric("値引き", f"-¥{est_calc['discount_applied']:,}")
        e3.metric("消費税(10%)", f"¥{est_calc['tax']:,}")
        e4.metric("税込合計", f"¥{est_calc['total']:,}")

        if st.button("💾 見積データを保存", key="billing_est_save", use_container_width=True):
            ok = _save_billing_record(ctx, concert_id, "見積", "自由入力", est_issue, est_due, est_calc)
            if ok:
                st.success("✅ 見積データをDBに保存しました。")
            else:
                st.warning("⚠️ 見積データ保存に失敗しました。（CONCERT_DB_BILLING を確認）")

        pdf_est = _build_billing_pdf("見積書", ctx.get("SELECTED_CONCERT_NAME", ""), est_issue, est_due, est_calc)
        st.download_button(
            "📄 見積書PDFを出力",
            data=pdf_est,
            file_name=f"見積書_{(ctx.get('SELECTED_CONCERT_NAME') or concert_id)}.pdf",
            mime="application/pdf",
            key="billing_est_pdf",
            use_container_width=True,
        )

    with tab_inv:
        st.markdown("#### 請求（実績連動）")
        c1, c2, c3 = st.columns(3)
        c1.metric("参加者数（実績）", f"{linked_members}人")
        c2.metric("練習回数（実績）", f"{linked_practices}回")
        c3.metric("税率", "10%")
        inv_option_default = 0
        inv_discount_default = 0
        inv_issue_default = issue_default
        inv_due_default = due_default
        if inv_saved:
            inv_option_default = _to_int(ext(inv_saved, BILLING_OPTION_KEYS), 0)
            inv_discount_default = _to_int(ext(inv_saved, BILLING_DISCOUNT_KEYS), 0)
            try:
                d1 = (ext(inv_saved, BILLING_ISSUE_DATE_KEYS) or "")[:10]
                d2 = (ext(inv_saved, BILLING_DUE_DATE_KEYS) or "")[:10]
                if d1:
                    y, m, d = map(int, d1.split("-")); inv_issue_default = date(y, m, d)
                if d2:
                    y, m, d = map(int, d2.split("-")); inv_due_default = date(y, m, d)
            except Exception:
                pass
        inv_option = st.number_input("オプション実費（円）", min_value=0, step=1000, value=inv_option_default, key="billing_inv_option")
        inv_discount = st.number_input("出精値引き（円）", min_value=0, step=1000, value=inv_discount_default, key="billing_inv_discount")
        inv_issue = st.date_input("発行日", value=inv_issue_default, key="billing_inv_issue")
        inv_due = st.date_input("支払期限", value=inv_due_default, key="billing_inv_due")

        inv_calc = _calc(linked_members, linked_practices, inv_option, inv_discount)
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("税抜小計", f"¥{inv_calc['subtotal']:,}")
        i2.metric("値引き", f"-¥{inv_calc['discount_applied']:,}")
        i3.metric("消費税(10%)", f"¥{inv_calc['tax']:,}")
        i4.metric("税込合計", f"¥{inv_calc['total']:,}")

        if st.button("💾 請求データを保存", key="billing_inv_save", use_container_width=True):
            ok = _save_billing_record(ctx, concert_id, "請求", "実績連動", inv_issue, inv_due, inv_calc)
            if ok:
                st.success("✅ 請求データをDBに保存しました。")
                _clear_billing_cache(concert_id)
                st.rerun()
            else:
                st.warning("⚠️ 請求データ保存に失敗しました。（CONCERT_DB_BILLING を確認）")

        pdf_inv = _build_billing_pdf("請求書", ctx.get("SELECTED_CONCERT_NAME", ""), inv_issue, inv_due, inv_calc)
        st.download_button(
            "🧾 請求書PDFを出力",
            data=pdf_inv,
            file_name=f"請求書_{(ctx.get('SELECTED_CONCERT_NAME') or concert_id)}.pdf",
            mime="application/pdf",
            key="billing_inv_pdf",
            use_container_width=True,
        )

    with tab_rec:
        st.markdown("#### 領収書（請求データ連動）")
        rec_saved = _find_doc_row("領収")
        if not inv_saved:
            st.warning("先に『請求（実績連動）』で請求データを保存してください。領収書は請求データに連動します。")
            return

        inv_total = _to_int(ext(inv_saved, BILLING_TOTAL_KEYS), 0)
        inv_issue_str = (ext(inv_saved, BILLING_ISSUE_DATE_KEYS) or "")[:10]
        rec_date_default = issue_default
        if inv_issue_str:
            try:
                y, m, d = map(int, inv_issue_str.split("-"))
                rec_date_default = date(y, m, d)
            except Exception:
                pass
        if rec_saved:
            ds = (ext(rec_saved, BILLING_ISSUE_DATE_KEYS) or "")[:10]
            if ds:
                try:
                    y, m, d = map(int, ds.split("-"))
                    rec_date_default = date(y, m, d)
                except Exception:
                    pass

        addressee_default = ""
        item_default = "管理代行費として"
        pay_methods = ["銀行振込", "現金", "その他"]
        pay_method_default = "銀行振込"
        issuer_default = "ArtéMis HARMONIA"
        if rec_saved:
            note_text = ext(rec_saved, BILLING_NOTE_KEYS) or ""
            for piece in [x.strip() for x in note_text.split("/") if x.strip()]:
                if piece.startswith("宛名:"):
                    addressee_default = piece.replace("宛名:", "", 1).strip()
                elif piece.startswith("但し書き:"):
                    item_default = piece.replace("但し書き:", "", 1).strip() or item_default
                elif piece.startswith("受領方法:"):
                    pm = piece.replace("受領方法:", "", 1).strip()
                    if pm in pay_methods:
                        pay_method_default = pm
                elif piece.startswith("発行者:"):
                    issuer_default = piece.replace("発行者:", "", 1).strip() or issuer_default

        st.metric("請求連動金額", f"¥{inv_total:,}")
        rec_date = st.date_input("受領日", value=rec_date_default, key="billing_rec_date")
        addressee = st.text_input("宛名", value=addressee_default, key="billing_rec_addressee", placeholder="例: Happy Hour Orchestre 御中")
        item_label = st.text_input("但し書き", value=item_default, key="billing_rec_item")
        payment_method = st.selectbox("受領方法", pay_methods, index=pay_methods.index(pay_method_default), key="billing_rec_method")
        issuer_name = st.text_input("発行者", value=issuer_default, key="billing_rec_issuer")

        rec_calc = {
            "member_count": _to_int(ext(inv_saved, BILLING_MEMBER_COUNT_KEYS), 0),
            "practice_count": _to_int(ext(inv_saved, BILLING_PRACTICE_COUNT_KEYS), 0),
            "option_actual": _to_int(ext(inv_saved, BILLING_OPTION_KEYS), 0),
            "discount_applied": _to_int(ext(inv_saved, BILLING_DISCOUNT_KEYS), 0),
            "subtotal": _to_int(ext(inv_saved, BILLING_SUBTOTAL_KEYS), 0),
            "tax": _to_int(ext(inv_saved, BILLING_TAX_KEYS), 0),
            "total": inv_total,
            "base_fee": 5000,
            "participant_fee": _to_int(ext(inv_saved, BILLING_MEMBER_COUNT_KEYS), 0) * 100,
            "practice_fee": _to_int(ext(inv_saved, BILLING_PRACTICE_COUNT_KEYS), 0) * 800,
        }
        if st.button("💾 領収データを保存", key="billing_rec_save", use_container_width=True):
            note = f"宛名:{addressee} / 但し書き:{item_label} / 受領方法:{payment_method} / 発行者:{issuer_name}"
            ok = _save_billing_record(ctx, concert_id, "領収", "請求連動", rec_date, rec_date, rec_calc, note=note)
            if ok:
                st.success("✅ 領収データをDBに保存しました。")
            else:
                st.warning("⚠️ 領収データ保存に失敗しました。（CONCERT_DB_BILLING を確認）")

        pdf_rec = _build_receipt_pdf(
            concert_name=ctx.get("SELECTED_CONCERT_NAME", ""),
            receipt_date=rec_date,
            amount_total=inv_total,
            addressee=addressee,
            item_label=item_label,
            payment_method=payment_method,
            issuer_name=issuer_name,
        )
        st.download_button(
            "🧾 領収書PDFを出力",
            data=pdf_rec,
            file_name=f"領収書_{(ctx.get('SELECTED_CONCERT_NAME') or concert_id)}.pdf",
            mime="application/pdf",
            key="billing_rec_pdf",
            use_container_width=True,
        )


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
    _pm_map_fin = load_part_master_map(ctx)
    for r in sorted(cast_rows, key=lambda x: (
        _pm_map_fin.get((ctx["extract_relation_ids_any"](x, PARTICIPANT_PART_REL_KEYS) or [""])[0], {}).get("name", "") or "",
        ext(x, PARTICIPANT_ROLE_KEYS) or "",
    )):
        rid      = r.get("id", "")
        pids     = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        pname    = player_name_map.get(pids[0], "") if pids else ""
        _pm_ids_fin = ctx["extract_relation_ids_any"](r, PARTICIPANT_PART_REL_KEYS)
        part     = _pm_map_fin.get(_pm_ids_fin[0], {}).get("name", "") if _pm_ids_fin else ""
        role     = ext(r, PARTICIPANT_ROLE_KEYS)     or ""
        role_ops = ext(r, PARTICIPANT_ROLE_OPS_KEYS) or ""
        fee_s    = ext(r, PARTICIPANT_FEE_KEYS) or "0"
        paid     = ext(r, PARTICIPANT_PAID_KEYS) == "True"
        try: fee = int(float(fee_s))
        except: fee = 0

        df_rows.append({"氏名": pname, "パート": part, "役職(音楽)": role,
                        "役職(運営)": role_ops, "参加費": fee, "入金済": paid})
        df_meta.append({"rid": rid, "pname": pname,
                        "cur_part": part, "cur_role": role, "cur_role_ops": role_ops,
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
            "パート":     st.column_config.TextColumn("パート",     disabled=True),
            "役職(音楽)": st.column_config.TextColumn("役職(音楽)", disabled=True),
            "役職(運営)": st.column_config.TextColumn("役職(運営)", disabled=True),
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
                                          new_fee, new_paid,
                                          role_ops=meta["cur_role_ops"])
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

    expense_rows_for_confirm = _load_expenses(ctx, concert_id)
    unconfirmed_expense_count = sum(
        1 for r in expense_rows_for_confirm
        if ctx["extract_prop_text_any"](r, EXPENSE_CONFIRMED_KEYS) != "True"
    )
    unconfirmed_rental_count = _count_unconfirmed_rentals(ctx, concert_id)
    st.caption(
        f"確定チェック: 経費未確定 {unconfirmed_expense_count} 件 / レンタル未確定 {unconfirmed_rental_count} 件"
    )

    c1, c2 = st.columns(2)
    if c1.button("✅ 収支確定", key=f"finance_confirm_{concert_id}", use_container_width=True):
        if unconfirmed_expense_count > 0 or unconfirmed_rental_count > 0:
            st.error("未確定の経費またはレンタルが残っているため、収支確定できません。")
        elif _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_FINANCE_KEYS, True, concert_name):
            st.success("✅ 収支確定を反映しました。")
            _clear_finance_cache(concert_id)
            st.rerun()
        else:
            st.warning("HARMONIA_CONCERT の『収支確定』列が見つからないか、更新に失敗しました。")
    if c2.button("↩ 収支確定を解除", key=f"finance_unconfirm_{concert_id}", use_container_width=True):
        if _set_harmonia_concert_checkbox(ctx, concert_id, HARMONIA_CONCERT_FINANCE_KEYS, False, concert_name):
            st.success("↩ 収支確定を解除しました。")
            _clear_finance_cache(concert_id)
            st.rerun()
        else:
            st.warning("HARMONIA_CONCERT の『収支確定』列が見つからないか、更新に失敗しました。")

    tab_expense, tab_budget, tab_billing, tab_payment, tab_pdf = st.tabs(
        ["経費管理", "予算計算機", "見積・請求計算", "振込管理", "収支報告PDF"]
    )

    with tab_expense:
        _render_expense_tab(ctx, concert_id, concert_name)
    with tab_budget:
        _render_budget_tab(ctx, concert_id)
    with tab_billing:
        _render_billing_tab(ctx, concert_id)
    with tab_payment:
        _render_payment_tab(ctx, concert_id)
    with tab_pdf:
        _render_finance_pdf_tab(ctx, concert_id, concert_name)
