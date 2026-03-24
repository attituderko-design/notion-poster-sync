"""
concert.pages.rental
レンタル必要楽器の逆算・見積登録・費用集計画面。
"""
import streamlit as st
from concert.services.rental_calc import calc_rental_requirements, calc_rental_for_all_practices

CONCERT_NAME_KEYS = ["名称", "タイトル", "演奏会名", "PK名称"]
CONCERT_DATE_KEYS = ["日時", "日付", "出演日", "体験日", "リリース日"]
CONCERT_MEDIA_KEYS = ["媒体", "MEDIA_TYPE", "メディア", "種類"]

PRACTICE_NAME_KEYS = ["練習名", "タイトル", "PK練習名"]
PRACTICE_DATE_KEYS = ["日時", "日付"]
PRACTICE_CONCERT_DAY_KEYS = ["演奏会当日フラグ", "本番フラグ"]
PRACTICE_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]

INSTRUMENT_NAME_KEYS = ["楽器名", "タイトル", "PK楽器名"]

RENTAL_RECORD_KEYS = ["レコード名", "タイトル", "PKレコード名"]
RENTAL_INST_REL_KEYS = ["楽器種別", "楽器", "担当楽器", "FK楽器種別"]
RENTAL_PRACTICE_REL_KEYS = ["練習", "演奏会", "出演", "FK練習"]
RENTAL_VENDOR_KEYS = ["業者名", "ベンダー", "vendor"]
RENTAL_QTY_KEYS = ["台数", "数量", "qty"]
RENTAL_UNIT_PRICE_KEYS = ["単価（円）", "単価", "unit_price"]
RENTAL_CONFIRMED_KEYS = ["確定フラグ", "確定", "is_confirmed"]
RENTAL_NOTE_KEYS = ["備考", "メモ"]
RENTAL_KEY_KEYS = ["rental_key", "RentalKey", "見積キー", "PK見積キー"]


# ============================================================
# キャッシュ／ロードヘルパー
# ============================================================

def _clear_rental_cache():
    for k in list(st.session_state.keys()):
        if k.startswith("rental_list_"):
            st.session_state.pop(k, None)
    st.session_state.pop("rental_concert_list", None)


def _concert_media_values(c: dict) -> list[str]:
    props = (c or {}).get("properties", {}) or {}
    out: list[str] = []
    for key in CONCERT_MEDIA_KEYS:
        meta = props.get(key) or {}
        ptype = meta.get("type")
        if ptype == "select":
            n = ((meta.get("select") or {}).get("name") or "").strip()
            if n:
                out.append(n)
        elif ptype == "multi_select":
            for it in (meta.get("multi_select") or []):
                n = (it.get("name") or "").strip()
                if n:
                    out.append(n)
        elif ptype in ("rich_text", "title"):
            txt = "".join((x.get("plain_text") or "") for x in (meta.get(ptype) or [])).strip()
            if txt:
                out.extend([s.strip() for s in txt.replace("／", "/").split("/") if s.strip()])
        elif ptype == "formula":
            f = meta.get("formula") or {}
            if f.get("type") == "string":
                txt = (f.get("string") or "").strip()
                if txt:
                    out.extend([s.strip() for s in txt.replace("／", "/").split("/") if s.strip()])
    return list(dict.fromkeys(out))


def _is_performance_media_concert(c: dict) -> bool:
    return "出演" in _concert_media_values(c)


def _load_concerts(ctx) -> list[dict]:
    if "rental_concert_list" not in st.session_state:
        rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
        st.session_state["rental_concert_list"] = [r for r in rows if _is_performance_media_concert(r)]
    return st.session_state.get("rental_concert_list", [])


def _get_global_concert_filter(ctx: dict, concert_opts: dict[str, str]) -> tuple[str, str]:
    gid = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    gname = (ctx.get("SELECTED_CONCERT_NAME") or "").strip()
    if not gid:
        return "", ""
    if not gname:
        for n, cid in (concert_opts or {}).items():
            if cid == gid:
                gname = n
                break
    return gid, gname


def _load_practices(ctx, concert_id: str) -> list[dict]:
    key = f"practice_list_{concert_id}"
    if key not in st.session_state:
        f = None
        if concert_id:
            type_map = ctx["get_prop_types"](ctx["CONCERT_DB_PRACTICE"])
            rel_name = ctx["find_prop_name"](type_map, PRACTICE_CONCERT_REL_KEYS)
            if rel_name:
                f = {"filter": {"property": rel_name, "relation": {"contains": concert_id}}}
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], f)
    return st.session_state.get(key, [])


def _load_instruments(ctx) -> list[dict]:
    if "instrument_list" not in st.session_state:
        st.session_state["instrument_list"] = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    return st.session_state.get("instrument_list", [])


def _load_rentals(ctx, practice_id: str) -> list[dict]:
    key = f"rental_list_{practice_id}"
    if key not in st.session_state:
        f = None
        type_map = ctx["get_prop_types"](ctx["CONCERT_DB_RENTAL"])
        rel_name = ctx["find_prop_name"](type_map, RENTAL_PRACTICE_REL_KEYS)
        if rel_name:
            f = {"filter": {"property": rel_name, "relation": {"contains": practice_id}}}
        rows = ctx["query_all"](ctx["CONCERT_DB_RENTAL"], f)
        st.session_state[key] = rows
    return st.session_state.get(key, [])


def _concert_name(c: dict, ctx: dict) -> str:
    n  = ctx["extract_prop_text_any"](c, CONCERT_NAME_KEYS) or ctx["extract_title"](c)
    dt = ctx["extract_prop_text_any"](c, CONCERT_DATE_KEYS)
    return f"{n}（{dt[:10] if dt else '日時未設定'}）"


def _practice_name(p: dict, ctx: dict) -> str:
    n  = ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ctx["extract_title"](p)
    dt = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
    suffix = "　🎼【本番】" if ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS) == "True" else ""
    return f"{n}（{dt[:10] if dt else ''}）{suffix}"


def _instrument_name(i: dict, ctx: dict) -> str:
    return ctx["extract_prop_text_any"](i, INSTRUMENT_NAME_KEYS) or ctx["extract_title"](i) or i.get("id", "")


# ============================================================
# レンタル見積 CRUD
# ============================================================

def _create_rental(ctx: dict, practice_id: str, practice_label: str,
                   instrument_id: str, instrument_name: str,
                   vendor: str, qty: int, unit_price: int,
                   confirmed: bool, note: str) -> bool:
    db_id    = ctx["CONCERT_DB_RENTAL"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("レンタル見積DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    ctx["put_prop_any"](props, type_map, RENTAL_RECORD_KEYS, f"{instrument_name} × {practice_label} / {vendor}")
    ctx["put_prop_any"](props, type_map, RENTAL_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, type_map, RENTAL_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, type_map, RENTAL_VENDOR_KEYS, vendor)
    ctx["put_prop_any"](props, type_map, RENTAL_QTY_KEYS, qty)
    ctx["put_prop_any"](props, type_map, RENTAL_UNIT_PRICE_KEYS, unit_price)
    ctx["put_prop_any"](props, type_map, RENTAL_CONFIRMED_KEYS, confirmed)
    ctx["put_prop_any"](props, type_map, RENTAL_NOTE_KEYS, note)
    ctx["put_key_any"](props, type_map, RENTAL_KEY_KEYS, practice_id, instrument_id, vendor, prefix="rental")
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_rental(ctx: dict, page_id: str, practice_id: str, practice_label: str,
                   instrument_id: str, instrument_name: str,
                   vendor: str, qty: int, unit_price: int,
                   confirmed: bool, note: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_RENTAL"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, RENTAL_RECORD_KEYS, f"{instrument_name} × {practice_label} / {vendor}")
    ctx["put_prop_any"](props, type_map, RENTAL_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, type_map, RENTAL_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, type_map, RENTAL_VENDOR_KEYS, vendor)
    ctx["put_prop_any"](props, type_map, RENTAL_QTY_KEYS, qty)
    ctx["put_prop_any"](props, type_map, RENTAL_UNIT_PRICE_KEYS, unit_price)
    ctx["put_prop_any"](props, type_map, RENTAL_CONFIRMED_KEYS, confirmed)
    ctx["put_prop_any"](props, type_map, RENTAL_NOTE_KEYS, note)
    ctx["put_key_any"](props, type_map, RENTAL_KEY_KEYS, practice_id, instrument_id, vendor, prefix="rental")
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"properties": props})
    return res is not None and res.status_code == 200


def _archive_page(ctx: dict, page_id: str) -> bool:
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                             json={"archived": True})
    return res is not None and res.status_code == 200


# ============================================================
# 逆算タブ
# ============================================================

def _render_calc_tab(ctx: dict):
    st.caption("奏者の出欠・持参可フラグをもとに、各練習日のレンタル必要台数を自動算出します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, concert_opts)
    if global_concert_id:
        concert_id = global_concert_id
        st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")
    else:
        selected = st.selectbox("演奏会を選択", list(concert_opts.keys()), key="rental_calc_concert")
        concert_id = concert_opts.get(selected, "")
    if not concert_id:
        return

    if st.button("🔍 レンタル必要楽器を試算", type="primary", key="run_rental_calc"):
        with st.spinner("計算中..."):
            results = calc_rental_for_all_practices(ctx, concert_id)
        st.session_state["rental_calc_results"] = results

    results = st.session_state.get("rental_calc_results")
    if not results:
        return

    # 全練習分を日付順に表示
    practices = _load_practices(ctx, concert_id)

    def _prac_date(p):
        d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
        return d[:10] if d else "9999"

    prac_by_id = {p.get("id"): p for p in practices}

    has_any_rental = False
    for pid, data in sorted(
        results.items(),
        key=lambda kv: _prac_date(prac_by_id.get(kv[0], {}))
    ):
        reqs = data.get("requirements", [])
        rental_reqs = [r for r in reqs if r["rental_needed"] > 0]
        prac_label = data.get("name") or pid

        with st.expander(
            f"{prac_label}　{'⚠️ レンタル必要' if rental_reqs else '✅ レンタル不要'}",
            expanded=bool(rental_reqs),
        ):
            if not reqs:
                st.info("必要楽器の情報がありません（楽曲・楽器管理で登録してください）。")
                continue

            col_inst, col_req, col_bring, col_rent = st.columns([3, 2, 2, 2])
            col_inst.markdown("**楽器**")
            col_req.markdown("**必要台数**")
            col_bring.markdown("**持参可能**")
            col_rent.markdown("**レンタル必要**")
            st.divider()

            for r in reqs:
                c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                c1.write(r["instrument_name"])
                c2.write(str(r["required"]))
                c3.write(str(r["bring_available"]))
                rent_val = r["rental_needed"]
                if rent_val > 0:
                    c4.markdown(f"**:red[{rent_val}]**")
                    has_any_rental = True
                else:
                    c4.write("0")

    if not has_any_rental:
        st.success("すべての練習日でレンタルは不要です。")


# ============================================================
# 見積登録タブ
# ============================================================

def _render_estimate_tab(ctx: dict):
    st.caption("業者から取得した見積情報を登録します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, concert_opts)
    if global_concert_id:
        concert_id = global_concert_id
        selected_concert = global_concert_name or global_concert_id
        st.caption(f"対象演奏会: {selected_concert}")
    else:
        selected_concert = st.selectbox("演奏会", list(concert_opts.keys()), key="est_concert")
        concert_id = concert_opts.get(selected_concert, "")
    if not concert_id:
        return

    practices = _load_practices(ctx, concert_id)
    if not practices:
        st.info("この演奏会に練習が登録されていません。")
        return

    def _prac_date(p):
        d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
        return d[:10] if d else "9999"

    practice_opts = {_practice_name(p, ctx): p.get("id", "")
                     for p in sorted(practices, key=_prac_date)}
    selected_practice = st.selectbox("練習日", list(practice_opts.keys()), key="est_practice")
    practice_id = practice_opts.get(selected_practice, "")
    if not practice_id:
        return

    instruments = _load_instruments(ctx)
    inst_opts   = {_instrument_name(i, ctx): i.get("id", "")
                   for i in sorted(instruments, key=lambda x: _instrument_name(x, ctx))}

    # この練習日の既存見積一覧
    rental_rows = _load_rentals(ctx, practice_id)

    # ── 新規見積登録フォーム ──
    with st.expander("➕ 見積を追加", expanded=(len(rental_rows) == 0)):
        with st.form("rental_new_form", border=True):
            inst_sel   = st.selectbox("楽器種別 *", list(inst_opts.keys()), key="rn_inst")
            vendor     = st.text_input("業者名", placeholder="例：○○楽器レンタル", key="rn_vendor")
            col1, col2 = st.columns(2)
            qty        = col1.number_input("台数 *", min_value=1, value=1, step=1, key="rn_qty")
            unit_price = col2.number_input("単価（円）*", min_value=0, value=0, step=100, key="rn_price")
            confirmed  = st.checkbox("確定（見積→確定）", value=False, key="rn_confirmed")
            note       = st.text_area("備考", height=60, placeholder="見積番号等", key="rn_note")

            if st.form_submit_button("💾 登録", use_container_width=True, type="primary"):
                inst_id   = inst_opts.get(inst_sel, "")
                if not inst_id:
                    st.error("楽器種別を選択してください。")
                else:
                    with st.spinner("登録中..."):
                        ok = _create_rental(
                            ctx, practice_id, selected_practice,
                            inst_id, inst_sel,
                            vendor, int(qty), int(unit_price),
                            confirmed, note,
                        )
                    if ok:
                        st.success("✅ 見積を登録しました。")
                        _clear_rental_cache()
                        st.rerun()
                    else:
                        st.error("❌ 登録に失敗しました。")

    st.divider()

    if not rental_rows:
        st.info("この練習日の見積がまだ登録されていません。")
        return

    st.subheader(f"登録済み見積（{len(rental_rows)}件）")

    for row in rental_rows:
        rid       = row.get("id", "")
        ext_any   = ctx["extract_prop_text_any"]
        ext_rel   = ctx["extract_relation_ids_any"]
        inst_ids  = ext_rel(row, RENTAL_INST_REL_KEYS)
        inst_id   = inst_ids[0] if inst_ids else ""
        inst_name = next((k for k, v in inst_opts.items() if v == inst_id), ext_any(row, INSTRUMENT_NAME_KEYS) or "不明")
        vendor    = ext_any(row, RENTAL_VENDOR_KEYS)
        qty_str   = ext_any(row, RENTAL_QTY_KEYS)
        price_str = ext_any(row, RENTAL_UNIT_PRICE_KEYS)
        qty       = int(float(qty_str)) if qty_str else 0
        price     = int(float(price_str)) if price_str else 0
        confirmed = ext_any(row, RENTAL_CONFIRMED_KEYS) == "True"
        note      = ext_any(row, RENTAL_NOTE_KEYS)

        status_badge = "✅ 確定" if confirmed else "📋 見積"
        label = f"{status_badge}　{inst_name}　{vendor or '業者未設定'}　{qty}台　¥{price:,}/台　小計 ¥{qty * price:,}"

        with st.expander(label, expanded=False):
            cur_inst_name = next((k for k, v in inst_opts.items() if v == inst_id), list(inst_opts.keys())[0] if inst_opts else "")
            with st.form(f"rental_edit_{rid}", border=True):
                inst_sel_e  = st.selectbox("楽器種別", list(inst_opts.keys()),
                                            index=list(inst_opts.keys()).index(cur_inst_name) if cur_inst_name in inst_opts else 0,
                                            key=f"re_inst_{rid}")
                vendor_e    = st.text_input("業者名", value=vendor, key=f"re_vendor_{rid}")
                col1, col2  = st.columns(2)
                qty_e       = col1.number_input("台数", min_value=1, value=max(qty, 1), step=1, key=f"re_qty_{rid}")
                price_e     = col2.number_input("単価（円）", min_value=0, value=price, step=100, key=f"re_price_{rid}")
                confirmed_e = st.checkbox("確定", value=confirmed, key=f"re_confirmed_{rid}")
                note_e      = st.text_area("備考", value=note, height=60, key=f"re_note_{rid}")

                col_upd, col_del = st.columns([3, 1])
                if col_upd.form_submit_button("💾 更新", use_container_width=True):
                    inst_id_e = inst_opts.get(inst_sel_e, inst_id)
                    with st.spinner("更新中..."):
                        ok = _update_rental(
                            ctx, rid, practice_id, selected_practice,
                            inst_id_e, inst_sel_e,
                            vendor_e, int(qty_e), int(price_e),
                            confirmed_e, note_e,
                        )
                    if ok:
                        st.success("✅ 更新しました。")
                        _clear_rental_cache()
                        st.rerun()
                    else:
                        st.error("❌ 更新に失敗しました。")

                if col_del.form_submit_button("🗑️ 削除", use_container_width=True):
                    with st.spinner("削除中..."):
                        ok = _archive_page(ctx, rid)
                    if ok:
                        st.success("✅ 削除しました。")
                        _clear_rental_cache()
                        st.rerun()
                    else:
                        st.error("❌ 削除に失敗しました。")


# ============================================================
# 費用集計タブ
# ============================================================

def _render_summary_tab(ctx: dict):
    st.caption("演奏会全体のレンタル費用を集計します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, concert_opts)
    if global_concert_id:
        concert_id = global_concert_id
        st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")
    else:
        selected = st.selectbox("演奏会を選択", list(concert_opts.keys()), key="summary_concert")
        concert_id = concert_opts.get(selected, "")
    if not concert_id:
        return

    practices = _load_practices(ctx, concert_id)
    if not practices:
        st.info("この演奏会に練習が登録されていません。")
        return

    # 全練習日の見積を集計
    total_all       = 0
    total_confirmed = 0
    rows_for_table: list[dict] = []

    def _prac_date(p):
        d = ctx["extract_prop_text"](p, "日時")
        return d[:10] if d else "9999"

    for prac in sorted(practices, key=_prac_date):
        pid        = prac.get("id", "")
        prac_label = _practice_name(prac, ctx)
        rentals    = _load_rentals(ctx, pid)
        prac_total = 0

        for row in rentals:
            ext       = ctx["extract_prop_text"]
            ext_rel   = ctx["extract_relation_ids"]
            qty_str   = ext(row, "台数")
            price_str = ext(row, "単価（円）")
            qty       = int(float(qty_str)) if qty_str else 0
            price     = int(float(price_str)) if price_str else 0
            subtotal  = qty * price
            confirmed = ext(row, "確定フラグ") == "True"

            inst_ids  = ext_rel(row, "楽器種別")
            inst_id   = inst_ids[0] if inst_ids else ""
            inst_rows = _load_instruments(ctx)
            inst_name = next(
                (_instrument_name(i, ctx) for i in inst_rows if i.get("id") == inst_id),
                "不明"
            )

            rows_for_table.append({
                "練習日":     prac_label,
                "楽器":       inst_name,
                "業者":       ext(row, "業者名") or "—",
                "台数":       qty,
                "単価":       price,
                "小計":       subtotal,
                "確定":       "✅" if confirmed else "📋",
            })
            prac_total       += subtotal
            total_all        += subtotal
            if confirmed:
                total_confirmed += subtotal

    # サマリーカード
    col1, col2, col3 = st.columns(3)
    col1.metric("合計（全見積）", f"¥{total_all:,}")
    col2.metric("確定済み合計", f"¥{total_confirmed:,}")
    col3.metric("見積中", f"¥{total_all - total_confirmed:,}")

    st.divider()

    if not rows_for_table:
        st.info("見積が登録されていません。")
        return

    st.subheader("明細")
    st.dataframe(
        rows_for_table,
        use_container_width=True,
        column_config={
            "単価":  st.column_config.NumberColumn(format="¥%d"),
            "小計":  st.column_config.NumberColumn(format="¥%d"),
        },
        hide_index=True,
    )

    # 練習日ごとの小計
    st.subheader("練習日別小計")
    prac_totals: dict[str, int] = {}
    for r in rows_for_table:
        prac_totals[r["練習日"]] = prac_totals.get(r["練習日"], 0) + r["小計"]
    for label, total in prac_totals.items():
        st.write(f"- {label}：**¥{total:,}**")


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("📦 レンタル管理")

    tab_calc, tab_estimate, tab_summary = st.tabs(["レンタル試算", "見積登録", "費用集計"])

    with tab_calc:
        _render_calc_tab(ctx)

    with tab_estimate:
        _render_estimate_tab(ctx)

    with tab_summary:
        _render_summary_tab(ctx)
