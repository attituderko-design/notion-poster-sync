"""
concert.pages.rental
レンタル必要楽器の逆算・見積登録・費用集計画面。
"""
import streamlit as st
import pandas as pd
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
RENTAL_ITEM_NAME_KEYS = ["品目名", "item_name", "品目"]
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
                   item_name: str,
                   vendor: str, qty: int, unit_price: int,
                   confirmed: bool, note: str) -> bool:
    db_id    = ctx["CONCERT_DB_RENTAL"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("レンタル見積DBのプロパティ取得に失敗しました。")
        return False
    props: dict = {}
    display_name = item_name.strip() if item_name.strip() else instrument_name
    ctx["put_prop_any"](props, type_map, RENTAL_RECORD_KEYS, f"{display_name} × {practice_label} / {vendor}")
    ctx["put_prop_any"](props, type_map, RENTAL_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, type_map, RENTAL_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, type_map, RENTAL_ITEM_NAME_KEYS, item_name)
    ctx["put_prop_any"](props, type_map, RENTAL_VENDOR_KEYS, vendor)
    ctx["put_prop_any"](props, type_map, RENTAL_QTY_KEYS, qty)
    ctx["put_prop_any"](props, type_map, RENTAL_UNIT_PRICE_KEYS, unit_price)
    ctx["put_prop_any"](props, type_map, RENTAL_CONFIRMED_KEYS, confirmed)
    ctx["put_prop_any"](props, type_map, RENTAL_NOTE_KEYS, note)
    ctx["put_key_any"](props, type_map, RENTAL_KEY_KEYS, practice_id, instrument_id, item_name or instrument_name, vendor, prefix="rental")
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_rental(ctx: dict, page_id: str, practice_id: str, practice_label: str,
                   instrument_id: str, instrument_name: str,
                   item_name: str,
                   vendor: str, qty: int, unit_price: int,
                   confirmed: bool, note: str) -> bool:
    type_map = ctx["get_prop_types"](ctx["CONCERT_DB_RENTAL"])
    props: dict = {}
    display_name = item_name.strip() if item_name.strip() else instrument_name
    ctx["put_prop_any"](props, type_map, RENTAL_RECORD_KEYS, f"{display_name} × {practice_label} / {vendor}")
    ctx["put_prop_any"](props, type_map, RENTAL_INST_REL_KEYS, instrument_id)
    ctx["put_prop_any"](props, type_map, RENTAL_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, type_map, RENTAL_ITEM_NAME_KEYS, item_name)
    ctx["put_prop_any"](props, type_map, RENTAL_VENDOR_KEYS, vendor)
    ctx["put_prop_any"](props, type_map, RENTAL_QTY_KEYS, qty)
    ctx["put_prop_any"](props, type_map, RENTAL_UNIT_PRICE_KEYS, unit_price)
    ctx["put_prop_any"](props, type_map, RENTAL_CONFIRMED_KEYS, confirmed)
    ctx["put_prop_any"](props, type_map, RENTAL_NOTE_KEYS, note)
    ctx["put_key_any"](props, type_map, RENTAL_KEY_KEYS, practice_id, instrument_id, item_name or instrument_name, vendor, prefix="rental")
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

def _get_rented_inst_ids(ctx: dict, practice_id: str) -> set[str]:
    """この練習日に見積登録済みの楽器種別IDセットを返す。"""
    rental_rows = _load_rentals(ctx, practice_id)
    ids: set[str] = set()
    for r in rental_rows:
        iids = ctx["extract_relation_ids_any"](r, RENTAL_INST_REL_KEYS)
        if iids:
            ids.add(iids[0])
    return ids


def _render_calc_tab(ctx: dict):
    st.caption("奏者の出欠・持参可フラグをもとに、各練習日のレンタル必要台数を自動算出します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, concert_opts)
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    concert_id = global_concert_id
    st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")

    if st.button("🔍 レンタル必要楽器を試算", type="primary", key="run_rental_calc"):
        with st.spinner("計算中..."):
            results = calc_rental_for_all_practices(ctx, concert_id)
        st.session_state["rental_calc_results"] = results
        st.session_state.pop("rental_calc_concert_id", None)

    results = st.session_state.get("rental_calc_results")
    if not results:
        return

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
        reqs    = data.get("requirements", [])
        rental_reqs  = [r for r in reqs if r["rental_needed"] > 0]
        bring_reqs   = [r for r in reqs if r["rental_needed"] <= 0 and r["bring_available"] > 0]
        prac_label   = data.get("name") or pid
        rented_ids   = _get_rented_inst_ids(ctx, pid)

        if rental_reqs:
            has_any_rental = True

        with st.expander(
            f"{prac_label}　{'⚠️ レンタル必要' if rental_reqs else '✅ レンタル不要'}",
            expanded=bool(rental_reqs),
        ):
            if not reqs:
                st.info("必要楽器の情報がありません（楽曲・楽器管理で登録してください）。")
                continue

            # ── レンタル必要 ──────────────────────────────────
            if rental_reqs:
                st.markdown("**レンタル必要**")
                for r in rental_reqs:
                    iid = r["instrument_id"]
                    c1, c2, c3 = st.columns([4, 3, 3])
                    c1.markdown(f"🔴 {r['instrument_name']}")
                    c2.caption(f"必要 {r['required']}台 / 持参 {r['bring_available']}台")
                    already = iid in rented_ids
                    if already:
                        c3.success("✅ 見積登録済み")
                    else:
                        if c3.button(
                            "見積に追加",
                            key=f"add_rental_{pid}_{iid}",
                            use_container_width=True,
                        ):
                            st.session_state["est_prefill"] = {
                                "practice_id":   pid,
                                "practice_label": prac_label,
                                "inst_name":     r["instrument_name"],
                                "qty":           r["rental_needed"],
                            }
                            st.session_state["rental_active_tab"] = 1
                            st.rerun()

            # ── 持参可能 ──────────────────────────────────────
            if bring_reqs:
                if rental_reqs:
                    st.divider()
                st.markdown("**持参可能**")
                for r in bring_reqs:
                    iid = r["instrument_id"]
                    c1, c2, c3 = st.columns([4, 3, 3])
                    c1.markdown(f"🟢 {r['instrument_name']}")
                    c2.caption(f"必要 {r['required']}台 / 持参 {r['bring_available']}台")
                    already = iid in rented_ids
                    if already:
                        c3.warning("🔄 レンタルに振り替え済み")
                    else:
                        if c3.button(
                            "レンタルに振り替え",
                            key=f"switch_rental_{pid}_{iid}",
                            use_container_width=True,
                        ):
                            st.session_state["est_prefill"] = {
                                "practice_id":   pid,
                                "practice_label": prac_label,
                                "inst_name":     r["instrument_name"],
                                "qty":           r["required"],
                            }
                            st.session_state["rental_active_tab"] = 1
                            st.rerun()

    if not has_any_rental:
        st.success("すべての練習日でレンタルは不要です。")


# ============================================================
# 見積登録タブ
# ============================================================

def _render_estimate_tab(ctx: dict):
    st.caption("明細形式で見積を入力し、まとめて登録・更新できます。")

    # 逆算タブからのプリフィル情報を受け取る
    prefill = st.session_state.pop("est_prefill", None)

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    global_concert_id, global_concert_name = _get_global_concert_filter(ctx, concert_opts)
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return
    concert_id = global_concert_id
    st.caption(f"対象演奏会: {global_concert_name or global_concert_id}")

    practices = _load_practices(ctx, concert_id)
    if not practices:
        st.info("この演奏会に練習が登録されていません。")
        return

    def _prac_date(p):
        d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
        return d[:10] if d else "9999"

    practice_opts = {_practice_name(p, ctx): p.get("id", "")
                     for p in sorted(practices, key=_prac_date)}

    # プリフィルがあれば対象の練習日を自動選択
    prefill_practice_id = prefill.get("practice_id", "") if prefill else ""
    default_practice = next(
        (label for label, pid in practice_opts.items() if pid == prefill_practice_id),
        list(practice_opts.keys())[0] if practice_opts else None,
    )
    selected_practice = st.selectbox(
        "練習日", list(practice_opts.keys()),
        index=list(practice_opts.keys()).index(default_practice) if default_practice in practice_opts else 0,
        key="est_practice",
    )
    practice_id = practice_opts.get(selected_practice, "")
    if not practice_id:
        return

    # プリフィル通知
    if prefill and prefill.get("inst_name"):
        st.info(f"「{prefill['inst_name']}」を明細の先頭行にセットしました。業者名・単価を入力して保存してください。")

    instruments = _load_instruments(ctx)
    inst_names  = [_instrument_name(i, ctx) for i in
                   sorted(instruments, key=lambda x: _instrument_name(x, ctx))]
    inst_opts   = {_instrument_name(i, ctx): i.get("id", "")
                   for i in sorted(instruments, key=lambda x: _instrument_name(x, ctx))}

    rental_rows = _load_rentals(ctx, practice_id)

    # ── 既存データをDataFrameに変換 ──
    ext_any = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    existing: list[dict] = []
    row_ids: list[str] = []
    for r in rental_rows:
        inst_ids  = ext_rel(r, RENTAL_INST_REL_KEYS)
        inst_id   = inst_ids[0] if inst_ids else ""
        inst_name = next((k for k, v in inst_opts.items() if v == inst_id), "")
        existing.append({
            "楽器種別":   inst_name,
            "品目名":     ext_any(r, RENTAL_ITEM_NAME_KEYS) or "",
            "台数":       int(float(ext_any(r, RENTAL_QTY_KEYS) or "1")),
            "単価（円）": int(float(ext_any(r, RENTAL_UNIT_PRICE_KEYS) or "0")),
            "確定":       ext_any(r, RENTAL_CONFIRMED_KEYS) == "True",
            "備考":       ext_any(r, RENTAL_NOTE_KEYS) or "",
        })
        row_ids.append(r.get("id", ""))

    # プリフィル行を先頭に差し込む（逆算タブからの振り替え）
    if prefill and prefill.get("inst_name"):
        prefill_inst = prefill["inst_name"]
        prefill_qty  = prefill.get("qty", 1)
        existing.insert(0, {
            "楽器種別": prefill_inst if prefill_inst in inst_names else (inst_names[0] if inst_names else ""),
            "品目名":   prefill_inst,
            "台数":     prefill_qty,
            "単価（円）": 0,
            "確定":     False,
            "備考":     "",
        })
        row_ids.insert(0, "")  # 新規行

    # 新規行用の空行（既存0件のときは3行、あるときは1行）
    empty_rows = 3 if not existing else 1
    for _ in range(empty_rows):
        existing.append({
            "楽器種別": inst_names[0] if inst_names else "",
            "品目名": "", "台数": 1, "単価（円）": 0,
            "確定": False, "備考": "",
        })
        row_ids.append("")  # 空IDは新規行

    df_init = pd.DataFrame(existing)

    # ── 業者名（全行共通）──
    vendor_default = ""
    if rental_rows:
        vendor_default = ext_any(rental_rows[0], RENTAL_VENDOR_KEYS) or ""
    vendor = st.text_input(
        "業者名（全明細共通）",
        value=vendor_default,
        placeholder="例：○○楽器レンタル",
        key="est_vendor",
    )

    # ── data_editor ──
    st.caption("行を追加・削除して「保存」を押してください。品目名が空の場合は楽器種別名を使用します。")
    edited_df = st.data_editor(
        df_init,
        num_rows="dynamic",
        use_container_width=True,
        key="rental_editor",
        column_config={
            "楽器種別": st.column_config.SelectboxColumn(
                "楽器種別",
                options=inst_names,
                required=True,
            ),
            "品目名": st.column_config.TextColumn(
                '品目名（例：32" Timpani）',
                max_chars=100,
            ),
            "台数": st.column_config.NumberColumn(
                "台数",
                min_value=1,
                max_value=99,
                step=1,
                default=1,
            ),
            "単価（円）": st.column_config.NumberColumn(
                "単価（円）",
                min_value=0,
                step=100,
                format="¥%d",
                default=0,
            ),
            "確定": st.column_config.CheckboxColumn(
                "確定",
                default=False,
            ),
            "備考": st.column_config.TextColumn(
                "備考",
                max_chars=200,
            ),
        },
        hide_index=True,
    )

    # 小計表示
    if not edited_df.empty:
        try:
            subtotal = int((edited_df["台数"] * edited_df["単価（円）"]).sum())
            st.caption(f"小計：¥{subtotal:,}")
        except Exception:
            pass

    # ── 保存ボタン ──
    if st.button("💾 まとめて保存", type="primary", use_container_width=True, key="est_save"):
        ok_n = fail_n = skip_n = 0
        with st.spinner("保存中..."):
            # 保存後に削除すべき既存行を追跡
            saved_existing_ids: set[str] = set()

            for idx, row in edited_df.iterrows():
                inst_sel_v  = str(row.get("楽器種別") or "").strip()
                item_name_v = str(row.get("品目名") or "").strip()
                qty_v       = int(row.get("台数") or 1)
                price_v     = int(row.get("単価（円）") or 0)
                confirmed_v = bool(row.get("確定") or False)
                note_v      = str(row.get("備考") or "").strip()

                if not inst_sel_v:
                    skip_n += 1
                    continue

                inst_id_v = inst_opts.get(inst_sel_v, "")
                if not inst_id_v:
                    skip_n += 1
                    continue

                # 対応する既存レコードID（初期データの行番号で対応）
                existing_id = row_ids[idx] if idx < len(row_ids) else ""

                if existing_id:
                    ok = _update_rental(
                        ctx, existing_id, practice_id, selected_practice,
                        inst_id_v, inst_sel_v, item_name_v,
                        vendor, qty_v, price_v, confirmed_v, note_v,
                    )
                    if ok:
                        ok_n += 1
                        saved_existing_ids.add(existing_id)
                    else:
                        fail_n += 1
                else:
                    ok = _create_rental(
                        ctx, practice_id, selected_practice,
                        inst_id_v, inst_sel_v, item_name_v,
                        vendor, qty_v, price_v, confirmed_v, note_v,
                    )
                    if ok:
                        ok_n += 1
                    else:
                        fail_n += 1

            # 元の既存行のうち今回の編集後に消えた行をアーカイブ
            for rid in row_ids:
                if rid and rid not in saved_existing_ids:
                    _archive_page(ctx, rid)

        if fail_n == 0:
            st.success(f"✅ {ok_n}件を保存しました。（スキップ {skip_n}件）")
        else:
            st.warning(f"⚠️ 成功 {ok_n} / 失敗 {fail_n} / スキップ {skip_n}")
        _clear_rental_cache()
        st.rerun()


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
    global_concert_id = (ctx.get("SELECTED_CONCERT_ID") or "").strip()
    if not global_concert_id:
        st.info("サイドバーで演奏会を選択してください。")
        return

    # 逆算タブの「見積に追加」「レンタルに振り替え」ボタンでタブを切り替える
    active_tab = st.session_state.pop("rental_active_tab", 0)

    tab_calc, tab_estimate, tab_summary = st.tabs(["レンタル試算", "見積登録", "費用集計"])

    with tab_calc:
        _render_calc_tab(ctx)

    with tab_estimate:
        # プリフィル情報があれば見積登録タブに渡す
        _render_estimate_tab(ctx)

    with tab_summary:
        _render_summary_tab(ctx)

    # タブ切り替えはJSで実現（streamlitのタブはindex指定不可のため）
    if active_tab == 1:
        st.markdown(
            """<script>
            const tabs = window.parent.document.querySelectorAll('[data-baseweb="tab"]');
            if (tabs && tabs[1]) tabs[1].click();
            </script>""",
            unsafe_allow_html=True,
        )
