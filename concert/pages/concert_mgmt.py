"""
concert.pages.concert_mgmt
演奏会・練習情報の登録・一覧・編集画面。
"""
import streamlit as st
from datetime import date, datetime
import re
import requests

CONCERT_NAME_KEYS = ["名称", "タイトル", "演奏会名", "PK名称"]
CONCERT_DATE_KEYS = ["日時", "日付", "出演日", "体験日", "リリース日"]
CONCERT_VENUE_KEYS = ["会場名", "ロケーション", "場所", "会場", "Location"]
CONCERT_ADDRESS_KEYS = ["会場住所", "住所", "ロケーション", "場所", "Location"]
CONCERT_MEMO_KEYS = ["メモ", "備考"]
CONCERT_MEDIA_KEYS = ["媒体", "MEDIA_TYPE"]

PRACTICE_NAME_KEYS = ["練習名", "タイトル", "PK練習名"]
PRACTICE_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
PRACTICE_DATE_KEYS = ["日時", "日付"]
PRACTICE_VENUE_KEYS = ["会場名", "ロケーション", "場所", "会場", "Location"]
PRACTICE_ADDRESS_KEYS = ["会場住所", "住所", "ロケーション", "場所", "Location"]
PRACTICE_CONCERT_DAY_KEYS = ["演奏会当日フラグ", "本番フラグ"]
PRACTICE_REST_KEYS = ["打楽器休み", "休みフラグ", "休み", "Percussion休み"]
PRACTICE_MEMO_KEYS = ["メモ", "備考"]


# ============================================================
# ヘルパー
# ============================================================

def _ss(key, default=None):
    return st.session_state.get(key, default)


def _compose_notion_date_with_optional_time(d: date, start_hhmm: str, end_hhmm: str) -> tuple[str, str]:
    """
    練習日 + 任意時刻を Notion date.start/date.end 用のISO文字列に整形する。
    return: (start_iso, end_iso)
    """
    s = (start_hhmm or "").strip()
    e = (end_hhmm or "").strip()
    hhmm_re = re.compile(r"^\d{1,2}:\d{2}$")

    def _to_dt(hhmm: str) -> datetime:
        h, m = hhmm.split(":")
        return datetime(d.year, d.month, d.day, int(h), int(m), 0)

    if s and not hhmm_re.match(s):
        raise ValueError("開始時刻は HH:MM 形式で入力してください（例: 19:30）")
    if e and not hhmm_re.match(e):
        raise ValueError("終了時刻は HH:MM 形式で入力してください（例: 21:00）")

    if s:
        sdt = _to_dt(s)
        if not (0 <= sdt.hour <= 23 and 0 <= sdt.minute <= 59):
            raise ValueError("開始時刻が不正です。")
        if e:
            edt = _to_dt(e)
            if not (0 <= edt.hour <= 23 and 0 <= edt.minute <= 59):
                raise ValueError("終了時刻が不正です。")
            if edt < sdt:
                raise ValueError("終了時刻は開始時刻以降にしてください。")
            return sdt.isoformat(), edt.isoformat()
        return sdt.isoformat(), ""

    if e:
        raise ValueError("終了時刻のみは指定できません。開始時刻も入力してください。")

    return d.isoformat(), ""


def _contains_query(values: list[str], query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return True
    blob = " ".join([(v or "") for v in values]).lower()
    return q in blob


def _geocode_nominatim(query: str) -> list[dict]:
    q = (query or "").strip()
    if not q:
        return []
    try:
        res = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "jsonv2", "addressdetails": 1, "limit": 8},
            headers={"User-Agent": "artemis-harmonia/1.0"},
            timeout=10,
        )
        if res.status_code != 200:
            return []
        rows = res.json() or []
        out = []
        for r in rows:
            out.append(
                {
                    "name": r.get("display_name") or "",
                    "address": r.get("display_name") or "",
                }
            )
        return out
    except Exception:
        return []


def _clear_concert_cache(ctx):
    try:
        from concert.services.notion_client import get_concert_db_property_types
        get_concert_db_property_types.clear()
    except Exception:
        pass
    for k in ["concert_list", "practice_list"]:
        st.session_state.pop(k, None)


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        rows = []
        db_id = ctx["CONCERT_DB_CONCERT"]
        type_map = ctx["get_prop_types"](db_id)
        media_prop = ctx["find_prop_name"](type_map, CONCERT_MEDIA_KEYS)
        media_type = type_map.get(media_prop, "")
        if media_prop and media_type == "select":
            rows = ctx["query_all"](
                db_id,
                {"filter": {"property": media_prop, "select": {"equals": "出演"}}},
            )
        elif media_prop and media_type == "multi_select":
            rows = ctx["query_all"](
                db_id,
                {"filter": {"property": media_prop, "multi_select": {"contains": "出演"}}},
            )
        else:
            rows = ctx["query_all"](db_id)
        st.session_state["concert_list"] = rows
    return st.session_state.get("concert_list", [])


def _load_practices(ctx, concert_id: str = "") -> list[dict]:
    cache_key = f"practice_list_{concert_id}"
    if cache_key not in st.session_state:
        type_map = ctx["get_prop_types"](ctx["CONCERT_DB_PRACTICE"])
        rel_prop = ctx["find_prop_name"](type_map, PRACTICE_CONCERT_REL_KEYS)
        if concert_id:
            if rel_prop:
                rows = ctx["query_all"](
                    ctx["CONCERT_DB_PRACTICE"],
                    {"filter": {"property": rel_prop, "relation": {"contains": concert_id}}},
                )
            else:
                rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"])
        else:
            rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"])
        st.session_state[cache_key] = rows
    return st.session_state.get(cache_key, [])


def _concert_display_name(page: dict, ctx: dict) -> str:
    name = ctx["extract_prop_text_any"](page, CONCERT_NAME_KEYS)
    if not name:
        name = ctx["extract_title"](page)
    dt = ctx["extract_prop_text_any"](page, CONCERT_DATE_KEYS)
    return f"{name}（{dt[:10] if dt else '日時未設定'}）" if name else page.get("id", "")


def _practice_display_name(page: dict, ctx: dict) -> str:
    name = ctx["extract_prop_text_any"](page, PRACTICE_NAME_KEYS)
    if not name:
        name = ctx["extract_title"](page)
    dt = ctx["extract_prop_text_any"](page, PRACTICE_DATE_KEYS)
    return f"{name}（{dt[:10] if dt else '日時未設定'}）" if name else page.get("id", "")


# ============================================================
# 演奏会 CRUD
# ============================================================

def _create_concert(ctx: dict, name: str, dt_start: str, dt_end: str, venue: str, address: str, memo: str) -> bool:
    api   = ctx["api_request"]
    hdrs  = ctx["NOTION_HEADERS"]
    db_id = ctx["CONCERT_DB_CONCERT"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(db_id)
    if not type_map:
        st.error("演奏会DBのプロパティ取得に失敗しました。DB IDとインテグレーション接続を確認してください。")
        return False

    props: dict = {}
    ctx["put_prop_any"](props, type_map, CONCERT_NAME_KEYS, name)
    date_key = ctx["find_prop_name"](type_map, CONCERT_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    ctx["put_prop_any"](props, type_map, CONCERT_VENUE_KEYS, venue)
    ctx["put_prop_any"](props, type_map, CONCERT_ADDRESS_KEYS, address)
    ctx["put_prop_any"](props, type_map, CONCERT_MEMO_KEYS, memo)
    media_key = ctx["find_prop_name"](type_map, CONCERT_MEDIA_KEYS)
    if media_key:
        mtype = type_map.get(media_key, "")
        if mtype == "select":
            props[media_key] = {"select": {"name": "出演"}}
        elif mtype == "multi_select":
            props[media_key] = {"multi_select": [{"name": "出演"}]}

    res = api("post", "https://api.notion.com/v1/pages",
              json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_concert(ctx: dict, page_id: str, name: str, dt_start: str, dt_end: str, venue: str, address: str, memo: str) -> bool:
    api  = ctx["api_request"]
    hdrs = ctx["NOTION_HEADERS"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(ctx["CONCERT_DB_CONCERT"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, CONCERT_NAME_KEYS, name)
    date_key = ctx["find_prop_name"](type_map, CONCERT_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    ctx["put_prop_any"](props, type_map, CONCERT_VENUE_KEYS, venue)
    ctx["put_prop_any"](props, type_map, CONCERT_ADDRESS_KEYS, address)
    ctx["put_prop_any"](props, type_map, CONCERT_MEMO_KEYS, memo)

    res = api("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 練習 CRUD
# ============================================================

def _create_practice(ctx: dict, name: str, concert_id: str, dt_start: str, dt_end: str,
                     venue: str, address: str, is_concert_day: bool, is_rest_day: bool, memo: str) -> bool:
    api   = ctx["api_request"]
    db_id = ctx["CONCERT_DB_PRACTICE"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(db_id)
    if not type_map:
        st.error("練習DBのプロパティ取得に失敗しました。")
        return False

    props: dict = {}
    ctx["put_prop_any"](props, type_map, PRACTICE_NAME_KEYS, name)
    if concert_id:
        ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_REL_KEYS, concert_id)
    date_key = ctx["find_prop_name"](type_map, PRACTICE_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    ctx["put_prop_any"](props, type_map, PRACTICE_VENUE_KEYS, venue)
    ctx["put_prop_any"](props, type_map, PRACTICE_ADDRESS_KEYS, address)
    ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_DAY_KEYS, is_concert_day)
    ctx["put_prop_any"](props, type_map, PRACTICE_REST_KEYS, is_rest_day)
    ctx["put_prop_any"](props, type_map, PRACTICE_MEMO_KEYS, memo)

    res = api("post", "https://api.notion.com/v1/pages",
              json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _update_practice(ctx: dict, page_id: str, name: str, concert_id: str,
                     dt_start: str, dt_end: str, venue: str, address: str,
                     is_concert_day: bool, is_rest_day: bool, memo: str) -> bool:
    api   = ctx["api_request"]
    get_t = ctx["get_prop_types"]
    put_p = ctx["put_prop"]

    type_map = get_t(ctx["CONCERT_DB_PRACTICE"])
    props: dict = {}
    ctx["put_prop_any"](props, type_map, PRACTICE_NAME_KEYS, name)
    if concert_id:
        ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_REL_KEYS, concert_id)
    date_key = ctx["find_prop_name"](type_map, PRACTICE_DATE_KEYS)
    if dt_start and date_key:
        date_val: dict = {"start": dt_start}
        if dt_end and dt_end != dt_start:
            date_val["end"] = dt_end
        props[date_key] = {"date": date_val}
    ctx["put_prop_any"](props, type_map, PRACTICE_VENUE_KEYS, venue)
    ctx["put_prop_any"](props, type_map, PRACTICE_ADDRESS_KEYS, address)
    ctx["put_prop_any"](props, type_map, PRACTICE_CONCERT_DAY_KEYS, is_concert_day)
    ctx["put_prop_any"](props, type_map, PRACTICE_REST_KEYS, is_rest_day)
    ctx["put_prop_any"](props, type_map, PRACTICE_MEMO_KEYS, memo)

    res = api("patch", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# 演奏会フォーム
# ============================================================

def _render_concert_form(ctx: dict, existing: dict | None = None):
    """演奏会の新規登録 / 編集フォーム。existing が None なら新規。"""
    is_edit = existing is not None
    prefix  = f"conc_edit_{existing.get('id','')}_" if is_edit else "conc_new_"
    ext     = ctx["extract_prop_text_any"]

    venue_default = ext(existing, CONCERT_VENUE_KEYS) if is_edit else ""
    address_default = ext(existing, CONCERT_ADDRESS_KEYS) if is_edit else ""
    # ATLAS 側が「ロケーション」単独運用のデータでも会場欄に表示する
    if is_edit:
        location_fallback = ext(existing, ["ロケーション", "場所", "Location"])
        if not venue_default and location_fallback:
            venue_default = location_fallback
        if not address_default and location_fallback:
            address_default = location_fallback

    with st.form(key=f"{prefix}form", border=True):
        name = st.text_input(
            "演奏会名 *",
            value=ext(existing, CONCERT_NAME_KEYS) if is_edit else "",
            placeholder="例：第12回定期演奏会",
            key=f"{prefix}name",
        )

        col1, col2 = st.columns(2)
        with col1:
            dt_start_str = ext(existing, CONCERT_DATE_KEYS) if is_edit else ""
            dt_start_val = date.fromisoformat(dt_start_str[:10]) if dt_start_str else date.today()
            dt_start = st.date_input("開催日 *", value=dt_start_val, key=f"{prefix}dt_start")
        with col2:
            dt_end = st.date_input("終了日（任意）", value=dt_start_val, key=f"{prefix}dt_end")

        venue   = st.text_input("会場名", value=venue_default,
                                placeholder="例：○○ホール", key=f"{prefix}venue")
        address = st.text_input("会場住所", value=address_default,
                                placeholder="任意", key=f"{prefix}address")
        memo    = st.text_area("メモ", value=ext(existing, CONCERT_MEMO_KEYS) if is_edit else "",
                               height=80, key=f"{prefix}memo")

        label = "更新" if is_edit else "登録"
        submitted = st.form_submit_button(f"💾 {label}", use_container_width=True, type="primary")

    if submitted:
        if not name.strip():
            st.error("演奏会名は必須です。")
            return
        dt_s = dt_start.isoformat()
        dt_e = dt_end.isoformat() if dt_end and dt_end != dt_start else dt_s

        with st.spinner(f"{label}中..."):
            if is_edit:
                ok = _update_concert(ctx, existing["id"], name.strip(), dt_s, dt_e,
                                     venue, address, memo)
            else:
                ok = _create_concert(ctx, name.strip(), dt_s, dt_e, venue, address, memo)

        if ok:
            st.success(f"✅ 演奏会を{label}しました。")
            _clear_concert_cache(ctx)
            st.rerun()
        else:
            st.error(f"❌ {label}に失敗しました。Notion の接続・プロパティ名を確認してください。")


# ============================================================
# 練習フォーム
# ============================================================

def _render_practice_form(ctx: dict, concerts: list[dict], existing: dict | None = None):
    """練習の新規登録 / 編集フォーム。"""
    is_edit = existing is not None
    prefix  = f"prac_edit_{existing.get('id','')}_" if is_edit else "prac_new_"
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids"]

    # 演奏会セレクタ
    concert_options = {_concert_display_name(c, ctx): c.get("id", "") for c in concerts}
    concert_names   = ["（未選択）"] + list(concert_options.keys())

    current_concert_id = ""
    if is_edit:
        ids = ctx["extract_relation_ids_any"](existing, PRACTICE_CONCERT_REL_KEYS)
        current_concert_id = ids[0] if ids else ""
    current_concert_name = next(
        (k for k, v in concert_options.items() if v == current_concert_id), "（未選択）"
    )

    venue_default = ext(existing, PRACTICE_VENUE_KEYS) if is_edit else ""
    address_default = ext(existing, PRACTICE_ADDRESS_KEYS) if is_edit else ""
    prefill_venue_key = f"{prefix}prefill_venue"
    prefill_address_key = f"{prefix}prefill_address"
    if _ss(prefill_venue_key):
        venue_default = _ss(prefill_venue_key, "")
    if _ss(prefill_address_key):
        address_default = _ss(prefill_address_key, "")
    if is_edit:
        location_fallback = ext(existing, ["ロケーション", "場所", "Location"])
        if not venue_default and location_fallback:
            venue_default = location_fallback
        if not address_default and location_fallback:
            address_default = location_fallback

    # 会場検索（フォーム外）
    venue_q_key = f"{prefix}venue_query"
    venue_list_key = f"{prefix}venue_candidates"
    venue_sel_key = f"{prefix}venue_candidate_index"
    with st.expander("🗺️ 会場を検索して反映（任意）", expanded=False):
        c1, c2 = st.columns([4, 1])
        c1.text_input("会場検索ワード", key=venue_q_key, placeholder="例: 門真市民文化会館")
        if c2.button("🔎 検索", key=f"{prefix}venue_search_btn", use_container_width=True):
            st.session_state[venue_list_key] = _geocode_nominatim(_ss(venue_q_key, ""))
            st.session_state[venue_sel_key] = 0
        candidates = _ss(venue_list_key, [])
        if candidates:
            labels = [c.get("name", "") for c in candidates]
            idx = st.selectbox(
                "候補",
                options=list(range(len(labels))),
                format_func=lambda i: labels[i],
                index=min(_ss(venue_sel_key, 0), max(len(labels) - 1, 0)),
                key=venue_sel_key,
            )
            picked = candidates[idx]
            if st.button("✅ この候補をフォームに反映", key=f"{prefix}apply_venue_candidate"):
                st.session_state[prefill_venue_key] = picked.get("name", "")
                st.session_state[prefill_address_key] = picked.get("address", "")
                st.rerun()

    with st.form(key=f"{prefix}form", border=True):
        selected_concert_name = st.selectbox(
            "演奏会",
            concert_names,
            index=concert_names.index(current_concert_name) if current_concert_name in concert_names else 0,
            key=f"{prefix}concert",
        )
        selected_concert_id = concert_options.get(selected_concert_name, "")

        if is_edit:
            name = st.text_input(
                "練習名 *",
                value=ext(existing, PRACTICE_NAME_KEYS),
                placeholder="例：第3回練習",
                key=f"{prefix}name",
            )
            practice_round = None
        else:
            # 同演奏会の既存練習名から「第N回練習」を拾って次番号を提案
            max_round = 0
            if selected_concert_id:
                for row in _load_practices(ctx, selected_concert_id):
                    nm = ctx["extract_prop_text_any"](row, PRACTICE_NAME_KEYS) or ""
                    m = re.search(r"第\s*(\d+)\s*回練習", nm)
                    if m:
                        max_round = max(max_round, int(m.group(1)))
            suggested_round = max_round + 1 if max_round > 0 else 1
            practice_round = int(st.number_input(
                "練習回数 *",
                min_value=1,
                value=suggested_round,
                step=1,
                key=f"{prefix}round_no",
            ))
            auto_name = f"第{practice_round}回練習"
            name = st.text_input(
                "練習名（自動）",
                value=auto_name,
                disabled=True,
                key=f"{prefix}name_auto",
            )

        dt_start_str = ext(existing, PRACTICE_DATE_KEYS) if is_edit else ""
        dt_start_val = date.fromisoformat(dt_start_str[:10]) if dt_start_str else date.today()
        start_time_default = ""
        if dt_start_str and "T" in dt_start_str:
            try:
                start_time_default = dt_start_str.split("T", 1)[1][:5]
            except Exception:
                start_time_default = ""
        dt_start = st.date_input("練習日 *", value=dt_start_val, key=f"{prefix}dt_start")
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            start_time = st.text_input(
                "開始時刻（任意）",
                value=start_time_default,
                placeholder="例: 19:00",
                key=f"{prefix}start_time",
            )
        with col_t2:
            end_time = st.text_input(
                "終了時刻（任意）",
                value="",
                placeholder="例: 21:00",
                key=f"{prefix}end_time",
            )
        rest_default = (ext(existing, PRACTICE_REST_KEYS).strip().lower() == "true") if is_edit else False
        is_rest_day = st.checkbox(
            "打楽器休み（ON時は日時以外の入力を無効化）",
            value=rest_default,
            key=f"{prefix}rest_day",
        )

        col3, col4 = st.columns(2)
        with col3:
            venue = st.text_input("会場名", value=venue_default,
                                  placeholder="例：○○スタジオ", key=f"{prefix}venue", disabled=is_rest_day)
        with col4:
            address = st.text_input("会場住所", value=address_default,
                                    placeholder="任意", key=f"{prefix}address", disabled=is_rest_day)

        is_concert_day = st.checkbox(
            "演奏会当日フラグ（本番日の場合はチェック）",
            value=(ext(existing, PRACTICE_CONCERT_DAY_KEYS) == "True") if is_edit else False,
            key=f"{prefix}concert_day",
            disabled=is_rest_day,
        )
        memo = st.text_area("メモ", value=ext(existing, PRACTICE_MEMO_KEYS) if is_edit else "",
                            height=80, key=f"{prefix}memo", disabled=is_rest_day)

        label = "更新" if is_edit else "登録"
        submitted = st.form_submit_button(f"💾 {label}", use_container_width=True, type="primary")

    if submitted:
        if not name.strip():
            st.error("練習名は必須です。")
            return
        concert_id = selected_concert_id
        if not is_edit and practice_round:
            name = f"第{practice_round}回練習"
        try:
            dt_s, dt_e = _compose_notion_date_with_optional_time(dt_start, start_time, end_time)
        except ValueError as e:
            st.error(str(e))
            return

        if is_rest_day:
            venue = ""
            address = ""
            is_concert_day = False
            memo = ""

        with st.spinner(f"{label}中..."):
            if is_edit:
                ok = _update_practice(ctx, existing["id"], name.strip(), concert_id,
                                      dt_s, dt_e, venue, address, is_concert_day, is_rest_day, memo)
            else:
                ok = _create_practice(ctx, name.strip(), concert_id,
                                      dt_s, dt_e, venue, address, is_concert_day, is_rest_day, memo)

        if ok:
            st.success(f"✅ 練習を{label}しました。")
            st.session_state.pop(prefill_venue_key, None)
            st.session_state.pop(prefill_address_key, None)
            st.session_state.pop(venue_list_key, None)
            _clear_concert_cache(ctx)
            st.rerun()
        else:
            st.error(f"❌ {label}に失敗しました。")


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎼 演奏会・練習管理")

    tab_concert, tab_practice = st.tabs(["演奏会", "練習"])

    # ── 演奏会タブ ────────────────────────────────────────────
    with tab_concert:
        concerts = _load_concerts(ctx)

        with st.expander("➕ 新規演奏会を登録", expanded=(len(concerts) == 0)):
            _render_concert_form(ctx)

        st.divider()

        if not concerts:
            st.info("演奏会がまだ登録されていません。")
        else:
            st.subheader(f"登録済み演奏会（{len(concerts)}件）")
            st.caption("ここは演奏会の参照・選択が主目的です。編集は必要なときだけ開いてください。")
            col_search, col_refresh = st.columns([8, 1])
            concert_query = col_search.text_input(
                "演奏会を検索",
                value=_ss("concert_mgmt_concert_query", ""),
                placeholder="例: Osaka / 2026 / Summer / 門真",
                key="concert_mgmt_concert_query",
            )
            if col_refresh.button("🔄", key="refresh_concerts", help="一覧を再読み込み"):
                st.session_state.pop("concert_list", None)
                st.rerun()

            filtered_concerts = []
            for c in concerts:
                if not _contains_query(
                    [
                        _concert_display_name(c, ctx),
                        ctx["extract_prop_text_any"](c, CONCERT_NAME_KEYS),
                        ctx["extract_prop_text_any"](c, CONCERT_DATE_KEYS),
                        ctx["extract_prop_text_any"](c, CONCERT_VENUE_KEYS),
                        ctx["extract_prop_text_any"](c, CONCERT_ADDRESS_KEYS),
                        ctx["extract_prop_text_any"](c, CONCERT_MEMO_KEYS),
                    ],
                    concert_query,
                ):
                    continue
                filtered_concerts.append(c)

            st.caption(f"表示件数: {len(filtered_concerts)} / {len(concerts)}")
            if not filtered_concerts:
                st.info("検索条件に一致する演奏会がありません。")
            for c in filtered_concerts:
                label = _concert_display_name(c, ctx)
                with st.expander(label, expanded=False):
                    cid = c.get("id", "")
                    st.caption(f"ID: {cid}")
                    sel_col, edit_col = st.columns([2, 3])
                    if sel_col.button("✅ この演奏会を練習入力対象にする", key=f"use_concert_{cid}", use_container_width=True):
                        st.session_state["practice_filter_concert"] = label
                        st.success("練習タブでこの演奏会が選択されるように設定しました。")
                    edit_open = edit_col.checkbox("この演奏会を編集する", key=f"open_edit_concert_{cid}", value=False)
                    if edit_open:
                        _render_concert_form(ctx, existing=c)

    # ── 練習タブ ──────────────────────────────────────────────
    with tab_practice:
        concerts = _load_concerts(ctx)

        # 演奏会フィルタ
        concert_filter_opts = {"すべて": ""} | {
            _concert_display_name(c, ctx): c.get("id", "") for c in concerts
        }
        selected_filter = st.selectbox(
            "絞り込み：演奏会",
            list(concert_filter_opts.keys()),
            key="practice_filter_concert",
        )
        filter_concert_id = concert_filter_opts.get(selected_filter, "")

        with st.expander("➕ 新規練習を登録", expanded=False):
            _render_practice_form(ctx, concerts)

        st.divider()

        practices = _load_practices(ctx, filter_concert_id)

        if not practices:
            st.info("練習がまだ登録されていません。")
        else:
            st.subheader(f"登録済み練習（{len(practices)}件）")
            col_search, col_refresh = st.columns([8, 1])
            practice_query = col_search.text_input(
                "練習を検索",
                value=_ss("concert_mgmt_practice_query", ""),
                placeholder="例: 第3回 / 2026-07 / 本番 / スタジオ",
                key="concert_mgmt_practice_query",
            )
            if col_refresh.button("🔄", key="refresh_practices", help="一覧を再読み込み"):
                for k in list(st.session_state.keys()):
                    if k.startswith("practice_list_"):
                        st.session_state.pop(k, None)
                st.rerun()

            # 日付順ソート
            def _prac_date(p):
                d = ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS)
                return d[:10] if d else "9999"

            sorted_practices = sorted(practices, key=_prac_date)
            filtered_practices = []
            for p in sorted_practices:
                if not _contains_query(
                    [
                        _practice_display_name(p, ctx),
                        ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS),
                        ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS),
                        ctx["extract_prop_text_any"](p, PRACTICE_VENUE_KEYS),
                        ctx["extract_prop_text_any"](p, PRACTICE_ADDRESS_KEYS),
                        ctx["extract_prop_text_any"](p, PRACTICE_MEMO_KEYS),
                    ],
                    practice_query,
                ):
                    continue
                filtered_practices.append(p)

            st.caption(f"表示件数: {len(filtered_practices)} / {len(practices)}")
            if not filtered_practices:
                st.info("検索条件に一致する練習がありません。")
            for p in filtered_practices:
                label = _practice_display_name(p, ctx)
                is_concert_day = ctx["extract_prop_text_any"](p, PRACTICE_CONCERT_DAY_KEYS) == "True"
                if is_concert_day:
                    label = "🎼 " + label + "  【本番】"
                with st.expander(label, expanded=False):
                    _render_practice_form(ctx, concerts, existing=p)
