"""
concert.pages.assign
アサイン検討画面（希望入力 / 候補生成 / 反映）。
"""
from __future__ import annotations

import streamlit as st

from concert.services.assign_solver import solve_all


CONCERT_NAME_KEYS = ["名称", "タイトル", "演奏会名", "PK名称"]
CONCERT_DATE_KEYS = ["日時", "日付", "出演日", "体験日", "リリース日"]
CONCERT_MEDIA_KEYS = ["媒体", "MEDIA_TYPE"]

SONG_NAME_KEYS = ["タイトル", "曲名", "名称"]
SONG_CONCERT_REL_KEYS = ["出演", "演奏会", "FK演奏会"]

PLAYER_NAME_KEYS = ["氏名", "名前", "表示名", "タイトル"]

INST_NAME_KEYS = ["楽器名", "タイトル", "PK楽器名"]

PART_NAME_KEYS = ["パート名", "名称", "表示名", "タイトル"]
PART_SONG_REL_KEYS = ["楽曲", "演奏曲", "FK楽曲", "作品楽章", "作品マスタ"]
PART_INST_REL_KEYS = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PART_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]

PREF_RECORD_KEYS = ["レコード名", "タイトル", "名称"]
PREF_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
PREF_PLAYER_REL_KEYS = ["奏者", "出演者", "FK奏者", "演奏会参加者"]
PREF_SONG_REL_KEYS = ["楽曲", "演奏曲", "FK楽曲", "作品楽章", "作品マスタ"]
PREF_PART_REL_KEYS = ["パート", "パート定義", "FKパート"]
PREF_INST_REL_KEYS = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PREF_PRIORITY_KEYS = ["希望順位", "優先度", "希望", "希望区分"]
PREF_CAN_BRING_KEYS = ["持参可", "持参可フラグ", "持参"]

ASSIGN_RECORD_KEYS = ["レコード名", "タイトル", "名称"]
ASSIGN_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
ASSIGN_PLAYER_REL_KEYS = ["奏者", "出演者", "演奏会参加者", "FK奏者"]
ASSIGN_SONG_REL_KEYS = ["楽曲", "演奏曲", "スコア", "FK楽曲", "作品楽章"]
ASSIGN_INST_REL_KEYS = ["楽器", "楽器種別", "担当楽器", "FK楽器種別", "パート"]
ASSIGN_STATUS_KEYS = ["確定フラグ", "担当フラグ", "確定", "ステータス"]
ASSIGN_NOTE_KEYS = ["メモ", "備考"]


def _concert_label(ctx: dict, page: dict) -> str:
    name = ctx["extract_prop_text_any"](page, CONCERT_NAME_KEYS) or ctx["extract_title"](page)
    d = (ctx["extract_prop_text_any"](page, CONCERT_DATE_KEYS) or "")[:10]
    return f"{name}（{d if d else '日時未設定'}）"


def _load_concerts(ctx: dict) -> list[dict]:
    rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
    out = []
    for r in rows:
        media = ctx["extract_prop_text_any"](r, CONCERT_MEDIA_KEYS)
        if media and "出演" not in media:
            continue
        out.append(r)
    return out


def _load_songs_by_concert(ctx: dict, concert_id: str) -> list[dict]:
    rows = ctx["query_all"](ctx["CONCERT_DB_SONG"])
    t = ctx["get_prop_types"](ctx["CONCERT_DB_SONG"])
    rel = ctx["find_prop_name"](t, SONG_CONCERT_REL_KEYS)
    if not rel:
        return rows
    out = []
    for r in rows:
        ids = ctx["extract_relation_ids"](r, rel)
        if concert_id in ids:
            out.append(r)
    return out


def _load_parts(ctx: dict, concert_id: str, song_id: str) -> list[dict]:
    rows = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"])
    t = ctx["get_prop_types"](ctx["CONCERT_DB_PART_DEFINITION"])
    c_rel = ctx["find_prop_name"](t, PART_CONCERT_REL_KEYS)
    s_rel = ctx["find_prop_name"](t, PART_SONG_REL_KEYS)
    out = []
    for r in rows:
        ok = True
        if c_rel:
            ok = concert_id in ctx["extract_relation_ids"](r, c_rel)
        if ok and s_rel:
            ok = song_id in ctx["extract_relation_ids"](r, s_rel)
        if ok:
            out.append(r)
    return out


def _create_pref(ctx: dict, concert_id: str, player_id: str, song_id: str, part_id: str, inst_id: str, priority_label: str, can_bring: bool):
    db_id = ctx["CONCERT_DB_PREFERENCE"]
    t = ctx["get_prop_types"](db_id)
    props = {}
    ctx["put_prop_any"](props, t, PREF_RECORD_KEYS, f"{player_id[:6]}:{song_id[:6]}:{part_id[:6]}")
    ctx["put_prop_any"](props, t, PREF_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PREF_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, PREF_SONG_REL_KEYS, song_id)
    ctx["put_prop_any"](props, t, PREF_PART_REL_KEYS, part_id)
    if inst_id:
        ctx["put_prop_any"](props, t, PREF_INST_REL_KEYS, inst_id)
    ctx["put_prop_any"](props, t, PREF_PRIORITY_KEYS, priority_label)
    ctx["put_prop_any"](props, t, PREF_CAN_BRING_KEYS, can_bring)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
    return res is not None and res.status_code == 200


def _apply_candidate(ctx: dict, concert_id: str, candidate: dict) -> tuple[int, int]:
    db_id = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    t = ctx["get_prop_types"](db_id)
    ok_n = 0
    ng_n = 0
    for a in candidate.get("assignments", []):
        props = {}
        label = f"{a.get('song_name','')} / {a.get('part_name','')} / {a.get('player_name','')}"
        ctx["put_prop_any"](props, t, ASSIGN_RECORD_KEYS, label)
        ctx["put_prop_any"](props, t, ASSIGN_CONCERT_REL_KEYS, concert_id)
        ctx["put_prop_any"](props, t, ASSIGN_PLAYER_REL_KEYS, a.get("player_id", ""))
        ctx["put_prop_any"](props, t, ASSIGN_SONG_REL_KEYS, a.get("song_id", ""))
        if a.get("instrument_id"):
            ctx["put_prop_any"](props, t, ASSIGN_INST_REL_KEYS, a.get("instrument_id"))
        ctx["put_prop_any"](props, t, ASSIGN_STATUS_KEYS, True)
        ctx["put_prop_any"](props, t, ASSIGN_NOTE_KEYS, f"HARMONIA候補反映: {candidate.get('label','')}")
        res = ctx["api_request"]("post", "https://api.notion.com/v1/pages", json={"parent": {"database_id": db_id}, "properties": props})
        if res is not None and res.status_code == 200:
            ok_n += 1
        else:
            ng_n += 1
    return ok_n, ng_n


def render(ctx: dict):
    st.header("🎼 アサイン検討")
    st.caption("希望入力 → 候補生成（A〜E） → 採用案の反映、の順で進めます。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("出演演奏会が見つかりません。先に演奏会を登録してください。")
        return

    c_opts = {_concert_label(ctx, c): c for c in concerts}
    c_query = st.text_input("演奏会を検索", key="assign_concert_search", placeholder="例: 2026 / Happy Hour / 定期").strip().lower()
    if c_query:
        c_opts = {k: v for k, v in c_opts.items() if c_query in k.lower()}
    if not c_opts:
        st.warning("一致する演奏会がありません。")
        return

    c_name = st.selectbox("演奏会を選択", list(c_opts.keys()), key="assign_concert_sel")
    concert = c_opts[c_name]
    concert_id = concert.get("id", "")
    if not concert_id:
        return

    t1, t2, t3 = st.tabs(["希望入力", "アルゴリズム実行", "結果確認"])

    with t1:
        players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
        songs = _load_songs_by_concert(ctx, concert_id)
        if not players or not songs:
            st.info("奏者または演奏会紐づき楽曲が不足しています。")
        else:
            p_opts = {
                (ctx["extract_prop_text_any"](p, PLAYER_NAME_KEYS) or ctx["extract_title"](p) or p.get("id", "")): p
                for p in players
            }
            s_opts = {
                (ctx["extract_prop_text_any"](s, SONG_NAME_KEYS) or ctx["extract_title"](s) or s.get("id", "")): s
                for s in songs
            }
            col1, col2 = st.columns(2)
            with col1:
                p_name = st.selectbox("奏者", list(p_opts.keys()), key="pref_player")
            with col2:
                s_name = st.selectbox("楽曲", list(s_opts.keys()), key="pref_song")
            p_id = p_opts[p_name]["id"]
            s_id = s_opts[s_name]["id"]

            parts = _load_parts(ctx, concert_id, s_id)
            if not parts:
                st.warning("この曲のパート定義が見つかりません。先にパート定義DBを登録してください。")
            else:
                part_opts = {
                    (ctx["extract_prop_text_any"](x, PART_NAME_KEYS) or ctx["extract_title"](x) or x.get("id", "")): x
                    for x in parts
                }
                part_name = st.selectbox("パート", list(part_opts.keys()), key="pref_part")
                part = part_opts[part_name]
                part_id = part["id"]
                inst_id = (ctx["extract_relation_ids_any"](part, PART_INST_REL_KEYS) or [""])[0]

                c1, c2 = st.columns([2, 1])
                with c1:
                    pr = st.selectbox("希望順位", ["第1希望", "第2希望", "第3希望", "降り番希望", "絶対NG", "なし"], index=0, key="pref_priority")
                with c2:
                    can_bring = st.checkbox("持参可", value=False, key="pref_can_bring")

                if st.button("➕ 希望を保存", type="primary", use_container_width=True):
                    if _create_pref(ctx, concert_id, p_id, s_id, part_id, inst_id, pr, can_bring):
                        st.success("✅ 希望入力DBへ保存しました。")
                    else:
                        st.error("❌ 保存に失敗しました。")

    with t2:
        if st.button("🚀 割当候補を生成", type="primary", use_container_width=True):
            cands = solve_all(ctx, concert_id)
            st.session_state["assign_candidates"] = cands

        cands = st.session_state.get("assign_candidates", [])
        if not cands:
            st.info("候補はまだ生成されていません。")
        else:
            for i, cand in enumerate(cands):
                with st.expander(cand["label"], expanded=(i == 0)):
                    stats = cand.get("stats", {})
                    st.caption(
                        f"第1希望率: {stats.get('first_choice_rate',0):.2%} / "
                        f"総スコア: {stats.get('total_score',0)} / "
                        f"最低スコア: {stats.get('min_score',0)} / "
                        f"レンタル必要: {stats.get('rental_count',0)} / "
                        f"降り番分散: {stats.get('rest_std',0)}"
                    )
                    rows = []
                    for a in cand.get("assignments", []):
                        rows.append(
                            {
                                "楽曲": a.get("song_name", ""),
                                "パート": a.get("part_name", ""),
                                "奏者": a.get("player_name", ""),
                                "楽器": a.get("instrument_name", ""),
                                "由来": a.get("source", ""),
                            }
                        )
                    st.dataframe(rows, use_container_width=True, hide_index=True)
                    if st.button(f"✅ この候補を反映: {cand['label']}", key=f"apply_cand_{i}", use_container_width=True):
                        ok_n, ng_n = _apply_candidate(ctx, concert_id, cand)
                        if ng_n == 0:
                            st.success(f"✅ 反映完了: {ok_n}件")
                        else:
                            st.warning(f"⚠️ 反映: 成功 {ok_n} / 失敗 {ng_n}")

    with t3:
        st.caption("ここでは演奏会別の既存アサイン（楽曲別担当者DB）を確認します。")
        rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"])
        out = []
        for r in rows:
            cids = ctx["extract_relation_ids_any"](r, ASSIGN_CONCERT_REL_KEYS)
            if cids and concert_id not in cids:
                continue
            out.append(
                {
                    "ラベル": ctx["extract_prop_text_any"](r, ASSIGN_RECORD_KEYS) or ctx["extract_title"](r),
                    "確定": ctx["extract_prop_text_any"](r, ASSIGN_STATUS_KEYS),
                    "備考": ctx["extract_prop_text_any"](r, ASSIGN_NOTE_KEYS),
                }
            )
        st.dataframe(out, use_container_width=True, hide_index=True)

