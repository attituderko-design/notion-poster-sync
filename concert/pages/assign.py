"""
concert.pages.assign
パート割当画面。
  タブ1：希望入力（管理者が転記）
  タブ2：アルゴリズム実行・候補案表示
  タブ3：割当結果確認（マトリクス表示）
"""
import streamlit as st
from collections import defaultdict


# ============================================================
# 定数
# ============================================================

PRIORITY_OPTIONS = ["なし", "第1希望", "第2希望", "第3希望", "降り番希望", "絶対NG"]
PRIORITY_TO_INT  = {"第1希望": 1, "第2希望": 2, "第3希望": 3, "降り番希望": 0, "絶対NG": -1, "なし": None}
INT_TO_PRIORITY  = {1: "第1希望", 2: "第2希望", 3: "第3希望", 0: "降り番希望", -1: "絶対NG"}
SCORE_LABEL      = {3.0: "第1希望", 2.0: "第2希望", 1.0: "第3希望", 0.5: "フォールバック", 0.0: "降り番希望", -9999.0: "絶対NG"}


# ============================================================
# キャッシュ／ロードヘルパー
# ============================================================

def _clear_assign_cache():
    for k in list(st.session_state.keys()):
        if k.startswith(("assign_pref_", "assign_result_", "song_list_", "si_list_", "pi_list_")):
            st.session_state.pop(k, None)


def _load_concerts(ctx) -> list[dict]:
    if "concert_list" not in st.session_state:
        st.session_state["concert_list"] = ctx["query_all"](ctx["CONCERT_DB_CONCERT"])
    return st.session_state.get("concert_list", [])


def _load_players(ctx) -> list[dict]:
    if "player_list" not in st.session_state:
        st.session_state["player_list"] = ctx["query_all"](ctx["CONCERT_DB_PLAYER"])
    return st.session_state.get("player_list", [])


def _load_songs(ctx, concert_id: str) -> list[dict]:
    key = f"song_list_{concert_id}"
    if key not in st.session_state:
        st.session_state[key] = ctx["query_all"](
            ctx["CONCERT_DB_SONG"],
            {"filter": {"property": "演奏会", "relation": {"contains": concert_id}}},
        )
    return st.session_state.get(key, [])


def _load_song_instruments(ctx, song_id: str) -> list[dict]:
    key = f"si_list_{song_id}"
    if key not in st.session_state:
        st.session_state[key] = ctx["query_all"](
            ctx["CONCERT_DB_SONG_INSTRUMENT"],
            {"filter": {"property": "楽曲", "relation": {"contains": song_id}}},
        )
    return st.session_state.get(key, [])


def _load_player_instruments(ctx, concert_id: str) -> list[dict]:
    key = f"pi_list_{concert_id}"
    if key not in st.session_state:
        st.session_state[key] = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"])
    return st.session_state.get(key, [])


def _concert_name(c, ctx) -> str:
    n  = ctx["extract_prop_text"](c, "名称") or ctx["extract_title"](c)
    dt = ctx["extract_prop_text"](c, "日時")
    return f"{n}（{dt[:10] if dt else '日時未設定'}）"


def _player_name(p, ctx) -> str:
    return ctx["extract_prop_text"](p, "氏名") or ctx["extract_title"](p) or p.get("id", "")


def _song_name(s, ctx) -> str:
    return ctx["extract_prop_text"](s, "曲名") or ctx["extract_title"](s) or s.get("id", "")


def _instrument_name(i, ctx) -> str:
    return ctx["extract_prop_text"](i, "楽器名") or ctx["extract_title"](i) or i.get("id", "")


# ============================================================
# Notionへの希望保存
# ============================================================

def _save_preference(ctx, player_id: str, player_name: str,
                     song_id: str, song_name: str,
                     instrument_id: str, instrument_name: str,
                     priority_int: int,
                     existing_id: str = "") -> bool:
    """PlayerInstrumentに希望順位を書き込む。"""
    db_id    = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        return False
    props: dict = {}
    ctx["put_prop"](props, type_map, "レコード名",
                    f"{player_name} × {song_name} × {instrument_name}")
    ctx["put_prop"](props, type_map, "奏者",     player_id)
    ctx["put_prop"](props, type_map, "楽器種別", instrument_id)
    ctx["put_prop"](props, type_map, "楽曲",     song_id)
    ctx["put_prop"](props, type_map, "希望順位",
                    INT_TO_PRIORITY.get(priority_int, "なし"))

    if existing_id:
        res = ctx["api_request"]("patch",
                                 f"https://api.notion.com/v1/pages/{existing_id}",
                                 json={"properties": props})
    else:
        res = ctx["api_request"]("post",
                                 "https://api.notion.com/v1/pages",
                                 json={"parent": {"database_id": db_id},
                                       "properties": props})
    return res is not None and res.status_code == 200


# ============================================================
# assign_solver用データ変換
# ============================================================

def _build_solver_input(ctx, concert_id: str, songs: list, players: list):
    """
    NotionのDBからsolve_all()に渡すPrefs/Requirementsを構築する。
    assign_solver.pyのPref/Requirementデータクラスを直接使わず、
    dictで渡してsolve_all内で変換する形にする。
    """
    from concert.services.assign_solver import Pref, Requirement

    # 楽器マスタ
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}

    # Requirements: SongInstrumentから構築
    requirements: list[Requirement] = []
    for song in songs:
        sid   = song.get("id", "")
        sname = _song_name(song, ctx)
        si_rows = _load_song_instruments(ctx, sid)
        for si in si_rows:
            iids = ctx["extract_relation_ids"](si, "楽器種別")
            if not iids:
                continue
            iid   = iids[0]
            iname = inst_name_map.get(iid, iid)
            qty_str = ctx["extract_prop_text"](si, "必要台数")
            try:
                qty = max(int(float(qty_str)), 1) if qty_str else 1
            except ValueError:
                qty = 1
            note = ctx["extract_prop_text"](si, "備考") or ""
            part_id   = si.get("id", "")
            part_name = f"{iname}（{note}）" if note else iname
            requirements.append(Requirement(
                song_id=sid, song_name=sname,
                part_id=part_id, part_name=part_name,
                instrument_id=iid, instrument_name=iname,
                required_count=qty,
            ))

    # Prefs: PlayerInstrumentの希望順位から構築
    prefs: list[Pref] = []
    pi_rows = _load_player_instruments(ctx, concert_id)

    player_name_map = {p.get("id"): _player_name(p, ctx) for p in players}
    song_name_map   = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_id_set     = {s.get("id") for s in songs}

    for pi in pi_rows:
        player_ids = ctx["extract_relation_ids"](pi, "奏者")
        inst_ids   = ctx["extract_relation_ids"](pi, "楽器種別")
        song_ids   = ctx["extract_relation_ids"](pi, "楽曲")
        if not (player_ids and inst_ids and song_ids):
            continue
        pid    = player_ids[0]
        iid    = inst_ids[0]
        sid    = song_ids[0]
        if sid not in song_id_set:
            continue

        priority_str = ctx["extract_prop_text"](pi, "希望順位")
        priority_int = PRIORITY_TO_INT.get(priority_str)
        if priority_int is None:
            continue  # 「なし」はスキップ

        # part_idはSongInstrumentのIDを使う（楽曲×楽器の組合せで一意）
        matching_reqs = [r for r in requirements if r.song_id == sid and r.instrument_id == iid]
        part_id   = matching_reqs[0].part_id   if matching_reqs else f"{sid}_{iid}"
        part_name = matching_reqs[0].part_name  if matching_reqs else inst_name_map.get(iid, iid)

        prefs.append(Pref(
            player_id=pid,
            player_name=player_name_map.get(pid, pid),
            song_id=sid,
            song_name=song_name_map.get(sid, sid),
            part_id=part_id,
            part_name=part_name,
            instrument_id=iid,
            instrument_name=inst_name_map.get(iid, iid),
            priority=priority_int,
            can_bring=False,  # 持参可フラグはスコア計算から除外済み
        ))

    return prefs, requirements


# ============================================================
# タブ1：希望入力
# ============================================================

def _render_pref_tab(ctx: dict):
    st.caption("管理者がアンケート結果を転記する画面です。奏者×曲×楽器ごとに希望順位を選択して保存してください。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("先に演奏会を登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    selected_concert = st.selectbox("演奏会を選択", list(concert_opts.keys()), key="pref_concert_sel")
    concert_id = concert_opts.get(selected_concert, "")
    if not concert_id:
        return

    players = _load_players(ctx)
    songs   = _load_songs(ctx, concert_id)
    if not players:
        st.info("奏者を先に登録してください。")
        return
    if not songs:
        st.info("この演奏会に楽曲が登録されていません。")
        return

    # 楽器マスタ
    inst_rows     = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}

    # 既存のPlayerInstrumentを取得（奏者×楽曲×楽器 → レコードIDと現在の希望順位）
    pi_rows = _load_player_instruments(ctx, concert_id)
    pi_lookup: dict[tuple, dict] = {}  # (player_id, song_id, inst_id) → row
    for pi in pi_rows:
        pids = ctx["extract_relation_ids"](pi, "奏者")
        iids = ctx["extract_relation_ids"](pi, "楽器種別")
        sids = ctx["extract_relation_ids"](pi, "楽曲")
        if pids and iids and sids:
            pi_lookup[(pids[0], sids[0], iids[0])] = pi

    player_opts = {_player_name(p, ctx): p.get("id", "") for p in
                   sorted(players, key=lambda x: _player_name(x, ctx))}
    selected_player_name = st.selectbox("奏者を選択", list(player_opts.keys()), key="pref_player_sel")
    player_id = player_opts.get(selected_player_name, "")
    if not player_id:
        return

    st.subheader(f"希望入力：{selected_player_name}")

    for song in sorted(songs, key=lambda x: _song_name(x, ctx)):
        sid   = song.get("id", "")
        sname = _song_name(song, ctx)

        si_rows = _load_song_instruments(ctx, sid)
        if not si_rows:
            st.caption(f"　{sname}：必要楽器が未登録です。")
            continue

        with st.expander(f"🎵 {sname}", expanded=True):
            with st.form(f"pref_form_{player_id}_{sid}", border=True):
                changes = []
                for si in si_rows:
                    iids = ctx["extract_relation_ids"](si, "楽器種別")
                    if not iids:
                        continue
                    iid   = iids[0]
                    iname = inst_name_map.get(iid, iid)
                    note  = ctx["extract_prop_text"](si, "備考") or ""
                    label = f"{iname}（{note}）" if note else iname

                    existing = pi_lookup.get((player_id, sid, iid))
                    cur_priority_str = ctx["extract_prop_text"](existing, "希望順位") if existing else "なし"
                    if cur_priority_str not in PRIORITY_OPTIONS:
                        cur_priority_str = "なし"
                    cur_idx = PRIORITY_OPTIONS.index(cur_priority_str)

                    col_inst, col_sel = st.columns([3, 2])
                    col_inst.markdown(f"**{label}**")
                    priority_sel = col_sel.selectbox(
                        label, PRIORITY_OPTIONS, index=cur_idx,
                        label_visibility="collapsed",
                        key=f"pref_sel_{player_id}_{sid}_{iid}",
                    )
                    changes.append({
                        "iid":         iid,
                        "iname":       iname,
                        "priority_str": priority_sel,
                        "existing_id": existing.get("id", "") if existing else "",
                    })

                if st.form_submit_button("💾 保存", use_container_width=True, type="primary"):
                    ok_count = fail_count = 0
                    with st.spinner("保存中..."):
                        for ch in changes:
                            pri_int = PRIORITY_TO_INT.get(ch["priority_str"])
                            if pri_int is None and not ch["existing_id"]:
                                continue  # 「なし」かつ未登録はスキップ
                            if pri_int is None and ch["existing_id"]:
                                # 「なし」に変更 → アーカイブ
                                res = ctx["api_request"](
                                    "patch",
                                    f"https://api.notion.com/v1/pages/{ch['existing_id']}",
                                    json={"archived": True},
                                )
                                if res and res.status_code == 200:
                                    ok_count += 1
                                else:
                                    fail_count += 1
                                continue
                            ok = _save_preference(
                                ctx, player_id, selected_player_name,
                                sid, sname, ch["iid"], ch["iname"],
                                pri_int, ch["existing_id"],
                            )
                            if ok:
                                ok_count += 1
                            else:
                                fail_count += 1

                    if fail_count == 0:
                        st.success(f"✅ {ok_count}件を保存しました。")
                        st.session_state.pop(f"pi_list_{concert_id}", None)
                    else:
                        st.warning(f"⚠️ {ok_count}件成功、{fail_count}件失敗。")
                    st.rerun()


# ============================================================
# タブ2：アルゴリズム実行
# ============================================================

def _render_solver_tab(ctx: dict):
    st.caption("希望入力が完了したら、アルゴリズムを実行して候補案を生成します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    selected_concert = st.selectbox("演奏会を選択", list(concert_opts.keys()), key="solver_concert_sel")
    concert_id = concert_opts.get(selected_concert, "")
    if not concert_id:
        return

    players = _load_players(ctx)
    songs   = _load_songs(ctx, concert_id)
    if not players or not songs:
        st.info("奏者・楽曲が登録されていません。")
        return

    if st.button("▶ 割当を実行", type="primary", key="run_solver"):
        with st.spinner("アルゴリズム実行中..."):
            try:
                from concert.services.assign_solver import (
                    greedy_solve, local_search, _calc_stats,
                    _first_choice_rate, _total_score,
                    _min_player_score, _bring_count, _rest_std,
                )
                prefs, requirements = _build_solver_input(ctx, concert_id, songs, players)
                if not prefs:
                    st.warning("希望データがありません。先に希望入力を行ってください。")
                    return
                if not requirements:
                    st.warning("必要楽器が登録されていません。楽曲・楽器管理から登録してください。")
                    return

                all_pids = sorted({p.player_id for p in prefs})
                pref_map = {(p.player_id, p.song_id, p.part_id): p for p in prefs}
                base     = greedy_solve(prefs, requirements, set())

                def obj_a(sol):
                    return _first_choice_rate(sol, pref_map) * 10000 + _total_score(sol, pref_map)
                def obj_b(sol):
                    return _total_score(sol, pref_map)
                def obj_c(sol):
                    return _min_player_score(sol, pref_map) * 1000 + _total_score(sol, pref_map)
                def obj_d(sol):
                    return int((1000 - _rest_std(sol, all_pids) * 100) * 100) + _total_score(sol, pref_map)

                variants = [
                    ("候補A：第1希望率最大", obj_a),
                    ("候補B：総スコア最大",  obj_b),
                    ("候補C：公平性重視",    obj_c),
                    ("候補D：降り番均等",    obj_d),
                ]
                results = []
                for label, fn in variants:
                    sol   = local_search(base, pref_map, fn, max_iter=250)
                    stats = _calc_stats(sol, pref_map, all_pids)
                    results.append({
                        "label":       label,
                        "assignments": [a.__dict__ for a in sol],
                        "stats":       stats,
                        "pref_map":    {str(k): v.__dict__ for k, v in pref_map.items()},
                    })
                st.session_state[f"assign_result_{concert_id}"] = results
                st.success("✅ 候補案を生成しました。")
            except Exception as e:
                st.error(f"❌ 実行エラー：{e}")
                return

    results = st.session_state.get(f"assign_result_{concert_id}")
    if not results:
        return

    st.divider()
    st.subheader("候補案比較")

    # サマリーカード
    cols = st.columns(len(results))
    for i, (col, r) in enumerate(zip(cols, results)):
        st._arrow = None
        s = r["stats"]
        col.metric(r["label"].split("：")[1],
                   f"{s['total_score']:.1f}点",
                   f"第1希望率 {s['first_choice_rate']*100:.0f}%")

    st.divider()

    # 各候補の詳細
    tabs = st.tabs([r["label"] for r in results])
    song_name_map = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_order    = [s.get("id") for s in sorted(songs, key=lambda x: _song_name(x, ctx))]

    for tab, result in zip(tabs, results):
        with tab:
            s = result["stats"]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("総スコア",   f"{s['total_score']:.1f}点")
            m2.metric("第1希望率",  f"{s['first_choice_rate']*100:.1f}%")
            m3.metric("最低スコア", f"{s['min_score']:.1f}点")
            m4.metric("FB件数",     f"{sum(1 for a in result['assignments'] if a['source']=='fallback')}件")

            # 曲ごとの割当
            by_song: dict[str, list] = defaultdict(list)
            for a in result["assignments"]:
                by_song[a["song_id"]].append(a)

            for sid in song_order:
                items = by_song.get(sid, [])
                if not items:
                    continue
                st.markdown(f"**{song_name_map.get(sid, sid)}**")

                rows = []
                for a in items:
                    pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                    pref = result["pref_map"].get(pk)
                    if pref and pref["priority"] > 0:
                        hope = INT_TO_PRIORITY.get(pref["priority"], "—")
                        sc   = {1:3.0, 2:2.0, 3:1.0}.get(pref["priority"], 0.0)
                    elif a["source"] == "fallback":
                        hope = "フォールバック"
                        sc   = 0.5
                    else:
                        hope = "降り番希望"
                        sc   = 0.0
                    rows.append({
                        "奏者":   a["player_name"],
                        "パート": a["part_name"],
                        "希望":   hope,
                        "点数":   sc,
                        "FB":     "⚠️" if a["source"] == "fallback" else "",
                    })

                st.dataframe(
                    rows,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "点数": st.column_config.NumberColumn(format="%.1f"),
                        "FB":   st.column_config.TextColumn(width="small"),
                    },
                )

            # 採用ボタン
            st.divider()
            if st.button(f"✅ この案を採用してNotionに書き込む",
                         key=f"adopt_{result['label']}", type="primary"):
                _write_assignments_to_notion(ctx, result["assignments"], result["pref_map"])


def _write_assignments_to_notion(ctx: dict, assignments: list[dict], pref_map: dict):
    """採用した割当をPlayerInstrumentの担当フラグに書き込む。"""
    db_id    = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        st.error("PlayerInstrument DBのプロパティ取得に失敗しました。")
        return

    ok = fail = 0
    with st.spinner("Notionに書き込み中..."):
        for a in assignments:
            # 該当するPlayerInstrumentレコードを特定
            pi_rows = ctx["query_all"](
                db_id,
                {"filter": {"and": [
                    {"property": "奏者",    "relation": {"contains": a["player_id"]}},
                    {"property": "楽曲",    "relation": {"contains": a["song_id"]}},
                    {"property": "楽器種別","relation": {"contains": a["instrument_id"]}},
                ]}},
            )
            props: dict = {}
            ctx["put_prop"](props, type_map, "担当フラグ", True)

            if pi_rows:
                rid = pi_rows[0].get("id", "")
                res = ctx["api_request"]("patch",
                                         f"https://api.notion.com/v1/pages/{rid}",
                                         json={"properties": props})
            else:
                # レコードがなければ新規作成
                ctx["put_prop"](props, type_map, "レコード名",
                                f"{a['player_name']} × {a['song_name']} × {a['part_name']}")
                ctx["put_prop"](props, type_map, "奏者",     a["player_id"])
                ctx["put_prop"](props, type_map, "楽曲",     a["song_id"])
                ctx["put_prop"](props, type_map, "楽器種別", a["instrument_id"])
                res = ctx["api_request"]("post",
                                         "https://api.notion.com/v1/pages",
                                         json={"parent": {"database_id": db_id},
                                               "properties": props})
            if res and res.status_code == 200:
                ok += 1
            else:
                fail += 1

    if fail == 0:
        st.success(f"✅ {ok}件の担当フラグを書き込みました。")
        _clear_assign_cache()
    else:
        st.warning(f"⚠️ {ok}件成功、{fail}件失敗。")


# ============================================================
# タブ3：割当結果確認
# ============================================================

def _render_result_tab(ctx: dict):
    st.caption("採用済み割当のマトリクス表示。担当フラグが立っているレコードを表示します。")

    concerts = _load_concerts(ctx)
    if not concerts:
        st.info("演奏会を先に登録してください。")
        return

    concert_opts = {_concert_name(c, ctx): c.get("id", "") for c in concerts}
    selected_concert = st.selectbox("演奏会を選択", list(concert_opts.keys()), key="result_concert_sel")
    concert_id = concert_opts.get(selected_concert, "")
    if not concert_id:
        return

    col_h, col_r = st.columns([8, 1])
    col_h.subheader("担当パート一覧")
    if col_r.button("🔄", key="refresh_result", help="再読み込み"):
        _clear_assign_cache()
        st.rerun()

    players = _load_players(ctx)
    songs   = _load_songs(ctx, concert_id)
    if not players or not songs:
        st.info("奏者・楽曲が登録されていません。")
        return

    # 担当フラグ=Trueのレコードを取得
    pi_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"])
    assigned_rows = [r for r in pi_rows
                     if ctx["extract_prop_text"](r, "担当フラグ") == "True"]

    if not assigned_rows:
        st.info("まだ担当が確定していません。「アルゴリズム実行」タブから割当を実行してください。")
        return

    # 楽器マスタ
    inst_rows     = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"])
    inst_name_map = {r.get("id"): _instrument_name(r, ctx) for r in inst_rows}

    # 曲ごとにまとめて表示
    player_name_map = {p.get("id"): _player_name(p, ctx) for p in players}
    song_name_map   = {s.get("id"): _song_name(s, ctx) for s in songs}
    song_id_set     = {s.get("id") for s in songs}

    by_song: dict[str, list] = defaultdict(list)
    for r in assigned_rows:
        sids = ctx["extract_relation_ids"](r, "楽曲")
        if not sids or sids[0] not in song_id_set:
            continue
        pids = ctx["extract_relation_ids"](r, "奏者")
        iids = ctx["extract_relation_ids"](r, "楽器種別")
        sid  = sids[0]
        by_song[sid].append({
            "奏者":   player_name_map.get(pids[0], "不明") if pids else "不明",
            "楽器":   inst_name_map.get(iids[0], "不明") if iids else "不明",
            "備考":   ctx["extract_prop_text"](r, "備考") or "",
        })

    for song in sorted(songs, key=lambda x: _song_name(x, ctx)):
        sid   = song.get("id", "")
        sname = _song_name(song, ctx)
        items = by_song.get(sid, [])
        if not items:
            st.markdown(f"**{sname}**　—　割当なし")
            continue
        st.markdown(f"**{sname}**（{len(items)}パート）")
        st.dataframe(items, use_container_width=True, hide_index=True)
        st.write("")


# ============================================================
# メイン描画
# ============================================================

def render(ctx: dict):
    st.header("🎯 パート割当")

    tab_pref, tab_solver, tab_result = st.tabs([
        "希望入力",
        "アルゴリズム実行",
        "割当結果確認",
    ])

    with tab_pref:
        _render_pref_tab(ctx)

    with tab_solver:
        _render_solver_tab(ctx)

    with tab_result:
        _render_result_tab(ctx)
