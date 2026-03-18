def create_performance_participant_rows_service(ctx: dict, performance_page_id: str, performance_title: str, participants: list[dict]) -> tuple[int, int, str, dict]:
    NOTION_PERFORMANCE_CAST_DB_ID = ctx["NOTION_PERFORMANCE_CAST_DB_ID"]
    if not NOTION_PERFORMANCE_CAST_DB_ID:
        return 0, 0, "NOTION_PERFORMANCE_CAST_DB_ID 未設定", {}
    get_notion_db_property_types = ctx["get_notion_db_property_types"]
    type_map = get_notion_db_property_types(NOTION_PERFORMANCE_CAST_DB_ID)
    if not type_map:
        return 0, 0, "演奏会参加者DBのプロパティ取得失敗（Integration接続/DB IDを確認）", {}

    relation_props = [k for k, v in type_map.items() if v == "relation"]
    perf_rel_prop = next((k for k in ["出演", "演奏会", "公演"] if type_map.get(k) == "relation"), None)
    perf_rel_prop = perf_rel_prop or (relation_props[0] if relation_props else None)
    performer_rel_prop = next((k for k in ["出演者", "奏者", "演奏者"] if type_map.get(k) == "relation"), None)
    if performer_rel_prop is None and relation_props:
        performer_rel_prop = relation_props[1] if len(relation_props) >= 2 and relation_props[0] == perf_rel_prop else relation_props[0]
    inst_prop = "担当楽器" if type_map.get("担当楽器") in ("multi_select", "select") else None
    memo_prop = "メモ" if type_map.get("メモ") == "rich_text" else None
    title_prop = "タイトル" if type_map.get("タイトル") == "title" else ("Name" if type_map.get("Name") == "title" else None)
    display_prop = "表示名" if type_map.get("表示名") in ("rich_text", "formula") else None

    normalize_person_name = ctx["normalize_person_name"]
    split_instruments = ctx["split_instruments"]
    find_or_create_performer_id = ctx["find_or_create_performer_id"]
    put_notion_prop = ctx["put_notion_prop"]
    api_request = ctx["api_request"]
    NOTION_HEADERS = ctx["NOTION_HEADERS"]

    rows = [x for x in (participants or []) if (x.get("name") or "").strip()]
    if not rows:
        return 0, 0, "参加者入力なし", {}
    unique_rows = []
    seen_names = set()
    for r in rows:
        key = normalize_person_name(r.get("name", ""))
        if not key or key in seen_names:
            continue
        seen_names.add(key)
        unique_rows.append(r)
    rows = unique_rows
    existing_performer_ids = set()
    existing_cast_row_by_performer = {}
    if perf_rel_prop and performer_rel_prop:
        q = {"filter": {"property": perf_rel_prop, "relation": {"contains": performance_page_id}}, "page_size": 100}
        res = api_request(
            "post",
            f"https://api.notion.com/v1/databases/{NOTION_PERFORMANCE_CAST_DB_ID}/query",
            headers=NOTION_HEADERS,
            json=q,
        )
        if res is not None and res.status_code == 200:
            for pg in (res.json() or {}).get("results", []):
                cast_row_id = pg.get("id")
                rels = (((pg.get("properties", {}) or {}).get(performer_rel_prop) or {}).get("relation", []))
                for r in rels:
                    rid = r.get("id")
                    if rid:
                        existing_performer_ids.add(rid)
                        if cast_row_id:
                            existing_cast_row_by_performer[rid] = cast_row_id

    created, failed = 0, 0
    cast_row_map = {}
    for i, row in enumerate(rows, start=1):
        name = (row.get("name") or "").strip()
        memo = (row.get("memo") or "").strip()
        instruments = split_instruments(row.get("instruments") or "")
        performer_id = find_or_create_performer_id(name)
        if not performer_id:
            failed += 1
            continue
        if performer_id in existing_performer_ids:
            existing_row_id = existing_cast_row_by_performer.get(performer_id)
            if existing_row_id:
                update_props = {}
                if inst_prop:
                    put_notion_prop(update_props, type_map, inst_prop, instruments)
                if memo_prop:
                    put_notion_prop(update_props, type_map, memo_prop, memo)
                if update_props:
                    pres = api_request(
                        "patch",
                        f"https://api.notion.com/v1/pages/{existing_row_id}",
                        headers=NOTION_HEADERS,
                        json={"properties": update_props},
                    )
                    if pres is None or pres.status_code != 200:
                        failed += 1
            cast_row_map[normalize_person_name(name)] = existing_row_id
            continue

        props = {}
        if title_prop:
            put_notion_prop(props, type_map, title_prop, f"{performance_title} / {name}")
        if perf_rel_prop:
            put_notion_prop(props, type_map, perf_rel_prop, performance_page_id)
        if performer_rel_prop:
            put_notion_prop(props, type_map, performer_rel_prop, performer_id)
        if inst_prop:
            put_notion_prop(props, type_map, inst_prop, instruments)
        if memo_prop:
            put_notion_prop(props, type_map, memo_prop, memo)
        if display_prop:
            put_notion_prop(props, type_map, display_prop, f"{i:02d} / {name}")

        if not props:
            failed += 1
            continue
        res = api_request(
            "post",
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS,
            json={"parent": {"database_id": NOTION_PERFORMANCE_CAST_DB_ID}, "properties": props},
        )
        if res is not None and res.status_code == 200:
            created += 1
            cast_row_id = (res.json() or {}).get("id")
            if cast_row_id:
                cast_row_map[normalize_person_name(name)] = cast_row_id
            existing_performer_ids.add(performer_id)
        else:
            failed += 1
    return created, failed, "", cast_row_map


def create_setlist_rows_for_performance_service(ctx: dict, performance_page_id: str, performance_title: str, performance_date: str, main_items: list[dict], encore_items: list[dict], selected_scores: list[dict], score_pages: list[dict]) -> tuple[int, int, str, list[dict]]:
    NOTION_SCORE_DB_ID = ctx["NOTION_SCORE_DB_ID"]
    if not NOTION_SCORE_DB_ID:
        return 0, 0, "NOTION_SCORE_DB_ID 未設定", []
    get_notion_db_property_types = ctx["get_notion_db_property_types"]
    type_map = get_notion_db_property_types(NOTION_SCORE_DB_ID)
    if not type_map:
        return 0, 0, "演奏曲DBのプロパティ取得失敗（Integration接続/DB IDを確認）", []
    code_prop_candidates = ["国コード", "CountryCode", "country_code"]
    code_prop = next((k for k in code_prop_candidates if type_map.get(k) in ("rich_text", "title", "select", "multi_select")), None)
    creator_prop_candidates = ["クリエイター", "Creator", "作曲家"]
    creator_prop = next((k for k in creator_prop_candidates if type_map.get(k) in ("rich_text", "title", "select", "multi_select")), None)
    country_master_rel_prop = next(
        (k for k in ["国名マスタ", "CountryMaster", "Country Master", "国マスタ"] if type_map.get(k) == "relation"),
        None,
    )
    if not country_master_rel_prop:
        country_master_rel_prop = next(
            (k for k, v in (type_map or {}).items() if v == "relation" and ("国" in k or "country" in str(k).lower())),
            None,
        )

    find_score_page_by_title = ctx["find_score_page_by_title"]
    put_notion_prop = ctx["put_notion_prop"]
    split_instruments = ctx["split_instruments"]
    api_request = ctx["api_request"]
    NOTION_HEADERS = ctx["NOTION_HEADERS"]
    get_composer_country_code = ctx.get("get_composer_country_code")
    country_code_to_flag = ctx.get("country_code_to_flag")
    normalize_country_code_for_flag = ctx.get("normalize_country_code_for_flag")
    query_notion_database_all = ctx.get("query_notion_database_all")
    NOTION_COUNTRY_MASTER_DB_ID = ctx.get("NOTION_COUNTRY_MASTER_DB_ID")
    get_media_icon_url = ctx.get("get_media_icon_url")

    def _norm_cc(v: str) -> str:
        if callable(normalize_country_code_for_flag):
            return normalize_country_code_for_flag(v or "")
        return (v or "").strip().upper()

    def _prop_text(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype in ("rich_text", "title"):
            chunks = meta.get(ptype, []) or []
            return "".join((x.get("plain_text") or "") for x in chunks).strip()
        if ptype == "select":
            return ((meta.get("select") or {}).get("name") or "").strip()
        if ptype == "multi_select":
            vals = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
            return vals[0] if vals else ""
        return ""

    code_to_master_id = {}
    if country_master_rel_prop and NOTION_COUNTRY_MASTER_DB_ID and callable(get_notion_db_property_types) and callable(query_notion_database_all):
        master_type_map = get_notion_db_property_types(NOTION_COUNTRY_MASTER_DB_ID) or {}
        master_code_prop = next((k for k in code_prop_candidates if k in master_type_map), None)
        if master_code_prop:
            for row in (query_notion_database_all(NOTION_COUNTRY_MASTER_DB_ID) or []):
                rid = row.get("id")
                if not rid:
                    continue
                cc = _norm_cc(_prop_text(((row.get("properties") or {}).get(master_code_prop))))
                if cc and cc not in code_to_master_id:
                    code_to_master_id[cc] = rid

    title_to_id = {}
    title_to_composer = {}
    title_to_country = {}
    for s in (selected_scores or []):
        t = (s.get("title") or "").strip().lower()
        sid = s.get("id")
        scomp = (s.get("composer") or "").strip()
        scc = _norm_cc((s.get("composer_country") or "").strip())
        if t and sid and t not in title_to_id:
            title_to_id[t] = sid
        if t and scomp and t not in title_to_composer:
            title_to_composer[t] = scomp
        if t and scc and t not in title_to_country:
            title_to_country[t] = scc

    created, failed = 0, 0
    failure_reasons = []
    created_rows = []
    score_page_cache = {}
    rows = []
    for x in (main_items or []):
        rows.append(((x.get("section") or "本編"), x))
    for x in (encore_items or []):
        rows.append(((x.get("section") or "Encore"), x))
    if not rows:
        return 0, 0, "セットリスト入力なし", []

    section_allowed = set()
    play_prop = None
    for cand in ("Playflg", "PlayFlg", "演奏した"):
        if type_map.get(cand) == "checkbox":
            play_prop = cand
            break
    try:
        db_res = api_request("get", f"https://api.notion.com/v1/databases/{NOTION_SCORE_DB_ID}", headers=NOTION_HEADERS)
        if db_res is not None and db_res.status_code == 200:
            db_props = ((db_res.json() or {}).get("properties") or {})
            sec_meta = db_props.get("区分") or {}
            sec_type = sec_meta.get("type")
            if sec_type in ("select", "multi_select"):
                opts = ((sec_meta.get(sec_type) or {}).get("options") or [])
                section_allowed = {str(o.get("name") or "").strip() for o in opts if str(o.get("name") or "").strip()}
    except Exception:
        section_allowed = set()

    order = 1
    for section, item in rows:
        song_title = (item.get("title") or "").strip()
        if not song_title:
            continue
        row_order = int(item.get("order") or order)
        if row_order <= 0:
            row_order = order
        part = (item.get("part") or "").strip()
        played = bool(item.get("played", False) or part)
        song_key = song_title.lower()
        composer_name = (item.get("composer") or "").strip() or title_to_composer.get(song_key, "")
        preferred_cc = _norm_cc((item.get("composer_country") or "").strip() or title_to_country.get(song_key, ""))
        if section not in ("幕前", "ロビー", "本編", "Encore", "ソリストEncore"):
            section = "本編"
        if section_allowed and section not in section_allowed:
            # 以前はここで「本編」に強制代替していたが、区分情報が失われるため禁止。
            # DB側に該当optionが無い場合はAPI側の検証エラーとして返し、呼び出し元で明示する。
            failure_reasons.append(f"区分「{section}」はDB option未定義の可能性があります（そのまま登録を試行）")
        score_id = title_to_id.get(song_title.lower())
        if not score_id:
            found = find_score_page_by_title(score_pages or [], song_title)
            score_id = (found or {}).get("id")

        props = {}
        put_notion_prop(props, type_map, "タイトル", song_title)
        put_notion_prop(props, type_map, "出演", performance_page_id)
        put_notion_prop(props, type_map, "出演日", performance_date)
        put_notion_prop(props, type_map, "区分", section)
        put_notion_prop(props, type_map, "担当楽器", split_instruments(part) if played else [])
        if play_prop:
            put_notion_prop(props, type_map, play_prop, bool(played))
        put_notion_prop(props, type_map, "曲順", row_order)
        put_notion_prop(props, type_map, "演奏曲", score_id)
        put_notion_prop(props, type_map, "表示名", f"{performance_title} / {row_order:02d} / {section} / {song_title}")

        if not props:
            failed += 1
            failure_reasons.append(f"登録プロパティ生成に失敗: {song_title}")
            order += 1
            continue
        payload = {"parent": {"database_id": NOTION_SCORE_DB_ID}, "properties": props}

        # 新規作成時点で演奏曲DBアイコンを設定（作曲家国旗優先、未解決は媒体アイコン）
        icon_payload = None
        resolved_cc = preferred_cc
        if callable(country_code_to_flag) and callable(get_composer_country_code):
            composer_name = composer_name or ""
            src_props = {}
            if score_id:
                if score_id not in score_page_cache:
                    pres = api_request(
                        "get",
                        f"https://api.notion.com/v1/pages/{score_id}",
                        headers=NOTION_HEADERS,
                    )
                    score_page_cache[score_id] = pres.json() if (pres is not None and pres.status_code == 200) else {}
                src_props = ((score_page_cache.get(score_id) or {}).get("properties") or {})
                rt = ((src_props.get("クリエイター") or {}).get("rich_text") or [])
                src_composer_name = "".join([(t.get("plain_text") or "") for t in rt]).strip()
                # 呼び出し元（検索確定時）の作曲家名を優先し、未指定時のみ既存レコード値へフォールバック
                if not composer_name:
                    composer_name = src_composer_name
                if code_prop and not resolved_cc:
                    resolved_cc = _norm_cc(_prop_text(src_props.get(code_prop)))
                if creator_prop and composer_name:
                    put_notion_prop(props, type_map, creator_prop, composer_name)
            if composer_name:
                resolved_cc = _norm_cc(get_composer_country_code(composer_name) or resolved_cc or "")
                flag = country_code_to_flag(resolved_cc) if resolved_cc else ""
                if flag:
                    icon_payload = {"type": "emoji", "emoji": flag}
        if icon_payload is None and callable(get_media_icon_url):
            fallback = get_media_icon_url("演奏曲")
            if fallback:
                icon_payload = {"type": "external", "external": {"url": fallback}}
        if resolved_cc and code_prop:
            put_notion_prop(props, type_map, code_prop, resolved_cc)
        if resolved_cc and country_master_rel_prop and code_to_master_id.get(resolved_cc):
            put_notion_prop(props, type_map, country_master_rel_prop, code_to_master_id.get(resolved_cc))
        if icon_payload:
            payload["icon"] = icon_payload

        res = api_request("post", "https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=payload)
        if res is not None and res.status_code == 200:
            created += 1
            row_id = (res.json() or {}).get("id")
            if row_id:
                created_rows.append({"id": row_id, "title": song_title, "players": item.get("players", []) or [], "instruments": split_instruments(part) if played else []})
        else:
            failed += 1
            status = str(res.status_code) if res is not None else "None"
            message = ""
            try:
                message = ((res.json() or {}).get("message") or "").strip() if res is not None else ""
            except Exception:
                message = ""
            failure_reasons.append(f"{song_title}: Notion {status}" + (f" / {message}" if message else ""))
        order += 1
    reason = " / ".join(list(dict.fromkeys([r for r in failure_reasons if r]))[:3])
    return created, failed, reason, created_rows


def create_song_assignment_rows_service(ctx: dict, score_rows: list[dict], cast_row_map: dict) -> tuple[int, int, str]:
    NOTION_SONG_ASSIGN_DB_ID = ctx["NOTION_SONG_ASSIGN_DB_ID"]
    if not NOTION_SONG_ASSIGN_DB_ID:
        return 0, 0, "NOTION_SONG_ASSIGN_DB_ID 未設定"
    get_notion_db_property_types = ctx["get_notion_db_property_types"]
    type_map = get_notion_db_property_types(NOTION_SONG_ASSIGN_DB_ID)
    if not type_map:
        return 0, 0, "楽曲別担当者DBのプロパティ取得失敗（Integration接続/DB IDを確認）"
    if not score_rows:
        return 0, 0, "対象曲なし"

    def pick_prop(candidates: list[str], p_type: str) -> str | None:
        for c in candidates:
            if type_map.get(c) == p_type:
                return c
        return None

    relation_props = [k for k, v in type_map.items() if v == "relation"]
    score_rel_prop = pick_prop(["演奏曲", "演奏曲DB", "曲", "楽曲"], "relation")
    cast_rel_prop = pick_prop(["演奏会出演者", "出演者", "参加者", "演奏会参加者"], "relation")
    if score_rel_prop is None and relation_props:
        score_rel_prop = relation_props[0]
    if cast_rel_prop is None and relation_props:
        if len(relation_props) >= 2:
            cast_rel_prop = relation_props[1] if relation_props[0] == score_rel_prop else relation_props[0]
        else:
            cast_rel_prop = relation_props[0]

    title_prop = pick_prop(["タイトル", "Name", "名前"], "title")
    display_prop = pick_prop(["表示名", "Display Name"], "formula") or pick_prop(["表示名", "Display Name"], "rich_text")
    inst_prop = pick_prop(["担当楽器", "楽器"], "multi_select") or pick_prop(["担当楽器", "楽器"], "select")

    normalize_person_name = ctx["normalize_person_name"]
    put_notion_prop = ctx["put_notion_prop"]
    api_request = ctx["api_request"]
    NOTION_HEADERS = ctx["NOTION_HEADERS"]

    created, failed = 0, 0
    for row in score_rows:
        score_row_id = row.get("id")
        players = row.get("players", []) or []
        instruments = row.get("instruments", []) or []
        if not score_row_id or not players:
            continue
        seen = set()
        for nm in players:
            key = normalize_person_name(nm)
            if not key or key in seen:
                continue
            seen.add(key)
            cast_row_id = cast_row_map.get(key)
            if not cast_row_id:
                failed += 1
                continue
            props = {}
            if title_prop:
                put_notion_prop(props, type_map, title_prop, f"{row.get('title','')} / {nm}")
            if score_rel_prop:
                put_notion_prop(props, type_map, score_rel_prop, score_row_id)
            if cast_rel_prop:
                put_notion_prop(props, type_map, cast_rel_prop, cast_row_id)
            if inst_prop:
                put_notion_prop(props, type_map, inst_prop, instruments)
            if display_prop:
                put_notion_prop(props, type_map, display_prop, f"{row.get('title','')} / {nm}")
            if not props:
                failed += 1
                continue
            res = api_request(
                "post",
                "https://api.notion.com/v1/pages",
                headers=NOTION_HEADERS,
                json={"parent": {"database_id": NOTION_SONG_ASSIGN_DB_ID}, "properties": props},
            )
            if res is not None and res.status_code == 200:
                created += 1
            else:
                failed += 1
    return created, failed, ""


def get_cast_row_map_for_performance_service(ctx: dict, performance_page_id: str) -> dict:
    out = {}
    if not performance_page_id or not ctx["NOTION_PERFORMANCE_CAST_DB_ID"]:
        return out

    query_notion_database_all = ctx["query_notion_database_all"]
    get_notion_db_property_types = ctx["get_notion_db_property_types"]
    pick_prop_name = ctx["pick_prop_name"]
    extract_relation_ids = ctx["extract_relation_ids"]
    extract_page_title_by_type = ctx["extract_page_title_by_type"]
    tail_person_name = ctx["tail_person_name"]
    plain_text_join = ctx["plain_text_join"]
    normalize_person_name = ctx["normalize_person_name"]

    cast_rows = query_notion_database_all(ctx["NOTION_PERFORMANCE_CAST_DB_ID"])
    cast_type = get_notion_db_property_types(ctx["NOTION_PERFORMANCE_CAST_DB_ID"])
    rel_props = [k for k, v in cast_type.items() if v == "relation"]
    perf_rel = pick_prop_name(cast_type, ["出演", "演奏会", "公演"], "relation")
    perf_rel = perf_rel or (rel_props[0] if rel_props else None)
    performer_rel = pick_prop_name(cast_type, ["出演者", "奏者", "演奏者"], "relation")
    if performer_rel is None and rel_props:
        performer_rel = rel_props[1] if len(rel_props) >= 2 and rel_props[0] == perf_rel else rel_props[0]
    title_prop = pick_prop_name(cast_type, ["タイトル", "Name", "名前"], "title")

    performer_name_by_id = {}
    if ctx["NOTION_PERFORMER_DB_ID"]:
        p_rows = query_notion_database_all(ctx["NOTION_PERFORMER_DB_ID"])
        p_type = get_notion_db_property_types(ctx["NOTION_PERFORMER_DB_ID"])
        for pg in p_rows:
            pid = pg.get("id")
            nm = extract_page_title_by_type(pg.get("properties", {}), p_type, ["名前", "タイトル", "Name"])
            if pid and nm:
                performer_name_by_id[pid] = nm

    for row in cast_rows:
        props = row.get("properties", {})
        perf_ids = extract_relation_ids(props, perf_rel)
        if performance_page_id not in perf_ids:
            continue
        name = ""
        performer_ids = extract_relation_ids(props, performer_rel)
        if performer_ids:
            name = performer_name_by_id.get(performer_ids[0], "")
        if not name and title_prop:
            name = tail_person_name(plain_text_join((props.get(title_prop) or {}).get("title", [])))
        key = normalize_person_name(name)
        rid = row.get("id")
        if key and rid and key not in out:
            out[key] = rid
    return out
