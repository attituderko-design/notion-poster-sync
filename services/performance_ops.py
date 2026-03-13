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

    find_score_page_by_title = ctx["find_score_page_by_title"]
    put_notion_prop = ctx["put_notion_prop"]
    split_instruments = ctx["split_instruments"]
    api_request = ctx["api_request"]
    NOTION_HEADERS = ctx["NOTION_HEADERS"]

    title_to_id = {}
    for s in (selected_scores or []):
        t = (s.get("title") or "").strip().lower()
        sid = s.get("id")
        if t and sid and t not in title_to_id:
            title_to_id[t] = sid

    created, failed = 0, 0
    created_rows = []
    rows = [("本編", x) for x in (main_items or [])] + [("Encore", x) for x in (encore_items or [])]
    if not rows:
        return 0, 0, "セットリスト入力なし", []

    order = 1
    for section, item in rows:
        song_title = (item.get("title") or "").strip()
        if not song_title:
            continue
        part = (item.get("part") or "").strip()
        played = bool(item.get("played", False) or part)
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
        put_notion_prop(props, type_map, "曲順", order)
        put_notion_prop(props, type_map, "演奏曲", score_id)
        put_notion_prop(props, type_map, "表示名", f"{performance_title} / {order:02d} / {section} / {song_title}")

        if not props:
            failed += 1
            order += 1
            continue
        payload = {"parent": {"database_id": NOTION_SCORE_DB_ID}, "properties": props}
        res = api_request("post", "https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=payload)
        if res is not None and res.status_code == 200:
            created += 1
            row_id = (res.json() or {}).get("id")
            if row_id:
                created_rows.append({"id": row_id, "title": song_title, "players": item.get("players", []) or [], "instruments": split_instruments(part) if played else []})
        else:
            failed += 1
        order += 1
    return created, failed, "", created_rows


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
