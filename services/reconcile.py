from collections import defaultdict
from datetime import datetime


def analyze_performance_relation_integrity_service(ctx: dict, force_refresh: bool = False) -> dict:
    NOTION_PERFORMANCE_CAST_DB_ID = ctx["NOTION_PERFORMANCE_CAST_DB_ID"]
    NOTION_SCORE_DB_ID = ctx["NOTION_SCORE_DB_ID"]
    NOTION_SONG_ASSIGN_DB_ID = ctx["NOTION_SONG_ASSIGN_DB_ID"]
    NOTION_PERFORMER_DB_ID = ctx["NOTION_PERFORMER_DB_ID"]

    if not NOTION_PERFORMANCE_CAST_DB_ID:
        return {"error": "NOTION_PERFORMANCE_CAST_DB_ID 未設定"}

    get_performance_pages = ctx["get_performance_pages"]
    query_notion_database_all = ctx["query_notion_database_all"]
    get_notion_db_property_types = ctx["get_notion_db_property_types"]
    pick_prop_name = ctx["pick_prop_name"]
    extract_relation_ids = ctx["extract_relation_ids"]
    extract_page_title_by_type = ctx["extract_page_title_by_type"]
    extract_name_title = ctx["extract_name_title"]
    normalize_person_name = ctx["normalize_person_name"]
    tail_person_name = ctx["tail_person_name"]
    plain_text_join = ctx["plain_text_join"]

    perf_pages = get_performance_pages(force_refresh=force_refresh)
    perf_title_by_id = {p.get("id"): (p.get("title") or "") for p in perf_pages if p.get("id")}

    cast_rows = query_notion_database_all(NOTION_PERFORMANCE_CAST_DB_ID)
    score_rows = query_notion_database_all(NOTION_SCORE_DB_ID) if NOTION_SCORE_DB_ID else []
    assign_rows = query_notion_database_all(NOTION_SONG_ASSIGN_DB_ID) if NOTION_SONG_ASSIGN_DB_ID else []
    performer_rows = query_notion_database_all(NOTION_PERFORMER_DB_ID) if NOTION_PERFORMER_DB_ID else []

    cast_type = get_notion_db_property_types(NOTION_PERFORMANCE_CAST_DB_ID)
    score_type = get_notion_db_property_types(NOTION_SCORE_DB_ID) if NOTION_SCORE_DB_ID else {}
    assign_type = get_notion_db_property_types(NOTION_SONG_ASSIGN_DB_ID) if NOTION_SONG_ASSIGN_DB_ID else {}
    performer_type = get_notion_db_property_types(NOTION_PERFORMER_DB_ID) if NOTION_PERFORMER_DB_ID else {}

    cast_rel_props = [k for k, v in cast_type.items() if v == "relation"]
    cast_perf_prop = pick_prop_name(cast_type, ["出演", "演奏会", "公演"], "relation")
    cast_perf_prop = cast_perf_prop or (cast_rel_props[0] if cast_rel_props else None)
    cast_performer_prop = pick_prop_name(cast_type, ["出演者", "奏者", "演奏者"], "relation")
    if cast_performer_prop is None and cast_rel_props:
        cast_performer_prop = cast_rel_props[1] if len(cast_rel_props) >= 2 and cast_rel_props[0] == cast_perf_prop else cast_rel_props[0]

    cast_title_prop = pick_prop_name(cast_type, ["タイトル", "Name", "名前"], "title")
    score_perf_prop = pick_prop_name(score_type, ["出演", "演奏会", "公演"], "relation")
    if score_perf_prop is None:
        rels = [k for k, v in score_type.items() if v == "relation"]
        score_perf_prop = rels[0] if rels else None
    assign_score_prop = pick_prop_name(assign_type, ["演奏曲", "演奏曲DB", "曲", "楽曲"], "relation")
    assign_cast_prop = pick_prop_name(assign_type, ["演奏会出演者", "出演者", "参加者", "演奏会参加者"], "relation")
    assign_title_prop = pick_prop_name(assign_type, ["タイトル", "Name", "名前"], "title")
    assign_rel_props = [k for k, v in assign_type.items() if v == "relation"]
    if assign_score_prop is None and assign_rel_props:
        assign_score_prop = assign_rel_props[0]
    if assign_cast_prop is None and assign_rel_props:
        assign_cast_prop = assign_rel_props[1] if len(assign_rel_props) >= 2 and assign_rel_props[0] == assign_score_prop else assign_rel_props[0]

    performer_name_by_id = {}
    performer_ids_by_name = defaultdict(list)
    for pg in performer_rows:
        props = pg.get("properties", {})
        nm = extract_page_title_by_type(props, performer_type, ["名前", "タイトル", "Name"])
        if not nm:
            nm = extract_name_title(pg)
        pid = pg.get("id")
        if pid and nm:
            performer_name_by_id[pid] = nm
            performer_ids_by_name[normalize_person_name(nm)].append(pid)

    def _new_perf_stat(pid: str, title: str) -> dict:
        return {
            "performance_id": pid,
            "title": title or "(タイトル未設定)",
            "cast_total": 0,
            "cast_missing_performer": 0,
            "cast_duplicates": 0,
            "score_total": 0,
            "assign_total": 0,
            "assign_missing_cast": 0,
            "assign_missing_score": 0,
            "fixable_cast_missing_performer": 0,
            "fixable_assign_missing_cast": 0,
            "issue_count": 0,
        }

    perf_stats = {pid: _new_perf_stat(pid, ttl) for pid, ttl in perf_title_by_id.items()}
    cast_key_to_rows = defaultdict(list)
    cast_row_to_perf = {}
    cast_name_index = defaultdict(lambda: defaultdict(list))
    fix_cast_missing_performer = []
    duplicate_archive_ids = []

    for row in cast_rows:
        row_id = row.get("id")
        props = row.get("properties", {})
        perf_ids = extract_relation_ids(props, cast_perf_prop)
        performer_ids = extract_relation_ids(props, cast_performer_prop)
        row_title = ""
        if cast_title_prop:
            row_title = plain_text_join((props.get(cast_title_prop) or {}).get("title", []))
        row_name = performer_name_by_id.get(performer_ids[0], "") if performer_ids else ""
        if not row_name:
            row_name = tail_person_name(row_title)
        norm_row_name = normalize_person_name(row_name)

        if not perf_ids:
            continue

        for pid in perf_ids:
            if pid not in perf_stats:
                perf_stats[pid] = _new_perf_stat(pid, perf_title_by_id.get(pid, "（出演DB外のID）"))
            stt = perf_stats[pid]
            stt["cast_total"] += 1
            cast_row_to_perf[row_id] = pid
            if row_id and norm_row_name:
                cast_name_index[pid][norm_row_name].append(row_id)
            if not performer_ids:
                stt["cast_missing_performer"] += 1
                candidate_ids = performer_ids_by_name.get(norm_row_name, []) if norm_row_name else []
                if row_id and len(candidate_ids) == 1:
                    fix_cast_missing_performer.append({"row_id": row_id, "performer_id": candidate_ids[0], "performance_id": pid})
                    stt["fixable_cast_missing_performer"] += 1
            if performer_ids:
                cast_key_to_rows[(pid, performer_ids[0])].append(row_id)

    for (pid, _performer_id), row_ids in cast_key_to_rows.items():
        if len(row_ids) > 1:
            extra = row_ids[1:]
            perf_stats[pid]["cast_duplicates"] += len(extra)
            duplicate_archive_ids.extend([rid for rid in extra if rid])

    score_row_to_perf = {}
    for row in score_rows:
        row_id = row.get("id")
        props = row.get("properties", {})
        perf_ids = extract_relation_ids(props, score_perf_prop)
        if not row_id or not perf_ids:
            continue
        pid = perf_ids[0]
        score_row_to_perf[row_id] = pid
        if pid not in perf_stats:
            perf_stats[pid] = _new_perf_stat(pid, perf_title_by_id.get(pid, "（出演DB外のID）"))
        perf_stats[pid]["score_total"] += 1

    fix_assign_missing_cast = []
    unresolved_assign_missing_score = 0
    for row in assign_rows:
        row_id = row.get("id")
        props = row.get("properties", {})
        score_ids = extract_relation_ids(props, assign_score_prop)
        cast_ids = extract_relation_ids(props, assign_cast_prop)
        pid = score_row_to_perf.get(score_ids[0]) if score_ids else None
        if (not pid) and cast_ids:
            pid = cast_row_to_perf.get(cast_ids[0])
        if not score_ids:
            if pid:
                if pid not in perf_stats:
                    perf_stats[pid] = _new_perf_stat(pid, perf_title_by_id.get(pid, "（出演DB外のID）"))
                perf_stats[pid]["assign_total"] += 1
                perf_stats[pid]["assign_missing_score"] += 1
            else:
                unresolved_assign_missing_score += 1
            continue
        if not pid:
            continue
        if pid not in perf_stats:
            perf_stats[pid] = _new_perf_stat(pid, perf_title_by_id.get(pid, "（出演DB外のID）"))
        stt = perf_stats[pid]
        stt["assign_total"] += 1
        if cast_ids:
            continue
        stt["assign_missing_cast"] += 1
        assign_title = ""
        if assign_title_prop:
            assign_title = plain_text_join((props.get(assign_title_prop) or {}).get("title", []))
        candidate_name = normalize_person_name(tail_person_name(assign_title))
        candidate_rows = cast_name_index.get(pid, {}).get(candidate_name, [])
        if row_id and len(candidate_rows) == 1:
            fix_assign_missing_cast.append({"row_id": row_id, "cast_row_id": candidate_rows[0], "performance_id": pid})
            stt["fixable_assign_missing_cast"] += 1

    rows = []
    totals = {
        "performance_count": 0,
        "issue_performance_count": 0,
        "cast_total": 0,
        "cast_missing_performer": 0,
        "cast_duplicates": 0,
        "assign_total": 0,
        "assign_missing_cast": 0,
        "assign_missing_score_unresolved": 0,
        "fixable_cast_missing_performer": len(fix_cast_missing_performer),
        "fixable_assign_missing_cast": len(fix_assign_missing_cast),
        "duplicate_archive_candidates": len(duplicate_archive_ids),
    }
    for pid, stt in perf_stats.items():
        stt["issue_count"] = stt["cast_missing_performer"] + stt["cast_duplicates"] + stt["assign_missing_cast"] + stt["assign_missing_score"]
        rows.append(stt)
        totals["performance_count"] += 1
        totals["cast_total"] += stt["cast_total"]
        totals["cast_missing_performer"] += stt["cast_missing_performer"]
        totals["cast_duplicates"] += stt["cast_duplicates"]
        totals["assign_total"] += stt["assign_total"]
        totals["assign_missing_cast"] += stt["assign_missing_cast"]
        if stt["issue_count"] > 0:
            totals["issue_performance_count"] += 1
    totals["assign_missing_score_unresolved"] = unresolved_assign_missing_score
    rows.sort(key=lambda x: (x["issue_count"], x["cast_total"] + x["assign_total"], x["title"]), reverse=True)

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": rows,
        "totals": totals,
        "fix_candidates": {
            "cast_missing_performer": fix_cast_missing_performer,
            "assign_missing_cast": fix_assign_missing_cast,
            "cast_duplicate_archive": duplicate_archive_ids,
        },
        "props": {
            "cast_performer_prop": cast_performer_prop,
            "assign_cast_prop": assign_cast_prop,
        },
        "error": "",
    }


def run_performance_relation_repair_service(ctx: dict, report: dict, mode: str = "partial") -> tuple[dict, list[str]]:
    api_request = ctx["api_request"]
    NOTION_HEADERS = ctx["NOTION_HEADERS"]
    stats = {
        "cast_missing_performer_fixed": 0,
        "assign_missing_cast_fixed": 0,
        "duplicates_archived": 0,
        "failed": 0,
    }
    errors = []
    if not report or report.get("error"):
        return stats, ["整合チェック結果がありません。先にチェックを実行してください。"]
    if mode == "manual":
        return stats, ["手動モードのため自動修復は実行しません。"]

    cast_performer_prop = (report.get("props") or {}).get("cast_performer_prop")
    assign_cast_prop = (report.get("props") or {}).get("assign_cast_prop")

    for c in (report.get("fix_candidates") or {}).get("cast_missing_performer", []):
        row_id = c.get("row_id")
        performer_id = c.get("performer_id")
        if not row_id or not performer_id or not cast_performer_prop:
            continue
        payload = {"properties": {cast_performer_prop: {"relation": [{"id": performer_id}]}}}
        res = api_request("patch", f"https://api.notion.com/v1/pages/{row_id}", headers=NOTION_HEADERS, json=payload)
        if res is not None and res.status_code == 200:
            stats["cast_missing_performer_fixed"] += 1
        else:
            stats["failed"] += 1
            errors.append(f"出演者補完失敗: {row_id}")

    for c in (report.get("fix_candidates") or {}).get("assign_missing_cast", []):
        row_id = c.get("row_id")
        cast_row_id = c.get("cast_row_id")
        if not row_id or not cast_row_id or not assign_cast_prop:
            continue
        payload = {"properties": {assign_cast_prop: {"relation": [{"id": cast_row_id}]}}}
        res = api_request("patch", f"https://api.notion.com/v1/pages/{row_id}", headers=NOTION_HEADERS, json=payload)
        if res is not None and res.status_code == 200:
            stats["assign_missing_cast_fixed"] += 1
        else:
            stats["failed"] += 1
            errors.append(f"楽曲別担当者補完失敗: {row_id}")

    if mode == "full":
        for row_id in (report.get("fix_candidates") or {}).get("cast_duplicate_archive", []):
            if not row_id:
                continue
            res = api_request(
                "patch",
                f"https://api.notion.com/v1/pages/{row_id}",
                headers=NOTION_HEADERS,
                json={"archived": True},
            )
            if res is not None and res.status_code == 200:
                stats["duplicates_archived"] += 1
            else:
                stats["failed"] += 1
                errors.append(f"重複行アーカイブ失敗: {row_id}")
    return stats, errors
