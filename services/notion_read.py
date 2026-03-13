def query_notion_database_all(api_request_fn, notion_headers: dict, database_id: str) -> list:
    if not database_id:
        return []
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results, has_more, next_cursor = [], True, None
    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor
        res = api_request_fn("post", url, headers=notion_headers, json=payload)
        if res is None or res.status_code != 200:
            return all_results
        data = res.json()
        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")
    return all_results
