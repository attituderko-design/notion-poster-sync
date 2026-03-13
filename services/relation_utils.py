def clean_relation_ids(ids: list | None) -> list[str]:
    cleaned = []
    seen = set()
    for rid in (ids or []):
        if isinstance(rid, str) and rid.strip():
            v = rid.strip()
            if v not in seen:
                cleaned.append(v)
                seen.add(v)
    return cleaned


def prune_selected_relations(selected: list[dict], valid_pages: list[dict]) -> list[dict]:
    valid_ids = {p.get("id") for p in valid_pages if p.get("id")}
    pruned = []
    for x in (selected or []):
        xid = x.get("id")
        if xid in valid_ids:
            pruned.append(x)
    return pruned
