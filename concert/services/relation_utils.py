"""
concert/services/relation_utils.py
relationプロパティ名の特定ロジックを共通化。
"""

from __future__ import annotations


def find_relation_prop(
    type_map: dict,
    candidates: list[str],
    keywords: list[str],
    exclude: set[str] | None = None,
) -> str:
    """
    Notion DBスキーマからrelationプロパティ名を推定して返す。
    優先順:
    1) candidates完全一致
    2) keywords部分一致
    3) relation型の先頭
    """
    exclude = exclude or set()
    tm = type_map or {}

    for k in candidates:
        if k in exclude:
            continue
        if tm.get(k) == "relation":
            return k

    lowered_keywords = [kw.lower() for kw in keywords]
    for k, t in tm.items():
        if t != "relation" or k in exclude:
            continue
        ks = str(k).lower()
        if any(kw in ks for kw in lowered_keywords):
            return k

    for k, t in tm.items():
        if t == "relation" and k not in exclude:
            return k
    return ""

