"""
concert/services/part_master_utils.py
PART_MASTERに関するユーティリティ関数。
各ページ・レポートから共通で使用する。
"""
import streamlit as st
from concert.services.keys import (
    PARTMASTER_NAME_KEYS,
    PARTMASTER_TYPE_KEYS,
    PARTICIPANT_PART_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS,
)

_CACHE_KEY = "_part_master_map_cache"


def load_part_master_map(ctx) -> dict[str, dict]:
    """PART_MASTERを id→{name, type} のdictで返す。セッション内キャッシュ付き。"""
    cached = st.session_state.get(_CACHE_KEY)
    if cached:
        return cached
    try:
        rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
        ext  = ctx["extract_prop_text_any"]
        result = {
            r.get("id", ""): {
                "name": ext(r, PARTMASTER_NAME_KEYS) or "",
                "type": ext(r, PARTMASTER_TYPE_KEYS) or "",
            }
            for r in rows
        }
        st.session_state[_CACHE_KEY] = result
        return result
    except Exception:
        return {}


def clear_part_master_cache() -> None:
    """キャッシュを破棄する。PART_MASTERを更新した直後に呼ぶ。"""
    st.session_state.pop(_CACHE_KEY, None)


def part_name_from_cast(ctx, cast_row: dict, pm_map: dict) -> str:
    """CONCERT_CASTの1行からパート名を返す。"""
    pm_ids = ctx["extract_relation_ids_any"](cast_row, PARTICIPANT_PART_REL_KEYS)
    return pm_map.get(pm_ids[0], {}).get("name", "") if pm_ids else ""


def part_id_from_name(pm_map: dict, name: str) -> str:
    """パート名からPART_MASTERのIDを逆引きする。"""
    for pid, v in pm_map.items():
        if v["name"] == name:
            return pid
    return ""


def is_perc_from_pm(pm_map: dict, pm_id: str) -> bool:
    """PART_MASTERのIDから打楽器かどうかを判定する。"""
    return pm_map.get(pm_id, {}).get("type", "") == "打楽器"


def part_name_from_pm_ids(pm_map: dict, pm_ids: list) -> str:
    """PART_MASTERのIDリスト（先頭優先）からパート名を返す。"""
    return pm_map.get(pm_ids[0], {}).get("name", "") if pm_ids else ""


def build_player_part_map(ctx, cast_rows: list, pm_map: dict) -> dict[str, str]:
    """CONCERT_CASTのリストから player_id→パート名 のdictを構築する。"""
    result: dict[str, str] = {}
    for row in cast_rows:
        p_ids  = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
        pm_ids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PART_REL_KEYS)
        if p_ids:
            result[p_ids[0]] = pm_map.get(pm_ids[0], {}).get("name", "") if pm_ids else ""
    return result
