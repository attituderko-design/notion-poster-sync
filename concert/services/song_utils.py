"""
concert/services/song_utils.py
楽曲表示名の解決・楽曲一覧取得のユーティリティ。
CONCERT_SONG → APOLLO → MOVEMENT のチェーンで楽曲情報を解決する。
"""
import streamlit as st
from concert.services.keys import (
    SONG_NAME_KEYS, SONG_MOVEMENT_REL_KEYS, SONG_ALL_MOVEMENTS_KEYS,
    SONG_CONCERT_REL_KEYS, SONG_COMPOSER_KEYS, SONG_SCORE_URL_KEYS,
    MOVEMENT_NAME_KEYS, MOVEMENT_NO_KEYS, MOVEMENT_ROMAN_KEYS,
    CONCERT_SONG_CONCERT_REL_KEYS, CONCERT_SONG_SONG_REL_KEYS,
    CONCERT_SONG_ORDER_KEYS,
)

_MOVEMENT_MAP_CACHE = "_movement_map_cache"
_SONG_DISPLAY_CACHE = "_song_display_name_cache"


def load_movement_map(ctx) -> dict[str, dict]:
    """MOVEMENT DBをid→{name, no, roman}のdictで返す。セッション内キャッシュ付き。"""
    cached = st.session_state.get(_MOVEMENT_MAP_CACHE)
    if cached is not None:
        return cached
    try:
        db_id = ctx.get("CONCERT_DB_MOVEMENT", "")
        if not db_id:
            return {}
        rows = ctx["query_all"](db_id, None)
        ext  = ctx["extract_prop_text_any"]
        result = {
            r.get("id", ""): {
                "name":  ext(r, MOVEMENT_NAME_KEYS)  or "",
                "no":    ext(r, MOVEMENT_NO_KEYS)    or "",
                "roman": ext(r, MOVEMENT_ROMAN_KEYS) or "",
            }
            for r in rows
        }
        st.session_state[_MOVEMENT_MAP_CACHE] = result
        return result
    except Exception:
        return {}


def get_song_display_name(ctx, apollo_row: dict) -> str:
    """APOLLOの1レコードから表示用楽曲名を返す。

    - 全楽章フラグTrue → 曲名のみ
    - 楽章リレーションあり → 「曲名 / IV. 楽章名」
    - どちらもなし → 曲名のみ
    """
    # キャッシュ確認（apollo_row.idをキーに）
    row_id = apollo_row.get("id", "")
    cache: dict = st.session_state.get(_SONG_DISPLAY_CACHE) or {}
    if row_id and row_id in cache:
        return cache[row_id]

    ext   = ctx["extract_prop_text_any"]
    title = ext(apollo_row, SONG_NAME_KEYS) or ctx["extract_title"](apollo_row) or row_id

    display = title
    try:
        # 全楽章フラグがTrueなら楽章名は付けない
        if ext(apollo_row, SONG_ALL_MOVEMENTS_KEYS) == "True":
            display = title
        else:
            mv_ids = ctx["extract_relation_ids_any"](apollo_row, SONG_MOVEMENT_REL_KEYS)
            if mv_ids:
                mv_map = load_movement_map(ctx)
                mv = mv_map.get(mv_ids[0], {})
                mv_name  = mv.get("name", "")
                mv_roman = mv.get("roman", "")
                mv_no    = mv.get("no", "")
                if mv_name:
                    mv_label = (
                        f"{mv_roman}. {mv_name}" if mv_roman
                        else f"{mv_no}. {mv_name}" if mv_no
                        else mv_name
                    )
                    display = f"{title} / {mv_label}"
    except Exception:
        pass

    # キャッシュに保存
    if row_id:
        cache[row_id] = display
        st.session_state[_SONG_DISPLAY_CACHE] = cache

    return display


def get_songs_for_concert(ctx, concert_id: str) -> list[dict]:
    """CONCERT_SONG経由でこの演奏会のAPOLLOレコード一覧を曲順ソートで返す。"""
    try:
        ext     = ctx["extract_prop_text_any"]
        ext_rel = ctx["extract_relation_ids_any"]

        # CONCERT_SONGから演奏会に紐づく行を取得
        cs_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT_SONG"], None)
        cs_for_concert = [
            r for r in cs_rows
            if concert_id in ext_rel(r, CONCERT_SONG_CONCERT_REL_KEYS)
        ]
        # 曲順ソート
        def _order(r):
            v = ext(r, CONCERT_SONG_ORDER_KEYS) or ""
            try:
                return float(v)
            except Exception:
                return 9999.0
        cs_for_concert.sort(key=_order)

        # APOLLOのIDを収集
        apollo_ids = []
        for r in cs_for_concert:
            ids = ext_rel(r, CONCERT_SONG_SONG_REL_KEYS)
            if ids and ids[0] not in apollo_ids:
                apollo_ids.append(ids[0])

        if not apollo_ids:
            return []

        # APOLLOを取得してID順に並べ直す
        all_apollo = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
        apollo_map = {r.get("id", ""): r for r in all_apollo}
        return [apollo_map[aid] for aid in apollo_ids if aid in apollo_map]

    except Exception:
        return []


def build_song_name_map(ctx, songs: list[dict]) -> dict[str, str]:
    """APOLLOレコードのリストからid→表示名のdictを返す。"""
    return {s.get("id", ""): get_song_display_name(ctx, s) for s in songs}


def clear_song_display_cache() -> None:
    """楽曲表示名キャッシュをクリアする。"""
    st.session_state.pop(_SONG_DISPLAY_CACHE, None)
    st.session_state.pop(_MOVEMENT_MAP_CACHE, None)
