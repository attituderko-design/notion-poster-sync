"""
app/services/form_service.py

演奏会フォームのNotionビジネスロジック。
Streamlit依存なし。ctx（Notionクライアントdict）を受け取る純粋な関数群。
"""

from __future__ import annotations
import os
import time
from typing import Optional
from app.services import keys as _K

# ── キー定数（keys.py準拠） ───────────────────────────────────

# ATLAS（演奏会）DB
CONCERT_NAME_KEYS         = ["名称", "タイトル", "演奏会名", "PK名称"]
CONCERT_DATE_KEYS         = ["日時", "日付", "出演日", "体験日", "リリース日"]
CONCERT_VENUE_KEYS        = ["会場", "場所", "会場名", "Venue"]
CONCERT_CONDUCTOR_KEYS    = ["クリエイター", "指揮者", "Conductor"]
CONCERT_SOLOIST_KEYS      = ["ソリスト", "Soloist"]

# PRACTICE（練習）DB
PRACTICE_NAME_KEYS        = ["練習名", "タイトル", "PK練習名"]
PRACTICE_DATE_KEYS        = ["日時", "日付"]
PRACTICE_VENUE_KEYS       = ["会場名", "会場", "場所", "Venue"]
PRACTICE_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
PRACTICE_CONCERT_DAY_KEYS = ["本番日", "演奏会当日フラグ", "本番フラグ"]

# CONCERT_CAST（演奏会参加者）DB
PARTICIPANT_RECORD_KEYS      = ["タイトル", "participant_key", "PK参加者"]
PARTICIPANT_PLAYER_REL_KEYS  = ["出演者", "奏者", "FK奏者"]
PARTICIPANT_CONCERT_REL_KEYS = ["出演", "演奏会", "FK演奏会"]
PARTICIPANT_PART_REL_KEYS    = ["パート", "担当パート", "Part"]
PARTICIPANT_ROLE_KEYS        = ["役職_音楽", "役職", "Role"]
PARTICIPANT_ROLE_OPS_KEYS    = ["役職_運営", "RoleOps"]
PARTICIPANT_SYSTEM_ROLE_KEYS = ["システムロール", "system_role", "SystemRole"]
PARTICIPANT_FEE_KEYS         = ["参加費", "Fee"]
PARTICIPANT_PAID_KEYS        = ["入金済", "Paid"]

# PERFORMER（出演者）DB
PLAYER_NAME_KEYS          = ["氏名", "名前", "表示名", "タイトル"]
PLAYER_EMAIL_KEYS         = ["メールアドレス", "Email", "email"]
PLAYER_PASSWORD_HASH_KEYS = ["password_hash", "パスワードハッシュ"]
PLAYER_HN_KEYS            = ["H.N.", "ハンドルネーム", "HN"]
PLAYER_PHONE_KEYS         = ["電話番号", "Phone", "Tel"]
PLAYER_LINE_KEYS          = ["LINE ID", "LINE", "Line"]
PLAYER_RECEIVE_KEYS       = ["受信", "メール受信", "前日共有受信"]

# PART_MASTER DB
PARTMASTER_NAME_KEYS      = ["パート名", "名称", "タイトル", "Name"]
PARTMASTER_TYPE_KEYS      = ["種別", "type", "category", "カテゴリ"]

# PART_DEFINITION DB
PARTDEF_RECORD_KEYS       = ["part_key", "タイトル"]
PARTDEF_NAME_KEYS         = ["パート名", "名称", "表示名"]
PARTDEF_DISPLAY_NAME_KEYS = ["表示パート名", "display_part_name", "パート表示名"]
PARTDEF_SONG_REL_KEYS     = ["演奏曲", "楽曲", "FK楽曲", "作品楽章", "作品マスタ"]
PARTDEF_INST_REL_KEYS     = ["必要楽器", "楽器種別", "楽器", "FK楽器種別", "担当楽器"]
PARTDEF_PART_REL_KEYS     = ["パート区分", "パートマスタ", "part_type", "part_master"]
PARTDEF_NOTE_KEYS         = ["備考", "メモ", "注記"]

# ATTENDANCE（出欠）DB
ATTENDANCE_KEY_KEYS   = ["attendance_key", "AttendanceKey", "出欠キー", "PK出欠キー"]
ATT_PLAYER_REL_KEYS   = ["演奏会参加者", "奏者", "出演者", "FK奏者"]
ATT_PRACTICE_REL_KEYS = ["練習", "FK練習"]
ATT_STATUS_KEYS       = ["参加可否", "出欠", "ステータス", "Status"]
ATT_NOTE_KEYS         = ["コメント", "備考", "メモ"]

# PREFERENCE（希望入力）DB
PREFERENCE_KEY_KEYS   = ["preference_key", "PreferenceKey", "希望キー", "PK希望キー"]
PREF_PLAYER_REL_KEYS  = ["演奏会参加者", "奏者", "出演者", "FK奏者"]
PREF_PART_REL_KEYS    = ["パート定義", "パート", "FKパート"]
PREF_PRIORITY_KEYS    = ["希望順位", "優先度", "希望", "希望区分"]

# PLAYER_INSTRUMENT DB
PI_RECORD_KEYS        = ["assign_key", "レコード名", "タイトル"]
PI_PLAYER_REL_KEYS    = ["奏者", "出演者", "FK奏者"]
PI_INST_REL_KEYS      = ["楽器種別", "楽器", "担当楽器", "FK楽器種別"]
PI_CONCERT_REL_KEYS   = ["演奏会", "出演", "FK演奏会"]
PI_OWN_COUNT_KEYS     = ["所有台数", "持参台数", "持参数"]

# APOLLO（演奏曲）DB
SONG_NAME_KEYS        = ["曲名", "タイトル", "PK曲名", "作品名"]
SONG_CREATOR_KEYS     = ["クリエイター", "作曲家", "Composer", "作曲者"]
SONG_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]

# CONCERT_SONG（演奏会×曲）DB
CONCERT_SONG_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
CONCERT_SONG_SONG_REL_KEYS    = ["曲", "楽曲", "演奏曲", "song"]

# HARMONIA_CONCERT DB
HARMONIA_CONCERT_PLAN_KEYS   = ["案提示", "proposal_presented"]
HARMONIA_CONCERT_ASSIGN_KEYS = ["アサイン確定", "assign_confirmed"]

# CONCERT_ASSIGNMENT DB
ASSIGNMENT_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
ASSIGNMENT_PLAYER_REL_KEYS  = ["奏者", "出演者", "FK奏者", "player"]
ASSIGNMENT_PARTDEF_REL_KEYS = ["パート定義", "パート", "FKパート", "part"]
ASSIGNMENT_SONG_REL_KEYS    = ["演奏曲", "楽曲", "FK楽曲", "song"]
ASSIGNMENT_FLAG_KEYS        = ["担当フラグ", "担当", "assigned"]

# INSTRUMENT DB
INSTRUMENT_NAME_KEYS  = ["楽器名", "タイトル", "PK楽器名"]

ROLE_PLAYER  = 0
ROLE_LEADER  = 1
ROLE_MANAGER = 2

# keys.pyを唯一の真実源として上書き（旧定義は互換のため残置）
CONCERT_NAME_KEYS = _K.CONCERT_NAME_KEYS
CONCERT_DATE_KEYS = _K.CONCERT_DATE_KEYS
CONCERT_VENUE_KEYS = _K.CONCERT_VENUE_KEYS
CONCERT_CONDUCTOR_KEYS = _K.CONCERT_CONDUCTOR_KEYS
CONCERT_SOLOIST_KEYS = _K.CONCERT_SOLOIST_KEYS
PRACTICE_NAME_KEYS = _K.PRACTICE_NAME_KEYS
PRACTICE_DATE_KEYS = _K.PRACTICE_DATE_KEYS
PRACTICE_VENUE_KEYS = _K.PRACTICE_VENUE_KEYS
PRACTICE_CONCERT_REL_KEYS = _K.PRACTICE_CONCERT_REL_KEYS
PRACTICE_CONCERT_DAY_KEYS = _K.PRACTICE_CONCERT_DAY_KEYS
PARTICIPANT_RECORD_KEYS = _K.PARTICIPANT_RECORD_KEYS
PARTICIPANT_PLAYER_REL_KEYS = _K.PARTICIPANT_PLAYER_REL_KEYS
PARTICIPANT_CONCERT_REL_KEYS = _K.PARTICIPANT_CONCERT_REL_KEYS
PARTICIPANT_PART_REL_KEYS = _K.PARTICIPANT_PART_REL_KEYS
PARTICIPANT_ROLE_KEYS = _K.PARTICIPANT_ROLE_KEYS
PARTICIPANT_ROLE_OPS_KEYS = _K.PARTICIPANT_ROLE_OPS_KEYS
PARTICIPANT_SYSTEM_ROLE_KEYS = _K.PARTICIPANT_SYSTEM_ROLE_KEYS
PARTICIPANT_FEE_KEYS = _K.PARTICIPANT_FEE_KEYS
PARTICIPANT_PAID_KEYS = _K.PARTICIPANT_PAID_KEYS
PLAYER_NAME_KEYS = _K.PLAYER_NAME_KEYS
PLAYER_EMAIL_KEYS = _K.PLAYER_EMAIL_KEYS
PLAYER_PASSWORD_HASH_KEYS = _K.PLAYER_PASSWORD_HASH_KEYS
PLAYER_HN_KEYS = _K.PLAYER_HN_KEYS
PLAYER_PHONE_KEYS = _K.PLAYER_PHONE_KEYS
PLAYER_LINE_KEYS = _K.PLAYER_LINE_KEYS
PLAYER_RECEIVE_KEYS = _K.PLAYER_RECEIVE_KEYS
PARTMASTER_NAME_KEYS = _K.PARTMASTER_NAME_KEYS
PARTMASTER_TYPE_KEYS = _K.PARTMASTER_TYPE_KEYS
PARTDEF_RECORD_KEYS = _K.PARTDEF_RECORD_KEYS
PARTDEF_NAME_KEYS = _K.PARTDEF_NAME_KEYS
PARTDEF_DISPLAY_NAME_KEYS = _K.PARTDEF_DISPLAY_NAME_KEYS
PARTDEF_SONG_REL_KEYS = _K.PARTDEF_SONG_REL_KEYS
PARTDEF_INST_REL_KEYS = _K.PARTDEF_INST_REL_KEYS
PARTDEF_PART_REL_KEYS = _K.PARTDEF_PART_REL_KEYS
PARTDEF_NOTE_KEYS = _K.PARTDEF_NOTE_KEYS
PARTDEF_SCORE_URL_KEYS = _K.PARTDEF_SCORE_URL_KEYS
ATTENDANCE_KEY_KEYS = _K.ATTENDANCE_KEY_KEYS
ATT_PLAYER_REL_KEYS = _K.ATT_PLAYER_REL_KEYS
ATT_PRACTICE_REL_KEYS = _K.ATT_PRACTICE_REL_KEYS
ATT_STATUS_KEYS = _K.ATT_STATUS_KEYS
ATT_NOTE_KEYS = _K.ATT_NOTE_KEYS
PREFERENCE_KEY_KEYS = _K.PREFERENCE_KEY_KEYS
PREF_PLAYER_REL_KEYS = _K.PREF_PLAYER_REL_KEYS
PREF_PART_REL_KEYS = _K.PREF_PART_REL_KEYS
PREF_PRIORITY_KEYS = _K.PREF_PRIORITY_KEYS
PI_RECORD_KEYS = _K.PI_RECORD_KEYS
PI_PLAYER_REL_KEYS = _K.PI_PLAYER_REL_KEYS
PI_INST_REL_KEYS = _K.PI_INST_REL_KEYS
PI_CONCERT_REL_KEYS = _K.PI_CONCERT_REL_KEYS
PI_OWN_COUNT_KEYS = _K.PI_OWN_COUNT_KEYS
SONG_NAME_KEYS = _K.SONG_NAME_KEYS
SONG_CREATOR_KEYS = _K.SONG_CREATOR_KEYS
SONG_CONCERT_REL_KEYS = _K.SONG_CONCERT_REL_KEYS
SONG_SCORE_URL_KEYS = _K.SONG_SCORE_URL_KEYS
CONCERT_SONG_CONCERT_REL_KEYS = _K.CONCERT_SONG_CONCERT_REL_KEYS
CONCERT_SONG_SONG_REL_KEYS = _K.CONCERT_SONG_SONG_REL_KEYS
HARMONIA_CONCERT_PLAN_KEYS = _K.HARMONIA_CONCERT_PLAN_KEYS
HARMONIA_CONCERT_ASSIGN_KEYS = _K.HARMONIA_CONCERT_ASSIGN_KEYS
ASSIGNMENT_CONCERT_REL_KEYS = _K.ASSIGNMENT_CONCERT_REL_KEYS
ASSIGNMENT_PLAYER_REL_KEYS = _K.ASSIGNMENT_PLAYER_REL_KEYS
ASSIGNMENT_PARTDEF_REL_KEYS = _K.ASSIGNMENT_PARTDEF_REL_KEYS
ASSIGNMENT_SONG_REL_KEYS = _K.ASSIGNMENT_SONG_REL_KEYS
ASSIGNMENT_FLAG_KEYS = _K.ASSIGNMENT_FLAG_KEYS
INSTRUMENT_NAME_KEYS = _K.INSTRUMENT_NAME_KEYS


# ── ユーティリティ ────────────────────────────────────────────

def find_relation_prop(type_map: dict, candidates: list[str],
                        fallbacks: list[str] | None = None,
                        exclude: set | None = None) -> str:
    """プロパティ型マップからリレーション型フィールド名を検索する。"""
    exclude = exclude or set()
    all_candidates = list(candidates) + (fallbacks or [])
    for key in (type_map or {}):
        if key in exclude:
            continue
        if (type_map[key].get("type") == "relation" and
                any(c.lower() in key.lower() for c in all_candidates)):
            return key
    return ""


def is_perc(part_name_or_type: str) -> bool:
    """打楽器パートかどうかを判定する。"""
    t = (part_name_or_type or "").lower()
    return any(k in t for k in ["perc", "打楽器", "percussion"])


# ── データ一括取得 ────────────────────────────────────────────

def load_form_data(ctx: dict, concert_id: str) -> dict:
    """
    フォーム表示に必要な全データをNotionから取得して返す。

    Returns:
        {
            "concert": dict,
            "practices": list,
            "concert_day": dict | None,
            "songs": list,
            "partdefs": list,
            "inst_map": dict,
            "req_insts": list,
            "part_master_map": dict,
            "participant_rows_concert": list,
            "attendance_rows": list,
            "preference_rows": list,
        }
    """
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    cache_ttl = _form_data_cache_ttl_seconds()
    cache_key = (concert_id or "").replace("-", "")
    if cache_ttl > 0 and cache_key:
        hit = _FORM_DATA_CACHE.get(cache_key)
        if hit and (time.time() - hit[0] <= cache_ttl):
            return hit[1]

    # IDはハイフンあり・なし両方で渡される可能性があるため両形式を保持
    concert_id_raw  = concert_id.replace("-", "")
    concert_id_hyph = "-".join([
        concert_id_raw[0:8], concert_id_raw[8:12],
        concert_id_raw[12:16], concert_id_raw[16:20],
        concert_id_raw[20:32]
    ])

    def _match_id(a: str, b: str) -> bool:
        return (a or "").replace("-", "") == (b or "").replace("-", "")

    # 演奏会
    all_concerts = ctx["query_all"](ctx["CONCERT_DB_ATLAS"], None)
    concert = next((c for c in all_concerts if _match_id(c.get("id", ""), concert_id)), None)

    # 練習（本番当日除く）
    all_prac = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    practices = sorted(
        [p for p in all_prac
         if any(_match_id(rid, concert_id) for rid in ext_rel(p, PRACTICE_CONCERT_REL_KEYS))
         and not _is_true(ext(p, PRACTICE_CONCERT_DAY_KEYS))],
        key=lambda p: ext(p, PRACTICE_DATE_KEYS) or "9999"
    )
    concert_day = next(
        (p for p in all_prac
         if any(_match_id(rid, concert_id) for rid in ext_rel(p, PRACTICE_CONCERT_REL_KEYS))
         and _is_true(ext(p, PRACTICE_CONCERT_DAY_KEYS))),
        None
    )

    # 楽曲（CONCERT_SONG → ATLAS song ID → APOLLO.演奏曲リレーション で照合）
    concert_song_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT_SONG"], None)
    atlas_song_ids = set(
        sid
        for r in concert_song_rows
        if any(_match_id(rid, concert_id) for rid in ext_rel(r, CONCERT_SONG_CONCERT_REL_KEYS))
        for sid in ext_rel(r, CONCERT_SONG_SONG_REL_KEYS)
    )
    all_apollo = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    songs = [
        s for s in all_apollo
        if set(ext_rel(s, ["演奏曲", "FK演奏曲", "出演"])).intersection(atlas_song_ids)
    ]
    song_ids = {s.get("id", "") for s in songs}

    # パート定義（この演奏会の楽曲に紐づくものだけ）
    all_pd   = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    partdefs = [p for p in all_pd
                if any(_match_id(sid, pd_sid)
                       for sid in song_ids
                       for pd_sid in ext_rel(p, PARTDEF_SONG_REL_KEYS))]

    # 楽器マスタ
    instruments = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_map = {i.get("id", ""): ext(i, INSTRUMENT_NAME_KEYS) or ""
                for i in instruments}

    req_inst_ids: set[str] = set()
    for pd in partdefs:
        req_inst_ids.update(ext_rel(pd, PARTDEF_INST_REL_KEYS))

    # PART_MASTER
    part_master_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    part_master_map: dict[str, dict] = {
        r.get("id", ""): {
            "name": ext(r, PARTMASTER_NAME_KEYS) or "",
            "type": ext(r, PARTMASTER_TYPE_KEYS) or "",
        }
        for r in part_master_rows
    }

    # 参加者（この演奏会のみ）
    participant_rows_all = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    participant_rows_concert = [
        r for r in participant_rows_all
        if any(_match_id(rid, concert_id) for rid in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS))
    ]

    # 出欠・希望
    attendance_rows = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    preference_rows = ctx["query_all"](ctx["CONCERT_DB_PREFERENCE"], None)

    payload = {
        "concert":                  concert,
        "practices":                practices,
        "concert_day":              concert_day,
        "songs":                    songs,
        "partdefs":                 partdefs,
        "inst_map":                 inst_map,
        "req_insts":                sorted(req_inst_ids, key=lambda x: inst_map.get(x, x)),
        "part_master_map":          part_master_map,
        "participant_rows_concert": participant_rows_concert,
        "attendance_rows":          attendance_rows,
        "preference_rows":          preference_rows,
    }
    if cache_ttl > 0 and cache_key:
        _FORM_DATA_CACHE[cache_key] = (time.time(), payload)
    return payload


def load_attendance_data(ctx: dict, concert_id: str) -> dict:
    """
    出欠画面向け軽量取得。
    menu表示に必要な重い楽曲/パート定義取得を避けて遷移速度を上げる。
    """
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    concert_id_raw = concert_id.replace("-", "")

    def _match_id(a: str, b: str) -> bool:
        return (a or "").replace("-", "") == (b or "").replace("-", "")

    all_prac = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    practices = sorted(
        [p for p in all_prac
         if any(_match_id(rid, concert_id_raw) for rid in ext_rel(p, PRACTICE_CONCERT_REL_KEYS))
         and not _is_true(ext(p, PRACTICE_CONCERT_DAY_KEYS))],
        key=lambda p: ext(p, PRACTICE_DATE_KEYS) or "9999"
    )
    concert_day = next(
        (p for p in all_prac
         if any(_match_id(rid, concert_id_raw) for rid in ext_rel(p, PRACTICE_CONCERT_REL_KEYS))
         and _is_true(ext(p, PRACTICE_CONCERT_DAY_KEYS))),
        None
    )

    participant_rows_all = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    participant_rows_concert = [
        r for r in participant_rows_all
        if any(_match_id(rid, concert_id_raw) for rid in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS))
    ]

    attendance_rows = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    part_master_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    part_master_map: dict[str, dict] = {
        r.get("id", ""): {
            "name": ext(r, PARTMASTER_NAME_KEYS) or "",
            "type": ext(r, PARTMASTER_TYPE_KEYS) or "",
        }
        for r in part_master_rows
    }
    return {
        "practices": practices,
        "concert_day": concert_day,
        "participant_rows_concert": participant_rows_concert,
        "attendance_rows": attendance_rows,
        "part_master_map": part_master_map,
    }


_FORM_DATA_CACHE: dict[str, tuple[float, dict]] = {}


def _form_data_cache_ttl_seconds() -> int:
    try:
        return max(0, int(os.environ.get("FORM_DATA_CACHE_TTL_SECONDS", "180")))
    except Exception:
        return 180


def _is_true(v: str | None) -> bool:
    """Notionのcheckbox/text値をboolに変換。"""
    return (v or "").strip().lower() in ("true", "1", "yes", "on", "済", "完了")


# ── 出欠取得 ─────────────────────────────────────────────────

def get_cast_and_att_map(ctx: dict, concert_id: str, player_id: str,
                          participant_rows: list,
                          attendance_rows: list) -> tuple[str, dict[str, dict]]:
    """cast_id と 練習ID→{status, comment} のマップを返す。"""
    ext_txt = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    cast_id = ""
    for r in participant_rows:
        pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        if player_id in pids:
            cast_id = r.get("id", "")
            break

    att_map: dict[str, dict] = {}
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    t_att  = ctx["get_prop_types"](att_db)
    if not t_att:
        return cast_id, att_map

    practice_rel_key = find_relation_prop(t_att, ATT_PRACTICE_REL_KEYS)
    player_rel_key   = find_relation_prop(
        t_att, ATT_PLAYER_REL_KEYS,
        exclude={practice_rel_key} if practice_rel_key else set()
    )
    status_key = ctx["find_prop_name"](t_att, ATT_STATUS_KEYS)
    note_key   = ctx["find_prop_name"](t_att, ATT_NOTE_KEYS)

    if not player_rel_key or not practice_rel_key:
        return cast_id, att_map

    rel_targets = {player_id}
    if cast_id:
        rel_targets.add(cast_id)

    for row in attendance_rows:
        rel_ids = ext_rel(row, [player_rel_key])
        pr_ids  = ext_rel(row, [practice_rel_key])
        if not rel_ids or not pr_ids:
            continue
        if any(rid in rel_targets for rid in rel_ids):
            att_map[pr_ids[0]] = {
                "status":  ext_txt(row, [status_key]) or "未回答",
                "comment": ext_txt(row, [note_key])   or "",
            }
    return cast_id, att_map


# ── 希望取得 ─────────────────────────────────────────────────

def load_existing_prefs(ctx: dict, concert_id: str, player_id: str,
                         partdefs: list,
                         participant_rows: list,
                         preference_rows: list) -> dict[str, str]:
    """pd_id → priority のdictを返す。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    cast_id = ""
    for r in participant_rows:
        pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        if player_id in pids and concert_id in cids:
            cast_id = r.get("id", "")
            break

    targets = {player_id}
    if cast_id:
        targets.add(cast_id)
    pd_ids = {pd.get("id", "") for pd in partdefs}

    result: dict[str, str] = {}
    for r in preference_rows:
        pl_ids = set(ext_rel(r, PREF_PLAYER_REL_KEYS))
        pt_ids = ext_rel(r, PREF_PART_REL_KEYS)
        if not pl_ids.intersection(targets):
            continue
        if not pt_ids or pt_ids[0] not in pd_ids:
            continue
        priority = ext_txt(r, PREF_PRIORITY_KEYS) or "未回答"
        result[pt_ids[0]] = priority
    return result


# ── 全データ送信 ─────────────────────────────────────────────

def submit_all(ctx: dict,
               concert_id: str,
               concert_name: str,
               player_id: str,
               player_name: str,
               part_master_id: str,
               att: dict[str, str],
               att_comment: dict[str, str],
               pref: dict[str, str],
               own: dict[str, int],
               practices: list,
               concert_day: Optional[dict],
               inst_map: dict[str, str]) -> tuple[int, list[str], dict]:
    """
    出欠・希望・所有楽器をNotionに保存する。

    Returns:
        (成功件数, エラーリスト, デバッグ情報)
    """
    ext_rel  = ctx["extract_relation_ids_any"]
    ok_n     = 0
    errors: list[str] = []
    debug: dict = {}

    # ── CONCERT_CAST ──────────────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    t_cast  = ctx["get_prop_types"](cast_db)
    cast_id = ""
    if t_cast:
        all_cast = ctx["query_all"](cast_db, None)
        for r in all_cast:
            pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
            cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
            if player_id in pids and concert_id in cids:
                cast_id = r.get("id", "")
                break
        if not cast_id:
            props: dict = {}
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_PLAYER_REL_KEYS,  player_id)
            ctx["put_key_any"](props, t_cast, PARTICIPANT_RECORD_KEYS,
                               concert_id, player_id, prefix="participant")
            if part_master_id:
                ctx["put_prop_any"](props, t_cast, PARTICIPANT_PART_REL_KEYS, part_master_id)
            res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                     json={"parent": {"database_id": cast_db},
                                           "properties": props})
            if res and res.status_code == 200:
                cast_id = res.json().get("id", "")
                ok_n += 1
                debug["cast_id"] = cast_id
            else:
                errors.append(f"CONCERT_CAST登録失敗 (status={getattr(res,'status_code','?')})")
        else:
            debug["cast_id"] = cast_id + "（既存）"
            existing_row = next((r for r in all_cast if r.get("id", "") == cast_id), {})
            if not ext_rel(existing_row, PARTICIPANT_PART_REL_KEYS) and part_master_id:
                props_p: dict = {}
                ctx["put_prop_any"](props_p, t_cast, PARTICIPANT_PART_REL_KEYS, part_master_id)
                ctx["api_request"]("patch",
                    f"https://api.notion.com/v1/pages/{cast_id}",
                    json={"properties": props_p})

    debug["player_id"] = player_id

    # ── ATTENDANCE ────────────────────────────────────────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    t_att  = ctx["get_prop_types"](att_db)
    if t_att:
        all_att = ctx["query_all"](att_db, None)
        prac_name_map = {p.get("id", ""): ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ""
                         for p in practices}
        att_all = dict(att)
        if concert_day:
            att_all[concert_day.get("id", "")] = "○"
            prac_name_map[concert_day.get("id", "")] = (
                ctx["extract_prop_text_any"](concert_day, PRACTICE_NAME_KEYS) or "本番当日"
            )

        practice_rel_key = find_relation_prop(t_att, ATT_PRACTICE_REL_KEYS)
        player_rel_key   = find_relation_prop(
            t_att, ATT_PLAYER_REL_KEYS,
            exclude={practice_rel_key} if practice_rel_key else set()
        )
        status_key = ctx["find_prop_name"](t_att, ATT_STATUS_KEYS)
        note_key   = ctx["find_prop_name"](t_att, ATT_NOTE_KEYS)
        rel_target = cast_id if cast_id else player_id

        att_ids: list[str] = []
        for pr_id, status in att_all.items():
            existing_id = ""
            for r in all_att:
                pl = (r.get("properties", {}).get(player_rel_key, {}).get("relation", []) or [])
                pr = (r.get("properties", {}).get(practice_rel_key, {}).get("relation", []) or [])
                if rel_target in [x.get("id", "") for x in pl] and \
                   pr_id in [x.get("id", "") for x in pr]:
                    existing_id = r.get("id", "")
                    break
            props = {}
            ctx["put_key_any"](props, t_att, ATTENDANCE_KEY_KEYS,
                               rel_target, pr_id, prefix="att")
            if player_rel_key:
                ctx["put_prop"](props, t_att, player_rel_key, rel_target)
            if practice_rel_key:
                ctx["put_prop"](props, t_att, practice_rel_key, pr_id)
            if status_key:
                ctx["put_prop"](props, t_att, status_key, status)
            if note_key:
                cmt = (att_comment.get(pr_id, "") or "").strip()
                ctx["put_prop"](props, t_att, note_key, cmt)
            if existing_id:
                res = ctx["api_request"]("patch",
                    f"https://api.notion.com/v1/pages/{existing_id}",
                    json={"properties": props})
            else:
                res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                         json={"parent": {"database_id": att_db},
                                               "properties": props})
            if res and res.status_code == 200:
                ok_n += 1
                att_ids.append(res.json().get("id", "") if not existing_id else existing_id)
            else:
                pname = prac_name_map.get(pr_id, pr_id[:8])
                errors.append(f"出欠登録失敗：{pname} (status={getattr(res,'status_code','?')})")
        debug["att_ids"] = att_ids

    # ── パート希望 ────────────────────────────────────────────
    if pref:
        pref_db = ctx["CONCERT_DB_PREFERENCE"]
        t_pref  = ctx["get_prop_types"](pref_db)
        if t_pref:
            all_pref = ctx["query_all"](pref_db, None)
            for pd_id, priority in pref.items():
                if priority == "未回答":
                    continue
                existing_id = ""
                pref_target = cast_id if cast_id else player_id
                for r in all_pref:
                    pl = ext_rel(r, PREF_PLAYER_REL_KEYS)
                    pd = ext_rel(r, PREF_PART_REL_KEYS)
                    if pref_target in pl and pd_id in pd:
                        existing_id = r.get("id", "")
                        break
                props = {}
                ctx["put_key_any"](props, t_pref, PREFERENCE_KEY_KEYS,
                                   pref_target, pd_id, prefix="pref")
                ctx["put_prop_any"](props, t_pref, PREF_PLAYER_REL_KEYS, pref_target)
                ctx["put_prop_any"](props, t_pref, PREF_PART_REL_KEYS,   pd_id)
                ctx["put_prop_any"](props, t_pref, PREF_PRIORITY_KEYS,   priority)
                if existing_id:
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{existing_id}",
                        json={"properties": props})
                else:
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": pref_db},
                                                   "properties": props})
                if res and res.status_code == 200:
                    ok_n += 1
                else:
                    errors.append(f"希望登録失敗：{pd_id[:8]} (status={getattr(res,'status_code','?')})")

    # ── 所有楽器 ──────────────────────────────────────────────
    if own:
        pi_db = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
        t_pi  = ctx["get_prop_types"](pi_db)
        if t_pi:
            all_pi = ctx["query_all"](pi_db, None)
            for iid, cnt in own.items():
                if cnt == 0:
                    continue
                iname = inst_map.get(iid, iid)
                existing_id = ""
                for r in all_pi:
                    if (player_id in ext_rel(r, PI_PLAYER_REL_KEYS) and
                            iid in ext_rel(r, PI_INST_REL_KEYS) and
                            concert_id in ext_rel(r, PI_CONCERT_REL_KEYS)):
                        existing_id = r.get("id", "")
                        break
                props = {}
                ctx["put_key_any"](props, t_pi, PI_RECORD_KEYS,
                                   player_id, iid, prefix="assign")
                ctx["put_prop_any"](props, t_pi, PI_PLAYER_REL_KEYS,  player_id)
                ctx["put_prop_any"](props, t_pi, PI_INST_REL_KEYS,    iid)
                ctx["put_prop_any"](props, t_pi, PI_CONCERT_REL_KEYS, concert_id)
                ctx["put_prop_any"](props, t_pi, PI_OWN_COUNT_KEYS,   cnt)
                if existing_id:
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{existing_id}",
                        json={"properties": props})
                else:
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": pi_db},
                                                   "properties": props})
                if res and res.status_code == 200:
                    ok_n += 1
                else:
                    errors.append(f"楽器登録失敗：{iname} (status={getattr(res,'status_code','?')})")

    return ok_n, errors, debug


# ── カバーURL取得 ────────────────────────────────────────────

def get_cover_url(concert: dict) -> str:
    """NotionページのカバーURLを返す。external/file両対応。"""
    cover = (concert or {}).get("cover") or {}
    ctype = cover.get("type", "")
    if ctype == "external":
        return (cover.get("external") or {}).get("url", "")
    if ctype == "file":
        return (cover.get("file") or {}).get("url", "")
    return ""


# ── アサイン取得 ──────────────────────────────────────────────

def get_my_assign_rows(
    ctx: dict,
    concert_id: str,
    player_id: str,
    participant_rows: list,
    assignment_rows: list | None = None,
) -> list[dict]:
    """自分（または自パート）のアサイン結果行を返す。担当フラグTrueのみ。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    cast_id = ""
    for r in participant_rows:
        if player_id in ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS):
            cast_id = r.get("id", "")
            break
    targets = {player_id}
    if cast_id:
        targets.add(cast_id)

    if assignment_rows is None:
        try:
            all_assign = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
        except Exception:
            return []
    else:
        all_assign = assignment_rows

    cid_norm = concert_id.replace("-", "")
    result = []
    for r in all_assign:
        if not any(rid.replace("-","") == cid_norm for rid in ext_rel(r, ASSIGNMENT_CONCERT_REL_KEYS)):
            continue
        flag = ext_txt(r, ASSIGNMENT_FLAG_KEYS) or ""
        if flag.strip().lower() not in ("true", "1", "yes"):
            continue
        player_ids = ext_rel(r, ASSIGNMENT_PLAYER_REL_KEYS)
        if any(t in player_ids for t in targets):
            result.append(r)
    return result


def has_published_assignments(ctx: dict, concert_id: str, assignment_rows: list | None = None) -> bool:
    """担当フラグTrueのアサイン行がこの演奏会に1件でもあるか。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    cid_norm = concert_id.replace("-", "")
    if assignment_rows is None:
        try:
            all_assign = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
        except Exception:
            return False
    else:
        all_assign = assignment_rows
    return any(
        any(rid.replace("-","") == cid_norm for rid in ext_rel(r, ASSIGNMENT_CONCERT_REL_KEYS))
        and ext_txt(r, ASSIGNMENT_FLAG_KEYS).strip().lower() in ("true", "1", "yes")
        for r in all_assign
    )


def build_assignment_view_rows(ctx: dict, assignment_rows: list, songs: list, partdefs: list) -> list[dict]:
    """自分向けアサイン行を表示用に整形する。"""
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    song_name_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "") for s in songs or []}
    part_name_map = {
        p.get("id", ""): (ext(p, PARTDEF_DISPLAY_NAME_KEYS) or ext(p, PARTDEF_NAME_KEYS) or "")
        for p in partdefs or []
    }
    out = []
    for row in assignment_rows or []:
        sids = ext_rel(row, ASSIGNMENT_SONG_REL_KEYS)
        pdids = ext_rel(row, ASSIGNMENT_PARTDEF_REL_KEYS)
        out.append(
            {
                "song": song_name_map.get(sids[0], "未設定") if sids else "未設定",
                "part": part_name_map.get(pdids[0], "-") if pdids else "-",
            }
        )
    out.sort(key=lambda x: (x["song"], x["part"]))
    return out


def build_role_assignment_rows(
    ctx: dict,
    concert_id: str,
    role: int,
    my_part_id: str,
    partdefs: list,
    songs: list,
    participant_rows: list,
    assignment_rows: list | None = None,
    player_rows: list | None = None,
) -> list[dict]:
    """Leader/Manager向けアサイン一覧を返す。"""
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    cid_norm = (concert_id or "").replace("-", "")

    partdef_song: dict[str, str] = {}
    partdef_name: dict[str, str] = {}
    partdef_pm: dict[str, str] = {}
    for pd in partdefs or []:
        pdid = pd.get("id", "")
        if not pdid:
            continue
        song_ids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        pm_ids = ext_rel(pd, PARTDEF_PART_REL_KEYS)
        partdef_song[pdid] = song_ids[0] if song_ids else ""
        partdef_pm[pdid] = pm_ids[0] if pm_ids else ""
        partdef_name[pdid] = ext(pd, PARTDEF_DISPLAY_NAME_KEYS) or ext(pd, PARTDEF_NAME_KEYS) or "-"

    song_name_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "") for s in songs or []}
    if player_rows is None:
        player_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {r.get("id", ""): (ext(r, PLAYER_NAME_KEYS) or "") for r in player_rows}
    cast_to_player = {}
    for cast in participant_rows or []:
        pids = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
        if pids:
            cast_to_player[cast.get("id", "")] = pids[0]

    if assignment_rows is None:
        try:
            all_assign = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
        except Exception:
            return []
    else:
        all_assign = assignment_rows

    out = []
    for row in all_assign:
        if not any((rid or "").replace("-", "") == cid_norm for rid in ext_rel(row, ASSIGNMENT_CONCERT_REL_KEYS)):
            continue
        pdids = ext_rel(row, ASSIGNMENT_PARTDEF_REL_KEYS)
        if not pdids:
            continue
        pdid = pdids[0]
        if role == ROLE_LEADER and my_part_id and partdef_pm.get(pdid, "") != my_part_id:
            continue
        sids = ext_rel(row, ASSIGNMENT_SONG_REL_KEYS)
        sid = sids[0] if sids else partdef_song.get(pdid, "")

        rel_players = ext_rel(row, ASSIGNMENT_PLAYER_REL_KEYS)
        performer = "-"
        if rel_players:
            rid = rel_players[0]
            pid = cast_to_player.get(rid, rid)
            performer = player_name_map.get(pid, "-")

        out.append({"song": song_name_map.get(sid, "未設定"), "part": partdef_name.get(pdid, "-"), "player": performer})
    out.sort(key=lambda x: (x["song"], x["part"], x["player"]))
    return out


# ── ロール解決 ────────────────────────────────────────────────

def resolve_user_role(ctx: dict, player_id: str, concert_id: str,
                       participant_rows: list) -> int:
    """CONCERT_CASTの役職フィールドからロール（0=Player/1=Leader/2=Manager）を返す。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    MANAGER_KEYWORDS = ["代表", "会長", "運営", "manager", "Manager"]
    LEADER_KEYWORDS  = ["パートリーダー", "首席", "leader", "Leader"]

    for r in participant_rows:
        pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        if player_id not in pids or concert_id not in cids:
            continue
        role_ops = ext_txt(r, PARTICIPANT_ROLE_OPS_KEYS) or ""
        role_mus = ext_txt(r, PARTICIPANT_ROLE_KEYS) or ""
        if any(k in role_ops for k in MANAGER_KEYWORDS):
            return ROLE_MANAGER
        if any(k in role_mus for k in LEADER_KEYWORDS):
            return ROLE_LEADER
    return ROLE_PLAYER
