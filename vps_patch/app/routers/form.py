"""
app/routers/form.py
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
import os
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Form, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

from app.services.auth_service import (
    CODE_MAX_ATTEMPTS,
    INVITE_MAX_ATTEMPTS,
    INVITE_WINDOW_MINUTES,
    build_magic_code_email,
    code_expiry,
    generate_code,
    hash_code,
    hash_password,
    is_code_expired,
    verify_code,
    verify_password,
)
from app.services.form_service import (
    CONCERT_DATE_KEYS,
    CONCERT_NAME_KEYS,
    CONCERT_VENUE_KEYS,
    PRACTICE_NAME_KEYS,
    PRACTICE_DATE_KEYS,
    PRACTICE_VENUE_KEYS,
    HARMONIA_CONCERT_PLAN_KEYS,
    HARMONIA_CONCERT_ASSIGN_KEYS,
    PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS,
    PARTICIPANT_RECORD_KEYS,
    PARTICIPANT_ROLE_KEYS,
    PARTICIPANT_ROLE_OPS_KEYS,
    ATT_PLAYER_REL_KEYS,
    ATT_PRACTICE_REL_KEYS,
    ATT_STATUS_KEYS,
    PREF_PLAYER_REL_KEYS,
    PREF_PART_REL_KEYS,
    PREF_PRIORITY_KEYS,
    PARTMASTER_NAME_KEYS,
    PARTDEF_PART_REL_KEYS,
    PARTDEF_NAME_KEYS,
    PARTDEF_DISPLAY_NAME_KEYS,
    PARTDEF_SCORE_URL_KEYS,
    PARTDEF_SONG_REL_KEYS,
    SONG_NAME_KEYS,
    SONG_SCORE_URL_KEYS,
    INSTRUMENT_NAME_KEYS,
    PLAYER_EMAIL_KEYS,
    PLAYER_NAME_KEYS,
    PLAYER_PASSWORD_HASH_KEYS,
    ROLE_PLAYER,
    ROLE_LEADER,
    ROLE_MANAGER,
    is_perc,
    load_form_data,
    load_attendance_data,
    load_existing_prefs,
    resolve_user_role,
    submit_all,
    get_cover_url,
    get_my_assign_rows,
    has_published_assignments,
    build_assignment_view_rows,
    build_role_assignment_rows,
    ASSIGNMENT_CONCERT_REL_KEYS,
    ASSIGNMENT_PLAYER_REL_KEYS,
    ASSIGNMENT_PARTDEF_REL_KEYS,
    ASSIGNMENT_SONG_REL_KEYS,
    ASSIGNMENT_FLAG_KEYS,
)
from app.services.mailer import send_text
from app.services import keys as _K
from app.services.form_service import PARTDEF_INST_REL_KEYS

_ROOT_DIR = Path(__file__).resolve().parents[3]
if str(_ROOT_DIR) not in sys.path:
    sys.path.append(str(_ROOT_DIR))

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
DEBUG_ROLE_OVERRIDE_SESSION_KEY = "debug_role_override"
_ASSIGN_SOLVER_CACHE: dict[tuple[str, str], dict] = {}
SCHEDULE_PRACTICE_REL_KEYS = _K.SCHEDULE_PRACTICE_REL_KEYS
SCHEDULE_START_KEYS = _K.SCHEDULE_START_KEYS
SCHEDULE_END_KEYS = _K.SCHEDULE_END_KEYS
SCHEDULE_TYPE_KEYS = _K.SCHEDULE_TYPE_KEYS
SCHEDULE_CONTENT_KEYS = _K.SCHEDULE_CONTENT_KEYS
SCHEDULE_SONG_REL_KEYS = _K.SCHEDULE_SONG_REL_KEYS
SCHEDULE_ORDER_KEYS = _K.SCHEDULE_ORDER_KEYS
POKE_KEY_KEYS = _K.POKE_KEY_KEYS
POKE_SENDER_CAST_REL_KEYS = _K.POKE_SENDER_CAST_REL_KEYS
POKE_TARGET_CAST_REL_KEYS = _K.POKE_TARGET_CAST_REL_KEYS
POKE_CONCERT_REL_KEYS = _K.POKE_CONCERT_REL_KEYS
POKE_PRACTICE_REL_KEYS = _K.POKE_PRACTICE_REL_KEYS
POKE_TYPE_KEYS = _K.POKE_TYPE_KEYS
POKE_MESSAGE_KEYS = _K.POKE_MESSAGE_KEYS
POKE_STATUS_KEYS = _K.POKE_STATUS_KEYS
POKE_EXPIRES_AT_KEYS = _K.POKE_EXPIRES_AT_KEYS
ASSIGN_RESP_KEY_KEYS = _K.ASSIGN_RESP_KEY_KEYS
ASSIGN_RESP_CONCERT_REL_KEYS = _K.ASSIGN_RESP_CONCERT_REL_KEYS
ASSIGN_RESP_CAST_REL_KEYS = _K.ASSIGN_RESP_CAST_REL_KEYS
ASSIGN_RESP_PLAN_KEYS = _K.ASSIGN_RESP_PLAN_KEYS
ASSIGN_RESP_STATUS_KEYS = _K.ASSIGN_RESP_STATUS_KEYS
ASSIGN_RESP_COMMENT_KEYS = _K.ASSIGN_RESP_COMMENT_KEYS


def get_ctx():
    from app.services.notion_client import build_concert_ctx
    return build_concert_ctx()


def _perf_enabled() -> bool:
    return (os.environ.get("FORM_PERF_LOG", "").strip().lower() in ("1", "true", "yes", "on"))


def _flash_set(request: Request, key: str, val: str) -> None:
    request.session[key] = val


def _flash_pop(request: Request, key: str):
    return request.session.pop(key, None)


def _clear_keys(request: Request, keys: list[str]) -> None:
    for k in keys:
        request.session.pop(k, None)


def _get_player_by_email(ctx: dict, email: str):
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    email = (email or "").strip().lower()
    return next(
        (p for p in players if (ctx["extract_prop_text_any"](p, PLAYER_EMAIL_KEYS) or "").strip().lower() == email),
        None,
    )


def _get_player_by_id(ctx: dict, player_id: str):
    if not player_id:
        return None
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    return next((p for p in players if p.get("id") == player_id), None)


def _part_options(ctx: dict) -> list[tuple[str, str]]:
    ext = ctx["extract_prop_text_any"]
    rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    out = []
    for r in rows:
        pid = r.get("id", "")
        name = ext(r, PARTMASTER_NAME_KEYS) or ""
        if pid and name:
            out.append((pid, name))
    return sorted(out, key=lambda x: x[1].lower())


def _resolve_invite_concert(ctx: dict, code: str) -> tuple[str, str]:
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    code = (code or "").strip().upper()
    if not code:
        return "", "invalid"
    rows = ctx["query_all"](ctx["CONCERT_DB_HARMONIA_CONCERT"], None)
    matched = []
    for r in rows:
        v = (ext(r, ["招待コード", "invite_code", "INVITE_CODE"]) or "").strip().upper()
        if v and v == code:
            matched.extend(ext_rel(r, ["演奏会", "FK演奏会", "concert"]))
    uniq = sorted(set([x for x in matched if x]))
    if len(uniq) == 1:
        return uniq[0], ""
    if len(uniq) >= 2:
        return "", "ambiguous"
    return "", "not_found"


def _link_player_to_concert(ctx: dict, player_id: str, concert_id: str, part_id: str) -> tuple[bool, str]:
    ext_rel = ctx["extract_relation_ids_any"]
    db = ctx["CONCERT_DB_PARTICIPANT"]
    t = ctx["get_prop_types"](db)
    if not t:
        return False, "出演DB設定を取得できません。"
    if not part_id:
        return False, "担当パートを選択してください。"
    rows = ctx["query_all"](db, None)
    for r in rows:
        if player_id in ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) and concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS):
            return True, "already_linked"
    props: dict = {}
    ctx["put_prop_any"](props, t, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
    ctx["put_prop_any"](props, t, PARTICIPANT_PLAYER_REL_KEYS, player_id)
    ctx["put_prop_any"](props, t, PARTICIPANT_PART_REL_KEYS, part_id)
    ctx["put_key_any"](props, t, PARTICIPANT_RECORD_KEYS, concert_id, player_id, prefix="participant")
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": db}, "properties": props})
    if res and res.status_code == 200:
        return True, "linked"
    return False, f"紐付けに失敗しました (status={getattr(res, 'status_code', '?')})"


def _my_concerts(ctx: dict, player_id: str) -> list[dict]:
    ext_rel = ctx["extract_relation_ids_any"]
    ext = ctx["extract_prop_text_any"]
    cast = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    cids = []
    for r in cast:
        if player_id in ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS):
            rel = ext_rel(r, ["出演", "演奏会", "FK演奏会", "concert"])
            if rel:
                cids.append(rel[0])
    if not cids:
        return []
    concerts = ctx["query_all"](ctx["CONCERT_DB_ATLAS"], None)
    rows = [c for c in concerts if c.get("id") in set(cids)]
    rows.sort(key=lambda r: ext(r, CONCERT_DATE_KEYS) or "", reverse=True)
    return rows


def _atlas_concert_name(ctx: dict, concert_row: dict) -> str:
    """ATLASの演奏会名を安全に取得（候補キー -> title型プロパティの順で解決）。"""
    ext = ctx["extract_prop_text_any"]
    name = (ext(concert_row, CONCERT_NAME_KEYS) or "").strip()
    if name:
        return name
    db = ctx["CONCERT_DB_ATLAS"]
    t = ctx["get_prop_types"](db) or {}
    title_keys = [k for k, meta in t.items() if (meta or {}).get("type") == "title"]
    if title_keys:
        name = (ext(concert_row, title_keys) or "").strip()
    return name or concert_row.get("id", "")[:8]


def _my_concert_options(ctx: dict, player_id: str) -> list[dict]:
    """
    HARMONIA_CONCERT DB の「演奏会」リレーションを正として、
    参加中演奏会のプルダウン候補を組み立てる。
    """
    ext_txt = ctx["extract_prop_text_any"]
    # 参加演奏会取得は既存の安定ロジック（_my_concerts）を使う
    concerts = _my_concerts(ctx, player_id)
    options = []
    for c in concerts:
        cid = c.get("id", "")
        if not cid:
            continue
        cdate = (ext_txt(c, CONCERT_DATE_KEYS) or "").strip()
        cname = _atlas_concert_name(ctx, c)
        label = f"{cname}（{cdate[:10]}）" if cdate else cname
        options.append({"id": cid, "name": label})
    return options


def _find_concert(ctx: dict, concert_id: str) -> dict | None:
    concerts = ctx["query_all"](ctx["CONCERT_DB_ATLAS"], None)
    return next((c for c in concerts if c.get("id") == concert_id), None)


def _my_part_info(ctx: dict, player_id: str, concert_id: str, participant_rows: list[dict]) -> tuple[str, str]:
    ext_rel = ctx["extract_relation_ids_any"]
    pmap = {}
    for r in participant_rows:
        rid = r.get("id", "")
        if rid:
            pmap[rid] = r
    part_master_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    pm_name = {}
    pm_type = {}
    ext = ctx["extract_prop_text_any"]
    for r in part_master_rows:
        rid = r.get("id", "")
        pm_name[rid] = ext(r, PARTMASTER_NAME_KEYS) or ""
        pm_type[rid] = ext(r, ["種別", "type", "Type", "パート種別"]) or ""

    for row in participant_rows:
        if player_id not in ext_rel(row, PARTICIPANT_PLAYER_REL_KEYS):
            continue
        if concert_id not in ext_rel(row, PARTICIPANT_CONCERT_REL_KEYS):
            continue
        part_ids = ext_rel(row, PARTICIPANT_PART_REL_KEYS)
        if not part_ids:
            return "", ""
        pid = part_ids[0]
        return pm_name.get(pid, ""), pid
    return "", ""


def _my_music_role(ctx: dict, player_id: str, concert_id: str, participant_rows: list[dict]) -> str:
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    for row in participant_rows:
        if player_id not in ext_rel(row, PARTICIPANT_PLAYER_REL_KEYS):
            continue
        if concert_id not in ext_rel(row, PARTICIPANT_CONCERT_REL_KEYS):
            continue
        return (ext_txt(row, PARTICIPANT_ROLE_KEYS) or "").strip()
    return ""


def _my_ops_role(ctx: dict, player_id: str, concert_id: str, participant_rows: list[dict]) -> str:
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    for row in participant_rows:
        if player_id not in ext_rel(row, PARTICIPANT_PLAYER_REL_KEYS):
            continue
        if concert_id not in ext_rel(row, PARTICIPANT_CONCERT_REL_KEYS):
            continue
        return (ext_txt(row, PARTICIPANT_ROLE_OPS_KEYS) or "").strip()
    return ""


def _my_system_role(ctx: dict, player_id: str, concert_id: str, participant_rows: list[dict]) -> str:
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    for row in participant_rows:
        if player_id not in ext_rel(row, PARTICIPANT_PLAYER_REL_KEYS):
            continue
        if concert_id not in ext_rel(row, PARTICIPANT_CONCERT_REL_KEYS):
            continue
        return (ext_txt(row, ["システムロール", "system_role", "SystemRole"]) or "").strip()
    return ""


def _is_administrator_role(system_role: str) -> bool:
    v = (system_role or "").strip().lower()
    return v in {"administrator", "admin"}


def _role_from_override(override: str | None) -> int | None:
    v = (override or "").strip().lower()
    if v == "player":
        return ROLE_PLAYER
    if v == "leader":
        return ROLE_LEADER
    if v == "manager":
        return ROLE_MANAGER
    return None


def _role_label(role: int) -> str:
    if role >= ROLE_MANAGER:
        return "Manager"
    if role >= ROLE_LEADER:
        return "Leader"
    return "Player"


def _is_unanswered_status(v: str) -> bool:
    t = (v or "").strip()
    return t in {"", "未回答", "—", "-"}


def _is_maybe_status(v: str) -> bool:
    return (v or "").strip() == "△"


def _short_name_list(names: list[str], limit: int = 4) -> str:
    clean = [n for n in names if (n or "").strip()]
    if not clean:
        return ""
    if len(clean) <= limit:
        return " / ".join(clean)
    return f"{' / '.join(clean[:limit])} ほか{len(clean) - limit}名"


def _build_role_todo_items(
    ctx: dict,
    *,
    role: int,
    concert_id: str,
    my_part_id: str,
    participant_rows: list[dict],
    part_master_map: dict,
    player_map: dict,
    practices: list[dict],
    attendance_rows: list[dict],
    upcoming_practice_id: str,
    partdefs: list[dict],
    preference_rows: list[dict],
) -> list[dict[str, str]]:
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    people: list[dict] = []
    for cast in participant_rows or []:
        cast_id = cast.get("id", "")
        pm_ids = ext_rel(cast, PARTICIPANT_PART_REL_KEYS)
        player_ids = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
        if not cast_id or not pm_ids or not player_ids:
            continue
        part_id = pm_ids[0]
        player_id = player_ids[0]
        part_info = (part_master_map or {}).get(part_id, {}) or {}
        part_name = (part_info.get("name", "") or "").strip()
        part_type = (part_info.get("type", "") or "").strip()
        name = (ext_txt((player_map or {}).get(player_id, {}), PLAYER_NAME_KEYS) or "").strip() or "—"
        people.append({
            "cast_id": cast_id,
            "player_id": player_id,
            "part_id": part_id,
            "part_name": part_name,
            "name": name,
            "is_perc": is_perc(part_name) or is_perc(part_type),
        })

    if role >= ROLE_MANAGER:
        scoped_people = people
    elif role >= ROLE_LEADER:
        scoped_people = [p for p in people if p["part_id"] == my_part_id]
    else:
        scoped_people = []
    if not scoped_people:
        return []

    status_map: dict[tuple[str, str], str] = {}
    for row in attendance_rows or []:
        targets = ext_rel(row, ATT_PLAYER_REL_KEYS)
        pr_ids = ext_rel(row, ATT_PRACTICE_REL_KEYS)
        if not targets or not pr_ids:
            continue
        status = (ext_txt(row, ATT_STATUS_KEYS) or "").strip() or "未回答"
        for tid in targets:
            for pr_id in pr_ids:
                status_map.setdefault((tid, pr_id), status)

    def status_for(person: dict, practice_id: str) -> str:
        if not practice_id:
            return "未回答"
        return (
            status_map.get((person["cast_id"], practice_id))
            or status_map.get((person["player_id"], practice_id))
            or "未回答"
        )

    has_unanswered_by_person: dict[str, bool] = {}
    has_maybe_by_person: dict[str, bool] = {}
    practice_ids = [p.get("id", "") for p in (practices or []) if p.get("id", "")]
    for person in scoped_people:
        has_unanswered_by_person[person["player_id"]] = any(
            _is_unanswered_status(status_for(person, pr_id)) for pr_id in practice_ids
        )
        has_maybe_by_person[person["player_id"]] = _is_maybe_status(status_for(person, upcoming_practice_id))

    # 所有楽器は Percussion 所属のみ対象（行が1件でもあれば入力済み扱い）
    own_done_player_ids: set[str] = set()
    try:
        pi_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    except Exception:
        pi_rows = []
    cid_norm = (concert_id or "").replace("-", "")
    for row in pi_rows:
        rel_cids = [x.replace("-", "") for x in ext_rel(row, _K.PI_CONCERT_REL_KEYS)]
        if cid_norm not in rel_cids:
            continue
        for pid in ext_rel(row, _K.PI_PLAYER_REL_KEYS):
            if pid:
                own_done_player_ids.add(pid)
    own_missing_by_person = {
        p["player_id"]: (p["is_perc"] and p["player_id"] not in own_done_player_ids)
        for p in scoped_people
    }

    # パート希望: 自パートに紐づく partdef のうち未回答が1つでもあれば未回答扱い
    partdef_ids_by_part: dict[str, set[str]] = defaultdict(set)
    for pd in partdefs or []:
        pd_id = pd.get("id", "")
        if not pd_id:
            continue
        for pm_id in ext_rel(pd, PARTDEF_PART_REL_KEYS):
            if pm_id:
                partdef_ids_by_part[pm_id].add(pd_id)
    pref_answered: dict[tuple[str, str], bool] = {}
    for row in preference_rows or []:
        targets = ext_rel(row, PREF_PLAYER_REL_KEYS)
        pd_ids = ext_rel(row, PREF_PART_REL_KEYS)
        prio = (ext_txt(row, PREF_PRIORITY_KEYS) or "").strip()
        answered = prio not in {"", "未回答"}
        if not answered:
            continue
        for t in targets:
            for pd_id in pd_ids:
                pref_answered[(t, pd_id)] = True
    pref_missing_by_person: dict[str, bool] = {}
    for p in scoped_people:
        pd_ids = partdef_ids_by_part.get(p["part_id"], set())
        if not pd_ids:
            pref_missing_by_person[p["player_id"]] = False
            continue
        missing = False
        for pd_id in pd_ids:
            if pref_answered.get((p["cast_id"], pd_id)) or pref_answered.get((p["player_id"], pd_id)):
                continue
            missing = True
            break
        pref_missing_by_person[p["player_id"]] = missing

    todo_items: list[dict[str, str]] = []
    if role >= ROLE_MANAGER:
        by_part: dict[str, list[dict]] = defaultdict(list)
        for p in scoped_people:
            by_part[p["part_name"] or "（未設定）"].append(p)

        def part_labels(pred) -> list[str]:
            labels: list[str] = []
            for part_name, members in sorted(by_part.items(), key=lambda x: x[0].lower()):
                cnt = sum(1 for m in members if pred(m))
                if cnt > 0:
                    labels.append(f"{part_name}（{cnt}名）")
            return labels

        labels_unanswered = part_labels(lambda m: has_unanswered_by_person.get(m["player_id"], False))
        if labels_unanswered:
            todo_items.append({
                "title": "出欠未回答のあるパートへ催促",
                "desc": " / ".join(labels_unanswered[:4]) + (f" ほか{len(labels_unanswered)-4}パート" if len(labels_unanswered) > 4 else ""),
                "href": "/form?tab=att#role-menu-panels",
                "icon": "clipboard-data",
            })

        if upcoming_practice_id:
            labels_maybe = part_labels(lambda m: has_maybe_by_person.get(m["player_id"], False))
            if labels_maybe:
                todo_items.append({
                    "title": "直近練習で△回答のあるパートへ催促",
                    "desc": " / ".join(labels_maybe[:4]) + (f" ほか{len(labels_maybe)-4}パート" if len(labels_maybe) > 4 else ""),
                    "href": "/form?tab=att#role-menu-panels",
                    "icon": "calendar2-week",
                })

        labels_own = part_labels(lambda m: own_missing_by_person.get(m["player_id"], False))
        if labels_own:
            todo_items.append({
                "title": "所有楽器未入力のあるパートへ催促",
                "desc": " / ".join(labels_own[:4]) + (f" ほか{len(labels_own)-4}パート" if len(labels_own) > 4 else ""),
                "href": "/form?tab=ownmap#role-menu-panels",
                "icon": "collection",
            })

        labels_pref = part_labels(lambda m: pref_missing_by_person.get(m["player_id"], False))
        if labels_pref:
            todo_items.append({
                "title": "パート希望未入力のあるパートへ催促",
                "desc": " / ".join(labels_pref[:4]) + (f" ほか{len(labels_pref)-4}パート" if len(labels_pref) > 4 else ""),
                "href": "/form/pref",
                "icon": "music-note-list",
            })
        return todo_items

    # Leader: 人単位
    def names_by(pred) -> list[str]:
        return sorted([p["name"] for p in scoped_people if pred(p)], key=lambda x: x.lower())

    names_unanswered = names_by(lambda p: has_unanswered_by_person.get(p["player_id"], False))
    if names_unanswered:
        todo_items.append({
            "title": "出欠未回答メンバーへ催促",
            "desc": _short_name_list(names_unanswered),
            "href": "/form?tab=att#role-menu-panels",
            "icon": "clipboard-data",
        })

    if upcoming_practice_id:
        names_maybe = names_by(lambda p: has_maybe_by_person.get(p["player_id"], False))
        if names_maybe:
            todo_items.append({
                "title": "直近練習で△回答メンバーへ催促",
                "desc": _short_name_list(names_maybe),
                "href": "/form?tab=att#role-menu-panels",
                "icon": "calendar2-week",
            })

    names_own = names_by(lambda p: own_missing_by_person.get(p["player_id"], False))
    if names_own:
        todo_items.append({
            "title": "所有楽器未入力メンバーへ催促",
            "desc": _short_name_list(names_own),
            "href": "/form?tab=ownmap#role-menu-panels",
            "icon": "collection",
        })

    names_pref = names_by(lambda p: pref_missing_by_person.get(p["player_id"], False))
    if names_pref:
        todo_items.append({
            "title": "パート希望未入力メンバーへ催促",
            "desc": _short_name_list(names_pref),
            "href": "/form/pref",
            "icon": "music-note-list",
        })
    return todo_items


def _format_hhmm(v: str) -> str:
    txt = (v or "").strip()
    if not txt:
        return ""
    if len(txt) == 4 and txt.isdigit():
        return f"{txt[:2]}:{txt[2:]}"
    if len(txt) == 3 and txt.isdigit():
        return f"0{txt[0]}:{txt[1:]}"
    if len(txt) == 5 and txt[2] == ":":
        return txt
    if "T" in txt:
        t = txt.split("T", 1)[1][:5]
        if len(t) == 5 and t[2] == ":":
            return t
    return txt


def _schedule_type_class(type_name: str) -> str:
    v = (type_name or "").strip()
    if v == "練習":
        return "is-practice"
    if v == "開場":
        return "is-open"
    if v in ("休憩",):
        return "is-break"
    if v in ("搬入", "搬出", "退館"):
        return "is-move"
    if v in ("その他",):
        return "is-other"
    return "is-default"


def _harmonia_flags(ctx: dict, concert_id: str) -> dict:
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    rows = ctx["query_all"](ctx["CONCERT_DB_HARMONIA_CONCERT"], None)
    cid_norm = _norm_id(concert_id)
    target = None
    for r in rows:
        rel = ext_rel(r, ["演奏会", "FK演奏会", "concert"])
        rel_norm = {_norm_id(x) for x in rel if x}
        if cid_norm and cid_norm in rel_norm:
            target = r
            break
    if not target:
        return {"plan_done": False, "assign_done": False}
    v = (ext_txt(target, HARMONIA_CONCERT_PLAN_KEYS) or "").strip().lower()
    a = (ext_txt(target, HARMONIA_CONCERT_ASSIGN_KEYS) or "").strip().lower()
    return {
        "plan_done": v in ("true", "1", "yes", "on", "済", "完了"),
        "assign_done": a in ("true", "1", "yes", "on", "済", "完了"),
    }


def _harmonia_row(ctx: dict, concert_id: str) -> dict | None:
    ext_rel = ctx["extract_relation_ids_any"]
    rows = ctx["query_all"](ctx["CONCERT_DB_HARMONIA_CONCERT"], None)
    cid_norm = _norm_id(concert_id)
    for r in rows:
        rel = ext_rel(r, ["演奏会", "FK演奏会", "concert"])
        rel_norm = {_norm_id(x) for x in rel if x}
        if cid_norm and cid_norm in rel_norm:
            return r
    return None


def _set_harmonia_checkbox(ctx: dict, concert_id: str, key_candidates: list[str], checked: bool, concert_name: str = "") -> bool:
    row = _harmonia_row(ctx, concert_id)
    db_id = ctx["CONCERT_DB_HARMONIA_CONCERT"]
    t = ctx["get_prop_types"](db_id) or {}
    if not row:
        props: dict = {}
        ctx["put_key_any"](props, t, ["harmonia_key", "タイトル"], concert_id, concert_name or concert_id, prefix="harmonia")
        ctx["put_prop_any"](props, t, ["演奏会", "FK演奏会", "concert"], concert_id)
        res = ctx["api_request"](
            "post",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": db_id}, "properties": props},
        )
        if not (res and res.status_code == 200):
            return False
        row = res.json() or {}
    key = ctx["find_prop_name"](t, key_candidates)
    if not key:
        return False
    res = ctx["api_request"](
        "patch",
        f"https://api.notion.com/v1/pages/{row.get('id','')}",
        json={"properties": {key: {"checkbox": bool(checked)}}},
    )
    return bool(res and res.status_code == 200)


def _ensure_harmonia_row_id(ctx: dict, concert_id: str, concert_name: str = "") -> str:
    row = _harmonia_row(ctx, concert_id)
    if row and row.get("id"):
        return row.get("id", "")
    _set_harmonia_checkbox(ctx, concert_id, HARMONIA_CONCERT_PLAN_KEYS, False, concert_name or concert_id)
    row = _harmonia_row(ctx, concert_id)
    return (row or {}).get("id", "")


def _now_iso_local() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _norm_id(v: str) -> str:
    return (v or "").replace("-", "")


def _poke_type_label(poke_type: str) -> str:
    m = {
        "att_unanswered": "出欠未回答",
        "att_maybe": "直近練習△回答",
        "own_missing": "所有楽器未入力",
        "pref_missing": "パート希望未入力",
    }
    return m.get((poke_type or "").strip(), (poke_type or "").strip() or "催促")


def _poke_default_message(poke_type: str, target_name: str, practice_name: str = "") -> str:
    label = _poke_type_label(poke_type)
    if poke_type == "att_maybe" and practice_name:
        return f"{target_name}さんへ：{practice_name} の出欠（△）について確定回答をお願いします。"
    return f"{target_name}さんへ：{label}の対応をお願いします。"


def _parse_dt_safe(v: str) -> datetime | None:
    txt = (v or "").strip()
    if not txt:
        return None
    try:
        return datetime.fromisoformat(txt.replace("Z", ""))
    except Exception:
        return None


def _find_cast_row_for_player(ctx: dict, player_id: str, concert_id: str, participant_rows: list[dict]) -> dict | None:
    ext_rel = ctx["extract_relation_ids_any"]
    for row in participant_rows or []:
        if player_id in ext_rel(row, PARTICIPANT_PLAYER_REL_KEYS) and concert_id in ext_rel(row, PARTICIPANT_CONCERT_REL_KEYS):
            return row
    return None


def _is_target_in_scope(role: int, my_part_id: str, target_cast: dict, ext_rel_fn) -> bool:
    if role >= ROLE_MANAGER:
        return True
    if role >= ROLE_LEADER:
        pm_ids = ext_rel_fn(target_cast, PARTICIPANT_PART_REL_KEYS)
        return bool(pm_ids and pm_ids[0] == my_part_id)
    return False


def _poke_rows_for_concert(ctx: dict, harmonia_row_id: str) -> list[dict]:
    db_id = (ctx.get("CONCERT_DB_POKE_REQUESTS", "") or "").strip()
    if not db_id:
        return []
    rows = ctx["query_all"](db_id, None)
    ext_rel = ctx["extract_relation_ids_any"]
    hid = _norm_id(harmonia_row_id)
    return [
        r for r in rows
        if any(_norm_id(x) == hid for x in ext_rel(r, POKE_CONCERT_REL_KEYS))
    ]


def _poke_mark_status(ctx: dict, row_id: str, status: str) -> bool:
    db_id = (ctx.get("CONCERT_DB_POKE_REQUESTS", "") or "").strip()
    if not db_id:
        return False
    t = ctx["get_prop_types"](db_id) or {}
    props: dict = {}
    ctx["put_prop_any"](props, t, POKE_STATUS_KEYS, status)
    if status in {"read", "done", "cancelled", "expired"}:
        # 任意拡張フィールドがある場合のみ書く
        date_key_candidates = {
            "read": ["read_at"],
            "done": ["done_at"],
            "cancelled": ["cancelled_at"],
            "expired": ["expired_at"],
        }.get(status, [])
        if date_key_candidates:
            key = ctx["find_prop_name"](t, date_key_candidates)
            if key:
                props[key] = {"date": {"start": _now_iso_local()}}
    if not props:
        return False
    res = ctx["api_request"](
        "patch",
        f"https://api.notion.com/v1/pages/{row_id}",
        json={"properties": props},
    )
    return bool(res and res.status_code == 200)


def _expire_pokes_if_needed(ctx: dict, rows: list[dict]) -> None:
    ext_txt = ctx["extract_prop_text_any"]
    now = datetime.now()
    for r in rows:
        status = (ext_txt(r, POKE_STATUS_KEYS) or "").strip().lower()
        if status not in {"sent", "read"}:
            continue
        expires = _parse_dt_safe(ext_txt(r, POKE_EXPIRES_AT_KEYS) or "")
        if expires and expires < now:
            _poke_mark_status(ctx, r.get("id", ""), "expired")


def _create_poke_request(
    ctx: dict,
    *,
    sender_cast_id: str,
    target_cast_id: str,
    harmonia_row_id: str,
    poke_type: str,
    message: str,
    practice_id: str = "",
    expires_at_iso: str = "",
) -> tuple[bool, str]:
    db_id = (ctx.get("CONCERT_DB_POKE_REQUESTS", "") or "").strip()
    if not db_id:
        return False, "CONCERT_DB_POKE_REQUESTS が未設定です。"
    t = ctx["get_prop_types"](db_id) or {}
    if not t:
        return False, "Poke DB のプロパティ取得に失敗しました。"

    # 重複抑止: 同一 sender/target/type/practice で open(sent/read) があれば作らない
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    existing = _poke_rows_for_concert(ctx, harmonia_row_id)
    _expire_pokes_if_needed(ctx, existing)
    for row in existing:
        status = (ext_txt(row, POKE_STATUS_KEYS) or "").strip().lower()
        if status not in {"sent", "read"}:
            continue
        sids = ext_rel(row, POKE_SENDER_CAST_REL_KEYS)
        tids = ext_rel(row, POKE_TARGET_CAST_REL_KEYS)
        pids = ext_rel(row, POKE_PRACTICE_REL_KEYS)
        typ = (ext_txt(row, POKE_TYPE_KEYS) or "").strip()
        if sender_cast_id in sids and target_cast_id in tids and typ == poke_type:
            if (practice_id and practice_id in pids) or (not practice_id and not pids):
                return True, "既に同内容のPokeが送信済みです。"

    props: dict = {}
    ctx["put_key_any"](props, t, POKE_KEY_KEYS, sender_cast_id, target_cast_id, poke_type, prefix="poke")
    ctx["put_prop_any"](props, t, POKE_SENDER_CAST_REL_KEYS, sender_cast_id)
    ctx["put_prop_any"](props, t, POKE_TARGET_CAST_REL_KEYS, target_cast_id)
    ctx["put_prop_any"](props, t, POKE_CONCERT_REL_KEYS, harmonia_row_id)
    if practice_id:
        ctx["put_prop_any"](props, t, POKE_PRACTICE_REL_KEYS, practice_id)
    ctx["put_prop_any"](props, t, POKE_TYPE_KEYS, poke_type)
    ctx["put_prop_any"](props, t, POKE_MESSAGE_KEYS, message)
    ctx["put_prop_any"](props, t, POKE_STATUS_KEYS, "sent")
    if expires_at_iso:
        ctx["put_prop_any"](props, t, POKE_EXPIRES_AT_KEYS, expires_at_iso)

    res = ctx["api_request"](
        "post",
        "https://api.notion.com/v1/pages",
        json={"parent": {"database_id": db_id}, "properties": props},
    )
    if not (res and res.status_code == 200):
        return False, f"Poke作成に失敗しました (status={getattr(res,'status_code','?')})"
    return True, "Pokeを送信しました。"


def _poke_action_href(poke_type: str) -> str:
    t = (poke_type or "").strip()
    if t in {"att_unanswered", "att_maybe"}:
        return "/form/att"
    if t == "pref_missing":
        return "/form/pref"
    if t == "own_missing":
        return "/form/own"
    return "/form"


def _poke_status_label(status: str) -> str:
    m = {
        "sent": "送信済",
        "read": "既読",
        "done": "完了",
        "cancelled": "取消",
        "expired": "期限切れ",
    }
    return m.get((status or "").strip().lower(), (status or "").strip() or "-")


def _assign_resp_status_label(status: str) -> str:
    s = (status or "").strip().lower()
    if s == "agree":
        return "賛同"
    if s == "object":
        return "異議"
    return "未回答"


def _pick_prop_key_exact_first(type_map: dict, candidates: list[str]) -> str:
    keys = list((type_map or {}).keys())
    lower_map = {k.lower(): k for k in keys}
    for c in candidates:
        k = lower_map.get((c or "").lower())
        if k:
            return k
    for c in candidates:
        cl = (c or "").lower()
        for k in keys:
            kl = k.lower()
            if cl == kl or cl in kl:
                return k
    return ""


def _assign_response_rows_for_concert(ctx: dict, harmonia_row_id: str) -> list[dict]:
    db_id = (ctx.get("CONCERT_DB_ASSIGN_RESPONSE", "") or "").strip()
    if not db_id:
        return []
    rows = ctx["query_all"](db_id, None)
    ext_rel = ctx["extract_relation_ids_any"]
    hid = _norm_id(harmonia_row_id)
    return [
        r for r in rows
        if any(_norm_id(x) == hid for x in ext_rel(r, ASSIGN_RESP_CONCERT_REL_KEYS))
    ]


def _clear_assign_responses_for_concert(ctx: dict, concert_id: str) -> None:
    concert = _find_concert(ctx, concert_id) or {}
    harmonia_row_id = _ensure_harmonia_row_id(ctx, concert_id, _atlas_concert_name(ctx, concert))
    if not harmonia_row_id:
        return
    rows = _assign_response_rows_for_concert(ctx, harmonia_row_id)
    for r in rows:
        rid = r.get("id", "")
        if not rid:
            continue
        ctx["api_request"](
            "patch",
            f"https://api.notion.com/v1/pages/{rid}",
            json={"archived": True},
        )


def _upsert_assign_response(
    ctx: dict,
    *,
    concert_id: str,
    cast_id: str,
    status: str,
    plan_label: str = "",
    comment: str = "",
) -> tuple[bool, str]:
    db_id = (ctx.get("CONCERT_DB_ASSIGN_RESPONSE", "") or "").strip()
    if not db_id:
        return False, "CONCERT_DB_ASSIGN_RESPONSE が未設定です。"
    t = ctx["get_prop_types"](db_id) or {}
    if not t:
        return False, "ASSIGN_RESPONSE のプロパティ情報を取得できません。"
    concert = _find_concert(ctx, concert_id) or {}
    harmonia_row_id = _ensure_harmonia_row_id(ctx, concert_id, _atlas_concert_name(ctx, concert))
    if not harmonia_row_id:
        return False, "HARMONIA_CONCERTの行を解決できません。"

    ext_rel = ctx["extract_relation_ids_any"]
    rows = _assign_response_rows_for_concert(ctx, harmonia_row_id)
    target = None
    for r in rows:
        cast_ids = ext_rel(r, ASSIGN_RESP_CAST_REL_KEYS)
        if any(_norm_id(x) == _norm_id(cast_id) for x in cast_ids):
            target = r
            break

    props: dict = {}
    concert_key = _pick_prop_key_exact_first(t, ASSIGN_RESP_CONCERT_REL_KEYS)
    cast_key = _pick_prop_key_exact_first(t, ASSIGN_RESP_CAST_REL_KEYS)
    status_key = _pick_prop_key_exact_first(t, ASSIGN_RESP_STATUS_KEYS)
    if concert_key:
        props[concert_key] = {"relation": [{"id": str(harmonia_row_id)}]}
    if cast_key:
        props[cast_key] = {"relation": [{"id": str(cast_id)}]}
    if status_key:
        ptype = ((t.get(status_key) or {}).get("type") or "").strip().lower()
        if ptype == "select":
            props[status_key] = {"select": {"name": str(status)}}
        elif ptype == "rich_text":
            props[status_key] = {"rich_text": [{"text": {"content": str(status)}}]}
        else:
            props[status_key] = {"rich_text": [{"text": {"content": str(status)}}]}
    if plan_label:
        plan_key = ctx["find_prop_name"](t, ASSIGN_RESP_PLAN_KEYS)
        if plan_key:
            ptype = ((t.get(plan_key) or {}).get("type") or "").strip().lower()
            if ptype == "number":
                n = None
                txt = (plan_label or "").strip()
                if txt:
                    m = re.search(r"([A-D])", txt.upper())
                    if m:
                        n = ord(m.group(1)) - ord("A") + 1
                    else:
                        try:
                            n = float(txt)
                        except Exception:
                            n = None
                if n is not None:
                    props[plan_key] = {"number": n}
            else:
                ctx["put_prop_any"](props, t, ASSIGN_RESP_PLAN_KEYS, plan_label)
    if comment:
        ctx["put_prop_any"](props, t, ASSIGN_RESP_COMMENT_KEYS, comment)
    key_key = _pick_prop_key_exact_first(t, ASSIGN_RESP_KEY_KEYS)
    if key_key:
        ctx["put_key_any"](props, t, [key_key], concert_id, cast_id, status, prefix="assignresp")

    if target and target.get("id"):
        res = ctx["api_request"](
            "patch",
            f"https://api.notion.com/v1/pages/{target.get('id','')}",
            json={"properties": props},
        )
        return (bool(res and res.status_code == 200), "updated")

    res = ctx["api_request"](
        "post",
        "https://api.notion.com/v1/pages",
        json={"parent": {"database_id": db_id}, "properties": props},
    )
    return (bool(res and res.status_code == 200), "created")


def _build_assign_response_panel_data(
    ctx: dict,
    *,
    concert_id: str,
    participant_rows: list[dict],
    my_cast_id: str,
    role: int,
) -> dict:
    out = {
        "enabled": False,
        "my_status": "",
        "my_status_label": "未回答",
        "rows": [],
        "agree_count": 0,
        "object_count": 0,
        "unanswered_count": 0,
    }
    if not my_cast_id:
        return out
    concert = _find_concert(ctx, concert_id) or {}
    harmonia_row_id = _ensure_harmonia_row_id(ctx, concert_id, _atlas_concert_name(ctx, concert))
    if not harmonia_row_id:
        return out

    rows = _assign_response_rows_for_concert(ctx, harmonia_row_id)
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_map = {p.get("id", ""): p for p in players}
    cast_map = {r.get("id", ""): r for r in participant_rows}

    status_by_cast: dict[str, tuple[str, str]] = {}
    for r in rows:
        cast_ids = ext_rel(r, ASSIGN_RESP_CAST_REL_KEYS)
        cast_id = cast_ids[0] if cast_ids else ""
        if not cast_id:
            continue
        status = (ext_txt(r, ASSIGN_RESP_STATUS_KEYS) or "").strip().lower()
        updated_at = (r.get("last_edited_time", "") or r.get("created_time", "") or "").strip()
        prev = status_by_cast.get(cast_id)
        if (not prev) or (updated_at >= prev[1]):
            status_by_cast[cast_id] = (status, updated_at)

    my_status = (status_by_cast.get(my_cast_id, ("", ""))[0] or "").strip().lower()
    out["enabled"] = True
    out["my_status"] = my_status
    out["my_status_label"] = _assign_resp_status_label(my_status)

    if role >= ROLE_LEADER:
        rows_view: list[dict] = []
        for cast in participant_rows or []:
            cast_id = cast.get("id", "")
            if not cast_id:
                continue
            pids = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
            pid = pids[0] if pids else ""
            name = (ext_txt(player_map.get(pid, {}), PLAYER_NAME_KEYS) or "不明").strip()
            status = (status_by_cast.get(cast_id, ("", ""))[0] or "").strip().lower()
            rows_view.append({
                "cast_id": cast_id,
                "name": name,
                "status": status,
                "status_label": _assign_resp_status_label(status),
            })
        rows_view.sort(key=lambda x: ((0 if x["status"] == "object" else 1 if x["status"] == "agree" else 2), x["name"]))
        out["rows"] = rows_view
        out["agree_count"] = sum(1 for r in rows_view if r["status"] == "agree")
        out["object_count"] = sum(1 for r in rows_view if r["status"] == "object")
        out["unanswered_count"] = sum(1 for r in rows_view if r["status"] not in {"agree", "object"})
    return out


def _build_poke_panel_data(
    ctx: dict,
    *,
    concert_id: str,
    participant_rows: list[dict],
    practices: list[dict],
    my_cast_id: str,
) -> dict[str, list[dict] | int]:
    if not my_cast_id:
        return {"inbox": [], "sent": [], "inbox_count": 0}
    concert = _find_concert(ctx, concert_id) or {}
    harmonia_row_id = _ensure_harmonia_row_id(ctx, concert_id, _atlas_concert_name(ctx, concert))
    if not harmonia_row_id:
        return {"inbox": [], "sent": [], "inbox_count": 0}

    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    _expire_pokes_if_needed(ctx, rows)
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)

    ext_txt = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_map = {p.get("id", ""): p for p in players}
    cast_map = {r.get("id", ""): r for r in participant_rows}
    practice_map = {p.get("id", ""): p for p in (practices or [])}

    inbox: list[dict] = []
    sent: list[dict] = []
    for r in rows:
        row_id = r.get("id", "")
        if not row_id:
            continue
        status = (ext_txt(r, POKE_STATUS_KEYS) or "").strip().lower()
        sender_ids = ext_rel(r, POKE_SENDER_CAST_REL_KEYS)
        target_ids = ext_rel(r, POKE_TARGET_CAST_REL_KEYS)
        sender_cast_id = sender_ids[0] if sender_ids else ""
        target_cast_id = target_ids[0] if target_ids else ""
        sender_cast = cast_map.get(sender_cast_id, {})
        target_cast = cast_map.get(target_cast_id, {})
        sender_player_ids = ext_rel(sender_cast, PARTICIPANT_PLAYER_REL_KEYS)
        target_player_ids = ext_rel(target_cast, PARTICIPANT_PLAYER_REL_KEYS)
        sender_name = (ext_txt(player_map.get(sender_player_ids[0], {}), PLAYER_NAME_KEYS) or "不明").strip() if sender_player_ids else "不明"
        target_name = (ext_txt(player_map.get(target_player_ids[0], {}), PLAYER_NAME_KEYS) or "不明").strip() if target_player_ids else "不明"
        practice_ids = ext_rel(r, POKE_PRACTICE_REL_KEYS)
        practice_name = ""
        if practice_ids:
            pr = practice_map.get(practice_ids[0], {})
            practice_name = (ext_txt(pr, PRACTICE_NAME_KEYS) or "").strip()
        poke_type = (ext_txt(r, POKE_TYPE_KEYS) or "").strip()
        item = {
            "id": row_id,
            "status": status,
            "status_label": _poke_status_label(status),
            "poke_type": poke_type,
            "label": _poke_type_label(poke_type),
            "message": (ext_txt(r, POKE_MESSAGE_KEYS) or "").strip(),
            "practice_id": practice_ids[0] if practice_ids else "",
            "practice_name": practice_name,
            "sender_cast_id": sender_cast_id,
            "sender_name": sender_name,
            "target_cast_id": target_cast_id,
            "target_name": target_name,
            "created_at": (r.get("created_time", "") or "").strip(),
            "expires_at": (ext_txt(r, POKE_EXPIRES_AT_KEYS) or "").strip(),
            "action_href": _poke_action_href(poke_type),
        }
        if target_cast_id == my_cast_id and status in {"sent", "read"}:
            inbox.append(item)
        if sender_cast_id == my_cast_id:
            sent.append(item)

    inbox.sort(key=lambda x: (x.get("expires_at", ""), x.get("created_at", ""), x.get("id", "")))
    sent.sort(key=lambda x: (x.get("created_at", ""), x.get("id", "")), reverse=True)
    return {
        "inbox": inbox,
        "sent": sent[:30],
        "inbox_count": len(inbox),
    }


def _auto_complete_pokes_for_target(
    ctx: dict,
    *,
    concert_id: str,
    target_cast_id: str,
    poke_type: str,
    practice_id: str = "",
    require_no_practice_link: bool = False,
) -> int:
    if not target_cast_id:
        return 0
    concert = _find_concert(ctx, concert_id) or {}
    harmonia_row_id = _ensure_harmonia_row_id(ctx, concert_id, _atlas_concert_name(ctx, concert))
    if not harmonia_row_id:
        return 0
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    _expire_pokes_if_needed(ctx, rows)
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    ext_txt = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    done_count = 0
    for r in rows:
        status = (ext_txt(r, POKE_STATUS_KEYS) or "").strip().lower()
        if status not in {"sent", "read"}:
            continue
        if target_cast_id not in ext_rel(r, POKE_TARGET_CAST_REL_KEYS):
            continue
        typ = (ext_txt(r, POKE_TYPE_KEYS) or "").strip()
        if typ != (poke_type or "").strip():
            continue
        pids = ext_rel(r, POKE_PRACTICE_REL_KEYS)
        if practice_id:
            if practice_id not in pids:
                continue
        elif require_no_practice_link and pids:
            continue
        if _poke_mark_status(ctx, r.get("id", ""), "done"):
            done_count += 1
    return done_count


def _priority_to_int(v: str) -> int | None:
    m = {
        "第1希望": 1,
        "第2希望": 2,
        "第3希望": 3,
        "希望なし/降り番でも可": 0,
        "降り番希望": 0,
        "NG": -1,
        "絶対NG": -1,
    }
    return m.get((v or "").strip())


def _build_exact_solver_results(
    ctx: dict,
    concert_id: str,
    selected_song_id: str,
    selected_part_master_id: str,
    role: int,
    my_part_id: str,
    data: dict,
) -> tuple[list[dict], str]:
    try:
        from concert.services.assign_solver import Pref, Requirement, solve_exact
    except Exception as e:
        return [], f"assign_solverの読み込みに失敗しました: {e}"

    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    songs = data.get("songs", []) or []
    partdefs = data.get("partdefs", []) or []
    participant_rows = data.get("participant_rows_concert", []) or []
    preference_rows = data.get("preference_rows", []) or []

    scoped_part_master_id = selected_part_master_id if role >= ROLE_MANAGER else (my_part_id or "")
    song_ids = {s.get("id", "") for s in songs if s.get("id", "")}
    partdef_rows = []
    for pd in partdefs:
        pdid = pd.get("id", "")
        if not pdid:
            continue
        sids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        pmids = ext_rel(pd, PARTDEF_PART_REL_KEYS)
        sid = sids[0] if sids else ""
        pmid = pmids[0] if pmids else ""
        if selected_song_id and sid != selected_song_id:
            continue
        if scoped_part_master_id and pmid != scoped_part_master_id:
            continue
        if sid and sid not in song_ids:
            continue
        partdef_rows.append(pd)
    if not partdef_rows:
        return [], "対象範囲のパート定義が見つかりません。"

    cast_to_player: dict[str, str] = {}
    scope_player_ids: set[str] = set()
    for cast in participant_rows:
        pids = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
        pmids = ext_rel(cast, PARTICIPANT_PART_REL_KEYS)
        if not pids:
            continue
        pid = pids[0]
        pmid = pmids[0] if pmids else ""
        if scoped_part_master_id and pmid != scoped_part_master_id:
            continue
        cast_id = cast.get("id", "")
        if cast_id:
            cast_to_player[cast_id] = pid
        scope_player_ids.add(pid)
    if not scope_player_ids:
        return [], "対象範囲の奏者が見つかりません。"

    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_name_map = {r.get("id", ""): (ext(r, ["楽器名", "タイトル", "PK楽器名"]) or "") for r in inst_rows}
    song_name_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "未設定") for s in songs}
    part_name_map = {
        pd.get("id", ""): (ext(pd, PARTDEF_DISPLAY_NAME_KEYS) or ext(pd, PARTDEF_NAME_KEYS) or "-")
        for pd in partdef_rows
    }

    requirements: list[Requirement] = []
    for pd in partdef_rows:
        pdid = pd.get("id", "")
        sids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        iids = ext_rel(pd, PARTDEF_INST_REL_KEYS)
        sid = sids[0] if sids else ""
        iid = iids[0] if iids else ""
        qty_txt = ext(pd, ["必要台数", "必要人数", "台数", "人数"]) or ""
        try:
            qty = max(int(float(qty_txt)), 1) if qty_txt else 1
        except Exception:
            qty = 1
        requirements.append(
            Requirement(
                song_id=sid,
                song_name=song_name_map.get(sid, "未設定"),
                part_id=pdid,
                part_name=part_name_map.get(pdid, "-"),
                instrument_id=iid,
                instrument_name=inst_name_map.get(iid, ""),
                required_count=qty,
            )
        )

    player_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {p.get("id", ""): (ext(p, PLAYER_NAME_KEYS) or "") for p in player_rows}

    pref_map_keys: set[tuple[str, str, str]] = set()
    prefs: list[Pref] = []
    partdef_id_set = {pd.get("id", "") for pd in partdef_rows}
    for r in preference_rows:
        rel_players = ext_rel(r, PREF_PLAYER_REL_KEYS)
        rel_parts = ext_rel(r, PREF_PART_REL_KEYS)
        if not rel_players or not rel_parts:
            continue
        raw_pid = rel_players[0]
        pdid = rel_parts[0]
        if pdid not in partdef_id_set:
            continue
        pid = cast_to_player.get(raw_pid, raw_pid)
        if pid not in scope_player_ids:
            continue
        prio_txt = ext(r, PREF_PRIORITY_KEYS) or ""
        prio = _priority_to_int(prio_txt)
        if prio is None:
            continue
        sid = next((req.song_id for req in requirements if req.part_id == pdid), "")
        iid = next((req.instrument_id for req in requirements if req.part_id == pdid), "")
        key = (pid, sid, pdid)
        pref_map_keys.add(key)
        prefs.append(
            Pref(
                player_id=pid,
                player_name=player_name_map.get(pid, pid),
                song_id=sid,
                song_name=song_name_map.get(sid, "未設定"),
                part_id=pdid,
                part_name=part_name_map.get(pdid, "-"),
                instrument_id=iid,
                instrument_name=inst_name_map.get(iid, ""),
                priority=prio,
                can_bring=False,
            )
        )

    for req in requirements:
        for pid in sorted(scope_player_ids):
            key = (pid, req.song_id, req.part_id)
            if key in pref_map_keys:
                continue
            prefs.append(
                Pref(
                    player_id=pid,
                    player_name=player_name_map.get(pid, pid),
                    song_id=req.song_id,
                    song_name=req.song_name,
                    part_id=req.part_id,
                    part_name=req.part_name,
                    instrument_id=req.instrument_id,
                    instrument_name=req.instrument_name,
                    priority=0,
                    can_bring=False,
                )
            )

    if not prefs or not requirements:
        return [], "希望データまたはパート定義が不足しています。"
    results = solve_exact(
        prefs=prefs,
        requirements=requirements,
        absent_players=set(),
        all_player_ids=sorted(scope_player_ids),
        time_limit_sec=30.0,
    )
    if not results:
        return [], "厳密解を算出できませんでした。制約条件を見直してください。"
    return results, ""


def _write_assignment_rows(ctx: dict, concert_id: str, assignments: list[dict]) -> tuple[int, int]:
    db_id = ctx.get("CONCERT_DB_CONCERT_ASSIGNMENT", "")
    if not db_id:
        return 0, len(assignments)
    type_map = ctx["get_prop_types"](db_id)
    if not type_map:
        return 0, len(assignments)

    existing = ctx["query_all"](db_id, None)
    ext_rel = ctx["extract_relation_ids_any"]
    for r in existing:
        c_ids = ext_rel(r, ASSIGNMENT_CONCERT_REL_KEYS)
        if concert_id not in (c_ids or []):
            continue
        ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{r.get('id','')}", json={"archived": True})

    ok = 0
    fail = 0
    for a in assignments:
        player_id = a.get("player_id", "")
        song_id = a.get("song_id", "")
        part_id = a.get("part_id", "")
        if not (player_id and song_id and part_id):
            fail += 1
            continue
        props: dict = {}
        ctx["put_key_any"](props, type_map, ["assign_key", "レコード名", "タイトル"], concert_id, player_id, part_id, prefix="assign")
        ctx["put_prop_any"](props, type_map, ASSIGNMENT_CONCERT_REL_KEYS, concert_id)
        ctx["put_prop_any"](props, type_map, ASSIGNMENT_PLAYER_REL_KEYS, player_id)
        ctx["put_prop_any"](props, type_map, ASSIGNMENT_PARTDEF_REL_KEYS, part_id)
        ctx["put_prop_any"](props, type_map, ASSIGNMENT_SONG_REL_KEYS, song_id)
        ctx["put_prop_any"](props, type_map, ASSIGNMENT_FLAG_KEYS, True)
        note = f"{a.get('song_name','')} / {a.get('part_name','')} / {a.get('player_name','')}"
        ctx["put_prop_any"](props, type_map, ["備考", "note", "メモ"], note)
        res = ctx["api_request"](
            "post",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": db_id}, "properties": props},
        )
        if res and res.status_code == 200:
            ok += 1
        else:
            fail += 1
    return ok, fail


def _solver_cache_get(player_id: str, concert_id: str) -> dict:
    return _ASSIGN_SOLVER_CACHE.get((player_id or "", concert_id or ""), {}) or {}


def _solver_cache_set(player_id: str, concert_id: str, payload: dict) -> None:
    _ASSIGN_SOLVER_CACHE[(player_id or "", concert_id or "")] = payload or {}


def _all_relation_ids_from_row(row: dict) -> dict[str, list[str]]:
    """row.properties から relation型プロパティを全取得する。"""
    out: dict[str, list[str]] = {}
    props = (row or {}).get("properties", {}) or {}
    for key, val in props.items():
        if (val or {}).get("type") != "relation":
            continue
        rel = (val or {}).get("relation", []) or []
        ids = [x.get("id", "") for x in rel if x.get("id")]
        out[key] = ids
    return out


def _norm_id(v: str) -> str:
    return (v or "").replace("-", "").strip().lower()


def _has_norm_intersection(a: set[str], b: set[str]) -> bool:
    if not a or not b:
        return False
    na = {_norm_id(x) for x in a if x}
    nb = {_norm_id(x) for x in b if x}
    return bool(na.intersection(nb))


def _resolve_cast_id_robust(ctx: dict, player_id: str, concert_id: str, participant_rows: list[dict]) -> str:
    """
    CONCERT_CAST ID をできるだけ頑健に解決する。
    1) 既知キーで判定
    2) relation全探索で player_id と concert_id を同時に含む行を探す
    """
    ext_rel = ctx["extract_relation_ids_any"]
    n_player = _norm_id(player_id)
    n_concert = _norm_id(concert_id)
    # 正式プロパティを最優先
    for r in participant_rows:
        pids = ext_rel(r, ["出演者"])
        cids = ext_rel(r, ["出演"])
        if n_player in {_norm_id(x) for x in pids} and n_concert in {_norm_id(x) for x in cids}:
            return r.get("id", "")
    for r in participant_rows:
        pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        if n_player in {_norm_id(x) for x in pids} and n_concert in {_norm_id(x) for x in cids}:
            return r.get("id", "")
    for r in participant_rows:
        rel_map = _all_relation_ids_from_row(r)
        has_player = any(n_player in {_norm_id(x) for x in ids} for ids in rel_map.values())
        has_concert = any(n_concert in {_norm_id(x) for x in ids} for ids in rel_map.values())
        if has_player and has_concert:
            return r.get("id", "")
    return ""


def _attendance_map_robust(
    ctx: dict,
    player_id: str,
    concert_id: str,
    participant_rows: list[dict],
    attendance_rows: list[dict],
    practices: list[dict],
) -> tuple[str, dict[str, dict]]:
    """
    attendance の relationキー名ゆれを吸収して練習IDごとの出欠を返す。
    """
    ext = ctx["extract_prop_text_any"]
    cast_id = _resolve_cast_id_robust(ctx, player_id, concert_id, participant_rows)
    target_ids = {player_id}
    if cast_id:
        target_ids.add(cast_id)
    target_norm = {_norm_id(x) for x in target_ids if x}
    practice_ids = {p.get("id", "") for p in practices if p.get("id")}
    practice_norm_to_id = {_norm_id(pid): pid for pid in practice_ids}
    practice_norm = set(practice_norm_to_id.keys())
    att_map: dict[str, dict] = {}

    for row in attendance_rows:
        # 正式プロパティを最優先（演奏会参加者 / 練習）
        direct_targets = set(ctx["extract_relation_ids_any"](row, ["演奏会参加者"]))
        direct_practices = set(ctx["extract_relation_ids_any"](row, ["練習"]))
        if _has_norm_intersection(direct_targets, target_ids) and _has_norm_intersection(direct_practices, practice_ids):
            status = (ext(row, ["参加可否", "出欠", "status", "Status"]) or "未回答").strip() or "未回答"
            comment = (ext(row, ["コメント", "備考", "note", "comment"]) or "").strip()
            # practice_ids側の実IDで埋める
            dmap = {_norm_id(pid): pid for pid in practice_ids}
            for pr in direct_practices:
                np = _norm_id(pr)
                if np in dmap:
                    att_map[dmap[np]] = {"status": status, "comment": comment}
            continue

        rel_map = _all_relation_ids_from_row(row)
        row_practice_ids: set[str] = set()
        row_target_hit = False
        if rel_map:
            for ids in rel_map.values():
                nset = {_norm_id(x) for x in ids if x}
                if target_norm.intersection(nset):
                    row_target_hit = True
                for n in nset:
                    if n in practice_norm:
                        row_practice_ids.add(practice_norm_to_id[n])

        # relationが取れない既存データ向け fallback: record_key を使う
        if not row_target_hit or not row_practice_ids:
            rkey = (ext(row, ["record_key", "タイトル", "PK名称"]) or "").strip().lower()
            if rkey:
                if any(n in rkey for n in target_norm):
                    row_target_hit = True
                for n, pid in practice_norm_to_id.items():
                    if n and n in rkey:
                        row_practice_ids.add(pid)

        if not row_target_hit or not row_practice_ids:
            continue
        status = (ext(row, ["参加可否", "出欠", "status", "Status"]) or "未回答").strip() or "未回答"
        comment = (ext(row, ["コメント", "備考", "note", "comment"]) or "").strip()
        for pr_id in row_practice_ids:
            att_map[pr_id] = {"status": status, "comment": comment}
    return cast_id, att_map


def _too_many_invite_failures(request: Request) -> bool:
    now = datetime.now()
    window_start = now - timedelta(minutes=INVITE_WINDOW_MINUTES)
    key = "invite_fail_ts"
    ts = request.session.get(key, [])
    vals = []
    for x in ts:
        try:
            dt = datetime.fromisoformat(x)
            if dt >= window_start:
                vals.append(dt)
        except Exception:
            pass
    request.session[key] = [d.isoformat() for d in vals]
    return len(vals) >= INVITE_MAX_ATTEMPTS


def _record_invite_failure(request: Request) -> None:
    ts = request.session.get("invite_fail_ts", [])
    ts.append(datetime.now().isoformat())
    request.session["invite_fail_ts"] = ts


@router.get("/", response_class=HTMLResponse)
async def entry_page(request: Request, force_entry: bool = Query(default=False)):
    if request.session.get("player_id") and not force_entry:
        return RedirectResponse("/concert/select", status_code=302)
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "entry",
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
    })


@router.post("/entry/mode")
async def entry_mode(request: Request, mode: Annotated[str, Form()]):
    request.session.pop("invite_target", None)
    return RedirectResponse("/login" if mode == "login" else "/invite", status_code=302)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "login_email",
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "back_href": "/?force_entry=1",
    })


@router.post("/login/email")
async def login_email(request: Request, email: Annotated[str, Form()]):
    ctx = get_ctx()
    email = (email or "").strip().lower()
    if not email:
        _flash_set(request, "error", "メールアドレスを入力してください。")
        return RedirectResponse("/login", status_code=302)
    player = _get_player_by_email(ctx, email)
    if not player:
        request.session["reg_email"] = email
        request.session["invite_target"] = "register"
        _flash_set(request, "info", "ユーザー登録がありません。招待コードを入力してください。")
        return RedirectResponse("/invite", status_code=302)
    request.session["auth_email"] = email
    request.session["auth_player_id"] = player.get("id", "")
    has_pw = bool(ctx["extract_prop_text_any"](player, PLAYER_PASSWORD_HASH_KEYS))
    request.session["auth_has_pw"] = has_pw
    if has_pw:
        return RedirectResponse("/login/password", status_code=302)
    # PW未設定は「送信前確認」へ（自動送信しない）
    return RedirectResponse("/login/code/start", status_code=302)


@router.get("/login/code/start", response_class=HTMLResponse)
async def login_code_start_page(request: Request):
    if not request.session.get("auth_email"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "login_code_start",
        "email": request.session.get("auth_email", ""),
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "back_href": "/login",
    })


@router.post("/login/code/send")
async def login_code_send(request: Request):
    ctx = get_ctx()
    email = request.session.get("auth_email", "")
    player_id = request.session.get("auth_player_id", "")
    if not email or not player_id:
        return RedirectResponse("/login", status_code=302)
    player = _get_player_by_id(ctx, player_id)
    if not player:
        _flash_set(request, "error", "ユーザー情報が見つかりません。")
        return RedirectResponse("/login", status_code=302)
    code = generate_code()
    pname = ctx["extract_prop_text_any"](player, PLAYER_NAME_KEYS) or ""
    mail = build_magic_code_email(code, "HARMONIA")
    if not send_text(email, pname, mail["subject"], mail["body"]):
        _flash_set(request, "error", "確認コード送信に失敗しました。")
        return RedirectResponse("/login/code/start", status_code=302)
    request.session["auth_code_hash"] = hash_code(code)
    request.session["auth_code_expiry"] = code_expiry().isoformat()
    request.session["auth_attempts"] = 0
    return RedirectResponse("/login/code", status_code=302)


@router.get("/login/password", response_class=HTMLResponse)
async def login_password_page(request: Request):
    if not request.session.get("auth_player_id"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "login_password",
        "email": request.session.get("auth_email", ""),
        "error": _flash_pop(request, "error"),
        "back_href": "/login",
    })


@router.post("/login/password")
async def login_password(request: Request, password: Annotated[str, Form()]):
    ctx = get_ctx()
    pid = request.session.get("auth_player_id", "")
    player = _get_player_by_id(ctx, pid)
    if not player:
        _flash_set(request, "error", "ユーザー情報が見つかりません。")
        return RedirectResponse("/login", status_code=302)
    if not verify_password(password or "", ctx["extract_prop_text_any"](player, PLAYER_PASSWORD_HASH_KEYS)):
        _flash_set(request, "error", "パスワードが違います。")
        return RedirectResponse("/login/password", status_code=302)
    request.session["player_id"] = pid
    request.session["email"] = request.session.get("auth_email", "")
    request.session["player_name"] = ctx["extract_prop_text_any"](player, PLAYER_NAME_KEYS) or ""
    _clear_keys(request, ["auth_email", "auth_player_id", "auth_has_pw", "auth_code_hash", "auth_code_expiry", "auth_attempts"])
    return RedirectResponse("/concert/select", status_code=302)


@router.get("/login/code", response_class=HTMLResponse)
async def login_code_page(request: Request):
    if not request.session.get("auth_email"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("form/login_code.html", {
        "request": request,
        "email": request.session.get("auth_email", ""),
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "verify_action": "/login/verify",
        "back_href": "/login",
    })


@router.post("/login/verify")
async def login_verify(request: Request, code: Annotated[str, Form()]):
    ctx = get_ctx()
    expiry = request.session.get("auth_code_expiry", "")
    stored = request.session.get("auth_code_hash", "")
    attempts = int(request.session.get("auth_attempts", 0))
    if not expiry or not stored:
        return RedirectResponse("/login", status_code=302)
    if is_code_expired(datetime.fromisoformat(expiry)):
        _flash_set(request, "error", "確認コードの有効期限が切れました。")
        return RedirectResponse("/login", status_code=302)
    if attempts >= CODE_MAX_ATTEMPTS:
        _flash_set(request, "error", "試行回数を超えました。")
        return RedirectResponse("/login", status_code=302)
    if not verify_code(code or "", stored):
        attempts += 1
        request.session["auth_attempts"] = attempts
        _flash_set(request, "error", f"確認コードが違います。あと{CODE_MAX_ATTEMPTS - attempts}回入力できます。")
        return RedirectResponse("/login/code", status_code=302)
    pid = request.session.get("auth_player_id", "")
    player = _get_player_by_id(ctx, pid)
    if not player:
        return RedirectResponse("/login", status_code=302)
    request.session["player_id"] = pid
    request.session["email"] = request.session.get("auth_email", "")
    request.session["player_name"] = ctx["extract_prop_text_any"](player, PLAYER_NAME_KEYS) or ""
    _clear_keys(request, ["auth_email", "auth_player_id", "auth_has_pw", "auth_code_hash", "auth_code_expiry", "auth_attempts"])
    if not (ctx["extract_prop_text_any"](player, PLAYER_PASSWORD_HASH_KEYS) or ""):
        request.session["need_set_password"] = True
        return RedirectResponse("/login/set-password", status_code=302)
    return RedirectResponse("/concert/select", status_code=302)


@router.get("/register/start")
async def register_start(request: Request):
    email = request.session.get("reg_email", "")
    if not email:
        return RedirectResponse("/login", status_code=302)
    code = generate_code()
    mail = build_magic_code_email(code, "HARMONIA")
    if not send_text(email, "", mail["subject"], mail["body"]):
        _flash_set(request, "error", "確認コード送信に失敗しました。")
        return RedirectResponse("/login", status_code=302)
    request.session["reg_code_hash"] = hash_code(code)
    request.session["reg_code_expiry"] = code_expiry().isoformat()
    request.session["reg_attempts"] = 0
    return RedirectResponse("/register/code", status_code=302)


@router.get("/register/code", response_class=HTMLResponse)
async def register_code_page(request: Request):
    if not request.session.get("reg_email"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("form/login_code.html", {
        "request": request,
        "email": request.session.get("reg_email", ""),
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "verify_action": "/register/verify",
        "back_href": "/login",
    })


@router.post("/register/verify")
async def register_verify(request: Request, code: Annotated[str, Form()]):
    expiry = request.session.get("reg_code_expiry", "")
    stored = request.session.get("reg_code_hash", "")
    attempts = int(request.session.get("reg_attempts", 0))
    if not expiry or not stored:
        return RedirectResponse("/login", status_code=302)
    if is_code_expired(datetime.fromisoformat(expiry)):
        _flash_set(request, "error", "確認コードの有効期限が切れました。")
        return RedirectResponse("/login", status_code=302)
    if attempts >= CODE_MAX_ATTEMPTS:
        _flash_set(request, "error", "試行回数を超えました。")
        return RedirectResponse("/login", status_code=302)
    if not verify_code(code or "", stored):
        attempts += 1
        request.session["reg_attempts"] = attempts
        _flash_set(request, "error", f"確認コードが違います。あと{CODE_MAX_ATTEMPTS - attempts}回入力できます。")
        return RedirectResponse("/register/code", status_code=302)
    request.session["reg_verified"] = True
    return RedirectResponse("/register/profile", status_code=302)


@router.get("/register/profile", response_class=HTMLResponse)
async def register_profile_page(request: Request):
    if not request.session.get("reg_verified"):
        return RedirectResponse("/login", status_code=302)
    ctx = get_ctx()
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "register_profile",
        "email": request.session.get("reg_email", ""),
        "part_options": _part_options(ctx),
        "error": _flash_pop(request, "error"),
        "back_href": "/register/code",
    })


@router.post("/register/profile")
async def register_profile(request: Request, name: Annotated[str, Form()], part_id: Annotated[str, Form()]):
    email = request.session.get("reg_email", "")
    if not email:
        return RedirectResponse("/login", status_code=302)
    if not (name or "").strip():
        _flash_set(request, "error", "氏名を入力してください。")
        return RedirectResponse("/register/profile", status_code=302)
    if not (part_id or "").strip():
        _flash_set(request, "error", "担当パートを選択してください。")
        return RedirectResponse("/register/profile", status_code=302)
    ctx = get_ctx()
    t = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
    props: dict = {}
    ctx["put_prop_any"](props, t, PLAYER_NAME_KEYS, name.strip())
    ctx["put_prop_any"](props, t, PLAYER_EMAIL_KEYS, email)
    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                             json={"parent": {"database_id": ctx["CONCERT_DB_PLAYER"]}, "properties": props})
    if not (res and res.status_code == 200):
        _flash_set(request, "error", "新規登録に失敗しました。")
        return RedirectResponse("/register/profile", status_code=302)
    request.session["player_id"] = res.json().get("id", "")
    request.session["player_name"] = name.strip()
    request.session["email"] = email
    request.session["reg_part_id"] = part_id
    request.session["need_set_password"] = True
    _clear_keys(request, ["reg_email", "reg_code_hash", "reg_code_expiry", "reg_attempts", "reg_verified"])
    return RedirectResponse("/invite", status_code=302)


@router.get("/invite", response_class=HTMLResponse)
async def invite_page(request: Request):
    target = request.session.get("invite_target", "login")
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "invite",
        "invite_target": target,
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "back_href": "/login" if target == "register" else "/",
    })


@router.post("/invite/resolve")
async def invite_resolve(request: Request, invite_code: Annotated[str, Form()]):
    ctx = get_ctx()
    target = request.session.get("invite_target", "login")
    if _too_many_invite_failures(request):
        _flash_set(request, "error", f"入力失敗が多いため、{INVITE_WINDOW_MINUTES}分後に再試行してください。")
        return RedirectResponse("/invite", status_code=302)
    cid, err = _resolve_invite_concert(ctx, invite_code)
    if not cid:
        _record_invite_failure(request)
        return RedirectResponse("/invite/error", status_code=302)
    request.session.pop("invite_fail_ts", None)
    request.session["pending_invite_cid"] = cid
    if target == "register" and request.session.get("reg_email"):
        return RedirectResponse("/register/start", status_code=302)
    return RedirectResponse("/login", status_code=302)


@router.get("/invite/error", response_class=HTMLResponse)
async def invite_error_page(request: Request):
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "invite_error",
        "error": "招待コードを再確認するか、管理者に連絡してください。",
        "back_href": "/invite",
    })


@router.get("/login/set-password", response_class=HTMLResponse)
async def set_password_page(request: Request):
    if not request.session.get("player_id"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("form/set_password.html", {
        "request": request,
        "error": _flash_pop(request, "error"),
        "back_href": "/concert/select",
    })


@router.post("/login/set-password")
async def set_password_submit(request: Request, password: Annotated[str, Form()], password_confirm: Annotated[str, Form()]):
    pid = request.session.get("player_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if len((password or "").strip()) < 6:
        _flash_set(request, "error", "パスワードは6文字以上で入力してください。")
        return RedirectResponse("/login/set-password", status_code=302)
    if (password or "") != (password_confirm or ""):
        _flash_set(request, "error", "パスワードが一致しません。")
        return RedirectResponse("/login/set-password", status_code=302)
    ctx = get_ctx()
    t = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
    props: dict = {}
    ctx["put_prop_any"](props, t, PLAYER_PASSWORD_HASH_KEYS, hash_password(password))
    ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{pid}", json={"properties": props})
    request.session["need_set_password"] = False
    return RedirectResponse("/concert/select", status_code=302)


@router.get("/concert/select", response_class=HTMLResponse)
async def concert_select_page(request: Request):
    pid = request.session.get("player_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    ctx = get_ctx()
    pending = request.session.get("pending_invite_cid", "")
    reg_part_id = request.session.get("reg_part_id", "")
    if pending and reg_part_id:
        ok, _ = _link_player_to_concert(ctx, pid, pending, reg_part_id)
        if ok:
            request.session["concert_id"] = pending
            _clear_keys(request, ["pending_invite_cid", "reg_part_id"])
            return RedirectResponse("/form", status_code=302)
    concert_options = _my_concert_options(ctx, pid)
    return templates.TemplateResponse("form/concert_select.html", {
        "request": request,
        "player_name": request.session.get("player_name", ""),
        "concert_options": concert_options,
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "back_href": "/?force_entry=1",
    })


@router.post("/concert/select")
async def concert_select_submit(request: Request, concert_id: Annotated[str, Form()]):
    if not concert_id:
        _flash_set(request, "error", "演奏会を選択してください。")
        return RedirectResponse("/concert/select", status_code=302)
    request.session["concert_id"] = concert_id
    return RedirectResponse("/form", status_code=302)


@router.post("/concert/add-by-invite")
async def concert_add_by_invite(request: Request, invite_code: Annotated[str, Form()]):
    pid = request.session.get("player_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    ctx = get_ctx()
    cid, err = _resolve_invite_concert(ctx, invite_code)
    if not cid:
        _flash_set(request, "error", "管理者に招待コードを確認してください。")
        return RedirectResponse("/concert/select", status_code=302)
    request.session["pending_add_invite_cid"] = cid
    return RedirectResponse("/concert/add-part", status_code=302)


@router.get("/concert/add-part", response_class=HTMLResponse)
async def concert_add_part_page(request: Request):
    pid = request.session.get("player_id", "")
    pending_cid = request.session.get("pending_add_invite_cid", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not pending_cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    return templates.TemplateResponse("form/login.html", {
        "request": request,
        "view": "concert_add_part",
        "part_options": _part_options(ctx),
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "back_href": "/concert/select",
    })


@router.post("/concert/add-part")
async def concert_add_part_submit(request: Request, part_id: Annotated[str, Form()]):
    pid = request.session.get("player_id", "")
    cid = request.session.get("pending_add_invite_cid", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    ok, msg = _link_player_to_concert(ctx, pid, cid, part_id)
    if not ok:
        _flash_set(request, "error", msg)
        return RedirectResponse("/concert/add-part", status_code=302)
    request.session.pop("pending_add_invite_cid", None)
    request.session["concert_id"] = cid
    return RedirectResponse("/form", status_code=302)


@router.get("/form", response_class=HTMLResponse)
async def form_menu(request: Request, tab: str = Query(default="")):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    ext = ctx["extract_prop_text_any"]
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)

    t0 = time.perf_counter()
    if "clear_metrics" in ctx:
        ctx["clear_metrics"]()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", [])
    practices = data.get("practices", [])
    concert_day = data.get("concert_day")
    attendance_rows = data.get("attendance_rows", [])
    preference_rows = data.get("preference_rows", [])
    partdefs = data.get("partdefs", [])
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_map = {p.get("id", ""): p for p in players}

    base_role = resolve_user_role(ctx, pid, cid, participant_rows)
    cast_id, att_map = _attendance_map_robust(ctx, pid, cid, participant_rows, attendance_rows, practices)
    att_total = len(practices)
    att_answered = sum(1 for p in practices if (att_map.get(p.get("id", ""), {}).get("status", "未回答") != "未回答"))
    att_unanswered = max(0, att_total - att_answered)
    att_hint = f"未回答 {att_unanswered}件" if att_unanswered > 0 else (f"{att_answered}/{att_total}回 回答済" if att_total else "入力・変更")

    pref_map = load_existing_prefs(ctx, cid, pid, partdefs, participant_rows, preference_rows)
    pref_total = len(partdefs)
    pref_answered = sum(1 for pd in partdefs if (pref_map.get(pd.get("id", ""), "未回答") != "未回答"))
    my_part_name, my_part_id = _my_part_info(ctx, pid, cid, participant_rows)
    my_music_role = _my_music_role(ctx, pid, cid, participant_rows)
    my_ops_role = _my_ops_role(ctx, pid, cid, participant_rows)
    my_system_role = _my_system_role(ctx, pid, cid, participant_rows)
    is_admin_debug = _is_administrator_role(my_system_role)
    override_raw = request.session.get(DEBUG_ROLE_OVERRIDE_SESSION_KEY, "")
    override_role = _role_from_override(override_raw) if is_admin_debug else None
    role = override_role if override_role is not None else base_role
    if not is_admin_debug and DEBUG_ROLE_OVERRIDE_SESSION_KEY in request.session:
        request.session.pop(DEBUG_ROLE_OVERRIDE_SESSION_KEY, None)
    is_perc_role = False
    if my_part_id:
        pm_info = (data.get("part_master_map", {}) or {}).get(my_part_id, {})
        pm_name = (pm_info.get("name", "") or "").strip()
        pm_type = (pm_info.get("type", "") or "").strip()
        is_perc_role = is_perc(pm_name) or is_perc(pm_type)

    flags = _harmonia_flags(ctx, cid)
    proposal_done = bool(flags.get("plan_done"))
    assign_done = bool(flags.get("assign_done"))
    cover_url = get_cover_url(concert)
    all_assign_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
    published_assign = has_published_assignments(ctx, cid, assignment_rows=all_assign_rows)
    can_show_assign = (role >= ROLE_LEADER) or proposal_done or published_assign
    my_assign_rows = get_my_assign_rows(
        ctx, cid, pid, participant_rows, assignment_rows=all_assign_rows
    ) if can_show_assign else []
    assign_summary = build_assignment_view_rows(ctx, my_assign_rows, data.get("songs", []), data.get("partdefs", []))
    role_assignment_rows = build_role_assignment_rows(
        ctx=ctx,
        concert_id=cid,
        role=role,
        my_part_id=my_part_id,
        partdefs=data.get("partdefs", []),
        songs=data.get("songs", []),
        participant_rows=participant_rows,
        assignment_rows=all_assign_rows,
        player_rows=players,
    ) if role >= ROLE_LEADER else []
    assign_song_options = [
        {"id": s.get("id", ""), "name": (ext(s, SONG_NAME_KEYS) or "未設定")}
        for s in (data.get("songs", []) or [])
        if s.get("id", "")
    ]
    assign_part_options = []
    if role >= ROLE_MANAGER:
        pm_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
        for pm in pm_rows:
            pmid = pm.get("id", "")
            pmname = (ext(pm, PARTMASTER_NAME_KEYS) or "").strip()
            if pmid and pmname:
                assign_part_options.append({"id": pmid, "name": pmname})
        assign_part_options.sort(key=lambda x: x["name"].lower())

    assign_state_all = request.session.get("assign_solver_state") or {}
    assign_state = assign_state_all.get(cid, {}) if isinstance(assign_state_all, dict) else {}
    if isinstance(assign_state, dict) and "results" in assign_state:
        assign_state.pop("results", None)
        assign_state_all[cid] = assign_state
        request.session["assign_solver_state"] = assign_state_all
    solver_cache = _solver_cache_get(pid, cid)
    candidate_results = solver_cache.get("results", []) if isinstance(solver_cache, dict) else []
    selected_idx = 0
    try:
        selected_idx = int(assign_state.get("selected", 0) or 0)
    except Exception:
        selected_idx = 0
    if selected_idx < 0:
        selected_idx = 0
    if selected_idx >= len(candidate_results):
        selected_idx = 0
    selected_candidate = candidate_results[selected_idx] if candidate_results else None
    assign_scope_song_id = (assign_state.get("scope_song_id", "") if isinstance(assign_state, dict) else "") or ""
    assign_scope_part_id = (assign_state.get("scope_part_id", "") if isinstance(assign_state, dict) else "") or ""
    manager_part_options: list[str] = []
    if role >= ROLE_MANAGER:
        ext_rel = ctx["extract_relation_ids_any"]
        pm_map = data.get("part_master_map", {}) or {}
        seen_pm: set[str] = set()
        for cast in participant_rows:
            pm_ids = ext_rel(cast, PARTICIPANT_PART_REL_KEYS)
            pmid = pm_ids[0] if pm_ids else ""
            if not pmid or pmid in seen_pm:
                continue
            pm_name = (pm_map.get(pmid, {}) or {}).get("name", "") or ""
            if pm_name:
                seen_pm.add(pmid)
                manager_part_options.append(pm_name)
        manager_part_options = sorted(set(manager_part_options), key=lambda x: x.lower())
    if pref_total == 0:
        pref_hint = ""
    elif assign_done:
        pref_hint = "アサイン確定済"
    elif proposal_done:
        pref_hint = "アサイン案提示中"
    elif pref_answered == pref_total:
        pref_hint = "入力済み"
    else:
        pref_hint = f"{pref_answered}/{pref_total}パート 回答済"

    # 直近練習（現在以降を優先、なければ最初の練習）
    now_dt = datetime.now()
    def _parse_practice_dt(v: str) -> datetime | None:
        txt = (v or "").strip()
        if not txt:
            return None
        base = txt.replace("Z", "")
        try:
            return datetime.fromisoformat(base[:19])
        except Exception:
            return None
    upcoming_practice = None
    for p in practices:
        dtxt = (ext(p, PRACTICE_DATE_KEYS) or "").strip()
        pd = _parse_practice_dt(dtxt)
        if pd is None:
            continue
        if pd >= now_dt:
            upcoming_practice = p
            break
    if upcoming_practice is None and practices:
        upcoming_practice = practices[0]

    upcoming_schedule_rows: list[dict] = []
    if upcoming_practice:
        up_practice_id = upcoming_practice.get("id", "")
        schedule_db_id = (ctx.get("CONCERT_DB_SCHEDULE", "") or os.environ.get("CONCERT_DB_SCHEDULE", "") or "").strip()
        if up_practice_id and schedule_db_id:
            ext_rel = ctx["extract_relation_ids_any"]
            song_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "").strip() for s in (data.get("songs", []) or [])}
            all_sched = ctx["query_all"](schedule_db_id, None)
            for row in all_sched:
                if up_practice_id not in ext_rel(row, SCHEDULE_PRACTICE_REL_KEYS):
                    continue
                sids = ext_rel(row, SCHEDULE_SONG_REL_KEYS)
                st_raw = (ext(row, SCHEDULE_START_KEYS) or "").strip()
                ed_raw = (ext(row, SCHEDULE_END_KEYS) or "").strip()
                st = _format_hhmm(st_raw)
                ed = _format_hhmm(ed_raw)
                if st and ed:
                    time_label = st if st == ed else f"{st} - {ed}"
                elif st or ed:
                    time_label = st or ed
                else:
                    time_label = "時刻未設定"
                type_name = (ext(row, SCHEDULE_TYPE_KEYS) or "").strip()
                upcoming_schedule_rows.append({
                    "start": st,
                    "end": ed,
                    "time_label": time_label,
                    "type": type_name,
                    "type_class": _schedule_type_class(type_name),
                    "content": (ext(row, SCHEDULE_CONTENT_KEYS) or "").strip(),
                    "song": (song_map.get(sids[0], "") if sids else ""),
                    "order": (ext(row, SCHEDULE_ORDER_KEYS) or "").strip(),
                })
            def _sort_key(r: dict):
                order_v = (r.get("order", "") or "").strip()
                try:
                    o = int(float(order_v))
                except Exception:
                    o = 999999
                st = (r.get("start", "") or "")
                return (o, st)
            upcoming_schedule_rows.sort(key=_sort_key)

    show_pref = role in (ROLE_PLAYER, ROLE_LEADER, ROLE_MANAGER)
    show_own = role in (ROLE_PLAYER, ROLE_LEADER, ROLE_MANAGER) and is_perc_role
    role_mode = role >= ROLE_LEADER
    allowed_tabs = {"att", "member", "assign", "material", "ownmap"}
    initial_role_tab = (tab or "").strip().lower()
    if initial_role_tab not in allowed_tabs:
        initial_role_tab = ""
    if initial_role_tab == "ownmap" and role < ROLE_LEADER:
        initial_role_tab = ""
    upcoming_practice_id = (upcoming_practice or {}).get("id", "") if upcoming_practice else ""
    my_cast_row = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    my_cast_id = (my_cast_row or {}).get("id", "")
    proposal_phase = proposal_done and not assign_done
    assign_response_panel = _build_assign_response_panel_data(
        ctx,
        concert_id=cid,
        participant_rows=participant_rows,
        my_cast_id=my_cast_id,
        role=role,
    )
    poke_panels = _build_poke_panel_data(
        ctx,
        concert_id=cid,
        participant_rows=participant_rows,
        practices=practices,
        my_cast_id=my_cast_id,
    )
    if role >= ROLE_LEADER:
        todo_items = _build_role_todo_items(
            ctx,
            role=role,
            concert_id=cid,
            my_part_id=my_part_id,
            participant_rows=participant_rows,
            part_master_map=data.get("part_master_map", {}) or {},
            player_map=player_map,
            practices=practices,
            attendance_rows=attendance_rows,
            upcoming_practice_id=upcoming_practice_id,
            partdefs=partdefs,
            preference_rows=preference_rows,
        )
        if not proposal_done:
            todo_items.append({
                "title": "アサイン案を提示",
                "desc": "アサインタブで厳密解を生成",
                "href": "/form?tab=assign#role-menu-panels",
                "icon": "bullseye",
            })
        elif not published_assign:
            todo_items.append({
                "title": "アサインを確定",
                "desc": "提示中の案を確定して公開",
                "href": "/form?tab=assign#role-menu-panels",
                "icon": "check2-square",
            })
        if upcoming_practice and not upcoming_schedule_rows:
            todo_items.append({
                "title": "直近練習の進行表を確認",
                "desc": "この練習日のスケジュールが未登録",
                "href": "",
                "icon": "clock-history",
            })
    else:
        todo_items: list[dict[str, str]] = []
        if att_unanswered > 0:
            todo_items.append({
                "title": "出欠入力を完了",
                "desc": f"未回答 {att_unanswered}件",
                "href": "/form/att",
                "icon": "calendar-check",
            })
        if show_pref and pref_total > 0 and pref_answered < pref_total:
            todo_items.append({
                "title": "パート希望を入力",
                "desc": f"{pref_total - pref_answered}件 未入力",
                "href": "/form/pref",
                "icon": "music-note-list",
            })
        if show_own:
            # 所有楽器入力画面へ導線を常時表示（Percussionのみ）
            todo_items.append({
                "title": "所有楽器を確認",
                "desc": "入力・更新",
                "href": "/form/own",
                "icon": "collection",
            })
    song_names = []
    for s in (data.get("songs", []) or []):
        n = (ext(s, SONG_NAME_KEYS) or "").strip()
        if n:
            song_names.append(n)
    # 同名重複を除去しつつ順序は維持
    seen = set()
    song_names = [x for x in song_names if not (x in seen or seen.add(x))]
    own_role_rows: list[dict] = []
    own_role_song_options: list[dict] = []
    if role >= ROLE_LEADER:
        own_role_rows, own_role_song_options = _build_role_own_rows(
            ctx,
            concert_id=cid,
            role=role,
            my_part_id=my_part_id,
            participant_rows=participant_rows,
            partdefs=data.get("partdefs", []) or [],
            songs=data.get("songs", []) or [],
            player_rows=players,
        )
    resp = templates.TemplateResponse("form/menu.html", {
        "request": request,
        "concert": concert,
        "concert_name": _atlas_concert_name(ctx, concert),
        "concert_date": (ext(concert, CONCERT_DATE_KEYS) or "")[:10],
        "concert_venue": ext(concert, CONCERT_VENUE_KEYS) or "",
        "concert_conductor": ext(concert, ["クリエイター", "指揮者", "Conductor"]) or "",
        "concert_songs_line": " / ".join(song_names),
        "player_name": request.session.get("player_name", ""),
        "my_part": my_part_name,
        "my_music_role": my_music_role,
        "my_ops_role": my_ops_role,
        "my_system_role": my_system_role,
        "debug_role_switch_enabled": is_admin_debug,
        "debug_role_current_label": _role_label(role),
        "debug_role_base_label": _role_label(base_role),
        "debug_role_override": (override_raw if override_role is not None else ""),
        "upcoming_practice": {
            "name": (ext(upcoming_practice, PRACTICE_NAME_KEYS) or "").strip(),
            "date": (ext(upcoming_practice, PRACTICE_DATE_KEYS) or "").strip()[:16].replace("T", " "),
            "venue": (ext(upcoming_practice, PRACTICE_VENUE_KEYS) or "").strip(),
        } if upcoming_practice else None,
        "att_unanswered": att_unanswered,
        "att_hint": att_hint,
        "pref_total": pref_total if show_pref else 0,
        "pref_answered": pref_answered if show_pref else 0,
        "pref_hint": pref_hint,
        "proposal_done": proposal_done,
        "assign_done": assign_done,
        "proposal_phase": proposal_phase,
        "is_perc": show_own,
        "cover_url": cover_url,
        "can_show_assign": can_show_assign,
        "assign_summary": assign_summary,
        "assign_panel_title": ("あなたへのアサイン案" if proposal_phase else "あなたのアサイン状況"),
        "assign_response_panel": assign_response_panel,
        "show_role_panel": role_mode,
        "show_material_tab": True,
        "initial_role_tab": initial_role_tab,
        "todo_items": todo_items,
        "poke_inbox_items": poke_panels.get("inbox", []),
        "poke_sent_items": poke_panels.get("sent", []),
        "poke_inbox_count": poke_panels.get("inbox_count", 0),
        "is_manager": role >= ROLE_MANAGER,
        "manager_part_options": manager_part_options,
        "practice_cols": _build_practice_cols(data.get("practices", [])),
        "attendance_table_rows": _build_attendance_table(
            ctx, cid, participant_rows, data.get("practices", []), data.get("attendance_rows", []),
            data.get("part_master_map", {}), my_part_id, role, player_map=player_map
        ),
        "member_table_rows": _build_member_table(
            ctx, participant_rows, data.get("part_master_map", {}), my_part_id, role, player_map=player_map
        ),
        "material_rows": _build_material_rows(ctx, data.get("partdefs", []), data.get("songs", []), my_part_id, role) if role_mode else [],
        "material_links": _build_material_links(ctx, data.get("partdefs", []), data.get("songs", []), my_part_id, role),
        "material_practices": _build_material_practice_items(ctx, data.get("practices", [])),
        "role_assignment_rows": role_assignment_rows,
        "own_role_rows": own_role_rows,
        "own_role_song_options": own_role_song_options,
        "upcoming_schedule_rows": upcoming_schedule_rows,
        "assign_solver_candidates": candidate_results,
        "assign_solver_selected_idx": selected_idx,
        "assign_solver_selected": selected_candidate,
        "assign_song_options": assign_song_options,
        "assign_part_options": assign_part_options,
        "assign_scope_song_id": assign_scope_song_id,
        "assign_scope_part_id": assign_scope_part_id,
        "assign_flags": flags,
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
    })
    if _perf_enabled():
        total_ms = int((time.perf_counter() - t0) * 1000)
        m = ctx["collect_metrics"]() if "collect_metrics" in ctx else []
        top = sorted(m, key=lambda x: x.get("ms", 0), reverse=True)[:8]
        print(f"[perf] form_menu total={total_ms}ms cid={cid[:8]} pid={pid[:8]} calls={len(m)} top={top}")
    return resp


@router.get("/form/material/practice-pdf")
async def form_material_practice_pdf(request: Request, practice_id: str):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)

    data = load_attendance_data(ctx, cid)
    practices = data.get("practices", []) or []
    target = next((p for p in practices if p.get("id", "") == (practice_id or "")), None)
    if not target:
        return Response("practice not found", status_code=404, media_type="text/plain; charset=utf-8")

    try:
        pdf_bytes = _generate_practice_pdf_bytes(
            ctx=ctx,
            practice_id=practice_id,
            concert_name=_atlas_concert_name(ctx, concert),
            practice=target,
            attendance_rows=data.get("attendance_rows", []) or [],
        )
    except Exception as e:
        return Response(f"PDF生成に失敗しました: {e}", status_code=500, media_type="text/plain; charset=utf-8")

    ext = ctx["extract_prop_text_any"]
    pr_name = (ext(target, PRACTICE_NAME_KEYS) or "practice").replace("/", "-")
    filename = f"練習情報_{pr_name}.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@router.post("/form/debug/role")
async def form_debug_role_switch(request: Request, role_mode: Annotated[str, Form()]):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", [])
    my_system_role = _my_system_role(ctx, pid, cid, participant_rows)
    if not _is_administrator_role(my_system_role):
        return RedirectResponse("/form", status_code=302)

    mode = (role_mode or "").strip().lower()
    if mode in {"player", "leader", "manager"}:
        request.session[DEBUG_ROLE_OVERRIDE_SESSION_KEY] = mode
    else:
        request.session.pop(DEBUG_ROLE_OVERRIDE_SESSION_KEY, None)
    return RedirectResponse("/form", status_code=302)


@router.post("/form/poke/send")
async def form_poke_send(
    request: Request,
    target_cast_id: Annotated[str, Form()],
    poke_type: Annotated[str, Form()],
    practice_id: Annotated[str, Form()] = "",
    message: Annotated[str, Form()] = "",
    expires_hours: Annotated[int, Form()] = 72,
):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)

    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", []) or []
    base_role = resolve_user_role(ctx, pid, cid, participant_rows)
    my_system_role = _my_system_role(ctx, pid, cid, participant_rows)
    override_raw = request.session.get(DEBUG_ROLE_OVERRIDE_SESSION_KEY, "")
    override_role = _role_from_override(override_raw) if _is_administrator_role(my_system_role) else None
    role = override_role if override_role is not None else base_role
    if role < ROLE_LEADER:
        _flash_set(request, "error", "PokeはLeader以上のみ実行できます。")
        return RedirectResponse("/form", status_code=302)

    ext_rel = ctx["extract_relation_ids_any"]
    my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    if not my_cast:
        _flash_set(request, "error", "あなたの参加者情報（CONCERT_CAST）が見つかりません。")
        return RedirectResponse("/form", status_code=302)
    my_part_ids = ext_rel(my_cast, PARTICIPANT_PART_REL_KEYS)
    my_part_id = my_part_ids[0] if my_part_ids else ""

    target_cast_id = (target_cast_id or "").strip()
    target_cast = next((r for r in participant_rows if r.get("id", "") == target_cast_id), None)
    if not target_cast:
        _flash_set(request, "error", "対象メンバーが見つかりません。")
        return RedirectResponse("/form", status_code=302)
    if not _is_target_in_scope(role, my_part_id, target_cast, ext_rel):
        _flash_set(request, "error", "対象メンバーに対するPoke権限がありません。")
        return RedirectResponse("/form", status_code=302)

    target_player_ids = ext_rel(target_cast, PARTICIPANT_PLAYER_REL_KEYS)
    target_player_id = target_player_ids[0] if target_player_ids else ""
    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    pmap = {p.get("id", ""): p for p in players}
    target_name = (ctx["extract_prop_text_any"](pmap.get(target_player_id, {}), PLAYER_NAME_KEYS) or "対象者").strip()
    practice_name = ""
    if (practice_id or "").strip():
        practices = data.get("practices", []) or []
        target_pr = next((p for p in practices if p.get("id", "") == practice_id.strip()), None)
        if target_pr:
            practice_name = (ctx["extract_prop_text_any"](target_pr, PRACTICE_NAME_KEYS) or "").strip()

    poke_type = (poke_type or "").strip()
    if not poke_type:
        _flash_set(request, "error", "poke_type は必須です。")
        return RedirectResponse("/form", status_code=302)
    body = (message or "").strip() or _poke_default_message(poke_type, target_name, practice_name)
    expires_hours = max(1, min(int(expires_hours or 72), 24 * 14))
    expires_at_iso = (datetime.now() + timedelta(hours=expires_hours)).replace(microsecond=0).isoformat()

    concert = _find_concert(ctx, cid) or {}
    c_name = _atlas_concert_name(ctx, concert) if concert else cid
    harmonia_row_id = _ensure_harmonia_row_id(ctx, cid, c_name)
    if not harmonia_row_id:
        _flash_set(request, "error", "HARMONIA_CONCERT の行を解決できませんでした。")
        return RedirectResponse("/form", status_code=302)

    ok, msg = _create_poke_request(
        ctx,
        sender_cast_id=my_cast.get("id", ""),
        target_cast_id=target_cast_id,
        harmonia_row_id=harmonia_row_id,
        poke_type=poke_type,
        message=body,
        practice_id=(practice_id or "").strip(),
        expires_at_iso=expires_at_iso,
    )
    if ok:
        _flash_set(request, "info", msg)
    else:
        _flash_set(request, "error", msg)
    return RedirectResponse("/form", status_code=302)


@router.get("/form/poke/inbox", response_class=JSONResponse)
async def form_poke_inbox(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return JSONResponse({"ok": False, "message": "not_logged_in"}, status_code=401)
    if not cid:
        return JSONResponse({"ok": False, "message": "concert_not_selected"}, status_code=400)

    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", []) or []
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    if not my_cast:
        return JSONResponse({"ok": True, "items": []})

    harmonia_row_id = _ensure_harmonia_row_id(ctx, cid, _atlas_concert_name(ctx, _find_concert(ctx, cid) or {}))
    if not harmonia_row_id:
        return JSONResponse({"ok": True, "items": []})
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    _expire_pokes_if_needed(ctx, rows)
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)

    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    pmap = {p.get("id", ""): p for p in players}
    cast_by_id = {r.get("id", ""): r for r in participant_rows}

    items: list[dict] = []
    my_cast_id = my_cast.get("id", "")
    for r in rows:
        status = (ext_txt(r, POKE_STATUS_KEYS) or "").strip().lower()
        if status not in {"sent", "read"}:
            continue
        target_ids = ext_rel(r, POKE_TARGET_CAST_REL_KEYS)
        if my_cast_id not in target_ids:
            continue
        sender_ids = ext_rel(r, POKE_SENDER_CAST_REL_KEYS)
        sender_cast = cast_by_id.get(sender_ids[0], {}) if sender_ids else {}
        sender_player_ids = ext_rel(sender_cast, PARTICIPANT_PLAYER_REL_KEYS)
        sender_player = pmap.get(sender_player_ids[0], {}) if sender_player_ids else {}
        sender_name = (ext_txt(sender_player, PLAYER_NAME_KEYS) or "不明").strip()
        practice_ids = ext_rel(r, POKE_PRACTICE_REL_KEYS)
        practice_name = ""
        if practice_ids:
            pr = next((p for p in (data.get("practices", []) or []) if p.get("id", "") == practice_ids[0]), None)
            if pr:
                practice_name = (ext_txt(pr, PRACTICE_NAME_KEYS) or "").strip()
        items.append({
            "id": r.get("id", ""),
            "sender_name": sender_name,
            "poke_type": (ext_txt(r, POKE_TYPE_KEYS) or "").strip(),
            "label": _poke_type_label(ext_txt(r, POKE_TYPE_KEYS) or ""),
            "message": (ext_txt(r, POKE_MESSAGE_KEYS) or "").strip(),
            "status": status,
            "practice_name": practice_name,
            "created_at": ((ext_txt(r, ["created_at", "作成日時"]) or "").strip() or (r.get("created_time", "") or "").strip()),
            "expires_at": (ext_txt(r, POKE_EXPIRES_AT_KEYS) or "").strip(),
        })
    items.sort(key=lambda x: (x.get("expires_at", ""), x.get("id", "")))
    return JSONResponse({"ok": True, "items": items})


@router.post("/form/poke/{poke_id}/read")
async def form_poke_mark_read(request: Request, poke_id: str):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", []) or []
    my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    if not my_cast:
        _flash_set(request, "error", "参加者情報が見つかりません。")
        return RedirectResponse("/form", status_code=302)
    harmonia_row_id = _ensure_harmonia_row_id(ctx, cid, _atlas_concert_name(ctx, _find_concert(ctx, cid) or {}))
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    ext_rel = ctx["extract_relation_ids_any"]
    row = next((r for r in rows if r.get("id", "") == (poke_id or "")), None)
    if not row or my_cast.get("id", "") not in ext_rel(row, POKE_TARGET_CAST_REL_KEYS):
        _flash_set(request, "error", "対象Pokeが見つからないか権限がありません。")
        return RedirectResponse("/form", status_code=302)
    _poke_mark_status(ctx, row.get("id", ""), "read")
    _flash_set(request, "info", "Pokeを既読にしました。")
    return RedirectResponse("/form", status_code=302)


@router.post("/form/poke/{poke_id}/done")
async def form_poke_mark_done(request: Request, poke_id: str):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", []) or []
    my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    if not my_cast:
        _flash_set(request, "error", "参加者情報が見つかりません。")
        return RedirectResponse("/form", status_code=302)
    harmonia_row_id = _ensure_harmonia_row_id(ctx, cid, _atlas_concert_name(ctx, _find_concert(ctx, cid) or {}))
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    ext_rel = ctx["extract_relation_ids_any"]
    row = next((r for r in rows if r.get("id", "") == (poke_id or "")), None)
    if not row or my_cast.get("id", "") not in ext_rel(row, POKE_TARGET_CAST_REL_KEYS):
        _flash_set(request, "error", "対象Pokeが見つからないか権限がありません。")
        return RedirectResponse("/form", status_code=302)
    _poke_mark_status(ctx, row.get("id", ""), "done")
    _flash_set(request, "info", "Pokeを完了にしました。")
    return RedirectResponse("/form", status_code=302)


@router.post("/form/poke/{poke_id}/cancel")
async def form_poke_cancel(request: Request, poke_id: str):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", []) or []
    my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    if not my_cast:
        _flash_set(request, "error", "参加者情報が見つかりません。")
        return RedirectResponse("/form", status_code=302)
    harmonia_row_id = _ensure_harmonia_row_id(ctx, cid, _atlas_concert_name(ctx, _find_concert(ctx, cid) or {}))
    rows = _poke_rows_for_concert(ctx, harmonia_row_id)
    ext_rel = ctx["extract_relation_ids_any"]
    row = next((r for r in rows if r.get("id", "") == (poke_id or "")), None)
    if not row or my_cast.get("id", "") not in ext_rel(row, POKE_SENDER_CAST_REL_KEYS):
        _flash_set(request, "error", "対象Pokeが見つからないか権限がありません。")
        return RedirectResponse("/form", status_code=302)
    _poke_mark_status(ctx, row.get("id", ""), "cancelled")
    _flash_set(request, "info", "Pokeを取消しました。")
    return RedirectResponse("/form", status_code=302)


@router.post("/form/assign/run")
async def form_assign_run(
    request: Request,
    target_song_id: Annotated[str, Form()] = "",
    target_part_id: Annotated[str, Form()] = "",
):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)

    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", [])
    base_role = resolve_user_role(ctx, pid, cid, participant_rows)
    my_system_role = _my_system_role(ctx, pid, cid, participant_rows)
    override_raw = request.session.get(DEBUG_ROLE_OVERRIDE_SESSION_KEY, "")
    override_role = _role_from_override(override_raw) if _is_administrator_role(my_system_role) else None
    role = override_role if override_role is not None else base_role
    if role < ROLE_LEADER:
        _flash_set(request, "error", "アサイン案の生成はLeader以上のみ実行できます。")
        return RedirectResponse("/form", status_code=302)

    _, my_part_id = _my_part_info(ctx, pid, cid, participant_rows)
    song_id = (target_song_id or "").strip()
    part_id = (target_part_id or "").strip()
    try:
        results, err = _build_exact_solver_results(
            ctx=ctx,
            concert_id=cid,
            selected_song_id=song_id,
            selected_part_master_id=part_id,
            role=role,
            my_part_id=my_part_id,
            data=data,
        )
    except Exception as e:
        _flash_set(request, "error", f"厳密解の生成に失敗しました: {e}")
        return RedirectResponse("/form", status_code=302)
    if err:
        _flash_set(request, "error", err)
        return RedirectResponse("/form", status_code=302)

    state_all = request.session.get("assign_solver_state") or {}
    if not isinstance(state_all, dict):
        state_all = {}
    generated_at = datetime.now().isoformat(timespec="seconds")
    _solver_cache_set(pid, cid, {
        "results": results,
        "generated_at": generated_at,
    })
    state_all[cid] = {
        "selected": 0,
        "scope_song_id": song_id,
        "scope_part_id": part_id if role >= ROLE_MANAGER else my_part_id,
        "generated_at": generated_at,
    }
    request.session["assign_solver_state"] = state_all
    _flash_set(request, "info", "厳密解A〜Dを生成しました。")
    return RedirectResponse("/form", status_code=302)


@router.post("/form/assign/select")
async def form_assign_select(request: Request, candidate_index: Annotated[int, Form()]):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    state_all = request.session.get("assign_solver_state") or {}
    if not isinstance(state_all, dict):
        return RedirectResponse("/form", status_code=302)
    state = state_all.get(cid, {})
    solver_cache = _solver_cache_get(pid, cid)
    results = solver_cache.get("results", []) if isinstance(solver_cache, dict) else []
    if results:
        idx = max(0, min(int(candidate_index), len(results) - 1))
        state["selected"] = idx
        state_all[cid] = state
        request.session["assign_solver_state"] = state_all
    return RedirectResponse("/form", status_code=302)


@router.post("/form/assign/propose")
async def form_assign_propose(
    request: Request,
    candidate_index: Annotated[str, Form()] = "",
):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)

    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", [])
    base_role = resolve_user_role(ctx, pid, cid, participant_rows)
    my_system_role = _my_system_role(ctx, pid, cid, participant_rows)
    override_raw = request.session.get(DEBUG_ROLE_OVERRIDE_SESSION_KEY, "")
    override_role = _role_from_override(override_raw) if _is_administrator_role(my_system_role) else None
    role = override_role if override_role is not None else base_role
    if role < ROLE_LEADER:
        _flash_set(request, "error", "案提示はLeader以上のみ実行できます。")
        return RedirectResponse("/form", status_code=302)

    state_all = request.session.get("assign_solver_state") or {}
    state = state_all.get(cid, {}) if isinstance(state_all, dict) else {}
    solver_cache = _solver_cache_get(pid, cid)
    results = solver_cache.get("results", []) if isinstance(solver_cache, dict) else []
    selected = int(state.get("selected", 0) or 0) if results else 0
    if results and str(candidate_index).strip() != "":
        try:
            selected = int(str(candidate_index).strip())
        except Exception:
            pass
    if not results:
        _flash_set(request, "error", "提示する候補がありません。先にA〜Dを生成してください。")
        return RedirectResponse("/form", status_code=302)
    selected = max(0, min(selected, len(results) - 1))
    selected_label = (results[selected].get("label", "") or f"候補{chr(ord('A') + selected)}").strip()
    assignments = results[selected].get("assignments", [])
    ok, fail = _write_assignment_rows(ctx, cid, assignments)
    if fail > 0 or ok == 0:
        _flash_set(request, "error", f"案提示時の反映に失敗しました（成功{ok} / 失敗{fail}）。")
        return RedirectResponse("/form", status_code=302)
    _clear_assign_responses_for_concert(ctx, cid)
    if isinstance(state, dict):
        state["selected"] = selected
        state["proposed_selected"] = selected
        state["proposed_label"] = selected_label
        state["proposed_at"] = datetime.now().isoformat(timespec="seconds")
        state_all[cid] = state
        request.session["assign_solver_state"] = state_all
    concert = _find_concert(ctx, cid) or {}
    c_name = _atlas_concert_name(ctx, concert) if concert else cid
    _set_harmonia_checkbox(ctx, cid, HARMONIA_CONCERT_PLAN_KEYS, True, c_name)
    _set_harmonia_checkbox(ctx, cid, HARMONIA_CONCERT_ASSIGN_KEYS, False, c_name)
    _flash_set(request, "info", f"{selected_label} を案提示しました（{ok}件反映）。PDF出力が可能です。")
    return RedirectResponse("/form", status_code=302)


@router.post("/form/assign/confirm")
async def form_assign_confirm(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)

    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", [])
    base_role = resolve_user_role(ctx, pid, cid, participant_rows)
    my_system_role = _my_system_role(ctx, pid, cid, participant_rows)
    override_raw = request.session.get(DEBUG_ROLE_OVERRIDE_SESSION_KEY, "")
    override_role = _role_from_override(override_raw) if _is_administrator_role(my_system_role) else None
    role = override_role if override_role is not None else base_role
    if role < ROLE_LEADER:
        _flash_set(request, "error", "確定はLeader以上のみ実行できます。")
        return RedirectResponse("/form", status_code=302)
    flags = _harmonia_flags(ctx, cid)
    if not bool(flags.get("plan_done")):
        _flash_set(request, "error", "先に『案提示する』を実行してください。")
        return RedirectResponse("/form", status_code=302)

    state_all = request.session.get("assign_solver_state") or {}
    state = state_all.get(cid, {}) if isinstance(state_all, dict) else {}
    solver_cache = _solver_cache_get(pid, cid)
    results = solver_cache.get("results", []) if isinstance(solver_cache, dict) else []
    if not results:
        _flash_set(request, "error", "確定できる候補がありません。先に厳密解A〜Dを生成してください。")
        return RedirectResponse("/form", status_code=302)
    selected = max(0, min(int(state.get("selected", 0) or 0), len(results) - 1))
    assignments = results[selected].get("assignments", [])
    ok, fail = _write_assignment_rows(ctx, cid, assignments)
    if fail > 0 or ok == 0:
        _flash_set(request, "error", f"確定時の書き込みに失敗しました（成功{ok} / 失敗{fail}）。")
        return RedirectResponse("/form", status_code=302)
    concert = _find_concert(ctx, cid) or {}
    c_name = _atlas_concert_name(ctx, concert) if concert else cid
    _set_harmonia_checkbox(ctx, cid, HARMONIA_CONCERT_PLAN_KEYS, True, c_name)
    _set_harmonia_checkbox(ctx, cid, HARMONIA_CONCERT_ASSIGN_KEYS, True, c_name)
    _flash_set(request, "info", f"アサインを確定しました（{ok}件）。")
    return RedirectResponse("/form", status_code=302)


@router.post("/form/assign/respond")
async def form_assign_respond(
    request: Request,
    response: Annotated[str, Form()],
):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)

    status = (response or "").strip().lower()
    if status not in {"agree", "object"}:
        _flash_set(request, "error", "回答種別が不正です。")
        return RedirectResponse("/form#my-assign-summary", status_code=302)

    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", []) or []
    my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
    my_cast_id = (my_cast or {}).get("id", "")
    if not my_cast_id:
        _flash_set(request, "error", "出演者情報を特定できませんでした。")
        return RedirectResponse("/form#my-assign-summary", status_code=302)

    flags = _harmonia_flags(ctx, cid)
    if not bool(flags.get("plan_done")) or bool(flags.get("assign_done")):
        _flash_set(request, "error", "アサイン案の受付期間ではありません。")
        return RedirectResponse("/form#my-assign-summary", status_code=302)

    ok, msg = _upsert_assign_response(
        ctx,
        concert_id=cid,
        cast_id=my_cast_id,
        status=status,
        plan_label=("案提示中" if bool(flags.get("plan_done")) else ""),
    )
    if not ok:
        _flash_set(request, "error", f"回答を保存できませんでした: {msg}")
        return RedirectResponse("/form#my-assign-summary", status_code=302)

    _flash_set(request, "info", ("アサイン案に賛同しました。" if status == "agree" else "アサイン案に異議を送信しました。"))
    return RedirectResponse("/form#my-assign-summary", status_code=302)


@router.post("/form/menu-action")
async def menu_action(request: Request, action: Annotated[str, Form()]):
    act = (action or "").strip().lower()
    if act == "att":
        return RedirectResponse("/form/att", status_code=302)
    if act == "pref":
        pid = request.session.get("player_id", "")
        cid = request.session.get("concert_id", "")
        if pid and cid:
            ctx = get_ctx()
            flags = _harmonia_flags(ctx, cid)
            if bool(flags.get("plan_done")) or bool(flags.get("assign_done")):
                return RedirectResponse("/form#my-assign-summary", status_code=302)
        return RedirectResponse("/form/pref", status_code=302)
    if act == "own":
        return RedirectResponse("/form/own", status_code=302)
    return RedirectResponse("/form", status_code=302)


@router.get("/form/att", response_class=HTMLResponse)
async def form_att_page(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx = get_ctx()
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)
    data = load_attendance_data(ctx, cid)
    practices = data.get("practices", [])
    participant_rows = data.get("participant_rows_concert", [])
    attendance_rows = data.get("attendance_rows", [])
    _, att_map = _attendance_map_robust(ctx, pid, cid, participant_rows, attendance_rows, practices)
    rows = []
    ext = ctx["extract_prop_text_any"]
    for p in practices:
        pr_id = p.get("id", "")
        info = att_map.get(pr_id, {})
        rows.append({
            "id": pr_id,
            "name": ext(p, PRACTICE_NAME_KEYS) or "練習",
            "date": (ext(p, PRACTICE_DATE_KEYS) or "")[:16],
            "venue": ext(p, PRACTICE_VENUE_KEYS) or "",
            "status": info.get("status", "未回答"),
            "comment": info.get("comment", ""),
        })
    return templates.TemplateResponse("form/attendance.html", {
        "request": request,
        "concert_name": _atlas_concert_name(ctx, concert),
        "player_name": request.session.get("player_name", ""),
        "rows": rows,
        "error": _flash_pop(request, "error"),
        "info": _flash_pop(request, "info"),
        "back_href": "/form",
    })


@router.post("/form/att/save")
async def form_att_save(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    form = await request.form()
    ctx = get_ctx()
    data = load_attendance_data(ctx, cid)
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)

    practices = data.get("practices", [])
    concert_day = data.get("concert_day")
    participant_rows = data.get("participant_rows_concert", [])
    _, my_part_id = _my_part_info(ctx, pid, cid, participant_rows)

    att: dict[str, str] = {}
    att_comment: dict[str, str] = {}
    for p in practices:
        pr_id = p.get("id", "")
        if not pr_id:
            continue
        st = (form.get(f"status_{pr_id}", "") or "").strip()
        cm = (form.get(f"comment_{pr_id}", "") or "").strip()
        att[pr_id] = st or "未回答"
        att_comment[pr_id] = cm

    ok_n, errors, _ = submit_all(
        ctx=ctx,
        concert_id=cid,
        concert_name=_atlas_concert_name(ctx, concert),
        player_id=pid,
        player_name=request.session.get("player_name", ""),
        part_master_id=my_part_id,
        att=att,
        att_comment=att_comment,
        pref={},
        own={},
        practices=practices,
        concert_day=concert_day,
        inst_map={},
    )
    if errors:
        _flash_set(request, "error", " / ".join(errors[:2]))
    else:
        my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
        my_cast_id = (my_cast or {}).get("id", "")
        if my_cast_id:
            # 全練習で未回答が消えたら、未回答催促Pokeを完了
            has_any_unanswered = any(_is_unanswered_status(att.get(p.get("id", ""), "未回答")) for p in practices)
            if not has_any_unanswered:
                _auto_complete_pokes_for_target(
                    ctx,
                    concert_id=cid,
                    target_cast_id=my_cast_id,
                    poke_type="att_unanswered",
                    require_no_practice_link=True,
                )
            # 直近練習の△が解消されたら、当該催促Pokeを完了
            now_dt = datetime.now()
            upcoming_practice = None
            for p in practices:
                dtxt = (ctx["extract_prop_text_any"](p, PRACTICE_DATE_KEYS) or "").strip()
                pd = _parse_dt_safe(dtxt)
                if pd is None:
                    continue
                if pd >= now_dt:
                    upcoming_practice = p
                    break
            if upcoming_practice is None and practices:
                upcoming_practice = practices[0]
            upcoming_practice_id = (upcoming_practice or {}).get("id", "")
            if upcoming_practice_id:
                upcoming_status = (att.get(upcoming_practice_id, "未回答") or "").strip()
                if not _is_maybe_status(upcoming_status):
                    _auto_complete_pokes_for_target(
                        ctx,
                        concert_id=cid,
                        target_cast_id=my_cast_id,
                        poke_type="att_maybe",
                        practice_id=upcoming_practice_id,
                    )
        _flash_set(request, "info", f"出欠を保存しました（{ok_n}件更新）。")
    return RedirectResponse("/form/att", status_code=302)


def _build_practice_cols(practices: list) -> list:
    """練習一覧を出欠テーブルの列定義に変換。"""
    from datetime import date as _date
    cols = []
    for p in practices:
        pid   = p.get("id", "")
        dstr  = (p.get("properties", {}).get("日時", {}).get("date", {}) or {}).get("start", "") or ""
        label = dstr[:10] if dstr else pid[:6]
        try:
            wd = ["月","火","水","木","金","土","日"][_date.fromisoformat(dstr[:10]).weekday()]
            label = f"{dstr[5:10]}({wd})"
        except Exception:
            pass
        cols.append({"id": pid, "label": label})
    return cols


def _build_attendance_table(
    ctx,
    concert_id,
    participant_rows,
    practices,
    attendance_rows,
    part_master_map,
    my_part_id,
    role,
    player_map: dict | None = None,
) -> list:
    """出欠テーブル行を構築。LeaderはPART_MASTERが同一のメンバーのみ、Managerは全員。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    if player_map is None:
        players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
        player_map = {p.get("id", ""): p for p in players}
    rows = []
    for cast in participant_rows:
        pm_ids = ext_rel(cast, PARTICIPANT_PART_REL_KEYS)
        if role < ROLE_MANAGER and (not pm_ids or pm_ids[0] != my_part_id):
            continue
        pm_id   = pm_ids[0] if pm_ids else ""
        pm_info = part_master_map.get(pm_id, {})
        part    = pm_info.get("name", "")
        pids    = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
        pid     = pids[0] if pids else ""
        player  = player_map.get(pid, {})
        name    = ext_txt(player, PLAYER_NAME_KEYS) or "—"
        targets = {pid, cast.get("id", "")}
        cells: dict[str, str] = {}
        comments: dict[str, str] = {}
        for p in practices:
            pr_id = p.get("id", "")
            status = "—"
            comment = ""
            for att in attendance_rows:
                att_pids = set(ext_rel(att, ATT_PLAYER_REL_KEYS))
                att_prids = ext_rel(att, ATT_PRACTICE_REL_KEYS)
                if targets.intersection(att_pids) and pr_id in att_prids:
                    status = ext_txt(att, ATT_STATUS_KEYS) or "—"
                    comment = (ext_txt(att, _K.ATT_NOTE_KEYS) or "").strip()
                    break
            cells[pr_id] = status
            comments[pr_id] = comment
        rows.append({"cast_id": cast.get("id", ""), "part": part, "name": name, "cells": cells, "comments": comments})
    rows.sort(key=lambda r: (r["part"], r["name"]))
    return rows


def _build_member_table(ctx, participant_rows, part_master_map, my_part_id, role, player_map: dict | None = None) -> list:
    """メンバーテーブル行を構築。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    if player_map is None:
        players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
        player_map = {p.get("id", ""): p for p in players}
    rows = []
    for cast in participant_rows:
        pm_ids = ext_rel(cast, PARTICIPANT_PART_REL_KEYS)
        if role < ROLE_MANAGER and (not pm_ids or pm_ids[0] != my_part_id):
            continue
        pm_id   = pm_ids[0] if pm_ids else ""
        pm_info = part_master_map.get(pm_id, {})
        part    = pm_info.get("name", "")
        pids    = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
        pid     = pids[0] if pids else ""
        player  = player_map.get(pid, {})
        name       = ext_txt(player, PLAYER_NAME_KEYS) or "—"
        role_music = ext_txt(cast, PARTICIPANT_ROLE_KEYS) or ""
        role_ops   = ext_txt(cast, PARTICIPANT_ROLE_OPS_KEYS) or ""
        role_system = ext_txt(cast, ["システムロール", "system_role", "SystemRole"]) or ""
        rows.append({
            "part": part,
            "name": name,
            "role_music": role_music,
            "role_ops": role_ops,
            "role_system": role_system,
        })
    rows.sort(key=lambda r: (r["part"], r["name"]))
    return rows


def _build_role_own_rows(
    ctx: dict,
    *,
    concert_id: str,
    role: int,
    my_part_id: str,
    participant_rows: list[dict],
    partdefs: list[dict],
    songs: list[dict],
    player_rows: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Leader/Manager向け: 曲×必要楽器ごとの所有楽器状況を構築。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    song_name_map = {s.get("id", ""): (ext_txt(s, SONG_NAME_KEYS) or "未設定").strip() for s in songs}
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_name_map = {r.get("id", ""): (ext_txt(r, INSTRUMENT_NAME_KEYS) or "").strip() for r in inst_rows}
    player_name_map = {p.get("id", ""): (ext_txt(p, PLAYER_NAME_KEYS) or "不明").strip() for p in (player_rows or [])}

    scope_cast_ids: set[str] = set()
    scope_player_ids: set[str] = set()
    for cast in participant_rows or []:
        pm_ids = ext_rel(cast, PARTICIPANT_PART_REL_KEYS)
        if role < ROLE_MANAGER:
            if not pm_ids or pm_ids[0] != my_part_id:
                continue
        cast_id = cast.get("id", "")
        pids = ext_rel(cast, PARTICIPANT_PLAYER_REL_KEYS)
        if cast_id:
            scope_cast_ids.add(cast_id)
        if pids:
            scope_player_ids.add(pids[0])

    owner_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    pi_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    cid_norm = _norm_id(concert_id)
    for r in pi_rows:
        rel_cids = {_norm_id(x) for x in ext_rel(r, _K.PI_CONCERT_REL_KEYS)}
        if cid_norm not in rel_cids:
            continue
        rel_targets = set(ext_rel(r, _K.PI_PLAYER_REL_KEYS))
        target_pid = ""
        if rel_targets.intersection(scope_player_ids):
            target_pid = next(iter(rel_targets.intersection(scope_player_ids)))
        elif rel_targets.intersection(scope_cast_ids):
            cast_id = next(iter(rel_targets.intersection(scope_cast_ids)))
            cast_row = next((c for c in participant_rows if c.get("id", "") == cast_id), {})
            pids = ext_rel(cast_row, PARTICIPANT_PLAYER_REL_KEYS)
            target_pid = pids[0] if pids else ""
        if not target_pid:
            continue
        inst_ids = ext_rel(r, _K.PI_INST_REL_KEYS)
        if not inst_ids:
            continue
        inst_id = inst_ids[0]
        raw = (ext_txt(r, _K.PI_OWN_COUNT_KEYS) or "0").strip()
        try:
            qty = max(0, int(float(raw or "0")))
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        owner_counts[inst_id][target_pid] += qty

    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()
    song_option_map: dict[str, str] = {}
    for pd in partdefs or []:
        song_ids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        part_ids = ext_rel(pd, PARTDEF_PART_REL_KEYS)
        if role < ROLE_MANAGER and my_part_id:
            if not part_ids or part_ids[0] != my_part_id:
                continue
        song_id = song_ids[0] if song_ids else ""
        if song_id:
            song_option_map[song_id] = song_name_map.get(song_id, "未設定")
        inst_ids = ext_rel(pd, PARTDEF_INST_REL_KEYS)
        for inst_id in inst_ids:
            key = (song_id, inst_id)
            if key in seen:
                continue
            seen.add(key)
            owners = owner_counts.get(inst_id, {})
            owner_badges = [
                {"player_id": pid, "name": player_name_map.get(pid, "不明"), "qty": qty}
                for pid, qty in owners.items()
            ]
            owner_badges.sort(key=lambda x: x["name"].lower())
            rows.append({
                "song_id": song_id,
                "song_name": song_name_map.get(song_id, "未設定"),
                "instrument_id": inst_id,
                "instrument_name": inst_name_map.get(inst_id, inst_id or "—"),
                "owner_badges": owner_badges,
                "owner_count": len(owner_badges),
            })
    rows.sort(key=lambda x: (x["song_name"].lower(), x["instrument_name"].lower()))
    song_options = [{"id": sid, "name": name} for sid, name in song_option_map.items()]
    song_options.sort(key=lambda x: x["name"].lower())
    return rows, song_options


def _build_material_rows(ctx, partdefs, songs, my_part_id, role) -> list:
    """資料タブ向けの楽譜リンク一覧。"""
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    song_name_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "") for s in songs}
    song_score_map = {s.get("id", ""): (ext(s, SONG_SCORE_URL_KEYS) or "") for s in songs}
    rows = []
    for pd in partdefs:
        part_ids = ext_rel(pd, PARTDEF_PART_REL_KEYS)
        if role < ROLE_MANAGER and my_part_id:
            if not part_ids or part_ids[0] != my_part_id:
                continue
        pd_name = ext(pd, PARTDEF_DISPLAY_NAME_KEYS) or ext(pd, PARTDEF_NAME_KEYS) or "-"
        song_ids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        song_id = song_ids[0] if song_ids else ""
        song_name = song_name_map.get(song_id, "未設定")
        part_url = ext(pd, PARTDEF_SCORE_URL_KEYS) or ""
        song_url = song_score_map.get(song_id, "") if song_id else ""
        score_url = part_url or song_url
        rows.append({"song": song_name, "part": pd_name, "url": score_url})
    rows.sort(key=lambda x: (x["song"], x["part"]))
    return rows


def _build_material_links(ctx, partdefs, songs, my_part_id, role) -> list[dict]:
    """資料タブ向けの楽譜リンク（Streamlit準拠: URL重複排除）。"""
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    song_name_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "") for s in songs}
    song_score_map = {s.get("id", ""): (ext(s, SONG_SCORE_URL_KEYS) or "") for s in songs}
    out: list[dict] = []
    seen_urls: set[str] = set()
    for pd in partdefs:
        part_ids = ext_rel(pd, PARTDEF_PART_REL_KEYS)
        if role < ROLE_MANAGER and my_part_id:
            if not part_ids or part_ids[0] != my_part_id:
                continue
        pd_url = (ext(pd, PARTDEF_SCORE_URL_KEYS) or "").strip()
        pd_lbl = (ext(pd, PARTDEF_NAME_KEYS) or "楽譜").strip()
        if pd_url:
            if pd_url not in seen_urls:
                seen_urls.add(pd_url)
                out.append({"label": pd_lbl, "url": pd_url})
            continue

        song_ids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        if not song_ids:
            continue
        song_id = song_ids[0]
        song_url = (song_score_map.get(song_id, "") or "").strip()
        if not song_url:
            continue
        song_lbl = (song_name_map.get(song_id, "楽譜") or "楽譜").strip()
        if song_url not in seen_urls:
            seen_urls.add(song_url)
            out.append({"label": f"{song_lbl}（全体）", "url": song_url})
    return out


def _build_material_practice_items(ctx, practices: list[dict]) -> list[dict]:
    ext = ctx["extract_prop_text_any"]
    items: list[dict] = []
    for p in practices or []:
        pid = p.get("id", "")
        if not pid:
            continue
        items.append({
            "id": pid,
            "name": (ext(p, PRACTICE_NAME_KEYS) or "練習").strip(),
            "date": (ext(p, PRACTICE_DATE_KEYS) or "")[:10],
            "venue": (ext(p, PRACTICE_VENUE_KEYS) or "").strip(),
        })
    return items


def _generate_simple_practice_pdf_bytes(ctx: dict, concert_name: str, practice: dict, attendance_rows: list[dict]) -> bytes:
    """VPS版の簡易練習情報PDF（1練習分）を生成。"""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except Exception as e:
        raise RuntimeError(f"reportlab が利用できません: {e}") from e

    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    pid = practice.get("id", "")
    p_name = (ext(practice, PRACTICE_NAME_KEYS) or "練習").strip()
    p_date = (ext(practice, PRACTICE_DATE_KEYS) or "")[:16].replace("T", " ")
    p_venue = (ext(practice, PRACTICE_VENUE_KEYS) or "").strip()

    status_count = {"○": 0, "△": 0, "×": 0, "未回答": 0}
    for att in attendance_rows or []:
        pr_ids = ext_rel(att, ATT_PRACTICE_REL_KEYS)
        if pid not in pr_ids:
            continue
        s = (ext(att, ATT_STATUS_KEYS) or "未回答").strip()
        if s not in status_count:
            s = "未回答"
        status_count[s] += 1

    font_name = "Helvetica"
    try:
        font_path = "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"
        pdfmetrics.registerFont(TTFont("IPAGothic", font_path))
        font_name = "IPAGothic"
    except Exception:
        pass

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4
    y = h - 48
    c.setFont(font_name, 16)
    c.drawString(40, y, "ArtéMis HARMONIA 練習情報PDF")
    y -= 28
    c.setFont(font_name, 12)
    c.drawString(40, y, f"演奏会: {concert_name}")
    y -= 20
    c.drawString(40, y, f"練習: {p_name}")
    y -= 18
    if p_date:
        c.drawString(40, y, f"日時: {p_date}")
        y -= 18
    if p_venue:
        c.drawString(40, y, f"会場: {p_venue}")
        y -= 26
    else:
        y -= 8

    c.setFont(font_name, 13)
    c.drawString(40, y, "出欠サマリー")
    y -= 18
    c.setFont(font_name, 11)
    c.drawString(40, y, f"○ 参加: {status_count['○']}人")
    y -= 16
    c.drawString(40, y, f"△ 条件付き: {status_count['△']}人")
    y -= 16
    c.drawString(40, y, f"× 欠席: {status_count['×']}人")
    y -= 16
    c.drawString(40, y, f"未回答: {status_count['未回答']}人")
    y -= 28
    c.setFont(font_name, 9)
    c.drawString(40, y, "※ 本PDFはVPS版の簡易出力です。")
    c.showPage()
    c.save()
    return buf.getvalue()


def _generate_practice_pdf_bytes(ctx: dict, practice_id: str, concert_name: str, practice: dict, attendance_rows: list[dict]) -> bytes:
    """
    まず app.services.practice_report.generate_practice_report を利用し、
    使えない環境のみ簡易PDFへフォールバックする。
    """
    # 1) VPS配置の app.services.practice_report を最優先
    try:
        from app.services.practice_report import generate_practice_report  # type: ignore
        return generate_practice_report(ctx, practice_id)
    except Exception:
        pass

    # 2) 互換: concert/services に置いた場合
    try:
        from concert.services.practice_report import generate_practice_report  # type: ignore
        return generate_practice_report(ctx, practice_id)
    except Exception:
        pass

    # 3) 最終フォールバック
    return _generate_simple_practice_pdf_bytes(
        ctx=ctx,
        concert_name=concert_name,
        practice=practice,
        attendance_rows=attendance_rows,
    )


def _visible_partdefs(ctx: dict, partdefs: list, part_master_id: str) -> list:
    ext_rel = ctx["extract_relation_ids_any"]
    if not part_master_id:
        return []
    return [pd for pd in partdefs if part_master_id in ext_rel(pd, PARTDEF_PART_REL_KEYS)]


def _build_song_groups(ctx: dict, visible_pds: list, songs: list, existing_pref: dict) -> list:
    from collections import defaultdict
    ext = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    song_map = {s.get("id", ""): (ext(s, SONG_NAME_KEYS) or "曲目未設定") for s in songs}
    pd_by_song: dict[str, list] = defaultdict(list)
    for pd in visible_pds:
        sids = ext_rel(pd, PARTDEF_SONG_REL_KEYS)
        sid = sids[0] if sids else "__none__"
        pd_by_song[sid].append(pd)
    groups = []
    for sid, pds in pd_by_song.items():
        rows = []
        for pd in pds:
            pd_id = pd.get("id", "")
            display_name = (
                ext(pd, ["表示パート名", "display_name"]) or
                ext(pd, PARTDEF_NAME_KEYS) or
                pd_id[:8]
            )
            rows.append({
                "pd_id":        pd_id,
                "display_name": display_name,
                "priority":     existing_pref.get(pd_id, "未回答"),
            })
        groups.append({"song_name": song_map.get(sid, "曲目未設定"), "rows": rows})
    return groups


@router.get("/form/pref", response_class=HTMLResponse)
async def form_pref_page(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx     = get_ctx()
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)
    data             = load_form_data(ctx, cid)
    partdefs         = data.get("partdefs", [])
    songs            = data.get("songs", [])
    participant_rows = data.get("participant_rows_concert", [])
    preference_rows  = data.get("preference_rows", [])
    _, my_part_id    = _my_part_info(ctx, pid, cid, participant_rows)
    visible_pds      = _visible_partdefs(ctx, partdefs, my_part_id)
    existing_pref    = load_existing_prefs(ctx, cid, pid, visible_pds, participant_rows, preference_rows)
    song_groups      = _build_song_groups(ctx, visible_pds, songs, existing_pref)
    flags            = _harmonia_flags(ctx, cid)
    return templates.TemplateResponse("form/pref.html", {
        "request":       request,
        "concert_name":  _atlas_concert_name(ctx, concert),
        "player_name":   request.session.get("player_name", ""),
        "my_part":       request.session.get("my_part", ""),
        "song_groups":   song_groups,
        "proposal_done": flags.get("plan_done", False),
        "error":         _flash_pop(request, "error"),
        "info":          _flash_pop(request, "info"),
        "back_href":     "/form",
    })


@router.post("/form/pref/save")
async def form_pref_save(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx     = get_ctx()
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)
    flags = _harmonia_flags(ctx, cid)
    if flags.get("plan_done", False):
        _flash_set(request, "error", "アサイン案提示後は希望を変更できません。")
        return RedirectResponse("/form/pref", status_code=302)
    data             = load_form_data(ctx, cid)
    partdefs         = data.get("partdefs", [])
    songs            = data.get("songs", [])
    participant_rows = data.get("participant_rows_concert", [])
    inst_map         = data.get("inst_map", {})
    practices        = data.get("practices", [])
    concert_day      = data.get("concert_day")
    _, my_part_id    = _my_part_info(ctx, pid, cid, participant_rows)
    visible_pds      = _visible_partdefs(ctx, partdefs, my_part_id)
    form             = await request.form()
    PRIORITY_OPTS    = ["未回答", "第1希望", "第2希望", "第3希望", "希望なし/降り番でも可", "NG"]
    pref: dict[str, str] = {}
    for pd in visible_pds:
        pd_id = pd.get("id", "")
        if not pd_id:
            continue
        val = (form.get(f"pref_{pd_id}", "") or "").strip()
        pref[pd_id] = val if val in PRIORITY_OPTS else "未回答"
    ok_n, errors, _ = submit_all(
        ctx=ctx,
        concert_id=cid,
        concert_name=_atlas_concert_name(ctx, concert),
        player_id=pid,
        player_name=request.session.get("player_name", ""),
        part_master_id=my_part_id,
        att={},
        att_comment={},
        pref=pref,
        own={},
        practices=practices,
        concert_day=concert_day,
        inst_map=inst_map,
    )
    if errors:
        _flash_set(request, "error", " / ".join(errors[:2]))
    else:
        my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
        my_cast_id = (my_cast or {}).get("id", "")
        if my_cast_id:
            answered_all = all((v or "").strip() not in ("", "未回答") for v in pref.values())
            if answered_all:
                _auto_complete_pokes_for_target(
                    ctx,
                    concert_id=cid,
                    target_cast_id=my_cast_id,
                    poke_type="pref_missing",
                    require_no_practice_link=True,
                )
        answered = sum(1 for v in pref.values() if v not in ("未回答", ""))
        _flash_set(request, "info", f"希望を保存しました（{answered}件入力済み）。")
    return RedirectResponse("/form/pref", status_code=302)


@router.get("/form/own", response_class=HTMLResponse)
async def form_own_page(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx     = get_ctx()
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)
    data             = load_form_data(ctx, cid)
    inst_map         = data.get("inst_map", {})
    req_insts        = data.get("req_insts", [])
    ext_rel          = ctx["extract_relation_ids_any"]
    all_pi           = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    current: dict[str, int] = {}
    for r in all_pi:
        if pid not in ext_rel(r, ["奏者", "出演者", "FK奏者"]):
            continue
        if not any(x.replace("-","") == cid.replace("-","") for x in ext_rel(r, ["演奏会", "出演", "FK演奏会"])):
            continue
        iids = ext_rel(r, ["楽器種別", "楽器", "担当楽器", "FK楽器種別"])
        cnt  = ctx["extract_prop_text_any"](r, ["所有台数", "持参台数", "持参数"])
        if iids:
            try:
                current[iids[0]] = int(float(cnt or "0"))
            except Exception:
                current[iids[0]] = 0
    instruments = [
        {"id": iid, "name": inst_map.get(iid, iid), "current": current.get(iid, 0)}
        for iid in req_insts if iid
    ]
    return templates.TemplateResponse("form/own.html", {
        "request":      request,
        "concert_name": _atlas_concert_name(ctx, concert),
        "player_name":  request.session.get("player_name", ""),
        "instruments":  instruments,
        "error":        _flash_pop(request, "error"),
        "info":         _flash_pop(request, "info"),
        "back_href":    "/form",
    })


@router.post("/form/own/save")
async def form_own_save(request: Request):
    pid = request.session.get("player_id", "")
    cid = request.session.get("concert_id", "")
    if not pid:
        return RedirectResponse("/login", status_code=302)
    if not cid:
        return RedirectResponse("/concert/select", status_code=302)
    ctx     = get_ctx()
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)
    data             = load_form_data(ctx, cid)
    inst_map         = data.get("inst_map", {})
    req_insts        = data.get("req_insts", [])
    practices        = data.get("practices", [])
    concert_day      = data.get("concert_day")
    participant_rows = data.get("participant_rows_concert", [])
    _, my_part_id    = _my_part_info(ctx, pid, cid, participant_rows)
    form = await request.form()
    own: dict[str, int] = {}
    for iid in req_insts:
        if not iid:
            continue
        val = (form.get(f"own_{iid}", "") or "").strip()
        try:
            own[iid] = max(0, min(10, int(float(val or "0"))))
        except Exception:
            own[iid] = 0
    ok_n, errors, _ = submit_all(
        ctx=ctx,
        concert_id=cid,
        concert_name=_atlas_concert_name(ctx, concert),
        player_id=pid,
        player_name=request.session.get("player_name", ""),
        part_master_id=my_part_id,
        att={},
        att_comment={},
        pref={},
        own=own,
        practices=practices,
        concert_day=concert_day,
        inst_map=inst_map,
    )
    if errors:
        _flash_set(request, "error", " / ".join(errors[:2]))
    else:
        my_cast = _find_cast_row_for_player(ctx, pid, cid, participant_rows)
        my_cast_id = (my_cast or {}).get("id", "")
        if my_cast_id:
            _auto_complete_pokes_for_target(
                ctx,
                concert_id=cid,
                target_cast_id=my_cast_id,
                poke_type="own_missing",
                require_no_practice_link=True,
            )
        saved = sum(1 for v in own.values() if v > 0)
        _flash_set(request, "info", f"所有楽器を保存しました（{saved}件）。")
    return RedirectResponse("/form/own", status_code=302)


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)


@router.get("/debug/att-map")
async def debug_att_map(
    request: Request,
    player_id: str = "",
    concert_id: str = "",
):
    """
    出欠マッピング診断用。
    player_id / concert_id をクエリで明示指定可能。
    """
    pid = (player_id or request.session.get("player_id", "")).strip()
    cid = (concert_id or request.session.get("concert_id", "")).strip()
    if not pid or not cid:
        return JSONResponse(
            {
                "ok": False,
                "message": "player_id or concert_id is empty",
                "session_player_id": request.session.get("player_id", ""),
                "session_concert_id": request.session.get("concert_id", ""),
            },
            status_code=400,
        )
    ctx = get_ctx()
    data = load_form_data(ctx, cid)
    practices = data.get("practices", [])
    participant_rows = data.get("participant_rows_concert", [])
    attendance_rows = data.get("attendance_rows", [])
    cast_id, att_map = _attendance_map_robust(
        ctx=ctx,
        player_id=pid,
        concert_id=cid,
        participant_rows=participant_rows,
        attendance_rows=attendance_rows,
        practices=practices,
    )
    sample = []
    ext = ctx["extract_prop_text_any"]
    for row in attendance_rows[:10]:
        rel_map = _all_relation_ids_from_row(row)
        sample.append(
            {
                "id": row.get("id", ""),
                "record_key": ext(row, ["attendance_key", "record_key", "タイトル", "PK名称"]),
                "status": ext(row, ["参加可否", "出欠", "status", "Status"]),
                "practice_ids_from_relation": list(
                    {
                        rid
                        for ids in rel_map.values()
                        for rid in ids
                        if rid in {p.get("id", "") for p in practices}
                    }
                ),
                "relation_keys": list(rel_map.keys()),
            }
        )
    return JSONResponse(
        {
            "ok": True,
            "player_id": pid,
            "concert_id": cid,
            "cast_id": cast_id,
            "participant_count": len(participant_rows),
            "attendance_count": len(attendance_rows),
            "practice_count": len(practices),
            "att_map_size": len(att_map),
            "att_map": att_map,
            "attendance_sample": sample,
        }
    )
