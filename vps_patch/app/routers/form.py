"""
app/routers/form.py
"""
from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Form, Request
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
    PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS,
    PARTICIPANT_RECORD_KEYS,
    PARTICIPANT_ROLE_KEYS,
    PARTICIPANT_ROLE_OPS_KEYS,
    ATT_PLAYER_REL_KEYS,
    ATT_PRACTICE_REL_KEYS,
    ATT_STATUS_KEYS,
    PARTMASTER_NAME_KEYS,
    PARTDEF_PART_REL_KEYS,
    PARTDEF_NAME_KEYS,
    PARTDEF_DISPLAY_NAME_KEYS,
    PARTDEF_SCORE_URL_KEYS,
    PARTDEF_SONG_REL_KEYS,
    SONG_NAME_KEYS,
    SONG_SCORE_URL_KEYS,
    PLAYER_EMAIL_KEYS,
    PLAYER_NAME_KEYS,
    PLAYER_PASSWORD_HASH_KEYS,
    ROLE_PLAYER,
    ROLE_LEADER,
    ROLE_MANAGER,
    is_perc,
    load_form_data,
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

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


def get_ctx():
    from app.services.notion_client import build_concert_ctx
    return build_concert_ctx()


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


def _harmonia_flags(ctx: dict, concert_id: str) -> dict:
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
    rows = ctx["query_all"](ctx["CONCERT_DB_HARMONIA_CONCERT"], None)
    target = None
    for r in rows:
        rel = ext_rel(r, ["演奏会", "FK演奏会", "concert"])
        if concert_id in rel:
            target = r
            break
    if not target:
        return {"plan_done": False}
    v = (ext_txt(target, HARMONIA_CONCERT_PLAN_KEYS) or "").strip().lower()
    return {"plan_done": v in ("true", "1", "yes", "on", "済", "完了")}


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
async def entry_page(request: Request):
    if request.session.get("player_id"):
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
        "back_href": "/",
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
        "back_href": "/login",
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
async def form_menu(request: Request):
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

    data = load_form_data(ctx, cid)
    participant_rows = data.get("participant_rows_concert", [])
    practices = data.get("practices", [])
    concert_day = data.get("concert_day")
    attendance_rows = data.get("attendance_rows", [])
    preference_rows = data.get("preference_rows", [])
    partdefs = data.get("partdefs", [])

    role = resolve_user_role(ctx, pid, cid, participant_rows)
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
    is_perc_role = False
    if my_part_id:
        pm_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
        target_pm = next((r for r in pm_rows if r.get("id") == my_part_id), None)
        if target_pm:
            pm_name = ext(target_pm, PARTMASTER_NAME_KEYS) or ""
            pm_type = ext(target_pm, ["種別", "type", "Type", "パート種別"]) or ""
            is_perc_role = is_perc(pm_name) or is_perc(pm_type)

    flags = _harmonia_flags(ctx, cid)
    proposal_done = bool(flags.get("plan_done"))
    cover_url = get_cover_url(concert)
    can_show_assign = (role >= ROLE_LEADER) or proposal_done or has_published_assignments(ctx, cid)
    my_assign_rows = get_my_assign_rows(ctx, cid, pid, participant_rows) if can_show_assign else []
    assign_summary = build_assignment_view_rows(ctx, my_assign_rows, data.get("songs", []), data.get("partdefs", []))
    role_assignment_rows = build_role_assignment_rows(
        ctx=ctx,
        concert_id=cid,
        role=role,
        my_part_id=my_part_id,
        partdefs=data.get("partdefs", []),
        songs=data.get("songs", []),
        participant_rows=participant_rows,
    ) if role >= ROLE_LEADER else []
    if pref_total == 0:
        pref_hint = ""
    elif proposal_done:
        pref_hint = "アサイン案提示中"
    elif pref_answered == pref_total:
        pref_hint = "入力済み"
    else:
        pref_hint = f"{pref_answered}/{pref_total}パート 回答済"

    show_pref = role in (ROLE_PLAYER, ROLE_LEADER, ROLE_MANAGER)
    show_own = role in (ROLE_PLAYER, ROLE_LEADER, ROLE_MANAGER) and is_perc_role
    role_mode = role >= ROLE_LEADER
    song_names = []
    for s in (data.get("songs", []) or []):
        n = (ext(s, SONG_NAME_KEYS) or "").strip()
        if n:
            song_names.append(n)
    # 同名重複を除去しつつ順序は維持
    seen = set()
    song_names = [x for x in song_names if not (x in seen or seen.add(x))]
    return templates.TemplateResponse("form/menu.html", {
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
        "att_unanswered": att_unanswered,
        "att_hint": att_hint,
        "pref_total": pref_total if show_pref else 0,
        "pref_answered": pref_answered if show_pref else 0,
        "pref_hint": pref_hint,
        "proposal_done": proposal_done,
        "is_perc": show_own,
        "cover_url": cover_url,
        "can_show_assign": can_show_assign,
        "assign_summary": assign_summary,
        "show_role_panel": role_mode,
        "is_manager": role >= ROLE_MANAGER,
        "practice_cols": _build_practice_cols(data.get("practices", [])),
        "attendance_table_rows": _build_attendance_table(ctx, cid, participant_rows, data.get("practices", []), data.get("attendance_rows", []), data.get("part_master_map", {}), my_part_id, role),
        "member_table_rows": _build_member_table(ctx, participant_rows, data.get("part_master_map", {}), my_part_id, role),
        "material_rows": _build_material_rows(ctx, data.get("partdefs", []), data.get("songs", []), my_part_id, role) if role_mode else [],
        "material_links": _build_material_links(ctx, data.get("partdefs", []), data.get("songs", []), my_part_id, role) if role_mode else [],
        "material_practices": _build_material_practice_items(ctx, data.get("practices", [])) if role_mode else [],
        "role_assignment_rows": role_assignment_rows,
    })


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

    data = load_form_data(ctx, cid)
    practices = data.get("practices", []) or []
    target = next((p for p in practices if p.get("id", "") == (practice_id or "")), None)
    if not target:
        return Response("practice not found", status_code=404, media_type="text/plain; charset=utf-8")

    try:
        pdf_bytes = _generate_simple_practice_pdf_bytes(
            ctx=ctx,
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


@router.post("/form/menu-action")
async def menu_action(request: Request, action: Annotated[str, Form()]):
    act = (action or "").strip().lower()
    if act == "att":
        return RedirectResponse("/form/att", status_code=302)
    if act == "pref":
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
    data = load_form_data(ctx, cid)
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
    data = load_form_data(ctx, cid)
    concert = _find_concert(ctx, cid)
    if not concert:
        return RedirectResponse("/concert/select", status_code=302)

    practices = data.get("practices", [])
    concert_day = data.get("concert_day")
    part_master_map = data.get("part_master_map", {})
    participant_rows = data.get("participant_rows_concert", [])
    my_part_name, my_part_id = _my_part_info(ctx, pid, cid, participant_rows)

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


def _build_attendance_table(ctx, concert_id, participant_rows, practices, attendance_rows, part_master_map, my_part_id, role) -> list:
    """出欠テーブル行を構築。LeaderはPART_MASTERが同一のメンバーのみ、Managerは全員。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
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
        for p in practices:
            pr_id = p.get("id", "")
            status = "—"
            for att in attendance_rows:
                att_pids = set(ext_rel(att, ATT_PLAYER_REL_KEYS))
                att_prids = ext_rel(att, ATT_PRACTICE_REL_KEYS)
                if targets.intersection(att_pids) and pr_id in att_prids:
                    status = ext_txt(att, ATT_STATUS_KEYS) or "—"
                    break
            cells[pr_id] = status
        rows.append({"part": part, "name": name, "cells": cells})
    rows.sort(key=lambda r: (r["part"], r["name"]))
    return rows


def _build_member_table(ctx, participant_rows, part_master_map, my_part_id, role) -> list:
    """メンバーテーブル行を構築。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]
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
        rows.append({"part": part, "name": name, "role_music": role_music, "role_ops": role_ops})
    rows.sort(key=lambda r: (r["part"], r["name"]))
    return rows


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
