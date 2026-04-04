import os
import hmac
import hashlib
import base64
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]  # LINE送信先DB
HARMONIA_CONCERT_DB_ID = os.environ["HARMONIA_CONCERT_DB_ID"]
HARMONIA_PUSH_API_KEY = os.environ["HARMONIA_PUSH_API_KEY"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]  # ATLAS DB

NOTION_VERSION = "2022-06-28"

LINE_DESTINATION_ID_KEYS = ["destinationId", "groupId", "DestinationId", "GroupId"]
LINE_SOURCE_TYPE_KEYS = ["sourceType", "source_type"]
LINE_EVENT_TYPE_KEYS = ["eventType", "event_type"]
LINE_TITLE_KEYS = ["Title", "title", "名前", "氏名", "name"]
LINE_MESSAGE_TEXT_KEYS = ["messageText", "message_text"]
LINE_USER_ID_KEYS = ["userId", "user_id"]
LINE_CONCERT_REL_KEYS = ["concert", "Concert"]

CONCERT_AUTH_CODE_KEYS = ["認証コード", "招待コード", "authCode", "inviteCode", "auth_code", "invite_code"]
CONCERT_ENDED_KEYS = ["演奏会終了", "終了", "ended", "isEnded", "concertEnded"]
HARMONIA_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
ATLAS_CONCERT_NAME_KEYS = ["演奏会名", "名称", "Title", "title"]

EVENT_TYPE_GROUP_JOIN = "join"
EVENT_TYPE_USER_PENDING_NAME = "pending_name"
EVENT_TYPE_USER_VERIFIED = "verified_user"


def validate_line_signature(body: bytes, signature: str) -> bool:
    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body,
        hashlib.sha256
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature)


def line_headers() -> dict:
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def get_group_summary(group_id: str) -> dict:
    url = f"https://api.line.me/v2/bot/group/{group_id}/summary"
    res = requests.get(url, headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}, timeout=30)
    print(f"group_summary_status={res.status_code} group_summary_body={res.text}", flush=True)
    res.raise_for_status()
    return res.json()


def reply_text_message(reply_token: str, message: str) -> dict:
    url = "https://api.line.me/v2/bot/message/reply"
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": message[:5000]}],
    }
    res = requests.post(url, headers=line_headers(), json=payload, timeout=30)
    print(f"line_reply_status={res.status_code} line_reply_body={res.text} payload={payload}", flush=True)
    res.raise_for_status()
    return res.json() if res.text else {"ok": True}


def validate_push_api_key(request_obj) -> bool:
    provided = request_obj.headers.get("X-HARMONIA-API-KEY", "")
    return hmac.compare_digest(provided, HARMONIA_PUSH_API_KEY)


def push_text_message(destination_id: str, message: str) -> dict:
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": destination_id,
        "messages": [
            {
                "type": "text",
                "text": message[:5000],
            }
        ],
    }

    res = requests.post(url, headers=headers, json=payload, timeout=30)
    print(f"line_push_status={res.status_code} line_push_body={res.text} payload={payload}", flush=True)
    res.raise_for_status()
    if res.text:
        try:
            return res.json()
        except Exception:
            return {"raw": res.text}
    return {"ok": True}


def notion_headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_all(database_id: str, payload: dict | None = None) -> list[dict]:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    start_cursor = None

    while True:
        body = dict(payload or {})
        body.setdefault("page_size", 100)
        if start_cursor:
            body["start_cursor"] = start_cursor

        res = requests.post(url, headers=notion_headers(), json=body, timeout=30)
        print(f"notion_query_all_status={res.status_code} notion_query_all_body={res.text[:500]}", flush=True)
        res.raise_for_status()
        data = res.json() or {}
        all_results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        if not start_cursor:
            break

    return all_results


def notion_get_db_properties(database_id: str) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}"
    res = requests.get(url, headers=notion_headers(), timeout=30)
    print(f"notion_db_status={res.status_code} notion_db_body={res.text[:500]}", flush=True)
    res.raise_for_status()
    return (res.json() or {}).get("properties", {}) or {}


def normalize_key(value: str) -> str:
    return str(value or "").replace(" ", "").replace("_", "").replace("-", "").strip().lower()


def find_prop_name(properties: dict, candidates: list[str], allowed_types: list[str] | None = None) -> str:
    if not properties:
        return ""
    allowed = set(allowed_types or [])
    normalized_map = {normalize_key(k): (k, v) for k, v in properties.items()}

    for cand in candidates:
        hit = normalized_map.get(normalize_key(cand))
        if not hit:
            continue
        key, meta = hit
        if allowed and (meta.get("type") not in allowed):
            continue
        return key
    return ""


def extract_plain_text(prop: dict) -> str:
    if not isinstance(prop, dict):
        return ""
    ptype = prop.get("type")
    if ptype == "title":
        return "".join((x.get("plain_text") or "") for x in (prop.get("title") or [])).strip()
    if ptype == "rich_text":
        return "".join((x.get("plain_text") or "") for x in (prop.get("rich_text") or [])).strip()
    if ptype == "select":
        return str((prop.get("select") or {}).get("name") or "").strip()
    if ptype == "status":
        return str((prop.get("status") or {}).get("name") or "").strip()
    if ptype == "date":
        return str((prop.get("date") or {}).get("start") or "").strip()
    if ptype == "checkbox":
        return "True" if bool(prop.get("checkbox")) else "False"
    if ptype == "formula":
        f = prop.get("formula") or {}
        ftype = f.get("type")
        if ftype == "string":
            return str(f.get("string") or "").strip()
        if ftype == "number":
            num = f.get("number")
            return "" if num is None else str(num)
        if ftype == "boolean":
            return "True" if bool(f.get("boolean")) else "False"
        if ftype == "date":
            return str((f.get("date") or {}).get("start") or "").strip()
    if ptype == "number":
        num = prop.get("number")
        return "" if num is None else str(num)
    return ""


def extract_relation_ids(prop: dict) -> list[str]:
    if not isinstance(prop, dict) or prop.get("type") != "relation":
        return []
    return [r.get("id") for r in (prop.get("relation") or []) if r.get("id")]


def extract_bool(prop: dict) -> bool:
    if not isinstance(prop, dict):
        return False
    if prop.get("type") == "checkbox":
        return bool(prop.get("checkbox"))
    txt = extract_plain_text(prop).strip().lower()
    return txt in ("true", "1", "yes", "on", "checked", "チェック済み")


def build_title_prop(text: str) -> dict:
    return {"title": [{"text": {"content": text[:2000]}}]}


def build_rich_text_prop(text: str) -> dict:
    return {"rich_text": [{"text": {"content": text[:2000]}}]}


def build_select_prop(name: str) -> dict:
    return {"select": {"name": name[:100]}} if name else {"select": None}


def build_relation_prop(ids: list[str]) -> dict:
    seen = set()
    unique_ids = []
    for rid in ids:
        if not rid or rid in seen:
            continue
        seen.add(rid)
        unique_ids.append({"id": rid})
    return {"relation": unique_ids}


def find_existing_group_page(group_id: str) -> dict | None:
    props = notion_get_db_properties(NOTION_DATABASE_ID)
    destination_key = find_prop_name(props, LINE_DESTINATION_ID_KEYS, ["rich_text", "title"])
    source_type_key = find_prop_name(props, LINE_SOURCE_TYPE_KEYS, ["select", "status"])
    if not destination_key:
        return None

    rows = notion_query_all(NOTION_DATABASE_ID)
    for row in rows:
        row_props = row.get("properties", {}) or {}
        destination_value = extract_plain_text(row_props.get(destination_key) or {})
        source_type_value = extract_plain_text(row_props.get(source_type_key) or {}) if source_type_key else ""
        if destination_value == group_id and source_type_value == "group":
            return row
    return None


def create_group_destination_page(event: dict) -> None:
    source = event.get("source", {})
    group_id = source.get("groupId", "")
    source_type = source.get("type", "")
    event_type = event.get("type", "")
    user_id = source.get("userId", "")
    group_name = ""

    if not group_id:
        print("skip_create_group_destination_page: no group_id", flush=True)
        return

    existing = find_existing_group_page(group_id)
    if existing:
        print(f"skip_create_group_destination_page: group already exists group_id={group_id}", flush=True)
        return

    try:
        summary = get_group_summary(group_id)
        group_name = summary.get("groupName", "")
    except Exception as e:
        print(f"failed_to_get_group_summary error={e}", flush=True)

    title_text = group_name if group_name else group_id
    props = notion_get_db_properties(NOTION_DATABASE_ID)

    title_key = find_prop_name(props, LINE_TITLE_KEYS, ["title"])
    destination_key = find_prop_name(props, LINE_DESTINATION_ID_KEYS, ["rich_text", "title"])
    source_type_key = find_prop_name(props, LINE_SOURCE_TYPE_KEYS, ["select", "status"])
    event_type_key = find_prop_name(props, LINE_EVENT_TYPE_KEYS, ["select", "status"])
    user_id_key = find_prop_name(props, LINE_USER_ID_KEYS, ["rich_text", "title"])
    message_text_key = find_prop_name(props, LINE_MESSAGE_TEXT_KEYS, ["rich_text", "title"])
    received_at_key = find_prop_name(props, ["receivedAt", "received_at"], ["date"])
    group_name_key = find_prop_name(props, ["groupName", "group_name"], ["rich_text", "title"])

    notion_props = {}
    if title_key:
        notion_props[title_key] = build_title_prop(title_text)
    if destination_key:
        notion_props[destination_key] = build_title_prop(group_id) if (props.get(destination_key) or {}).get("type") == "title" else build_rich_text_prop(group_id)
    if source_type_key:
        notion_props[source_type_key] = build_select_prop(source_type)
    if event_type_key:
        notion_props[event_type_key] = build_select_prop(event_type)
    if user_id_key:
        notion_props[user_id_key] = build_title_prop(user_id) if (props.get(user_id_key) or {}).get("type") == "title" else build_rich_text_prop(user_id)
    if message_text_key:
        notion_props[message_text_key] = build_title_prop("") if (props.get(message_text_key) or {}).get("type") == "title" else build_rich_text_prop("")
    if received_at_key:
        notion_props[received_at_key] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}
    if group_name_key:
        notion_props[group_name_key] = build_title_prop(group_name) if (props.get(group_name_key) or {}).get("type") == "title" else build_rich_text_prop(group_name)

    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": notion_props}
    print(f"creating_group_destination_page payload={payload}", flush=True)

    res = requests.post("https://api.notion.com/v1/pages", headers=notion_headers(), json=payload, timeout=30)
    print(f"notion_create_group_status={res.status_code} notion_create_group_body={res.text}", flush=True)
    res.raise_for_status()


def find_active_concert_by_auth_code(auth_code: str) -> dict | None:
    code = (auth_code or "").strip()
    if not code:
        return None

    props = notion_get_db_properties(HARMONIA_CONCERT_DB_ID)
    auth_key = find_prop_name(props, CONCERT_AUTH_CODE_KEYS)
    ended_key = find_prop_name(props, CONCERT_ENDED_KEYS)
    if not auth_key:
        print("auth_code_property_not_found", flush=True)
        return None

    rows = notion_query_all(HARMONIA_CONCERT_DB_ID)
    for row in rows:
        row_props = row.get("properties", {}) or {}
        row_code = extract_plain_text(row_props.get(auth_key) or {})
        if row_code != code:
            continue
        if ended_key and extract_bool(row_props.get(ended_key) or {}):
            continue
        return row
    return None


def find_concert_name_from_harmonia_concert_row(harmonia_concert_row: dict) -> str:
    if not harmonia_concert_row:
        return ""

    hc_props = notion_get_db_properties(HARMONIA_CONCERT_DB_ID)
    concert_rel_key = find_prop_name(hc_props, HARMONIA_CONCERT_REL_KEYS, ["relation"])
    if not concert_rel_key:
        return ""

    atlas_ids = extract_relation_ids(
        ((harmonia_concert_row.get("properties", {}) or {}).get(concert_rel_key) or {})
    )
    if not atlas_ids:
        return ""

    atlas_concert_id = atlas_ids[0]

    atlas_rows = notion_query_all(NOTION_DB_ID)
    atlas_row = next((r for r in atlas_rows if r.get("id") == atlas_concert_id), None)
    if not atlas_row:
        return ""

    atlas_props = notion_get_db_properties(NOTION_DB_ID)
    concert_name_key = find_prop_name(atlas_props, ATLAS_CONCERT_NAME_KEYS)
    if not concert_name_key:
        return ""

    return extract_plain_text(
        ((atlas_row.get("properties", {}) or {}).get(concert_name_key) or {})
    ).strip()


def find_user_destination_page(user_id: str) -> dict | None:
    if not user_id:
        return None

    props = notion_get_db_properties(NOTION_DATABASE_ID)
    destination_key = find_prop_name(props, LINE_DESTINATION_ID_KEYS)
    source_type_key = find_prop_name(props, LINE_SOURCE_TYPE_KEYS)
    if not destination_key:
        return None

    rows = notion_query_all(NOTION_DATABASE_ID)
    for row in rows:
        row_props = row.get("properties", {}) or {}
        destination_value = extract_plain_text(row_props.get(destination_key) or {})
        source_type_value = extract_plain_text(row_props.get(source_type_key) or {}) if source_type_key else ""
        if destination_value == user_id and source_type_value == "user":
            return row
    return None


def is_pending_name_user(row: dict) -> bool:
    props = notion_get_db_properties(NOTION_DATABASE_ID)
    event_type_key = find_prop_name(props, LINE_EVENT_TYPE_KEYS)
    if not event_type_key:
        return False
    row_props = row.get("properties", {}) or {}
    return extract_plain_text(row_props.get(event_type_key) or {}) == EVENT_TYPE_USER_PENDING_NAME


def upsert_user_destination_pending_name(user_id: str, auth_code: str, concert_row: dict) -> dict:
    props = notion_get_db_properties(NOTION_DATABASE_ID)

    title_key = find_prop_name(props, LINE_TITLE_KEYS, ["title"])
    destination_key = find_prop_name(props, LINE_DESTINATION_ID_KEYS, ["rich_text", "title"])
    source_type_key = find_prop_name(props, LINE_SOURCE_TYPE_KEYS, ["select", "status"])
    event_type_key = find_prop_name(props, LINE_EVENT_TYPE_KEYS, ["select", "status"])
    user_id_key = find_prop_name(props, LINE_USER_ID_KEYS, ["rich_text", "title"])
    message_text_key = find_prop_name(props, LINE_MESSAGE_TEXT_KEYS, ["rich_text", "title"])
    received_at_key = find_prop_name(props, ["receivedAt", "received_at"], ["date"])
    concert_key = find_prop_name(props, LINE_CONCERT_REL_KEYS, ["relation"])

    existing = find_user_destination_page(user_id)
    concert_id = concert_row.get("id", "")

    if existing:
        row_props = existing.get("properties", {}) or {}
        relation_ids = extract_relation_ids(row_props.get(concert_key) or {}) if concert_key else []
        if concert_id and concert_id not in relation_ids:
            relation_ids.append(concert_id)

        patch_props = {}
        if event_type_key:
            patch_props[event_type_key] = build_select_prop(EVENT_TYPE_USER_PENDING_NAME)
        if message_text_key:
            patch_props[message_text_key] = build_title_prop(auth_code) if (props.get(message_text_key) or {}).get("type") == "title" else build_rich_text_prop(auth_code)
        if received_at_key:
            patch_props[received_at_key] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}
        if concert_key:
            patch_props[concert_key] = build_relation_prop(relation_ids)

        res = requests.patch(
            f"https://api.notion.com/v1/pages/{existing.get('id')}",
            headers=notion_headers(),
            json={"properties": patch_props},
            timeout=30,
        )
        print(f"notion_update_pending_status={res.status_code} notion_update_pending_body={res.text}", flush=True)
        res.raise_for_status()
        return res.json() or existing

    title_text = f"user:{user_id[-6:]}" if user_id else "user"
    create_props = {}
    if title_key:
        create_props[title_key] = build_title_prop(title_text)
    if destination_key:
        create_props[destination_key] = build_title_prop(user_id) if (props.get(destination_key) or {}).get("type") == "title" else build_rich_text_prop(user_id)
    if source_type_key:
        create_props[source_type_key] = build_select_prop("user")
    if event_type_key:
        create_props[event_type_key] = build_select_prop(EVENT_TYPE_USER_PENDING_NAME)
    if user_id_key:
        create_props[user_id_key] = build_title_prop(user_id) if (props.get(user_id_key) or {}).get("type") == "title" else build_rich_text_prop(user_id)
    if message_text_key:
        create_props[message_text_key] = build_title_prop(auth_code) if (props.get(message_text_key) or {}).get("type") == "title" else build_rich_text_prop(auth_code)
    if received_at_key:
        create_props[received_at_key] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}
    if concert_key and concert_id:
        create_props[concert_key] = build_relation_prop([concert_id])

    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": create_props}
    res = requests.post("https://api.notion.com/v1/pages", headers=notion_headers(), json=payload, timeout=30)
    print(f"notion_create_user_pending_status={res.status_code} notion_create_user_pending_body={res.text}", flush=True)
    res.raise_for_status()
    return res.json() or {}


def finalize_user_destination_name(user_row: dict, full_name: str) -> dict:
    props = notion_get_db_properties(NOTION_DATABASE_ID)

    title_key = find_prop_name(props, LINE_TITLE_KEYS, ["title"])
    event_type_key = find_prop_name(props, LINE_EVENT_TYPE_KEYS, ["select", "status"])
    received_at_key = find_prop_name(props, ["receivedAt", "received_at"], ["date"])
    group_name_key = find_prop_name(props, ["groupName", "group_name"], ["rich_text", "title"])

    patch_props = {}
    if title_key:
        patch_props[title_key] = build_title_prop(full_name)
    if group_name_key:
        patch_props[group_name_key] = build_title_prop(full_name) if (props.get(group_name_key) or {}).get("type") == "title" else build_rich_text_prop(full_name)
    if event_type_key:
        patch_props[event_type_key] = build_select_prop(EVENT_TYPE_USER_VERIFIED)
    if received_at_key:
        patch_props[received_at_key] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}

    res = requests.patch(
        f"https://api.notion.com/v1/pages/{user_row.get('id')}",
        headers=notion_headers(),
        json={"properties": patch_props},
        timeout=30,
    )
    print(f"notion_finalize_user_status={res.status_code} notion_finalize_user_body={res.text}", flush=True)
    res.raise_for_status()
    return res.json() or user_row


def process_user_message(event: dict) -> None:
    source = event.get("source", {}) or {}
    message = event.get("message", {}) or {}
    reply_token = event.get("replyToken", "")

    user_id = str(source.get("userId", "")).strip()
    message_type = str(message.get("type", "")).strip()
    text = str(message.get("text", "")).strip()

    if not user_id or message_type != "text" or not text:
        print(f"skip_process_user_message user_id={user_id} message_type={message_type} text={text!r}", flush=True)
        return

    existing = find_user_destination_page(user_id)
    if existing and is_pending_name_user(existing):
        finalize_user_destination_name(existing, text)
        if reply_token:
            reply_text_message(
                reply_token,
                "登録が完了しました。\n以後、この演奏会に関する案内をお送りします。"
            )
        return

    concert = find_active_concert_by_auth_code(text)
    if not concert:
        if reply_token:
            reply_text_message(
                reply_token,
                "認証コードを確認してください。"
            )
        return

    concert_name = find_concert_name_from_harmonia_concert_row(concert)
    if not concert_name:
        concert_name = "演奏会"

    upsert_user_destination_pending_name(user_id, text, concert)
    if reply_token:
        reply_text_message(
            reply_token,
            f"{concert_name}の認証コードを確認しました。\n続けて、お名前をフルネームで送信してください。"
        )


@app.get("/")
def index():
    return "alive", 200


@app.post("/push")
def push():
    if not validate_push_api_key(request):
        print("invalid_push_api_key", flush=True)
        return jsonify({"ok": False, "error": "invalid api key"}), 401

    data = request.get_json(silent=True) or {}
    destination_id = str(data.get("groupId", "")).strip()
    message = str(data.get("message", "")).strip()

    if not destination_id:
        return jsonify({"ok": False, "error": "groupId is required"}), 400

    if not message:
        return jsonify({"ok": False, "error": "message is required"}), 400

    try:
        result = push_text_message(destination_id, message)
        return jsonify({
            "ok": True,
            "groupId": destination_id,
            "result": result,
        }), 200
    except requests.HTTPError as e:
        status_code = getattr(getattr(e, "response", None), "status_code", 500)
        response_text = getattr(getattr(e, "response", None), "text", str(e))
        print(f"push_http_error status={status_code} body={response_text}", flush=True)
        return jsonify({
            "ok": False,
            "error": "line push failed",
            "status_code": status_code,
            "detail": response_text,
        }), status_code
    except Exception as e:
        print(f"push_unexpected_error error={e}", flush=True)
        return jsonify({
            "ok": False,
            "error": "unexpected error",
            "detail": str(e),
        }), 500


@app.post("/webhook")
def webhook():
    signature = request.headers.get("x-line-signature", "")
    body = request.get_data()

    if not validate_line_signature(body, signature):
        print("invalid_signature", flush=True)
        return jsonify({"ok": False, "error": "invalid signature"}), 401

    data = request.get_json(silent=True) or {}
    events = data.get("events", [])

    print(f"events_count={len(events)} payload={data}", flush=True)

    for event in events:
        source = event.get("source", {})
        event_type = event.get("type")

        print(f"event_type={event_type} source_type={source.get('type')} source={source}", flush=True)

        if source.get("type") == "group" and event_type == "join":
            create_group_destination_page(event)
        elif source.get("type") == "user" and event_type == "message":
            process_user_message(event)

    return jsonify({"ok": True}), 200
