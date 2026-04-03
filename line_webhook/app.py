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
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

NOTION_VERSION = "2022-06-28"


def validate_line_signature(body: bytes, signature: str) -> bool:
    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body,
        hashlib.sha256
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature)


def get_group_summary(group_id: str) -> dict:
    url = f"https://api.line.me/v2/bot/group/{group_id}/summary"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }

    res = requests.get(url, headers=headers, timeout=30)

    print(
        f"group_summary_status={res.status_code} group_summary_body={res.text}",
        flush=True
    )

    res.raise_for_status()
    return res.json()


def notion_headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def find_existing_group_page(group_id: str) -> dict | None:
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "groupId",
            "rich_text": {
                "equals": group_id
            }
        },
        "page_size": 1
    }

    res = requests.post(url, headers=notion_headers(), json=payload, timeout=30)

    print(
        f"notion_query_status={res.status_code} notion_query_body={res.text}",
        flush=True
    )

    res.raise_for_status()

    data = res.json()
    results = data.get("results", [])
    return results[0] if results else None


def create_notion_page(event: dict) -> None:
    source = event.get("source", {})

    group_id = source.get("groupId", "")
    source_type = source.get("type", "")
    event_type = event.get("type", "")
    user_id = source.get("userId", "")
    group_name = ""

    if not group_id:
        print("skip_create_notion_page: no group_id", flush=True)
        return

    existing = find_existing_group_page(group_id)
    if existing:
        print(f"skip_create_notion_page: group already exists group_id={group_id}", flush=True)
        return

    try:
        summary = get_group_summary(group_id)
        group_name = summary.get("groupName", "")
    except Exception as e:
        print(f"failed_to_get_group_summary error={e}", flush=True)

    title_text = group_name if group_name else group_id

    payload = {
        "parent": {
            "database_id": NOTION_DATABASE_ID
        },
        "properties": {
            "Title": {
                "title": [
                    {
                        "text": {
                            "content": title_text[:2000]
                        }
                    }
                ]
            },
            "groupId": {
                "rich_text": [
                    {
                        "text": {
                            "content": group_id[:2000]
                        }
                    }
                ]
            },
            "groupName": {
                "rich_text": [
                    {
                        "text": {
                            "content": group_name[:2000]
                        }
                    }
                ]
            },
            "sourceType": {
                "select": {"name": source_type} if source_type else None
            },
            "eventType": {
                "select": {"name": event_type} if event_type else None
            },
            "userId": {
                "rich_text": [
                    {
                        "text": {
                            "content": user_id[:2000]
                        }
                    }
                ]
            },
            "messageText": {
                "rich_text": []
            },
            "receivedAt": {
                "date": {
                    "start": datetime.now(timezone.utc).isoformat()
                }
            },
        },
    }

    print(f"creating_notion_page payload={payload}", flush=True)

    url = "https://api.notion.com/v1/pages"
    res = requests.post(url, headers=notion_headers(), json=payload, timeout=30)

    print(
        f"notion_create_status={res.status_code} notion_create_body={res.text}",
        flush=True
    )

    res.raise_for_status()


@app.get("/")
def index():
    return "alive", 200


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

        print(
            f"event_type={event_type} source_type={source.get('type')} source={source}",
            flush=True
        )

        if source.get("type") == "group" and event_type == "join":
            create_notion_page(event)

    return jsonify({"ok": True}), 200
