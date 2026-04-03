import os
import hmac
import hashlib
import base64
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
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


def create_notion_page(event: dict) -> None:
    source = event.get("source", {})
    message = event.get("message", {})

    group_id = source.get("groupId", "")
    source_type = source.get("type", "")
    event_type = event.get("type", "")
    user_id = source.get("userId", "")
    message_text = message.get("text", "") if message.get("type") == "text" else ""

    title_text = group_id if group_id else f"{source_type}:{event_type}"

    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
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
                "rich_text": [
                    {
                        "text": {
                            "content": message_text[:2000]
                        }
                    }
                ]
            },
            "receivedAt": {
                "date": {
                    "start": datetime.now(timezone.utc).isoformat()
                }
            },
        },
    }

    print(f"creating_notion_page payload={payload}", flush=True)

    res = requests.post(url, headers=headers, json=payload, timeout=30)

    print(
        f"notion_response_status={res.status_code} notion_response_body={res.text}",
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
        print(
            f"event_type={event.get('type')} source_type={event.get('source', {}).get('type')} source={event.get('source')}",
            flush=True
        )
        source = event.get("source", {})
        if source.get("type") == "group":
            create_notion_page(event)

    return jsonify({"ok": True}), 200
