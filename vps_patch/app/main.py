"""
/home/ubuntu/harmonia_form/main.py
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.routers import form as form_router

BASE_DIR = Path(__file__).parent.parent

app = FastAPI(title="ArtéMis HARMONIA Form")

# セッションミドルウェア（signed cookie）
app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SESSION_SECRET", "change-me"),
    session_cookie="harmonia_session",
    max_age=60 * 60 * 24 * 30,  # 30日
    https_only=True,
    same_site="lax",
)

# 静的ファイル
app.mount("/static", StaticFiles(directory=BASE_DIR / "app" / "static"), name="static")

# ルーター
app.include_router(form_router.router)


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug/notion")
def debug_notion(token: str | None = Query(default=None)):
    # 任意: 本番で公開したくない場合はトークン必須にする
    debug_token = os.environ.get("DEBUG_ADMIN_TOKEN", "")
    if debug_token and token != debug_token:
        raise HTTPException(status_code=403, detail="forbidden")

    from app.services.notion_client import build_concert_ctx

    required = [
        "NOTION_CONCERT_API_KEY",
        "CONCERT_DB_PLAYER",
        "CONCERT_DB_PARTICIPANT",
        "CONCERT_DB_HARMONIA_CONCERT",
        "CONCERT_DB_ATLAS",
    ]
    env_status = {k: bool(os.environ.get(k)) for k in required}

    try:
        ctx = build_concert_ctx()
        player_db = ctx.get("CONCERT_DB_PLAYER", "")
        if not player_db:
            return {
                "ok": False,
                "stage": "config",
                "message": "CONCERT_DB_PLAYER is empty",
                "env": env_status,
            }

        players = ctx["query_all"](player_db, None)
        return {
            "ok": True,
            "stage": "query",
            "env": env_status,
            "player_count": len(players),
        }
    except Exception as e:
        return {
            "ok": False,
            "stage": "exception",
            "env": env_status,
            "error": str(e),
        }
