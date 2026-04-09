"""
/home/ubuntu/harmonia_form/main.py
"""
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from fastapi import FastAPI, HTTPException, Query
from fastapi import Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
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


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    ms = int((time.perf_counter() - t0) * 1000)
    response.headers["X-Server-Time-Ms"] = str(ms)
    if os.environ.get("FORM_PERF_LOG", "").strip().lower() in ("1", "true", "yes", "on"):
        print(f"[perf] {request.method} {request.url.path} -> {response.status_code} {ms}ms")
    return response


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/manifest.webmanifest")
def manifest():
    path = BASE_DIR / "app" / "static" / "manifest.webmanifest"
    return FileResponse(path, media_type="application/manifest+json")


@app.get("/sw.js")
def sw():
    path = BASE_DIR / "app" / "static" / "sw.js"
    resp = FileResponse(path, media_type="application/javascript")
    resp.headers["Service-Worker-Allowed"] = "/"
    resp.headers["Cache-Control"] = "no-cache"
    return resp

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
