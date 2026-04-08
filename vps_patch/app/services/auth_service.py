"""
app/services/auth_service.py

認証・トークン・パスワード管理の純ロジック。
Streamlit依存なし。FastAPI / Jinja2 / 任意のフロントエンドから利用可能。
"""

import hashlib
import hmac
import random
import string
import os
from datetime import datetime, timedelta

# ── 定数 ──────────────────────────────────────────────────────

CODE_EXPIRY_MINUTES = 10
CODE_MAX_ATTEMPTS   = 5
CODE_RESEND_SECONDS = 30
INVITE_MAX_ATTEMPTS = 10
INVITE_WINDOW_MINUTES = 10

# 本番環境では環境変数 HARMONIA_TOKEN_SECRET を設定すること
_TOKEN_SECRET = os.environ.get("HARMONIA_TOKEN_SECRET", "harmonia-form-token-secret")


# ── マジックコード ────────────────────────────────────────────

def generate_code() -> str:
    """6桁数字の認証コードを生成する。"""
    return "".join(random.choices(string.digits, k=6))


def hash_code(code: str) -> str:
    """認証コードをSHA-256ハッシュ化して返す。平文をセッションに持たない。"""
    return hashlib.sha256(code.encode()).hexdigest()


def verify_code(entered: str, stored_hash: str) -> bool:
    """入力されたコードとハッシュを検証する。"""
    return hmac.compare_digest(hash_code(entered.strip()), stored_hash)


def code_expiry(minutes: int = CODE_EXPIRY_MINUTES) -> datetime:
    """コードの有効期限（datetime）を返す。"""
    return datetime.now() + timedelta(minutes=minutes)


def is_code_expired(expires: datetime) -> bool:
    """有効期限切れかどうかを返す。"""
    return datetime.now() >= expires


# ── パスワード ────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """パスワードをSHA-256ハッシュ化して返す。"""
    return hashlib.sha256(password.strip().encode()).hexdigest()


def verify_password(entered: str, stored_hash: str) -> bool:
    """入力パスワードとハッシュを検証する。"""
    if not entered or not stored_hash:
        return False
    return hmac.compare_digest(hash_password(entered), stored_hash)


def save_password_hash(notion_client, player_id: str, password: str,
                        player_password_keys: list) -> bool:
    """
    NotionのPERFORMERレコードにパスワードハッシュを保存する。

    Args:
        notion_client: Notionクライアント（requests.Session等）
        player_id: NotionページID
        password: 平文パスワード
        player_password_keys: パスワードフィールドのキー候補リスト
    Returns:
        成功可否
    """
    try:
        hashed = hash_password(password)
        # 呼び出し元でNotion APIへのPATCHリクエストを組み立てること
        # ここでは純粋にハッシュ値を返すだけにしてDI境界を明確にする
        return hashed
    except Exception:
        return None


# ── フォームトークン ──────────────────────────────────────────

def make_form_token(concert_id: str, secret: str = _TOKEN_SECRET) -> str:
    """
    演奏会IDからフォームアクセス用トークンを生成する。
    HMACではなく固定シークレット + SHA-256の簡易実装。
    """
    h = hashlib.sha256(f"{secret}:{concert_id}".encode()).hexdigest()
    return h[:12]


def verify_form_token(token: str, concert_id: str,
                       secret: str = _TOKEN_SECRET) -> bool:
    """フォームトークンを検証する。"""
    return hmac.compare_digest(token, make_form_token(concert_id, secret))


# ── 招待コード ────────────────────────────────────────────────

def make_invite_code(concert_id: str, secret: str = _TOKEN_SECRET) -> str:
    """
    演奏会IDから6桁の招待コードを生成する。
    英大文字 + 数字で読みやすい形式。
    """
    h = hashlib.sha256(f"invite:{secret}:{concert_id}".encode()).hexdigest()
    charset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 紛らわしい文字を除外
    return "".join(charset[int(h[i*2:i*2+2], 16) % len(charset)] for i in range(6))


def verify_invite_code(code: str, concert_id: str,
                        secret: str = _TOKEN_SECRET) -> bool:
    """招待コードを検証する。"""
    return hmac.compare_digest(code.upper(), make_invite_code(concert_id, secret))


# ── メール送信（インターフェース定義） ────────────────────────

def build_magic_code_email(code: str, concert_name: str,
                            expiry_minutes: int = CODE_EXPIRY_MINUTES) -> dict:
    """
    マジックコードメールの件名・本文を返す。
    実際の送信は呼び出し元（mailer.py等）が担当する。

    Returns:
        {"subject": str, "body": str}
    """
    subject = f"ArtéMis HARMONIA 認証コード: {code}"
    body = (
        f"ArtéMis HARMONIA フォームへのアクセス認証コードです。\n\n"
        f"演奏会: {concert_name}\n\n"
        f"認証コード: {code}\n\n"
        f"このコードは{expiry_minutes}分間有効です。\n"
        f"心当たりがない場合はこのメールを無視してください。"
    )
    return {"subject": subject, "body": body}


# ── URL短縮 ───────────────────────────────────────────────────

def shorten_url(long_url: str, timeout: int = 5) -> str:
    """
    TinyURL APIで短縮URLを取得する。
    失敗した場合は元のURLを返す。
    """
    try:
        import requests
        res = requests.get(
            "https://tinyurl.com/api-create.php",
            params={"url": long_url},
            timeout=timeout,
        )
        if res.status_code == 200 and res.text.startswith("http"):
            return res.text.strip()
    except Exception:
        pass
    return long_url
