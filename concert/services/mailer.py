"""
concert/services/mailer.py
SMTP経由でPDFを添付してメール送信するモジュール。
"""
from __future__ import annotations
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dataclasses import dataclass


@dataclass
class MailConfig:
    host: str
    port: int
    user: str
    password: str
    from_addr: str


@dataclass
class SendResult:
    success: bool
    sent: list[str]       # 送信成功アドレス
    failed: list[str]     # 送信失敗アドレス
    errors: list[str]     # エラーメッセージ


def _load_config(ctx: dict) -> MailConfig:
    """secrets.tomlからSMTP設定を読み込む。"""
    import streamlit as st
    return MailConfig(
        host      = st.secrets.get("SMTP_HOST",     "smtp.gmail.com"),
        port      = int(st.secrets.get("SMTP_PORT", 587)),
        user      = st.secrets.get("SMTP_USER",     ""),
        password  = st.secrets.get("SMTP_PASSWORD", ""),
        from_addr = st.secrets.get("SMTP_FROM",     ""),
    )


def _build_message(
    cfg: MailConfig,
    to_addr: str,
    subject: str,
    body: str,
    pdf_bytes: bytes | None = None,
    pdf_filename: str = "attachment.pdf",
) -> MIMEMultipart:
    """MIMEメッセージを構築する。"""
    msg = MIMEMultipart()
    msg["From"]    = cfg.from_addr
    msg["To"]      = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    if pdf_bytes:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(pdf_bytes)
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{pdf_filename}"',
        )
        msg.attach(part)

    return msg


def send_pdf_to_all(
    ctx: dict,
    recipients: list[dict],   # [{"name": str, "email": str}, ...]
    subject: str,
    body: str,
    pdf_bytes: bytes,
    pdf_filename: str = "attachment.pdf",
) -> SendResult:
    """
    全受信者にPDFを添付してメール送信する。
    1件ずつ個別送信（BCCなし・宛名が個別に入れられる）。

    Parameters
    ----------
    recipients : list[dict]
        送信先リスト。{"name": 氏名, "email": メールアドレス}
    """
    cfg = _load_config(ctx)
    if not cfg.user or not cfg.password:
        return SendResult(False, [], [], ["SMTP認証情報が設定されていません。secrets.tomlを確認してください。"])

    sent, failed, errors = [], [], []

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(cfg.host, cfg.port, timeout=15) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(cfg.user, cfg.password)

            for r in recipients:
                email = r.get("email", "").strip()
                name  = r.get("name", "")
                if not email:
                    failed.append(f"{name}（メールアドレス未登録）")
                    continue
                try:
                    # 本文の宛名を個別に置換
                    personal_body = body.replace("{name}", name)
                    msg = _build_message(cfg, email, subject, personal_body,
                                         pdf_bytes, pdf_filename)
                    server.sendmail(cfg.user, email, msg.as_string())
                    sent.append(email)
                except smtplib.SMTPException as e:
                    failed.append(email)
                    errors.append(f"{email}: {e}")

    except smtplib.SMTPAuthenticationError:
        return SendResult(False, [], [], ["SMTP認証に失敗しました。アプリパスワードを確認してください。"])
    except (smtplib.SMTPException, OSError) as e:
        return SendResult(False, [], [], [f"SMTPサーバーへの接続に失敗しました: {e}"])

    return SendResult(
        success = len(failed) == 0,
        sent    = sent,
        failed  = failed,
        errors  = errors,
    )


def get_recipients_from_players(ctx: dict, players: list[dict]) -> list[dict]:
    """
    playersリストから送信対象（受信=True かつ メールアドレス登録済み）を抽出する。
    受信フラグが未設定（None）の場合はオプトアウト扱い（対象外）。
    """
    from concert.services.keys import PLAYER_EMAIL_KEYS, PLAYER_RECEIVE_KEYS
    result = []
    for p in players:
        # 受信フラグ確認（チェックボックス型）
        receive = ctx["extract_prop_text_any"](p, PLAYER_RECEIVE_KEYS)
        # Notionのチェックボックスはtrue/falseの文字列で返ることがある
        if isinstance(receive, str):
            receive = receive.lower() in ("true", "1", "yes", "はい", "○")
        elif receive is None:
            receive = False  # 未設定はオプトアウト
        if not receive:
            continue
        email = ctx["extract_prop_text_any"](p, PLAYER_EMAIL_KEYS) or ""
        name  = ctx["extract_prop_text_any"](p, ["氏名", "名前", "表示名", "タイトル"]) or p.get("id", "")
        result.append({"name": name, "email": email.strip()})
    return result
