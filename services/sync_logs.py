def build_update_log(
    log_title,
    src,
    need_notion,
    notion_ok,
    need_drive,
    drive_ok,
    meta_ok,
    updated,
    is_refresh=False,
) -> str:
    parts = []
    if is_refresh:
        parts.append("🔄 リフレッシュ")
    if need_notion:
        parts.append("Notion " + ("✅" if notion_ok else "❌"))
    if need_drive:
        parts.append("Drive " + ("✅" if drive_ok else "❌"))
    if updated:
        parts.append("メタデータ[" + " / ".join(updated) + "] " + ("✅" if meta_ok else "❌"))
    if not parts:
        return f"⏸️ 維持(OK): {log_title}"
    return f"{log_title}　{src}　{'　'.join(parts)}"
