"""
concert/services/practice_report.py
練習情報PDF生成スクリプト

含まれる情報:
  - 練習基本情報（日時・会場・住所）
  - タイムスケジュール
  - 出欠一覧
  - 持参楽器一覧
  - レンタル一覧
  - 練習する曲一覧
"""
import io
from collections import defaultdict
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from concert.services.keys import (
    PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS, PRACTICE_VENUE_KEYS,
    PRACTICE_ADDRESS_KEYS, PRACTICE_MEMO_KEYS, PRACTICE_CONCERT_DAY_KEYS,
    PRACTICE_SONG_REL_KEYS,
    SCHEDULE_START_KEYS, SCHEDULE_END_KEYS, SCHEDULE_TYPE_KEYS,
    SCHEDULE_CONTENT_KEYS, SCHEDULE_SONG_REL_KEYS, SCHEDULE_ORDER_KEYS,
    ATT_PLAYER_REL_KEYS, ATT_STATUS_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS,
    PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_BRING_ASSIGN_KEYS,
    PI_OWN_COUNT_KEYS, PI_BRING_COUNT_KEYS, PI_CONCERT_REL_KEYS,
    RENTAL_INST_REL_KEYS, RENTAL_ITEM_NAME_KEYS, RENTAL_VENDOR_KEYS,
    RENTAL_QTY_KEYS, RENTAL_UNIT_PRICE_KEYS, RENTAL_CONFIRMED_KEYS, RENTAL_COST_TYPE_KEYS,
    PLAYER_NAME_KEYS, INSTRUMENT_NAME_KEYS, SONG_NAME_KEYS,
)


def _make_maps_url(address: str) -> str:
    import urllib.parse
    return f"https://maps.google.com/?q={urllib.parse.quote(address)}"


def _make_qr_image(url: str):
    """QRコード画像のBytesIOを返す。qrcodeが使えない場合はNone。"""
    try:
        import qrcode
        import io as _io
        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=4, border=2,
        )
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = _io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf
    except Exception:
        return None


def _venue_qr_block(address: str, venue: str, font, font_b, W):
    """会場情報+QRコードのFlowableリストを返す。"""
    from reportlab.platypus import Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors

    if not address or address == "—":
        return []

    maps_url = _make_maps_url(address)
    qr_buf   = _make_qr_image(maps_url)

    cap_sty = ParagraphStyle("qrcap", fontName=font_b, fontSize=8, leading=11, alignment=TA_LEFT)
    url_sty = ParagraphStyle("qrurl", fontName=font,   fontSize=6, leading=9,
                              textColor=colors.HexColor("#1a73e8"), alignment=TA_LEFT)
    addr_sty= ParagraphStyle("qradr", fontName=font,   fontSize=8, leading=11, alignment=TA_LEFT)

    info = [
        Paragraph(venue or address, cap_sty),
        Paragraph(address, addr_sty),
        Spacer(1, 1*mm),
        Paragraph(maps_url, url_sty),
    ]

    if qr_buf:
        from reportlab.platypus import Image as RLImage
        qr_img = RLImage(qr_buf, width=24*mm, height=24*mm)
        tbl = Table([[qr_img, info]], colWidths=[27*mm, W - 27*mm])
        tbl.hAlign = "LEFT"
        tbl.setStyle(TableStyle([
            ("VALIGN",       (0,0),(-1,-1), "TOP"),
            ("LEFTPADDING",  (0,0),(-1,-1), 0),
            ("RIGHTPADDING", (0,0),(-1,-1), 2),
            ("TOPPADDING",   (0,0),(-1,-1), 0),
            ("BOTTOMPADDING",(0,0),(-1,-1), 0),
        ]))
        return [tbl, Spacer(1, 3*mm)]
    else:
        return info + [Spacer(1, 3*mm)]

FONT_PATH_REGULAR = "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"
FONT_PATH_BOLD    = "/usr/share/fonts/opentype/ipafont-gothic/ipagp.ttf"

# 種別ごとの色
TYPE_COLORS = {
    "練習": colors.HexColor("#E8F0FE"),
    "休憩": colors.HexColor("#FEF9E7"),
    "搬入": colors.HexColor("#EAF7EA"),
    "搬出": colors.HexColor("#FDEDEC"),
    "その他": colors.HexColor("#F5EEF8"),
}


def _register_fonts():
    try:
        pdfmetrics.registerFont(TTFont("IPAGothic",  FONT_PATH_REGULAR))
        pdfmetrics.registerFont(TTFont("IPAGothicB", FONT_PATH_BOLD))
        return "IPAGothic", "IPAGothicB"
    except Exception:
        return "Helvetica", "Helvetica-Bold"


def _styles(font, font_b):
    return {
        "title":   ParagraphStyle("title", alignment=TA_LEFT,   fontName=font_b, fontSize=15, spaceAfter=2),
        "subtitle":ParagraphStyle("sub", alignment=TA_LEFT,     fontName=font,   fontSize=9,  spaceAfter=6,
                                  textColor=colors.HexColor("#555555")),
        "cellb_wht": ParagraphStyle("cellb_wht", alignment=TA_LEFT, fontName=font_b,
                       fontSize=8, leading=11, textColor=colors.white),
        "h2":      ParagraphStyle("h2", alignment=TA_LEFT,      fontName=font_b, fontSize=11, spaceBefore=14, spaceAfter=6,
                                  textColor=colors.HexColor("#2C2C6C")),
        "h3":      ParagraphStyle("h3", alignment=TA_LEFT,      fontName=font_b, fontSize=9,  spaceBefore=14, spaceAfter=6),
        "body":    ParagraphStyle("body", alignment=TA_LEFT,    fontName=font,   fontSize=9),
        "cell":    ParagraphStyle("cell", alignment=TA_LEFT,    fontName=font,   fontSize=8,  leading=11),
        "cellb":   ParagraphStyle("cellb", alignment=TA_LEFT,   fontName=font_b, fontSize=8,  leading=11),
        "small":   ParagraphStyle("small", alignment=TA_LEFT,   fontName=font,   fontSize=7,
                                  textColor=colors.HexColor("#666666")),
    }


def _tbl_style(header_bg="#E8E6F0"):
    return TableStyle([
        ("FONT",         (0,0), (-1,-1), "IPAGothic",  8),
        ("FONT",         (0,0), (-1, 0), "IPAGothicB", 8),
        ("BACKGROUND",   (0,0), (-1, 0), colors.HexColor(header_bg)),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("ALIGN",        (0,0), (-1,-1), "LEFT"),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("ALIGN",        (0,0), (-1,-1), "LEFT"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F7F6FA")]),
    ])


def generate_practice_report(
    ctx: dict,
    practice_id: str,
) -> bytes:
    """
    1つの練習日について練習情報PDFを生成してbytesで返す。
    """
    font, font_b = _register_fonts()
    st_map = _styles(font, font_b)
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    W = A4[0] - 20*mm  # 有効幅

    # ── データ取得 ────────────────────────────────────────────

    # 練習情報
    practice_rows = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    practice = next((r for r in practice_rows if r.get("id") == practice_id), {})
    prac_name    = ext(practice, PRACTICE_NAME_KEYS) or ""
    prac_date    = ext(practice, PRACTICE_DATE_KEYS) or ""
    prac_venue   = ext(practice, PRACTICE_VENUE_KEYS) or ""
    prac_address = ext(practice, PRACTICE_ADDRESS_KEYS) or ""
    prac_memo    = ext(practice, PRACTICE_MEMO_KEYS) or ""
    is_concert_day = ext(practice, PRACTICE_CONCERT_DAY_KEYS) == "True"

    # 演奏会情報
    concert_id = ""
    from concert.services.keys import PRACTICE_CONCERT_REL_KEYS
    c_ids = ext_rel(practice, PRACTICE_CONCERT_REL_KEYS)
    concert_id = c_ids[0] if c_ids else ""
    concert_name = ""
    if concert_id:
        concert_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
        from concert.services.keys import CONCERT_NAME_KEYS
        concert = next((r for r in concert_rows if r.get("id") == concert_id), {})
        concert_name = ext(concert, CONCERT_NAME_KEYS) or ""

    # 練習する曲
    practice_song_ids = ext_rel(practice, PRACTICE_SONG_REL_KEYS)
    all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    song_name_map = {s.get("id"): ext(s, SONG_NAME_KEYS) or "" for s in all_songs}
    if practice_song_ids:
        practice_songs = [song_name_map.get(sid, "") for sid in practice_song_ids if song_name_map.get(sid)]
    else:
        # 未設定の場合は演奏会の全曲
        from concert.services.keys import SONG_CONCERT_REL_KEYS
        practice_songs = [ext(s, SONG_NAME_KEYS) or "" for s in all_songs
                          if concert_id in ext_rel(s, SONG_CONCERT_REL_KEYS)]

    # スケジュール
    sched_t    = ctx["get_prop_types"](ctx["CONCERT_DB_SCHEDULE"])
    sched_rel  = ctx["find_prop_name"](sched_t, SCHEDULE_ORDER_KEYS)
    sched_rows = ctx["query_all"](ctx["CONCERT_DB_SCHEDULE"],
                                   {"filter": {"property": ctx["find_prop_name"](sched_t, ["練習", "FK練習"]),
                                               "relation": {"contains": practice_id}}}
                                   if ctx["find_prop_name"](sched_t, ["練習", "FK練習"]) else None)
    def _sched_order(r):
        v = ext(r, SCHEDULE_ORDER_KEYS)
        try: return int(float(v)) if v else 9999
        except: return 9999
    def _sched_key(r):
        v = ctx["extract_prop_text_any"](r, SCHEDULE_ORDER_KEYS)
        try:
            if v: return (int(float(v)), "")
        except: pass
        t = ctx["extract_prop_text_any"](r, SCHEDULE_START_KEYS) or "99:99"
        return (9999, t)
    sched_rows = sorted(sched_rows, key=_sched_key)

    # 出欠
    att_t   = ctx["get_prop_types"](ctx["CONCERT_DB_ATTENDANCE"])
    att_rel = ctx["find_prop_name"](att_t, ["練習", "FK練習"])
    att_rows = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"],
                                 {"filter": {"property": att_rel, "relation": {"contains": practice_id}}}
                                 if att_rel else None)
    # 演奏会参加者ID→出演者IDマップ
    participant_rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    part_to_player = {r.get("id",""): (ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]
                      for r in participant_rows}
    # 全奏者
    player_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {r.get("id",""): ext(r, PLAYER_NAME_KEYS) or "" for r in player_rows}

    # 出欠マップ: player_id → status
    att_map: dict[str, str] = {}
    for r in att_rows:
        raw_ids = ext_rel(r, ATT_PLAYER_REL_KEYS)
        if not raw_ids: continue
        pid = part_to_player.get(raw_ids[0], raw_ids[0])
        att_map[pid] = ext(r, ATT_STATUS_KEYS) or "—"

    # 参加者（演奏会参加者DB経由）
    from concert.services.keys import PARTICIPANT_CONCERT_REL_KEYS, PARTICIPANT_PART_KEYS
    concert_participants = [r for r in participant_rows
                             if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    participant_player_ids = []
    for r in concert_participants:
        p_ids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        if p_ids: participant_player_ids.append(p_ids[0])

    # 持参楽器（演奏会×出席者）
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_name_map = {r.get("id",""): ext(r, INSTRUMENT_NAME_KEYS) or "" for r in inst_rows}

    pi_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    # 演奏会フィルタ
    pi_rows = [r for r in pi_rows
               if not concert_id or concert_id in ext_rel(r, PI_CONCERT_REL_KEYS)]
    # 持参担当フラグTrueのみ（担当が未設定の場合は持参可フラグにフォールバック）
    bring_rows = [r for r in pi_rows if ext(r, PI_BRING_ASSIGN_KEYS) == "True"]

    # レンタル
    from concert.services.keys import RENTAL_PRACTICE_REL_KEYS
    rent_t   = ctx["get_prop_types"](ctx["CONCERT_DB_RENTAL"])
    rent_rel = ctx["find_prop_name"](rent_t, RENTAL_PRACTICE_REL_KEYS)
    rent_rows = ctx["query_all"](ctx["CONCERT_DB_RENTAL"],
                                  {"filter": {"property": rent_rel, "relation": {"contains": practice_id}}}
                                  if rent_rel else None)

    # ── PDF構築 ──────────────────────────────────────────────

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=10*mm, rightMargin=10*mm,
        topMargin=10*mm, bottomMargin=10*mm,
    )
    story = []

    # タイトル
    prefix = "本番当日" if is_concert_day else "練習情報PDF"
    story.append(Paragraph(f"ArtéMis HARMONIA　{prefix}", st_map["subtitle"]))
    story.append(Spacer(1, 2*mm))
    title_str = ("【本番当日】" if is_concert_day else "") + prac_name
    story.append(Paragraph(title_str, st_map["title"]))
    story.append(Spacer(1, 3*mm))
    if concert_name:
        story.append(Paragraph(concert_name, st_map["subtitle"]))
        story.append(Spacer(1, 2*mm))
    story.append(HRFlowable(width=W, thickness=1, color=colors.HexColor("#CCCCCC"), spaceAfter=0))
    story.append(Spacer(1, 4*mm))

    # 基本情報
    date_str = prac_date[:16].replace("T", "　") if prac_date else "未設定"
    info_data = [["日時", date_str]]
    if prac_memo:
        info_data.append(["メモ", prac_memo])
    info_tbl = Table([[Paragraph(k, st_map["cellb"]), Paragraph(v, st_map["cell"])]
                       for k, v in info_data],
                      colWidths=[20*mm, W-20*mm])
    info_tbl.hAlign = "LEFT"
    info_tbl.setStyle(TableStyle([
        ("FONT",        (0,0), (-1,-1), font,  8),
        ("FONT",        (0,0), (0,-1),  font_b,8),
        ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#DDDDDD")),
        ("VALIGN",      (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",  (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1), 3),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING",(0,0), (-1,-1), 4),
    ]))
    story.append(info_tbl)
    story.append(Spacer(1, 3*mm))
    story.append(Spacer(1, 2*mm))
    # 会場情報 + QRコード
    story.extend(_venue_qr_block(prac_address, prac_venue, font, font_b, W))

    # 練習曲一覧
    if practice_songs:
        story.append(Paragraph("■ 練習曲", st_map["h2"]))
        songs_str = "　／　".join(practice_songs)
        story.append(Paragraph(songs_str, st_map["body"]))
        story.append(Spacer(1, 3*mm))

    # タイムスケジュール（データなしでも表示）
    _h_sched = Paragraph("■ タイムスケジュール", st_map["h2"])
    if not sched_rows:
        story.append(KeepTogether([_h_sched, Paragraph("スケジュールが登録されていません。", st_map["small"])]))
        story.append(Spacer(1, 3*mm))
    if sched_rows:
        sched_data = [["開始", "終了", "種別", "内容"]]
        sched_row_colors = [colors.HexColor("#E8E6F0")]
        for r in sched_rows:
            s_start   = ext(r, SCHEDULE_START_KEYS) or ""
            s_end     = ext(r, SCHEDULE_END_KEYS) or ""
            s_type    = ext(r, SCHEDULE_TYPE_KEYS) or ""
            s_content = ext(r, SCHEDULE_CONTENT_KEYS) or ""
            s_song_ids = ext_rel(r, SCHEDULE_SONG_REL_KEYS)
            if s_song_ids and not s_content:
                s_content = song_name_map.get(s_song_ids[0], "")
            elif s_song_ids:
                s_content = f"{s_content}（{song_name_map.get(s_song_ids[0], '')}）"
            sched_data.append([s_start, s_end, s_type, s_content])
            sched_row_colors.append(TYPE_COLORS.get(s_type, colors.white))

        sched_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if i == 0 else st_map["cell"])
              for c in row]
             for i, row in enumerate(sched_data)],
            colWidths=[18*mm, 18*mm, 18*mm, W-54*mm],
            repeatRows=1,
        )
        sched_style = _tbl_style()
        for i, bg in enumerate(sched_row_colors):
            sched_style.add("BACKGROUND", (0,i), (-1,i), bg)
        sched_tbl.hAlign = "LEFT"
        sched_tbl.setStyle(sched_style)
        story.append(KeepTogether([_h_sched, Spacer(1, 1*mm)]))
        story.append(sched_tbl)
        story.append(Spacer(1, 3*mm))

    # 出欠一覧（パート順ソート・パート列追加）
    _h_att = Paragraph("■ 出欠一覧", st_map["h2"])

    # パート情報取得（PART_MASTERリレーション経由）
    from concert.services.part_master_utils import load_part_master_map, build_player_part_map
    pm_map_pr = load_part_master_map(ctx)
    player_part_map_pr = build_player_part_map(ctx, concert_participants, pm_map_pr)
    # パート→氏名順でソート
    sorted_pids = sorted(participant_player_ids,
                         key=lambda pid: (player_part_map_pr.get(pid,"zzz"), player_name_map.get(pid,"")))

    att_data = [["パート", "奏者", "参加可否"]]
    cur_part_pr = None
    for pid in sorted_pids:
        pname  = player_name_map.get(pid, pid)
        status = att_map.get(pid, "未回答")
        part   = player_part_map_pr.get(pid, "")
        part_disp = part if part != cur_part_pr else ""
        if part != cur_part_pr:
            cur_part_pr = part
        att_data.append([part_disp, pname, status])

    if len(att_data) > 1:
        att_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if i == 0 else st_map["cell"])
              for c in row]
             for i, row in enumerate(att_data)],
            colWidths=[16*mm, 40*mm, 20*mm],
            repeatRows=1,
        )
        sty = _tbl_style()
        sty.add("BACKGROUND", (0,1), (0,-1), colors.HexColor("#F0EEF8"))
        for i, row in enumerate(att_data[1:], 1):
            status = row[2]
            if status == "○":
                sty.add("BACKGROUND", (2,i), (2,i), colors.HexColor("#EAF7EA"))
            elif status == "×":
                sty.add("BACKGROUND", (2,i), (2,i), colors.HexColor("#FDEDEC"))
            elif status == "△":
                sty.add("BACKGROUND", (2,i), (2,i), colors.HexColor("#FEF9E7"))
        att_tbl.hAlign = "LEFT"
        att_tbl.setStyle(sty)
        story.append(KeepTogether([_h_att, Spacer(1, 1*mm)]))
        story.append(att_tbl)
    else:
        story.append(KeepTogether([_h_att, Paragraph("出欠データがありません。", st_map["small"])]))
    story.append(Spacer(1, 3*mm))

    # 持参楽器一覧（出席者のみ）
    attending_pids = {pid for pid, s in att_map.items() if s in ("○", "△")}
    bring_items = []
    for r in bring_rows:
        p_ids = ext_rel(r, PI_PLAYER_REL_KEYS)
        if not p_ids or p_ids[0] not in attending_pids:
            continue
        i_ids = ext_rel(r, PI_INST_REL_KEYS)
        if not i_ids: continue
        # 持参台数（担当者が実際に持ってくる台数）
        cnt_str = ext(r, PI_BRING_COUNT_KEYS)
        try: cnt = int(float(cnt_str)) if cnt_str else 1
        except: cnt = 1
        bring_items.append({
            "player": player_name_map.get(p_ids[0], ""),
            "inst":   inst_name_map.get(i_ids[0], ""),
            "count":  cnt,
        })

    _h_bring = Paragraph("■ 打楽器奏者持参楽器一覧", st_map["h2"])
    if bring_items:
        bring_data = [["奏者", "楽器", "台数"]]
        for b in sorted(bring_items, key=lambda x: x["player"]):
            bring_data.append([b["player"], b["inst"], str(b["count"])])
        bring_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if i == 0 else st_map["cell"])
              for c in row]
             for i, row in enumerate(bring_data)],
            colWidths=[50*mm, 60*mm, 15*mm],
            repeatRows=1,
        )
        bring_tbl.hAlign = "LEFT"
        bring_tbl.setStyle(_tbl_style())
        story.append(KeepTogether([_h_bring, Spacer(1, 1*mm)]))
        story.append(bring_tbl)
    else:
        story.append(KeepTogether([_h_bring, Paragraph("持参楽器の登録がありません。", st_map["small"])]))
    story.append(Spacer(1, 3*mm))

    # レンタル一覧
    _h_rent = Paragraph("■ レンタル一覧", st_map["h2"])
    if rent_rows:
        rent_data = [["業者名", "品目", "台数", "単価", "小計", "確定"]]
        rent_total = rent_confirmed = 0
        for r in rent_rows:
            i_ids     = ext_rel(r, RENTAL_INST_REL_KEYS)
            inst_n    = inst_name_map.get(i_ids[0], "") if i_ids else ""
            item_n    = ext(r, RENTAL_ITEM_NAME_KEYS) or inst_n
            vendor    = ext(r, RENTAL_VENDOR_KEYS) or "—"
            qty_str   = ext(r, RENTAL_QTY_KEYS)        or "0"
            price_str = ext(r, RENTAL_UNIT_PRICE_KEYS) or "0"
            confirmed = ext(r, RENTAL_CONFIRMED_KEYS) == "True"
            try: qty   = int(float(qty_str))
            except: qty = 0
            try: price = int(float(price_str))
            except: price = 0
            subtotal = qty * price
            rent_total += subtotal
            if confirmed:
                rent_confirmed += subtotal
            rent_data.append([
                vendor, item_n, str(qty),
                f"¥{price:,}", f"¥{subtotal:,}",
                "確定" if confirmed else "見積"
            ])
        rent_data.append(["合計（確定）", "", "", "", f"¥{rent_confirmed:,}", ""])
        rent_data.append(["合計（全見積）", "", "", "", f"¥{rent_total:,}", ""])
        rent_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if (i == 0 or (i >= len(rent_data)-2 and j==0)) else st_map["cell"])
              for j, c in enumerate(row)]
             for i, row in enumerate(rent_data)],
            colWidths=[35*mm, 45*mm, 12*mm, 20*mm, 22*mm, 12*mm],
            repeatRows=1,
        )
        rent_sty = _tbl_style()
        for i in range(len(rent_data)-2, len(rent_data)):
            rent_sty.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#F0EEF8"))
        rent_tbl.hAlign = "LEFT"
        rent_tbl.setStyle(rent_sty)
        story.append(KeepTogether([_h_rent, Spacer(1, 1*mm)]))
        story.append(rent_tbl)
    else:
        story.append(KeepTogether([_h_rent, Paragraph("レンタル登録がありません。", st_map["small"])]))

    doc.build(story)
    buf.seek(0)
    return buf.read()
