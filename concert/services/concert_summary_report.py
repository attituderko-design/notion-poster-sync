"""
concert/services/concert_summary_report.py
演奏会全体サマリPDF生成スクリプト

含まれる情報:
  - 演奏会基本情報
  - 全練習日一覧（日程・会場・出欠人数）
  - 全奏者×全練習日 出欠マトリクス
  - レンタル費用小計（練習日ごと・全体）
  - 入金・活動資金のまとめ（枠のみ）
"""
import io
from collections import defaultdict
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether, PageBreak
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS, CONCERT_VENUE_KEYS,
    CONCERT_ADDRESS_KEYS, CONCERT_MEMO_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS, PRACTICE_VENUE_KEYS,
    PRACTICE_ADDRESS_KEYS, PRACTICE_CONCERT_DAY_KEYS, PRACTICE_CONCERT_REL_KEYS,
    ATT_PLAYER_REL_KEYS, ATT_STATUS_KEYS, ATT_PRACTICE_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    RENTAL_PRACTICE_REL_KEYS, RENTAL_QTY_KEYS, RENTAL_UNIT_PRICE_KEYS,
    RENTAL_CONFIRMED_KEYS, RENTAL_VENDOR_KEYS, RENTAL_ITEM_NAME_KEYS,
    RENTAL_INST_REL_KEYS, RENTAL_COST_TYPE_KEYS,
    PLAYER_NAME_KEYS, INSTRUMENT_NAME_KEYS,
)


def _make_maps_url(address: str) -> str:
    import urllib.parse
    return f"https://maps.google.com/?q={urllib.parse.quote(address)}"


def _make_qr_image(url: str):
    try:
        import qrcode
        import io as _io
        qr = qrcode.QRCode(version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=4, border=2)
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
    from reportlab.platypus import Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    if not address or address == "—":
        return []
    maps_url = _make_maps_url(address)
    qr_buf   = _make_qr_image(maps_url)
    cap_sty  = ParagraphStyle("qrcap2", fontName=font_b, fontSize=8, leading=11, alignment=TA_LEFT)
    url_sty  = ParagraphStyle("qrurl2", fontName=font,   fontSize=6, leading=9,
                               textColor=colors.HexColor("#1a73e8"), alignment=TA_LEFT)
    addr_sty = ParagraphStyle("qradr2", fontName=font,   fontSize=8, leading=11, alignment=TA_LEFT)
    info = [Paragraph(venue or address, cap_sty),
            Paragraph(address, addr_sty),
            Spacer(1, 1*mm),
            Paragraph(maps_url, url_sty)]
    if qr_buf:
        from reportlab.platypus import Image as RLImage
        qr_img = RLImage(qr_buf, width=24*mm, height=24*mm)
        tbl = Table([[qr_img, info]], colWidths=[27*mm, W-27*mm])
        tbl.setStyle(TableStyle([
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("LEFTPADDING",(0,0),(-1,-1),0),
            ("RIGHTPADDING",(0,0),(-1,-1),2),
            ("TOPPADDING",(0,0),(-1,-1),0),
            ("BOTTOMPADDING",(0,0),(-1,-1),0),
        ]))
        return [tbl, Spacer(1, 3*mm)]
    return info + [Spacer(1, 3*mm)]

FONT_PATH_REGULAR = "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"
FONT_PATH_BOLD    = "/usr/share/fonts/opentype/ipafont-gothic/ipagp.ttf"

STATUS_COLORS = {
    "○": colors.HexColor("#C8E6C9"),
    "△": colors.HexColor("#FFF9C4"),
    "×": colors.HexColor("#FFCDD2"),
    "未": colors.HexColor("#F5F5F5"),
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
        "title":   ParagraphStyle("title", alignment=TA_LEFT,  fontName=font_b, fontSize=15, spaceAfter=2),
        "subtitle":ParagraphStyle("sub", alignment=TA_LEFT,    fontName=font,   fontSize=9,  spaceAfter=6,
                                  textColor=colors.HexColor("#555555")),
        "h2":      ParagraphStyle("h2", alignment=TA_LEFT,     fontName=font_b, fontSize=11, spaceBefore=8, spaceAfter=3,
                                  textColor=colors.HexColor("#2C2C6C")),
        "body":    ParagraphStyle("body", alignment=TA_LEFT,   fontName=font,   fontSize=9),
        "cell":    ParagraphStyle("cell", alignment=TA_LEFT,   fontName=font,   fontSize=8,  leading=11),
        "cellb":   ParagraphStyle("cellb", alignment=TA_LEFT,  fontName=font_b, fontSize=8,  leading=11),
        "cellsm":  ParagraphStyle("cellsm", alignment=TA_LEFT, fontName=font,   fontSize=6,  leading=9),
        "cellbsm": ParagraphStyle("cellbsm", alignment=TA_LEFT,fontName=font_b, fontSize=6,  leading=9),
        "small":   ParagraphStyle("small", alignment=TA_LEFT,  fontName=font,   fontSize=7,
                                  textColor=colors.HexColor("#666666")),
        "placeholder": ParagraphStyle("ph", alignment=TA_LEFT, fontName=font,   fontSize=9,
                                      textColor=colors.HexColor("#AAAAAA")),
    }


def _base_style():
    return TableStyle([
        ("FONT",         (0,0), (-1,-1), "IPAGothic",  8),
        ("FONT",         (0,0), (-1, 0), "IPAGothicB", 8),
        ("BACKGROUND",   (0,0), (-1, 0), colors.HexColor("#E8E6F0")),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("ALIGN",        (0,0), (-1,-1), "LEFT"),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",   (0,0), (-1,-1), 2),
        ("BOTTOMPADDING",(0,0), (-1,-1), 2),
        ("LEFTPADDING",  (0,0), (-1,-1), 2),
        ("RIGHTPADDING", (0,0), (-1,-1), 2),
        ("ALIGN",        (0,0), (-1,-1), "LEFT"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F7F6FA")]),
    ])


def generate_concert_summary(ctx: dict, concert_id: str) -> bytes:
    font, font_b = _register_fonts()
    st_map = _styles(font, font_b)
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    # ── データ取得 ────────────────────────────────────────────

    # 演奏会
    concert_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
    concert = next((r for r in concert_rows if r.get("id") == concert_id), {})
    c_name    = ext(concert, CONCERT_NAME_KEYS) or ""
    c_date    = ext(concert, CONCERT_DATE_KEYS) or ""
    c_memo    = ext(concert, CONCERT_MEMO_KEYS) or ""

    # 練習一覧（この演奏会に紐づく）- 会場取得のため先に取得
    all_practices = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    practices = [p for p in all_practices
                 if concert_id in ext_rel(p, PRACTICE_CONCERT_REL_KEYS)]
    practices.sort(key=lambda p: ext(p, PRACTICE_DATE_KEYS) or "9999")

    # 会場・住所はPRACTICEの本番日レコードから取得（ATLASのロケーションは場所型で取れないため）
    c_venue   = ""
    c_address = ""
    for p in practices:
        if ext(p, PRACTICE_CONCERT_DAY_KEYS) == "True":
            c_venue   = ext(p, PRACTICE_VENUE_KEYS) or ""
            c_address = ext(p, PRACTICE_ADDRESS_KEYS) or ""
            break

    # 奏者一覧（演奏会参加者経由）
    participant_rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    concert_parts = [r for r in participant_rows
                     if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    player_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {r.get("id",""): ext(r, PLAYER_NAME_KEYS) or "" for r in player_rows}
    part_to_player = {r.get("id",""): (ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]
                      for r in participant_rows}
    participant_player_ids = []
    for r in concert_parts:
        p_ids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        if p_ids and p_ids[0] not in participant_player_ids:
            participant_player_ids.append(p_ids[0])
    participant_player_ids.sort(key=lambda pid: player_name_map.get(pid, ""))

    # 出欠データ全件
    all_att = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    # practice_id → player_id → status
    att_matrix: dict[str, dict[str, str]] = defaultdict(dict)
    for r in all_att:
        p_rel = ctx["find_prop_name"](ctx["get_prop_types"](ctx["CONCERT_DB_ATTENDANCE"]),
                                       ATT_PRACTICE_REL_KEYS)
        prac_ids = ext_rel(r, ATT_PRACTICE_REL_KEYS)
        if not prac_ids: continue
        raw_player = (ext_rel(r, ATT_PLAYER_REL_KEYS) or [""])[0]
        player_id  = part_to_player.get(raw_player, raw_player)
        status     = ext(r, ATT_STATUS_KEYS) or "—"
        att_matrix[prac_ids[0]][player_id] = status

    # レンタルデータ
    all_rentals = ctx["query_all"](ctx["CONCERT_DB_RENTAL"], None)
    inst_rows = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_name_map = {r.get("id",""): ext(r, INSTRUMENT_NAME_KEYS) or "" for r in inst_rows}
    # practice_id → [rental rows]
    rent_by_practice: dict[str, list] = defaultdict(list)
    for r in all_rentals:
        prac_ids = ext_rel(r, RENTAL_PRACTICE_REL_KEYS)
        if prac_ids:
            rent_by_practice[prac_ids[0]].append(r)

    # ── PDF構築 ──────────────────────────────────────────────
    buf = io.BytesIO()
    # 出欠マトリクスは横長なのでlandscapeを使う
    page_size = landscape(A4)  # 出欠マトリクスのため常に横向き
    W = page_size[0] - 16*mm

    doc = SimpleDocTemplate(
        buf, pagesize=page_size,
        leftMargin=8*mm, rightMargin=8*mm,
        topMargin=10*mm, bottomMargin=10*mm,
    )
    story = []

    # ── 表紙：演奏会基本情報 ─────────────────────────────────
    story.append(Paragraph("ArtéMis HARMONIA　演奏会サマリ", st_map["subtitle"]))
    story.append(Paragraph(c_name, st_map["title"]))
    story.append(HRFlowable(width=W, thickness=1,
                             color=colors.HexColor("#CCCCCC"), spaceAfter=4))

    date_str = c_date[:10] if c_date else "未設定"
    info_data = [
        ["本番日", date_str],
        ["会場",   c_venue   or "—"],
        ["住所",   c_address or "—"],
    ]
    if c_memo:
        info_data.append(["メモ", c_memo])
    info_tbl = Table(
        [[Paragraph(k, st_map["cellb"]), Paragraph(v, st_map["cell"])]
         for k, v in info_data],
        colWidths=[22*mm, W-22*mm],
    )
    info_tbl.setStyle(TableStyle([
        ("FONT",        (0,0),(-1,-1), font,   8),
        ("FONT",        (0,0),(0,-1),  font_b, 8),
        ("GRID",        (0,0),(-1,-1), 0.3, colors.HexColor("#DDDDDD")),
        ("VALIGN",      (0,0),(-1,-1), "TOP"),
        ("TOPPADDING",  (0,0),(-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1),3),
        ("LEFTPADDING", (0,0),(-1,-1), 4),
        ("RIGHTPADDING",(0,0),(-1,-1), 4),
    ]))
    story.append(info_tbl)
    story.append(Spacer(1, 3*mm))
    story.extend(_venue_qr_block(c_address, c_venue, font, font_b, W))

    # ── 全練習日一覧 ─────────────────────────────────────────
    story.append(Paragraph("■ 練習日一覧", st_map["h2"]))
    prac_list_data = [["回", "日付", "会場", "出欠（○/△/×/未）"]]
    for p in practices:
        pid   = p.get("id","")
        pname = ext(p, PRACTICE_NAME_KEYS) or ""
        pdate = (ext(p, PRACTICE_DATE_KEYS) or "")[:10]
        pvenue= ext(p, PRACTICE_VENUE_KEYS) or "—"
        is_cd = ext(p, PRACTICE_CONCERT_DAY_KEYS) == "True"
        disp  = "【本番当日】" if is_cd else pname
        att   = att_matrix.get(pid, {})
        o = sum(1 for v in att.values() if v == "○")
        t = sum(1 for v in att.values() if v == "△")
        x = sum(1 for v in att.values() if v == "×")
        u = len(participant_player_ids) - len(att)
        att_str = f"○{o} / △{t} / ×{x} / 未{u}"
        prac_list_data.append([disp, pdate, pvenue, att_str])

    pl_tbl = Table(
        [[Paragraph(str(c), st_map["cellb"] if i==0 else st_map["cell"]) for c in row]
         for i, row in enumerate(prac_list_data)],
        colWidths=[35*mm, 22*mm, 55*mm, W-112*mm],
        repeatRows=1,
    )
    pl_tbl.setStyle(_base_style())
    story.append(pl_tbl)
    story.append(Spacer(1, 5*mm))

    # ── 出欠マトリクス ───────────────────────────────────────
    _mat_title = Paragraph("■ 出欠マトリクス（全奏者×全練習日）", st_map["h2"])

    # ヘッダー行：練習日略称
    prac_labels_short = []
    for p in practices:
        d = ext(p, PRACTICE_DATE_KEYS) or ""
        is_cd = ext(p, PRACTICE_CONCERT_DAY_KEYS) == "True"
        label = "本番" if is_cd else (d[5:10] if d else "?")
        prac_labels_short.append(label)

    matrix_data = [["奏者"] + prac_labels_short]
    cell_styles = []  # (row, col, status) for coloring

    for row_i, pid in enumerate(participant_player_ids, 1):
        pname = player_name_map.get(pid, pid)
        row = [pname]
        for col_i, p in enumerate(practices, 1):
            pid_ = p.get("id","")
            status = att_matrix.get(pid_, {}).get(pid, "未")
            row.append(status)
            cell_styles.append((row_i, col_i, status))
        matrix_data.append(row)

    # 列幅計算（奏者列固定、残りを等分）
    name_col_w = 28*mm
    n_prac = len(practices)
    prac_col_w = min((W - name_col_w) / max(n_prac, 1), 20*mm)

    mat_tbl = Table(
        [[Paragraph(str(c),
                    st_map["cellbsm"] if (i==0 or j==0) else st_map["cellsm"])
          for j, c in enumerate(row)]
         for i, row in enumerate(matrix_data)],
        colWidths=[name_col_w] + [prac_col_w] * n_prac,
        repeatRows=1,
    )
    mat_sty = _base_style()
    mat_sty.add("ALIGN", (1,0), (-1,-1), "LEFT")
    mat_sty.add("FONT",  (0,0), (0,-1),  font_b, 7)
    # 出欠ごとに背景色
    for row_i, col_i, status in cell_styles:
        bg = STATUS_COLORS.get(status, colors.white)
        mat_sty.add("BACKGROUND", (col_i, row_i), (col_i, row_i), bg)
    mat_tbl.setStyle(mat_sty)
    story.append(KeepTogether([_mat_title, mat_tbl, Spacer(1, 5*mm)]))

    # ── レンタル費用小計 ─────────────────────────────────────
    rent_summary_data = [["練習日", "業者名", "品目", "台数", "単価", "小計", "確定"]]
    total_all = 0
    total_confirmed = 0

    for p in practices:
        pid    = p.get("id","")
        pname  = ext(p, PRACTICE_NAME_KEYS) or ""
        pdate  = (ext(p, PRACTICE_DATE_KEYS) or "")[:10]
        p_label = f"{pname}({pdate})"
        rents  = rent_by_practice.get(pid, [])
        if not rents:
            continue
        for r in rents:
            i_ids   = ext_rel(r, RENTAL_INST_REL_KEYS)
            inst_n  = inst_name_map.get(i_ids[0], "") if i_ids else ""
            item_n  = ext(r, RENTAL_ITEM_NAME_KEYS) or inst_n
            vendor  = ext(r, RENTAL_VENDOR_KEYS) or "—"
            qty_s   = ext(r, RENTAL_QTY_KEYS) or "0"
            price_s = ext(r, RENTAL_UNIT_PRICE_KEYS) or "0"
            conf    = ext(r, RENTAL_CONFIRMED_KEYS) == "True"
            try:
                qty   = int(float(qty_s))
                price = int(float(price_s))
            except Exception:
                qty = price = 0
            subtotal = qty * price
            total_all += subtotal
            if conf:
                total_confirmed += subtotal
            rent_summary_data.append([
                p_label, vendor, item_n,
                str(qty), f"¥{price:,}", f"¥{subtotal:,}",
                "確定" if conf else "見積",
            ])

    if len(rent_summary_data) > 1:
        rent_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if i==0 else st_map["cell"]) for c in row]
             for i, row in enumerate(rent_summary_data)],
            colWidths=[38*mm, 30*mm, 40*mm, 10*mm, 18*mm, 20*mm, 12*mm],
            repeatRows=1,
        )
        rent_tbl.setStyle(_base_style())
        total_para = Paragraph(
            f"合計（全見積）: ¥{total_all:,}　／　確定済み: ¥{total_confirmed:,}",
            st_map["body"]
        )
        story.append(KeepTogether([
            Paragraph("■ レンタル費用小計", st_map["h2"]),
            rent_tbl,
            Spacer(1, 2*mm),
            total_para,
        ]))
    else:
        story.append(Paragraph("レンタル登録がありません。", st_map["small"]))
    story.append(Spacer(1, 5*mm))

    # ── 入金・活動資金のまとめ（枠のみ） ─────────────────────
    # 入金セクション（KeepTogether）
    story.append(Spacer(1, 5*mm))
    story.append(Paragraph("■ 入金・活動資金のまとめ", st_map["h2"]))
    placeholder_data = [
        ["項目", "金額", "備考"],
        ["参加費合計（予定）", "—", ""],
        ["参加費合計（入金済）", "—", ""],
        ["レンタル費用（確定）", f"¥{total_confirmed:,}", ""],
        ["レンタル費用（全見積）", f"¥{total_all:,}", ""],
        ["その他経費", "—", ""],
        ["収支合計", "—", ""],
    ]
    ph_tbl = Table(
        [[Paragraph(str(c), st_map["cellb"] if (i==0 or j==0) else st_map["cell"])
          for j, c in enumerate(row)]
         for i, row in enumerate(placeholder_data)],
        colWidths=[55*mm, 30*mm, W-85*mm],
        repeatRows=1,
    )
    ph_sty = _base_style()
    ph_tbl.setStyle(ph_sty)
    story.append(ph_tbl)
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph("※ 入金データの入力機能は今後実装予定です。", st_map["small"]))

    doc.build(story)
    buf.seek(0)
    return buf.read()
