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
    CONCERT_CONDUCTOR_KEYS, CONCERT_SOLOIST_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS, PRACTICE_VENUE_KEYS,
    PRACTICE_ADDRESS_KEYS, PRACTICE_CONCERT_DAY_KEYS, PRACTICE_CONCERT_REL_KEYS,
    ATT_PLAYER_REL_KEYS, ATT_STATUS_KEYS, ATT_PRACTICE_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    RENTAL_PRACTICE_REL_KEYS, RENTAL_QTY_KEYS, RENTAL_UNIT_PRICE_KEYS,
    RENTAL_CONFIRMED_KEYS, RENTAL_VENDOR_KEYS, RENTAL_ITEM_NAME_KEYS,
    RENTAL_INST_REL_KEYS, RENTAL_COST_TYPE_KEYS,
    PLAYER_NAME_KEYS, INSTRUMENT_NAME_KEYS,
    SONG_NAME_KEYS, SONG_CONCERT_REL_KEYS, SONG_COMPOSER_KEYS,
    PARTICIPANT_CONCERT_REL_KEYS, PARTICIPANT_FEE_KEYS, PARTICIPANT_PAID_KEYS,
    PARTICIPANT_PART_KEYS, CONCERT_CONFIRMED_FEE_KEYS,
    EXPENSE_CONCERT_REL_KEYS, EXPENSE_TYPE_KEYS, EXPENSE_AMOUNT_KEYS,
    EXPENSE_CONFIRMED_KEYS,
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
        tbl.hAlign = "LEFT"
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
        "cellb_wht": ParagraphStyle("cellb_wht", alignment=TA_LEFT, fontName=font_b,
                       fontSize=8, leading=11, textColor=colors.white),
        "h2":      ParagraphStyle("h2", alignment=TA_LEFT,     fontName=font_b, fontSize=11, spaceBefore=14, spaceAfter=6,
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
    c_name      = ext(concert, CONCERT_NAME_KEYS)      or ""
    c_date      = ext(concert, CONCERT_DATE_KEYS)      or ""
    c_memo      = ext(concert, CONCERT_MEMO_KEYS)      or ""
    c_conductor = ext(concert, CONCERT_CONDUCTOR_KEYS) or ""
    c_soloist   = ext(concert, CONCERT_SOLOIST_KEYS)   or ""

    # 演奏曲目
    all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    concert_songs = [s for s in all_songs
                     if concert_id in ext_rel(s, SONG_CONCERT_REL_KEYS)]
    concert_songs.sort(key=lambda s: ext(s, SONG_NAME_KEYS) or "")

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
    # player_id → パート名のマップ
    player_part_map: dict[str, str] = {}
    participant_player_ids = []
    for r in concert_parts:
        p_ids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        if p_ids and p_ids[0] not in participant_player_ids:
            participant_player_ids.append(p_ids[0])
            player_part_map[p_ids[0]] = ext(r, PARTICIPANT_PART_KEYS) or ""
    # パート→氏名順でソート
    participant_player_ids.sort(key=lambda pid: (
        player_part_map.get(pid, "zzz"),
        player_name_map.get(pid, "")
    ))

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
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(c_name, st_map["title"]))
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width=W, thickness=1,
                             color=colors.HexColor("#CCCCCC"), spaceAfter=0))
    story.append(Spacer(1, 4*mm))

    date_str = c_date[:10] if c_date else "未設定"
    info_data = [
        ["本番日", date_str],
        ["会場",   c_venue     or "—"],
        ["住所",   c_address   or "—"],
    ]
    if c_conductor:
        info_data.append(["指揮", c_conductor])
    if c_soloist:
        info_data.append(["ソリスト", c_soloist])
    if concert_songs:
        songs_str = "　/　".join(
            f"{ext(s, SONG_NAME_KEYS) or ''}（{ext(s, SONG_COMPOSER_KEYS) or ''}）".strip("（）")
            for s in concert_songs
        )
        info_data.append(["演奏曲目", songs_str])
    if c_memo:
        info_data.append(["メモ", c_memo])
    info_tbl = Table(
        [[Paragraph(k, st_map["cellb"]), Paragraph(v, st_map["cell"])]
         for k, v in info_data],
        colWidths=[22*mm, W-22*mm],
    )
    info_tbl.hAlign = "LEFT"
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
    _h_prac = Paragraph("■ 練習日一覧", st_map["h2"])
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
    pl_tbl.hAlign = "LEFT"
    pl_tbl.setStyle(_base_style())
    story.append(KeepTogether([_h_prac, Spacer(1, 1*mm)]))
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

    matrix_data = [["パート", "奏者"] + prac_labels_short]
    cell_styles = []  # (row, col, status) for coloring

    cur_part = None
    for row_i, pid in enumerate(participant_player_ids, 1):
        pname = player_name_map.get(pid, pid)
        part  = player_part_map.get(pid, "")
        # パートが変わったら表示、同じパートは空欄
        part_disp = part if part != cur_part else ""
        if part != cur_part:
            cur_part = part
        row = [part_disp, pname]
        for col_i, p in enumerate(practices, 1):
            pid_ = p.get("id","")
            status = att_matrix.get(pid_, {}).get(pid, "未")
            row.append(status)
            cell_styles.append((row_i, col_i + 1, status))  # +1でパート列分オフセット
        matrix_data.append(row)

    # 列幅計算（パート列・奏者列固定、残りを等分）
    part_col_w = 16*mm
    name_col_w = 24*mm
    n_prac = len(practices)
    prac_col_w = min((W - part_col_w - name_col_w) / max(n_prac, 1), 20*mm)

    mat_tbl = Table(
        [[Paragraph(str(c),
                    st_map["cellbsm"] if (i==0 or j<=1) else st_map["cellsm"])
          for j, c in enumerate(row)]
         for i, row in enumerate(matrix_data)],
        colWidths=[part_col_w, name_col_w] + [prac_col_w] * n_prac,
        repeatRows=1,
    )
    mat_sty = _base_style()
    mat_sty.add("ALIGN", (2,0), (-1,-1), "CENTER")
    mat_sty.add("FONT",  (0,0), (1,-1),  font_b, 7)
    # パート列：同一パートをまとめて見せるため薄い背景
    mat_sty.add("BACKGROUND", (0,1), (0,-1), colors.HexColor("#F0EEF8"))
    # 出欠ごとに背景色
    for row_i, col_i, status in cell_styles:
        bg = STATUS_COLORS.get(status, colors.white)
        mat_sty.add("BACKGROUND", (col_i, row_i), (col_i, row_i), bg)
    mat_tbl.hAlign = "LEFT"
    mat_tbl.setStyle(mat_sty)
    story.append(KeepTogether([_mat_title, Spacer(1, 1*mm)]))
    story.append(mat_tbl)
    story.append(Spacer(1, 5*mm))

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
        rent_tbl.hAlign = "LEFT"
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

    # ── 入金・活動資金のまとめ（貸借形式） ─────────────────────
    story.append(Spacer(1, 5*mm))
    _h_fin = Paragraph("■ 入金・活動資金のまとめ", st_map["h2"])

    # 参加費集計
    participant_rows_s = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    cast_s = [r for r in participant_rows_s
              if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    fee_total_s = fee_paid_s = paid_count_s = 0
    for r in cast_s:
        fee_s = ext(r, PARTICIPANT_FEE_KEYS) or "0"
        try: fee = int(float(fee_s))
        except: fee = 0
        fee_total_s += fee
        if ext(r, PARTICIPANT_PAID_KEYS) == "True":
            fee_paid_s  += fee
            paid_count_s += 1

    # 確定参加費（1人あたり）をATLASから取得
    confirmed_fee_per_s = 0
    try:
        t_c = ctx["get_prop_types"](ctx["CONCERT_DB_CONCERT"])
        if t_c:
            fk = ctx["find_prop_name"](t_c, CONCERT_CONFIRMED_FEE_KEYS)
            if fk:
                concert_page = next(
                    (r for r in ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
                     if r.get("id","") == concert_id), {}
                )
                num = (concert_page.get("properties",{}) or {}).get(fk,{}).get("number")
                if num is not None:
                    confirmed_fee_per_s = int(num)
    except Exception:
        pass

    # 経費集計（CONCERT_EXPENSE）
    exp_db_s = ctx.get("CONCERT_DB_CONCERT_EXPENSE", "")
    exp_total_s = exp_conf_s = 0
    exp_by_type_s: dict = {}
    if exp_db_s:
        all_exp_s = ctx["query_all"](exp_db_s, None)
        for r in all_exp_s:
            if concert_id not in ext_rel(r, EXPENSE_CONCERT_REL_KEYS):
                continue
            type_s = ext(r, EXPENSE_TYPE_KEYS) or "その他"
            amt_s2 = ext(r, EXPENSE_AMOUNT_KEYS) or "0"
            conf_s = ext(r, EXPENSE_CONFIRMED_KEYS) == "True"
            try: amt2 = int(float(amt_s2))
            except: amt2 = 0
            exp_total_s += amt2
            if conf_s:
                exp_conf_s += amt2
            exp_by_type_s.setdefault(type_s, [0, 0])
            exp_by_type_s[type_s][1] += amt2
            if conf_s:
                exp_by_type_s[type_s][0] += amt2

    total_exp_conf_s = exp_conf_s + total_confirmed
    total_exp_all_s  = exp_total_s + total_all
    balance_conf_s   = fee_paid_s  - total_exp_conf_s
    balance_all_s    = fee_total_s - total_exp_all_s
    bal_color = colors.HexColor("#E8F5E9") if balance_conf_s >= 0 else colors.HexColor("#FFEBEE")

    # 収入行
    fee_per_str = f"¥{confirmed_fee_per_s:,}/人" if confirmed_fee_per_s else "未確定"
    income_rows = [
        ("参加費収入（入金済）",   f"¥{fee_paid_s:,}"),
        ("参加費収入（全額・予定）",f"¥{fee_total_s:,}"),
        ("確定参加費（1人あたり）", fee_per_str),
        (f"参加者数", f"{len(cast_s)}名（入金済 {paid_count_s}名）"),
    ]

    # 支出行（確定・見積を明確に分けて表示）
    expense_rows = []
    for type_s, (conf_a, all_a) in sorted(exp_by_type_s.items()):
        expense_rows.append((f"  {type_s}（確定）", f"¥{conf_a:,}"))
        if all_a != conf_a:
            expense_rows.append((f"  {type_s}（未確定含む見積）", f"¥{all_a:,}"))
    expense_rows.append(("  レンタル費用（確定）", f"¥{total_confirmed:,}"))
    if total_all != total_confirmed:
        expense_rows.append(("  レンタル費用（未確定含む見積）", f"¥{total_all:,}"))
    expense_rows.append(("支出合計（確定のみ）", f"¥{total_exp_conf_s:,}"))
    expense_rows.append(("支出合計（全見積）",    f"¥{total_exp_all_s:,}"))

    # 行数を揃える
    max_r = max(len(income_rows), len(expense_rows))
    while len(income_rows)  < max_r: income_rows.append(("", ""))
    while len(expense_rows) < max_r: expense_rows.append(("", ""))

    col_w = W / 2
    lb_header = [
        Paragraph("収　入", st_map["cellb_wht"]),
        Paragraph("金額",   st_map["cellb_wht"]),
        Paragraph("支　出", st_map["cellb_wht"]),
        Paragraph("金額",   st_map["cellb_wht"]),
    ]
    lb_rows = [lb_header]
    for (il, iv), (el, ev) in zip(income_rows, expense_rows):
        lb_rows.append([
            Paragraph(il, st_map["cell"]),
            Paragraph(iv, st_map["cellb"] if iv else st_map["cell"]),
            Paragraph(el, st_map["cell"]),
            Paragraph(ev, st_map["cellb"] if ev else st_map["cell"]),
        ])
    # 収支差引行（2行：現時点 / 着地予測）
    bal_now_str  = f"¥{balance_conf_s:,}　{'（黒字）' if balance_conf_s >= 0 else '（赤字）'}"
    bal_proj_str = f"¥{balance_all_s:,}　{'（黒字）' if balance_all_s >= 0 else '（赤字）'}"
    lb_rows.append([
        Paragraph("収支差引（現時点）", st_map["cellb"]),
        Paragraph(bal_now_str,  st_map["cellb"]),
        Paragraph("入金済 − 確定支出", st_map["small"]),
        Paragraph("", st_map["cell"]),
    ])
    lb_rows.append([
        Paragraph("収支差引（着地予測）", st_map["cellb"]),
        Paragraph(bal_proj_str, st_map["cellb"]),
        Paragraph("参加費全額 − 全見積支出", st_map["small"]),
        Paragraph("", st_map["cell"]),
    ])

    ph_tbl = Table(
        lb_rows,
        colWidths=[col_w*0.55, col_w*0.45, col_w*0.55, col_w*0.45],
    )
    ph_sty = TableStyle([
        ("FONT",          (0,0), (-1,-1), font,   8),
        ("FONT",          (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",    (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
        ("TEXTCOLOR",     (0,0), (-1, 0), colors.white),
        ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("LINEAFTER",     (1,0), (1,-1), 1.0, colors.HexColor("#2C2C6C")),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0), (-1,-1), 2),
        ("BOTTOMPADDING", (0,0), (-1,-1), 2),
        ("LEFTPADDING",   (0,0), (-1,-1), 3),
        ("ROWBACKGROUNDS",(0,1),(-1,-2), [colors.white, colors.HexColor("#F5F5F5")]),
        ("BACKGROUND",    (0,-1), (-1,-1), bal_color),
        ("SPAN",          (2,-1), (3,-1)),
    ])
    ph_tbl.hAlign = "LEFT"
    ph_tbl.setStyle(ph_sty)
    story.append(KeepTogether([_h_fin, Spacer(1, 1*mm)]))
    story.append(ph_tbl)

    doc.build(story)
    buf.seek(0)
    return buf.read()
