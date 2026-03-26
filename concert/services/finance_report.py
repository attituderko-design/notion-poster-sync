"""
concert/services/finance_report.py
収支報告PDF生成スクリプト
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
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS, PRACTICE_CONCERT_DAY_KEYS,
    PRACTICE_CONCERT_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PART_KEYS, PARTICIPANT_ROLE_KEYS,
    PARTICIPANT_FEE_KEYS, PARTICIPANT_PAID_KEYS,
    PLAYER_NAME_KEYS,
    EXPENSE_TYPE_KEYS, EXPENSE_CONTENT_KEYS, EXPENSE_AMOUNT_KEYS,
    EXPENSE_CONFIRMED_KEYS, EXPENSE_NOTE_KEYS, EXPENSE_CONCERT_REL_KEYS,
    RENTAL_PRACTICE_REL_KEYS, RENTAL_QTY_KEYS, RENTAL_UNIT_PRICE_KEYS,
    RENTAL_CONFIRMED_KEYS, RENTAL_VENDOR_KEYS, RENTAL_ITEM_NAME_KEYS,
)

FONT_PATH_REGULAR = "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"
FONT_PATH_BOLD    = "/usr/share/fonts/opentype/ipafont-gothic/ipagp.ttf"


def _register_fonts():
    try:
        pdfmetrics.registerFont(TTFont("IPAGothic",  FONT_PATH_REGULAR))
        pdfmetrics.registerFont(TTFont("IPAGothicB", FONT_PATH_BOLD))
        return "IPAGothic", "IPAGothicB"
    except Exception:
        return "Helvetica", "Helvetica-Bold"


def _styles(font, font_b):
    return {
        "title":   ParagraphStyle("title",   alignment=TA_LEFT, fontName=font_b, fontSize=16, spaceAfter=4),
        "subtitle":ParagraphStyle("sub",     alignment=TA_LEFT, fontName=font,   fontSize=9,  spaceAfter=6,
                                  textColor=colors.HexColor("#555555")),
        "h2":      ParagraphStyle("h2",      alignment=TA_LEFT, fontName=font_b, fontSize=12, spaceBefore=8, spaceAfter=4,
                                  textColor=colors.HexColor("#2C2C6C")),
        "h3":      ParagraphStyle("h3",      alignment=TA_LEFT, fontName=font_b, fontSize=10, spaceBefore=6, spaceAfter=3),
        "body":    ParagraphStyle("body",    alignment=TA_LEFT, fontName=font,   fontSize=9),
        "cell":    ParagraphStyle("cell",    alignment=TA_LEFT, fontName=font,   fontSize=8,  leading=11),
        "cellb":   ParagraphStyle("cellb",   alignment=TA_LEFT, fontName=font_b, fontSize=8,  leading=11),
        "small":   ParagraphStyle("small",   alignment=TA_LEFT, fontName=font,   fontSize=7,
                                  textColor=colors.HexColor("#666666")),
        "total":   ParagraphStyle("total",   alignment=TA_LEFT, fontName=font_b, fontSize=10, spaceBefore=4),
        "surplus": ParagraphStyle("surplus", alignment=TA_LEFT, fontName=font_b, fontSize=10,
                                  textColor=colors.HexColor("#2C6C2C")),
        "deficit": ParagraphStyle("deficit", alignment=TA_LEFT, fontName=font_b, fontSize=10,
                                  textColor=colors.HexColor("#8B0000")),
    }


def _base_style(font, font_b):
    return TableStyle([
        ("FONT",         (0,0), (-1,-1), font,   8),
        ("FONT",         (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",   (0,0), (-1, 0), colors.HexColor("#E8E6F0")),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("ALIGN",        (0,0), (-1,-1), "LEFT"),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",   (0,0), (-1,-1), 2),
        ("BOTTOMPADDING",(0,0), (-1,-1), 2),
        ("LEFTPADDING",  (0,0), (-1,-1), 3),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F5F5F5")]),
    ])


def generate_finance_report(ctx: dict, concert_id: str) -> bytes:
    font, font_b = _register_fonts()
    st_map = _styles(font, font_b)
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    W = 170*mm  # 有効幅

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=15*mm, bottomMargin=15*mm,
    )
    story = []

    # ── データ取得 ────────────────────────────────────────────

    # 演奏会
    concert_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
    concert = next((r for r in concert_rows if r.get("id") == concert_id), {})
    c_name = ext(concert, CONCERT_NAME_KEYS) or ""
    c_date = ext(concert, CONCERT_DATE_KEYS) or ""

    # 本番日
    all_practices = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    practices = [p for p in all_practices
                 if concert_id in ext_rel(p, PRACTICE_CONCERT_REL_KEYS)]
    concert_day = next(
        (p for p in practices if ext(p, PRACTICE_CONCERT_DAY_KEYS) == "True"), None
    )
    concert_date = ext(concert_day, PRACTICE_DATE_KEYS)[:10] if concert_day else (c_date[:10] if c_date else "未設定")

    # 参加者
    participant_rows = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    cast = [r for r in participant_rows
            if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    player_rows = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {r.get("id",""): ext(r, PLAYER_NAME_KEYS) or "" for r in player_rows}

    # 経費
    exp_db = ctx.get("CONCERT_DB_CONCERT_EXPENSE", "")
    expenses = []
    if exp_db:
        all_exp = ctx["query_all"](exp_db, None)
        expenses = [r for r in all_exp
                    if concert_id in ext_rel(r, EXPENSE_CONCERT_REL_KEYS)]

    # レンタル（練習日経由で演奏会に紐づく）
    practice_ids = {p.get("id","") for p in practices}
    all_rental = ctx["query_all"](ctx["CONCERT_DB_RENTAL"], None)
    rentals = [r for r in all_rental
               if any(pr_id in ext_rel(r, RENTAL_PRACTICE_REL_KEYS)
                      for pr_id in practice_ids)]

    # ── 集計 ─────────────────────────────────────────────────

    # 参加費
    fee_total    = 0
    fee_paid     = 0
    fee_unpaid   = 0
    paid_count   = 0
    unpaid_count = 0
    for r in cast:
        fee_s = ext(r, PARTICIPANT_FEE_KEYS) or "0"
        try: fee = int(float(fee_s))
        except: fee = 0
        paid = ext(r, PARTICIPANT_PAID_KEYS) == "True"
        fee_total += fee
        if paid:
            fee_paid  += fee
            paid_count += 1
        else:
            fee_unpaid  += fee
            unpaid_count += 1

    # 経費（CONCERT_EXPENSE）
    exp_by_type: dict[str, list] = defaultdict(list)
    exp_total_all       = 0
    exp_total_confirmed = 0
    for r in expenses:
        type_  = ext(r, EXPENSE_TYPE_KEYS)     or "その他"
        content= ext(r, EXPENSE_CONTENT_KEYS)  or ""
        amt_s  = ext(r, EXPENSE_AMOUNT_KEYS)   or "0"
        conf   = ext(r, EXPENSE_CONFIRMED_KEYS) == "True"
        note   = ext(r, EXPENSE_NOTE_KEYS)     or ""
        try: amt = int(float(amt_s))
        except: amt = 0
        exp_by_type[type_].append((content, amt, conf, note))
        exp_total_all += amt
        if conf:
            exp_total_confirmed += amt

    # レンタル費用
    rent_total_all       = 0
    rent_total_confirmed = 0
    rent_rows_data = []
    for r in rentals:
        qty_s   = ext(r, RENTAL_QTY_KEYS)        or "1"
        price_s = ext(r, RENTAL_UNIT_PRICE_KEYS) or "0"
        conf    = ext(r, RENTAL_CONFIRMED_KEYS)  == "True"
        vendor  = ext(r, RENTAL_VENDOR_KEYS)     or "—"
        item    = ext(r, RENTAL_ITEM_NAME_KEYS)  or "—"
        try:
            qty   = int(float(qty_s))
            price = int(float(price_s))
        except:
            qty = 1; price = 0
        subtotal = qty * price
        rent_total_all += subtotal
        if conf:
            rent_total_confirmed += subtotal
        rent_rows_data.append((vendor, item, qty, price, subtotal, conf))

    # 支出合計
    total_expense_all       = exp_total_all + rent_total_all
    total_expense_confirmed = exp_total_confirmed + rent_total_confirmed

    # 収支
    balance_all       = fee_total - total_expense_all
    balance_confirmed = fee_paid  - total_expense_confirmed

    # ── PDF構築 ──────────────────────────────────────────────

    # 表紙
    story.append(Paragraph("ArtéMis HARMONIA", st_map["subtitle"]))
    story.append(Paragraph("収支報告", st_map["title"]))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(c_name, st_map["h2"]))
    story.append(Paragraph(f"本番日：{concert_date}", st_map["body"]))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC")))
    story.append(Spacer(1, 6*mm))

    # ── 収支サマリ ────────────────────────────────────────────
    _h_sum = Paragraph("■ 収支サマリ", st_map["h2"])
    summary_data = [
        ["項目", "金額（確定）", "金額（全見積）"],
        ["参加費収入（入金済）",   f"¥{fee_paid:,}",              f"¥{fee_total:,}"],
        ["経費合計（EXPENSE）",    f"¥{exp_total_confirmed:,}",   f"¥{exp_total_all:,}"],
        ["レンタル費用",           f"¥{rent_total_confirmed:,}",  f"¥{rent_total_all:,}"],
        ["支出合計",               f"¥{total_expense_confirmed:,}", f"¥{total_expense_all:,}"],
        ["収支（参加費 - 支出）",  f"¥{balance_confirmed:,}",     f"¥{balance_all:,}"],
    ]
    sum_tbl = Table(
        [[Paragraph(str(c), st_map["cellb"] if (i==0 or j==0) else st_map["cell"])
          for j, c in enumerate(row)]
         for i, row in enumerate(summary_data)],
        colWidths=[70*mm, 45*mm, 45*mm],
    )
    sum_sty = _base_style(font, font_b)
    # 収支行（最終行）に色付け
    if balance_confirmed >= 0:
        sum_sty.add("BACKGROUND", (0, len(summary_data)-1), (-1, len(summary_data)-1),
                    colors.HexColor("#E8F5E9"))
    else:
        sum_sty.add("BACKGROUND", (0, len(summary_data)-1), (-1, len(summary_data)-1),
                    colors.HexColor("#FFEBEE"))
    sum_tbl.hAlign = "LEFT"
    sum_tbl.setStyle(sum_sty)
    story.append(KeepTogether([_h_sum, sum_tbl]))
    story.append(Spacer(1, 5*mm))

    # ── 参加費・振込状況 ──────────────────────────────────────
    _h_cast = Paragraph("■ 参加費・振込状況", st_map["h2"])
    story.append(Paragraph(
        f"参加者 {len(cast)}名  ／  入金済 {paid_count}名（¥{fee_paid:,}）  ／  未入金 {unpaid_count}名（¥{fee_unpaid:,}）",
        st_map["body"]
    ))
    story.append(Spacer(1, 2*mm))

    cast_data = [["氏名", "パート", "役職", "参加費", "入金済"]]
    for r in sorted(cast, key=lambda x: (ext(x, PARTICIPANT_PART_KEYS) or "", ext(x, PARTICIPANT_ROLE_KEYS) or "")):
        pids  = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        pname = player_name_map.get(pids[0], "—") if pids else "—"
        part  = ext(r, PARTICIPANT_PART_KEYS)  or "—"
        role  = ext(r, PARTICIPANT_ROLE_KEYS)  or "—"
        fee_s = ext(r, PARTICIPANT_FEE_KEYS)   or "0"
        paid  = ext(r, PARTICIPANT_PAID_KEYS)  == "True"
        try: fee = int(float(fee_s))
        except: fee = 0
        cast_data.append([pname, part, role, f"¥{fee:,}", "入金済" if paid else "未入金"])

    cast_tbl = Table(
        [[Paragraph(str(c), st_map["cellb"] if i==0 else (
            st_map["cell"] if not (j==4 and c=="未入金") else
            ParagraphStyle("unpaid", alignment=TA_LEFT, fontName=font_b, fontSize=8,
                           textColor=colors.HexColor("#C62828"))))
          for j, c in enumerate(row)]
         for i, row in enumerate(cast_data)],
        colWidths=[40*mm, 20*mm, 30*mm, 25*mm, 20*mm],
        repeatRows=1,
    )
    cast_sty = _base_style(font, font_b)
    cast_tbl.hAlign = "LEFT"
    cast_tbl.setStyle(cast_sty)
    story.append(KeepTogether([_h_cast, cast_tbl]))
    story.append(Spacer(1, 5*mm))

    # ── 経費明細（CONCERT_EXPENSE） ───────────────────────────
    if expenses:
        _h_exp = Paragraph("■ 経費明細", st_map["h2"])
        exp_data = [["種別", "内容", "金額", "確定", "備考"]]
        for type_, items in sorted(exp_by_type.items()):
            for content, amt, conf, note in items:
                exp_data.append([
                    type_, content, f"¥{amt:,}",
                    "確定" if conf else "見積", note
                ])
        exp_data.append(["合計（確定）", "", f"¥{exp_total_confirmed:,}", "", ""])
        exp_data.append(["合計（全見積）", "", f"¥{exp_total_all:,}", "", ""])

        exp_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if (i==0 or (i>=len(exp_data)-2 and j==0)) else st_map["cell"])
              for j, c in enumerate(row)]
             for i, row in enumerate(exp_data)],
            colWidths=[30*mm, 55*mm, 25*mm, 15*mm, W-125*mm],
            repeatRows=1,
        )
        exp_sty = _base_style(font, font_b)
        # 合計行に色付け
        for i in range(len(exp_data)-2, len(exp_data)):
            exp_sty.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#F0EEF8"))
        exp_tbl.hAlign = "LEFT"
        exp_tbl.setStyle(exp_sty)
        story.append(KeepTogether([_h_exp, exp_tbl]))
        story.append(Spacer(1, 5*mm))

    # ── レンタル費用明細 ──────────────────────────────────────
    if rent_rows_data:
        story.append(Paragraph("■ レンタル費用明細", st_map["h2"]))
        rent_data = [["業者名", "品目", "台数", "単価", "小計", "確定"]]
        for vendor, item, qty, price, subtotal, conf in rent_rows_data:
            rent_data.append([
                vendor, item, str(qty),
                f"¥{price:,}", f"¥{subtotal:,}",
                "確定" if conf else "見積"
            ])
        rent_data.append(["合計（確定）", "", "", "", f"¥{rent_total_confirmed:,}", ""])
        rent_data.append(["合計（全見積）", "", "", "", f"¥{rent_total_all:,}", ""])

        rent_tbl = Table(
            [[Paragraph(str(c), st_map["cellb"] if (i==0 or (i>=len(rent_data)-2 and j==0)) else st_map["cell"])
              for j, c in enumerate(row)]
             for i, row in enumerate(rent_data)],
            colWidths=[35*mm, 50*mm, 15*mm, 20*mm, 25*mm, 15*mm],
            repeatRows=1,
        )
        rent_sty = _base_style(font, font_b)
        for i in range(len(rent_data)-2, len(rent_data)):
            rent_sty.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#F0EEF8"))
        rent_tbl.hAlign = "LEFT"
        rent_tbl.setStyle(rent_sty)
        story.append(KeepTogether([
            Paragraph("■ レンタル費用明細", st_map["h2"]) if not expenses else Spacer(1, 0),
            rent_tbl,
        ]))
        story.append(Spacer(1, 5*mm))

    # ── 種別ごとの経費集計 ────────────────────────────────────
    _h_bt = Paragraph("■ 種別別経費集計", st_map["h2"])
    by_type_data = [["種別", "確定", "全見積"]]
    grand_conf = grand_all = 0
    for type_, items in sorted(exp_by_type.items()):
        conf_amt = sum(amt for _, amt, conf, _ in items if conf)
        all_amt  = sum(amt for _, amt, _, _  in items)
        by_type_data.append([type_, f"¥{conf_amt:,}", f"¥{all_amt:,}"])
        grand_conf += conf_amt
        grand_all  += all_amt
    # レンタルを追加
    if rent_rows_data:
        by_type_data.append(["レンタル（楽器等）",
                              f"¥{rent_total_confirmed:,}",
                              f"¥{rent_total_all:,}"])
        grand_conf += rent_total_confirmed
        grand_all  += rent_total_all
    by_type_data.append(["合計", f"¥{grand_conf:,}", f"¥{grand_all:,}"])

    bt_tbl = Table(
        [[Paragraph(str(c), st_map["cellb"] if (i==0 or i==len(by_type_data)-1 or j==0) else st_map["cell"])
          for j, c in enumerate(row)]
         for i, row in enumerate(by_type_data)],
        colWidths=[60*mm, 45*mm, 45*mm],
    )
    bt_sty = _base_style(font, font_b)
    bt_sty.add("BACKGROUND", (0, len(by_type_data)-1), (-1, len(by_type_data)-1),
               colors.HexColor("#E8E6F0"))
    bt_tbl.hAlign = "LEFT"
    bt_tbl.setStyle(bt_sty)
    story.append(KeepTogether([_h_bt, bt_tbl]))

    doc.build(story)
    buf.seek(0)
    return buf.read()
