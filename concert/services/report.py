"""
HARMONIAアサイン候補案 PDF生成スクリプト
concert/services/report.py として配置する
"""
import io
from collections import defaultdict
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

FONT_PATH_REGULAR = "/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"
FONT_PATH_BOLD    = "/usr/share/fonts/opentype/ipafont-gothic/ipagp.ttf"  # IPAゴシック（プロポーショナル）をBold代用

def _register_fonts():
    try:
        pdfmetrics.registerFont(TTFont("IPAGothic",  FONT_PATH_REGULAR))
        pdfmetrics.registerFont(TTFont("IPAGothicB", FONT_PATH_BOLD))
        return "IPAGothic", "IPAGothicB"
    except Exception:
        return "Helvetica", "Helvetica-Bold"

def _styles(font, font_b):
    return {
        "title":    ParagraphStyle("title",   fontName=font_b, fontSize=16, spaceAfter=4),
        "subtitle": ParagraphStyle("sub",     fontName=font,   fontSize=10, spaceAfter=2, textColor=colors.HexColor("#555555")),
        "h2":       ParagraphStyle("h2",      fontName=font_b, fontSize=12, spaceBefore=8, spaceAfter=4),
        "h3":       ParagraphStyle("h3",      fontName=font_b, fontSize=10, spaceBefore=6, spaceAfter=2),
        "body":     ParagraphStyle("body",    fontName=font,   fontSize=9),
        "small":    ParagraphStyle("small",   fontName=font,   fontSize=8,  textColor=colors.HexColor("#666666")),
        "caption":  ParagraphStyle("caption", fontName=font,   fontSize=7,  textColor=colors.HexColor("#888888")),
        "desc":     ParagraphStyle("desc",    fontName=font,   fontSize=8,  textColor=colors.HexColor("#444444"),
                                   spaceAfter=4, leading=12),
    }

# 候補ごとの説明文
CANDIDATE_DESC = {
    "候補A：第1希望率最大": "第1希望が叶う人数を最大化。「絶対やりたい」という強い希望をできるだけ通す。",
    "候補B：総スコア最大":  "全員の満足度スコア合計を最大化。第1×3点・第2×2点・第3×1点の総和が最大。",
    "候補C：公平性重視":    "最も不満な人のスコアを底上げ。誰か一人が割を食う状況を避け、希望不成立も最小化。",
    "候補D：降り番均等":    "降り番の偏りを最小化。特定の人だけ多くの曲で降り番にならないよう割当件数を均等化。",
    "候補E：降り番均等":    "降り番の偏りを最小化。特定の人だけ多くの曲で降り番にならないよう割当件数を均等化。",
}

SCORE_COLOR = {
    3.0: colors.HexColor("#3C3489"),
    2.0: colors.HexColor("#085041"),
    1.0: colors.HexColor("#633806"),
    0.5: colors.HexColor("#A32D2D"),
    0.0: colors.HexColor("#888888"),
}

def generate_assign_report(
    concert_name: str,
    results: list[dict],   # assign.pyが生成するresults（session_state["assign_result_..."]）
    songs: list[dict],
    players: list[dict],
    ctx: dict,
) -> bytes:
    """
    アサイン候補案PDFを生成してbytesで返す。
    """
    font, font_b = _register_fonts()
    st = _styles(font, font_b)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=15*mm,
        title=f"アサイン候補案 - {concert_name}",
    )
    story = []

    extract = ctx["extract_prop_text_any"]
    song_name_map  = {s.get("id"): (extract(s, ["曲名","タイトル"]) or s.get("id","")) for s in songs}
    player_name_map= {p.get("id"): (extract(p, ["氏名","名前","表示名","タイトル"]) or p.get("id","")) for p in players}
    song_order     = [s.get("id") for s in sorted(songs, key=lambda x: song_name_map.get(x.get("id",""),""))]

    # ── 表紙 ──────────────────────────────────────────────────
    story.append(Spacer(1, 20*mm))
    story.append(Paragraph("ArtéMis HARMONIA", st["subtitle"]))
    story.append(Paragraph("パート割当 候補案", st["title"]))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(concert_name, st["h2"]))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC")))
    story.append(Spacer(1, 6*mm))

    # ── 候補案サマリー比較表 ──────────────────────────────────
    story.append(Paragraph("■ 候補案 比較サマリー", st["h2"]))

    header = ["候補", "説明", "総スコア", "第1希望率", "最低スコア", "FB件数"]
    rows = [header]
    for r in results:
        s = r["stats"]
        fb = sum(1 for a in r["assignments"] if a["source"] == "fallback")
        desc = CANDIDATE_DESC.get(r["label"], "")
        rows.append([
            r["label"].split("：")[0],   # 候補A / 候補B ...
            Paragraph(desc, st["small"]),
            f"{s['total_score']:.1f}点",
            f"{s['first_choice_rate']*100:.0f}%",
            f"{s['min_score']:.1f}点",
            f"{fb}件",
        ])

    tbl = Table(rows, colWidths=[18*mm, 75*mm, 20*mm, 20*mm, 20*mm, 16*mm])
    tbl.setStyle(TableStyle([
        ("FONT",        (0,0), (-1,-1), font,   8),
        ("FONT",        (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#F0EFF8")),
        ("ALIGN",       (2,0), (-1,-1), "CENTER"),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
        ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#CCCCCC")),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAF9")]),
        ("TOPPADDING",  (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",(0,0),(-1,-1), 4),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 6*mm))

    # ── 奏者別スコアサマリー ──────────────────────────────────
    story.append(Paragraph("■ 奏者別スコア・希望不成立サマリー", st["h2"]))

    # 全候補横断で奏者×候補のスコアと希望不成立をまとめる
    all_player_ids = sorted(
        {a["player_id"] for r in results for a in r["assignments"]}
        | {v["player_id"] for r in results for v in r["pref_map"].values() if v["priority"] > 0},
        key=lambda pid: player_name_map.get(pid, pid)
    )

    score_header = ["奏者"] + [r["label"].split("：")[0] + "\nスコア" for r in results] \
                             + [r["label"].split("：")[0] + "\n希望不成立" for r in results]
    score_rows = [score_header]

    for pid in all_player_ids:
        pname = player_name_map.get(pid, pid)
        score_cells = []
        ua_cells = []
        for r in results:
            sc = sum(
                {1:3.0,2:2.0,3:1.0}.get(
                    (r["pref_map"].get(str((a["player_id"],a["song_id"],a["part_id"]))) or {}).get("priority", 0),
                    0.5
                )
                for a in r["assignments"] if a["player_id"] == pid
            )
            # 希望不成立（曲単位）
            wanted = {v["song_id"] for v in r["pref_map"].values()
                      if v["player_id"] == pid and v["priority"] > 0}
            assigned_songs = {a["song_id"] for a in r["assignments"] if a["player_id"] == pid}
            unmet = len(wanted - assigned_songs)
            score_cells.append(f"{sc:.1f}")
            ua_cells.append(f"{unmet}曲" if unmet > 0 else "—")
        score_rows.append([pname] + score_cells + ua_cells)

    col_w = [25*mm] + [18*mm] * len(results) * 2
    stbl = Table(score_rows, colWidths=col_w)
    stbl.setStyle(TableStyle([
        ("FONT",        (0,0), (-1,-1), font,   7.5),
        ("FONT",        (0,0), (-1, 0), font_b, 7.5),
        ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#F0EFF8")),
        ("ALIGN",       (1,0), (-1,-1), "CENTER"),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
        ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#CCCCCC")),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAF9")]),
        ("TOPPADDING",  (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ]))
    story.append(stbl)

    # ── 各候補の詳細（マトリクス）──────────────────────────────
    for r in results:
        story.append(PageBreak())
        label = r["label"]
        desc  = CANDIDATE_DESC.get(label, "")
        story.append(Paragraph(f"■ {label}", st["h2"]))
        if desc:
            story.append(Paragraph(desc, st["desc"]))

        # 奏者×曲マトリクス
        story.append(Paragraph("[ 奏者 x 曲 割当マトリクス ]", st["h3"]))
        matrix: dict[tuple, list] = defaultdict(list)
        for a in r["assignments"]:
            matrix[(a["player_id"], a["song_id"])].append(a["part_name"])

        # ヘッダー行：曲名（短縮せずParagraphで折り返し）
        mat_header = ["奏者"] + [song_name_map.get(sid, sid) for sid in song_order]
        mat_rows = [mat_header]
        for pid in all_player_ids:
            pname = player_name_map.get(pid, pid)
            row = [pname]
            for sid in song_order:
                parts = matrix.get((pid, sid), [])
                row.append("\n".join(parts) if parts else "—")
            mat_rows.append(row)

        # マトリクスのセル内容をParagraphで折り返し対応
        cell_style = ParagraphStyle("cell", fontName=font, fontSize=6.5, leading=9)
        hdr_style  = ParagraphStyle("hdr",  fontName=font_b, fontSize=6.5, leading=9)
        mat_rows_p = []
        for ri, row in enumerate(mat_rows):
            new_row = []
            for ci, cell in enumerate(row):
                if isinstance(cell, str):
                    s = hdr_style if (ri == 0 or ci == 0) else cell_style
                    new_row.append(Paragraph(cell.replace("/", "/<br/>"), s))
                else:
                    new_row.append(cell)
            mat_rows_p.append(new_row)

        available_w = 165*mm  # A4幅 - 余白
        name_col_w  = 22*mm
        song_col_w_each = (available_w - name_col_w) / max(len(song_order), 1)
        song_col_w = [name_col_w] + [song_col_w_each] * len(song_order)
        mtbl = Table(mat_rows_p, colWidths=song_col_w, repeatRows=1)
        mtbl.setStyle(TableStyle([
            ("FONT",        (0,0), (-1,-1), font,   6.5),
            ("FONT",        (0,0), (-1, 0), font_b, 6.5),
            ("FONT",        (0,0), ( 0,-1), font_b, 6.5),
            ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#F0EFF8")),
            ("BACKGROUND",  (0,0), ( 0,-1), colors.HexColor("#F8F8F8")),
            ("ALIGN",       (1,0), (-1,-1), "CENTER"),
            ("VALIGN",      (0,0), (-1,-1), "TOP"),
            ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#CCCCCC")),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAF9")]),
            ("TOPPADDING",  (0,0), (-1,-1), 3),
            ("BOTTOMPADDING",(0,0),(-1,-1), 3),
            ("LEFTPADDING", (0,0), (-1,-1), 3),
            ("RIGHTPADDING",(0,0), (-1,-1), 3),
        ]))
        story.append(mtbl)
        story.append(Spacer(1, 4*mm))

        # 曲別割当一覧
        story.append(Paragraph("[ 曲別 割当一覧 ]", st["h3"]))
        by_song: dict[str, list] = defaultdict(list)
        for a in r["assignments"]:
            by_song[a["song_id"]].append(a)

        for sid in song_order:
            items = by_song.get(sid, [])
            if not items:
                continue
            story.append(Paragraph(song_name_map.get(sid, sid), st["h3"]))
            detail_header = ["奏者", "パート", "希望", "点数"]
            detail_rows = [detail_header]
            for a in items:
                pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                pref = r["pref_map"].get(pk)
                if pref and pref["priority"] > 0:
                    hope = {1:"第1希望",2:"第2希望",3:"第3希望"}.get(pref["priority"],"—")
                    sc   = {1:3.0,2:2.0,3:1.0}.get(pref["priority"],0.0)
                elif a["source"] == "fallback":
                    hope = "FB"
                    sc   = 0.5
                else:
                    hope = "降り番"
                    sc   = 0.0
                tied_mark = " ⚠" if a.get("tied") else ""
                detail_rows.append([
                    player_name_map.get(a["player_id"], a["player_name"]),
                    a["part_name"],
                    hope + tied_mark,
                    f"{sc:.1f}",
                ])

            # 曲別一覧もParagraphで折り返し
            dcell_s = ParagraphStyle("dc", fontName=font,   fontSize=8, leading=11)
            dhdr_s  = ParagraphStyle("dh", fontName=font_b, fontSize=8, leading=11)
            detail_rows_p = []
            for ri, row in enumerate(detail_rows):
                new_row = []
                for ci, cell in enumerate(row):
                    if isinstance(cell, str):
                        s = dhdr_s if ri == 0 else dcell_s
                        new_row.append(Paragraph(cell, s))
                    else:
                        new_row.append(cell)
                detail_rows_p.append(new_row)

            dtbl = Table(detail_rows_p, colWidths=[28*mm, 90*mm, 18*mm, 14*mm], repeatRows=1)
            dtbl.setStyle(TableStyle([
                ("FONT",        (0,0), (-1,-1), font,   8),
                ("FONT",        (0,0), (-1, 0), font_b, 8),
                ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#F0EFF8")),
                ("ALIGN",       (2,0), (-1,-1), "CENTER"),
                ("VALIGN",      (0,0), (-1,-1), "TOP"),
                ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#CCCCCC")),
                ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAF9")]),
                ("TOPPADDING",  (0,0), (-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
                ("LEFTPADDING", (0,0), (-1,-1), 3),
                ("RIGHTPADDING",(0,0), (-1,-1), 3),
            ]))
            story.append(dtbl)
            story.append(Spacer(1, 2*mm))

    doc.build(story)
    buf.seek(0)
    return buf.read()
