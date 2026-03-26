"""
HARMONIAアサイン候補案 PDF生成スクリプト
concert/services/report.py として配置する
"""
import io
from collections import defaultdict
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
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
        "title":    ParagraphStyle("title", alignment=TA_LEFT,   fontName=font_b, fontSize=16, spaceAfter=4),
        "subtitle": ParagraphStyle("sub", alignment=TA_LEFT,     fontName=font,   fontSize=10, spaceAfter=2, textColor=colors.HexColor("#555555")),
        "h2":       ParagraphStyle("h2", alignment=TA_LEFT,      fontName=font_b, fontSize=12, spaceBefore=8, spaceAfter=4),
        "h3":       ParagraphStyle("h3", alignment=TA_LEFT,      fontName=font_b, fontSize=10, spaceBefore=6, spaceAfter=2),
        "body":     ParagraphStyle("body", alignment=TA_LEFT,    fontName=font,   fontSize=9),
        "small":    ParagraphStyle("small", alignment=TA_LEFT,   fontName=font,   fontSize=8,  textColor=colors.HexColor("#666666")),
        "caption":  ParagraphStyle("caption", alignment=TA_LEFT, fontName=font,   fontSize=7,  textColor=colors.HexColor("#888888")),
        "cell":     ParagraphStyle("cell",  alignment=TA_LEFT,   fontName=font,   fontSize=8,  leading=11),
        "cellb":    ParagraphStyle("cellb", alignment=TA_LEFT,   fontName=font_b, fontSize=8,  leading=11),
        "cellb_wht":ParagraphStyle("cellb_wht", alignment=TA_LEFT, fontName=font_b, fontSize=8, leading=11, textColor=colors.white),
        "desc":     ParagraphStyle("desc", alignment=TA_LEFT,    fontName=font,   fontSize=8,  textColor=colors.HexColor("#444444"),
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
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph("パート割当 候補案", st["title"]))
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph(concert_name, st["h2"]))
    story.append(Spacer(1, 4*mm))
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
    tbl.hAlign = "LEFT"
    tbl.setStyle(TableStyle([
        ("FONT",        (0,0), (-1,-1), font,   8),
        ("FONT",        (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#E4E2F0")),
        ("ALIGN",       (0,0), (-1,-1), "LEFT"),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
        ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F3F2F0")]),
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
    stbl.hAlign = "LEFT"
    stbl.setStyle(TableStyle([
        ("FONT",        (0,0), (-1,-1), font,   7.5),
        ("FONT",        (0,0), (-1, 0), font_b, 7.5),
        ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#E4E2F0")),
        ("ALIGN",       (0,0), (-1,-1), "LEFT"),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
        ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F3F2F0")]),
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
        cell_style = ParagraphStyle("cell", fontName=font, fontSize=6.5, leading=9, alignment=TA_LEFT)
        hdr_style  = ParagraphStyle("hdr",  fontName=font_b, fontSize=6.5, leading=9, alignment=TA_LEFT)
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
        mtbl.hAlign = "LEFT"
        mtbl.setStyle(TableStyle([
            ("FONT",        (0,0), (-1,-1), font,   6.5),
            ("FONT",        (0,0), (-1, 0), font_b, 6.5),
            ("FONT",        (0,0), ( 0,-1), font_b, 6.5),
            ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#E4E2F0")),
            ("BACKGROUND",  (0,0), ( 0,-1), colors.HexColor("#F8F8F8")),
            ("ALIGN",       (0,0), (-1,-1), "LEFT"),
            ("VALIGN",      (0,0), (-1,-1), "TOP"),
            ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F3F2F0")]),
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
            _song_title = Paragraph(song_name_map.get(sid, sid), st["h3"])
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
            dcell_s = ParagraphStyle("dc", fontName=font,   fontSize=8, leading=11, alignment=TA_LEFT)
            dhdr_s  = ParagraphStyle("dh", fontName=font_b, fontSize=8, leading=11, alignment=TA_LEFT)
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
            dtbl.hAlign = "LEFT"
            dtbl.setStyle(TableStyle([
                ("FONT",        (0,0), (-1,-1), font,   8),
                ("FONT",        (0,0), (-1, 0), font_b, 8),
                ("BACKGROUND",  (0,0), (-1, 0), colors.HexColor("#E4E2F0")),
                ("ALIGN",       (0,0), (-1,-1), "LEFT"),
                ("VALIGN",      (0,0), (-1,-1), "TOP"),
                ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
                ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F3F2F0")]),
                ("TOPPADDING",  (0,0), (-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
                ("LEFTPADDING", (0,0), (-1,-1), 3),
                ("RIGHTPADDING",(0,0), (-1,-1), 3),
            ]))
            story.append(KeepTogether([
                _song_title,
                dtbl,
                Spacer(1, 2*mm),
            ]))

    # ── アルゴリズム解説ページ ────────────────────────────────
    story.append(PageBreak())
    _tips_W = A4[0] - 30*mm  # A4幅 - 左右マージン(各15mm)
    story.append(Paragraph("ArtéMis HARMONIA　アサイン検討 Tips", st["subtitle"]))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph("候補案の読み方・選び方", st["title"]))
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC")))
    story.append(Spacer(1, 6*mm))

    # アルゴリズム概要
    story.append(Paragraph("■ 生成アルゴリズム", st["h2"]))
    story.append(Spacer(1, 2*mm))
    algo_data = [
        ["フェーズ", "手法", "説明"],
        ["第1段階", "貪欲法\n(Greedy)",
         "スコアの高い希望から順に割り当てていく。\n"
         "高速だが最初の割当に引きずられやすい。\n"
         "各候補案の「たたき台」として使用。"],
        ["第2段階", "局所探索\n(Local Search)",
         "貪欲法の結果を出発点に、2人の割当を交換して\n"
         "スコアが改善するなら採用する操作を繰り返す。\n"
         "最大250回の改善試行で候補案を洗練させる。"],
    ]
    algo_tbl = Table(
        [[Paragraph(str(c), st["cellb"] if i==0 else st["cell"])
          for c in row]
         for i, row in enumerate(algo_data)],
        colWidths=[22*mm, 28*mm, _tips_W - 50*mm],
        repeatRows=1,
    )
    algo_tbl.hAlign = "LEFT"
    algo_tbl.setStyle(TableStyle([
        ("FONT",         (0,0), (-1,-1), font,   8),
        ("FONT",         (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",   (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
        ("TEXTCOLOR",    (0,0), (-1, 0), colors.white),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F5F5F5")]),
    ]))
    story.append(algo_tbl)
    story.append(Spacer(1, 6*mm))

    # 候補案の説明
    story.append(Paragraph("■ 4つの候補案", st["h2"]))
    story.append(Spacer(1, 2*mm))
    candidates_data = [
        ["候補", "最適化の目標", "向いている状況"],
        ["候補A\n第1希望率最大",
         "第1希望が叶う人数を最大化",
         "「絶対これをやりたい」という強い希望を優先したいとき。\n"
         "第2・第3希望は後回しになりやすい点に注意。"],
        ["候補B\n総スコア最大",
         "全員の希望スコア合計（第1希望×3点・第2希望×2点・第3希望×1点）を最大化",
         "全体として「満足度の総量」を最も高くしたいとき。\n"
         "バランス型。多くの場合まずこれを確認するとよい。"],
        ["候補C\n公平性重視",
         "最も不満な人のスコアを底上げ（最低スコアを最大化）",
         "「誰か一人が割を食う」状況を避けたいとき。\n"
         "全体スコアより個人間の公平さを優先する。"],
        ["候補D\n降り番均等",
         "降り番（割当なし）の偏りを最小化（割当件数の標準偏差を最小化）",
         "特定の人だけ降り番が多くなる状況を避けたいとき。\n"
         "人数と曲数のバランスが悪い場合に有効。"],
    ]
    cand_tbl = Table(
        [[Paragraph(str(c), st["cellb"] if i==0 else st["cell"])
          for c in row]
         for i, row in enumerate(candidates_data)],
        colWidths=[28*mm, 55*mm, _tips_W - 83*mm],
        repeatRows=1,
    )
    cand_tbl.hAlign = "LEFT"
    cand_tbl.setStyle(TableStyle([
        ("FONT",         (0,0), (-1,-1), font,   8),
        ("FONT",         (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",   (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
        ("TEXTCOLOR",    (0,0), (-1, 0), colors.white),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F5F5F5")]),
    ]))
    story.append(cand_tbl)
    story.append(Spacer(1, 6*mm))

    # スコア説明
    story.append(Paragraph("■ スコアの見方", st["h2"]))
    story.append(Spacer(1, 2*mm))
    score_data = [
        ["希望順位", "スコア", "説明"],
        ["第1希望",              "3.0点", "最も優先度が高い希望"],
        ["第2希望",              "2.0点", "次点の希望"],
        ["第3希望",              "1.0点", "できれば希望"],
        ["希望なし/降り番でも可", "0.0点", "どちらでもよい"],
        ["フォールバック",        "0.5点", "希望外だが他に選択肢がなく割り当てられたパート"],
        ["NG",                   "対象外", "割り当て不可。このパートには絶対に割り当てない"],
    ]
    score_tbl = Table(
        [[Paragraph(str(c), st["cellb"] if i==0 else st["cell"])
          for c in row]
         for i, row in enumerate(score_data)],
        colWidths=[48*mm, 18*mm, _tips_W - 66*mm],
        repeatRows=1,
    )
    score_tbl.hAlign = "LEFT"
    score_tbl.setStyle(TableStyle([
        ("FONT",         (0,0), (-1,-1), font,   8),
        ("FONT",         (0,0), (-1, 0), font_b, 8),
        ("BACKGROUND",   (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
        ("TEXTCOLOR",    (0,0), (-1, 0), colors.white),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F5F5F5")]),
    ]))
    story.append(score_tbl)
    story.append(Spacer(1, 6*mm))

    # 使い方のヒント
    story.append(Paragraph("■ 候補案の選び方：実践的なヒント", st["h2"]))
    story.append(Spacer(1, 2*mm))
    tips = [
        "まず候補Bを確認する。総合満足度が最も高く、多くの場合これが出発点として最適。",
        "第1希望率が低い場合は候補Aと比較する。候補Aで第1希望率が大幅に上がる場合は検討の価値あり。",
        "最低スコアが低い奏者がいる場合は候補Cを確認する。特定の人が不満を持ちやすい構成かどうか確認できる。",
        "降り番が特定の人に集中している場合は候補Dを検討する。",
        "フォールバック（FB）件数が多い場合は希望不成立が多い状態。パート数と奏者数のバランスを再確認するとよい。",
        "どの候補案も完璧ではない。最終的な判断は人間が行い、特殊事情（楽器の持参可否・体力面など）を加味して調整する。",
    ]
    for i, tip in enumerate(tips, 1):
        story.append(Paragraph(f"{i}. {tip}", st["body"]))
        story.append(Spacer(1, 1.5*mm))

    doc.build(story)
    buf.seek(0)
    return buf.read()
