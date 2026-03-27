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
    "候補A：第1希望率最大":        "第1希望が叶う人数を最大化。「絶対やりたい」という強い希望をできるだけ通す。",
    "候補A：第1希望率最大（厳密解）": "第1希望が叶う人数を最大化（整数計画法による最適解）。",
    "候補B：総スコア最大":          "全員の満足度スコア合計を最大化。第1×3点・第2×2点・第3×1点の総和が最大。",
    "候補B：総スコア最大（厳密解）":  "総スコアを最大化（整数計画法による最適解）。",
    "候補C：公平性重視":            "最も不満な人のスコアを底上げ。誰か一人が割を食う状況を避け、希望不成立も最小化。",
    "候補C：公平性重視（厳密解）":    "公平性を最大化（整数計画法による最適解）。",
    "候補D：降り番均等":            "降り番の偏りを最小化。特定の人だけ多くの曲で降り番にならないよう割当件数を均等化。",
    "候補D：降り番均等（厳密解）":    "降り番の均等化を最適化（整数計画法による最適解）。",
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
    compare_results: list[dict] | None = None,  # 比較対象（厳密解 or ヒューリスティック）
    compare_label: str = "比較",
) -> bytes:
    """
    アサイン候補案PDFを生成してbytesで返す。
    compare_results が指定された場合、各候補の後に比較対象を並べて表示する。
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
            def _a_score(a, pm):
                pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                pref = pm.get(pk)
                src  = a.get("source", "")
                if pref and pref.get("priority", 0) > 0:
                    return {1:3.0,2:2.0,3:1.0}.get(pref["priority"], 0.0)
                elif pref and pref.get("priority", 0) == 0:
                    return 0.0   # 降り番希望
                elif src in ("fallback", "swap", "exact"):
                    return 0.5   # 補完割当
                return 0.0
            sc = sum(_a_score(a, r["pref_map"]) for a in r["assignments"] if a["player_id"] == pid)
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
            # スタイルをループの前に定義
            dcell_s = ParagraphStyle("dc",  fontName=font,   fontSize=7.5, leading=10, alignment=TA_LEFT)
            dname_s = ParagraphStyle("dn",  fontName=font,   fontSize=6.5, leading=9,  alignment=TA_LEFT)
            dhdr_s  = ParagraphStyle("dh",  fontName=font_b, fontSize=7.5, leading=10, alignment=TA_LEFT)
            for a in items:
                pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                pref = r["pref_map"].get(pk)
                if pref and pref["priority"] > 0:
                    hope = {1:"第1希望",2:"第2希望",3:"第3希望"}.get(pref["priority"],"—")
                    sc   = {1:3.0,2:2.0,3:1.0}.get(pref["priority"],0.0)
                elif pref and pref["priority"] == 0:
                    hope = "降り番"
                    sc   = 0.0
                elif a["source"] == "fallback":
                    hope = "FB"
                    sc   = 0.5
                elif a["source"] in ("swap", "exact"):
                    hope = "補完"
                    sc   = 0.5
                else:
                    hope = "降り番"
                    sc   = 0.0
                tied_mark = "※" if a.get("tied") else ""
                detail_rows.append([
                    Paragraph(player_name_map.get(a["player_id"], a["player_name"]), dname_s),
                    Paragraph(a["part_name"], dcell_s),
                    Paragraph(hope + tied_mark, dcell_s),
                    Paragraph(f"{sc:.1f}", dcell_s),
                ])

            # 曲別一覧もParagraphで折り返し
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

            dtbl = Table(detail_rows_p, colWidths=[32*mm, 85*mm, 22*mm, 11*mm], repeatRows=1)
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
        [[Paragraph(str(c), st["cellb_wht"] if i==0 else st["cell"])
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
        [[Paragraph(str(c), st["cellb_wht"] if i==0 else st["cell"])
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
        [[Paragraph(str(c), st["cellb_wht"] if i==0 else st["cell"])
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

    # 厳密解モードの場合のみ追加Tipsページ
    is_exact = (
        any("厳密解" in r.get("label", "") for r in results) or
        any("厳密解" in r.get("label", "") for r in (compare_results or []))
    )
    if is_exact:
        story.append(PageBreak())
        story.append(Paragraph("ArtéMis HARMONIA　アサイン検討 Tips", st["subtitle"]))
        story.append(Spacer(1, 2*mm))
        story.append(Paragraph("厳密解モードについて", st["title"]))
        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC")))
        story.append(Spacer(1, 6*mm))

        # 厳密解の仕組み
        story.append(Paragraph("■ 厳密解とは", st["h2"]))
        story.append(Spacer(1, 2*mm))
        exact_algo_data = [
            ["手法", "説明"],
            ["整数計画法\n(MILP)",
             "「すべての可能な割当の組み合わせ」の中から、\n"
             "数学的に証明された最良の解を求める手法。\n"
             "高速モード（ヒューリスティック）が「良い解」を\n"
             "素早く出すのに対し、厳密解は「最良の解」を保証する。\n"
             "scipy.optimize.milp（HiGHSソルバー）を使用。"],
        ]
        exact_algo_tbl = Table(
            [[Paragraph(str(c), st["cellb_wht"] if i==0 else st["cell"])
              for c in row]
             for i, row in enumerate(exact_algo_data)],
            colWidths=[28*mm, _tips_W - 28*mm],
            repeatRows=1,
        )
        exact_algo_tbl.hAlign = "LEFT"
        exact_algo_tbl.setStyle(TableStyle([
            ("FONT",          (0,0), (-1,-1), font,   8),
            ("FONT",          (0,0), (-1, 0), font_b, 8),
            ("BACKGROUND",    (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
            ("TEXTCOLOR",     (0,0), (-1, 0), colors.white),
            ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING",   (0,0), (-1,-1), 4),
        ]))
        story.append(exact_algo_tbl)
        story.append(Spacer(1, 6*mm))

        # 数式定義
        story.append(Paragraph("■ 問題の数式定義", st["h2"]))
        story.append(Spacer(1, 2*mm))
        math_data = [
            ["要素", "定義"],
            ["決定変数",
             "x[p, s, t] ∈ {0, 1}\n"
             "奏者pを曲sのパートtに割り当てるなら1、そうでなければ0"],
            ["制約 C1\n（必要数充足）",
             "Σ_p x[p,s,t] = req[s,t]　　各パートの必要人数を満たす"],
            ["制約 C2\n（1奏者1曲1パート）",
             "Σ_t x[p,s,t] ≤ 1　　同じ奏者は同じ曲で複数パートを掛け持ちしない"],
            ["制約 C3\n（欠席・NG除外）",
             "x[p,s,t] = 0　（欠席者、またはNGを指定した組み合わせ）"],
        ]
        math_tbl = Table(
            [[Paragraph(str(c), st["cellb_wht"] if i==0 else st["cell"])
              for c in row]
             for i, row in enumerate(math_data)],
            colWidths=[32*mm, _tips_W - 32*mm],
            repeatRows=1,
        )
        math_tbl.hAlign = "LEFT"
        math_tbl.setStyle(TableStyle([
            ("FONT",          (0,0), (-1,-1), font,   8),
            ("FONT",          (0,0), (-1, 0), font_b, 8),
            ("BACKGROUND",    (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
            ("TEXTCOLOR",     (0,0), (-1, 0), colors.white),
            ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING",   (0,0), (-1,-1), 4),
            ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.white, colors.HexColor("#F5F5F5")]),
        ]))
        story.append(math_tbl)
        story.append(Spacer(1, 6*mm))

        # 候補ごとの目的関数
        story.append(Paragraph("■ 各候補の目的関数（厳密解）", st["h2"]))
        story.append(Spacer(1, 2*mm))
        obj_data = [
            ["候補", "目的関数", "高速モードとの対応"],
            ["候補A\n第1希望率最大",
             "max Σ (10000 + score[p,s,t]) × x[p,s,t]\n（第1希望の割当に10000点ボーナス）",
             "完全一致\n第1希望率最大→同率なら総スコアで決定"],
            ["候補B\n総スコア最大",
             "max Σ score[p,s,t] × x[p,s,t]",
             "完全一致"],
            ["候補C\n公平性重視",
             "max m × 1000 + Σ score × x\nただし m ≤ y_p（全奏者）\ny_p = 奏者pの総得点",
             "完全一致\n最低スコア最大→同率なら総スコアで決定"],
            ["候補D\n降り番均等",
             "min 100 × (c_max − c_min) − Σ score × x\nc_max/c_min = 割当数の最大/最小",
             "近似一致\n割当数の範囲最小→同率なら総スコアで決定"],
        ]
        obj_tbl = Table(
            [[Paragraph(str(c), st["cellb_wht"] if i==0 else st["cell"])
              for c in row]
             for i, row in enumerate(obj_data)],
            colWidths=[26*mm, 72*mm, _tips_W - 98*mm],
            repeatRows=1,
        )
        obj_tbl.hAlign = "LEFT"
        obj_tbl.setStyle(TableStyle([
            ("FONT",          (0,0), (-1,-1), font,   8),
            ("FONT",          (0,0), (-1, 0), font_b, 8),
            ("BACKGROUND",    (0,0), (-1, 0), colors.HexColor("#2C2C6C")),
            ("TEXTCOLOR",     (0,0), (-1, 0), colors.white),
            ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING",   (0,0), (-1,-1), 4),
            ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.white, colors.HexColor("#F5F5F5")]),
        ]))
        story.append(obj_tbl)
        story.append(Spacer(1, 6*mm))

        # 厳密解の使い方Tips
        story.append(Paragraph("■ 厳密解モードの使い方", st["h2"]))
        story.append(Spacer(1, 2*mm))
        exact_tips = [
            "厳密解は「数学的に最良の解」を保証する。高速モードの結果と比較することで、ヒューリスティックの精度を確認できる。",
            "目的値（スコア合計など）が高速モードと一致すれば、高速モードが最適解を出せていた証拠。差がある場合は厳密解を採用する。",
            "奏者数×パート定義数が200以下なら数秒以内で解ける。それより大きい場合は時間がかかることがある（上限60秒）。",
            "候補Dの降り番均等は「割当数の範囲最小化」で近似しているため、高速モードの標準偏差最小化と完全一致しない場合がある。",
            "制約（欠席・NG・必要数）は数学的に厳密に守られる。希望未提出者は割当対象外。希望提出者の中でパート希望がない人への補完割当（補完）は発生しうる。",
        ]
        for i, tip in enumerate(exact_tips, 1):
            story.append(Paragraph(f"{i}. {tip}", st["body"]))
            story.append(Spacer(1, 1.5*mm))

    # ── 比較ページ（compare_resultsが指定された場合）────────────
    if compare_results:
        story.append(PageBreak())
        story.append(Paragraph("ヒューリスティック vs 厳密解　比較", st["h2"]))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                color=colors.HexColor("#CCCCCC")))
        story.append(Spacer(1, 4*mm))

        try:
            from concert.services.verify_results import verify as _vfy
        except ImportError:
            _vfy = None

        # スコア比較テーブル（候補ごと）
        if _vfy:
            for r_h, r_e in zip(results, compare_results):
                vh = _vfy(r_h["assignments"], r_h["pref_map"])
                ve = _vfy(r_e["assignments"], r_e["pref_map"])
                opt = ve["total_score"]
                cur = vh["total_score"]
                gap = opt - cur
                rate = (cur / opt * 100) if opt > 0 else 100.0
                cmp_rows = [
                    ["指標", "ヒューリスティック", "厳密解", "差"],
                    ["総スコア",
                     f"{vh['total_score']:.1f}", f"{ve['total_score']:.1f}", f"{gap:+.1f}"],
                    ["第1希望本数",
                     f"{vh['first_choice_count']}件", f"{ve['first_choice_count']}件",
                     f"{ve['first_choice_count']-vh['first_choice_count']:+d}"],
                    ["第1希望率",
                     f"{vh['first_choice_rate']:.1%}", f"{ve['first_choice_rate']:.1%}", "—"],
                    ["最低スコア",
                     f"{vh['min_player_score']:.1f}", f"{ve['min_player_score']:.1f}",
                     f"{ve['min_player_score']-vh['min_player_score']:+.1f}"],
                    ["最適解比率", f"{rate:.1f}%", "100%", "—"],
                ]
                cmp_ps = [
                    [Paragraph(str(c), st["cellb_wht"] if ri == 0 else st["cell"])
                     for c in row]
                    for ri, row in enumerate(cmp_rows)
                ]
                cmp_tbl = Table(cmp_ps, colWidths=[35*mm, 40*mm, 40*mm, 25*mm])
                cmp_tbl.setStyle(TableStyle([
                    ("BACKGROUND", (0,0), (-1,0),  colors.HexColor("#2C3E50")),
                    ("BACKGROUND", (0,1), (-1,-1), colors.HexColor("#F8F8F8")),
                    ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
                    ("FONT",       (0,0), (-1,-1), font,   8),
                    ("FONT",       (0,0), (-1,0),  font_b, 8),
                    ("TOPPADDING",    (0,0), (-1,-1), 4),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 4),
                ]))
                label_short = r_h["label"].split("：")[0]
                story.append(KeepTogether([
                    Paragraph(f"{label_short}　スコア比較", st["h3"]),
                    Spacer(1, 2*mm),
                    cmp_tbl,
                    Spacer(1, 5*mm),
                ]))

        # 曲別割当の横並び比較
        story.append(PageBreak())
        story.append(Paragraph("曲別割当　横並び比較", st["h2"]))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                color=colors.HexColor("#CCCCCC")))
        story.append(Spacer(1, 4*mm))

        cell_s = ParagraphStyle("cs", fontName=font,   fontSize=7, leading=10)
        cell_b = ParagraphStyle("cb", fontName=font_b, fontSize=7, leading=10,
                                textColor=colors.white)

        def _cmp_song_tbl(items, pm):
            rows = [["奏者", "パート", "希望"]]
            for a in sorted(items, key=lambda x: x["part_name"]):
                pk   = str((a["player_id"], a["song_id"], a["part_id"]))
                pref = pm.get(pk)
                if pref and pref.get("priority", 0) > 0:
                    hope = {1:"第1",2:"第2",3:"第3"}.get(pref["priority"], "—")
                elif a.get("source") in ("fallback","swap","exact"):
                    hope = "補完"
                else:
                    hope = "降り番"
                rows.append([
                    player_name_map.get(a["player_id"], a.get("player_name","")),
                    Paragraph(a["part_name"], cell_s),
                    hope,
                ])
            ps = [[Paragraph(str(c), cell_b if ri==0 else cell_s)
                   if isinstance(c, str) else c
                   for c in row]
                  for ri, row in enumerate(rows)]
            t = Table(ps, colWidths=[22*mm, 50*mm, 12*mm])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#2C3E50")),
                ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
                ("FONT",       (0,0), (-1,-1), font, 7),
                ("TOPPADDING",    (0,0), (-1,-1), 3),
                ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ]))
            return t

        for r_h, r_e in zip(results, compare_results):
            label_h = r_h["label"]
            label_e = r_e["label"]
            story.append(KeepTogether([
                Paragraph(f"{label_h.split('：')[0]}", st["h3"]),
                Spacer(1, 1*mm),
            ]))
            for sid in song_order:
                sname = song_name_map.get(sid, sid)
                items_h = [a for a in r_h["assignments"] if a["song_id"] == sid]
                items_e = [a for a in r_e["assignments"] if a["song_id"] == sid]
                if not items_h and not items_e:
                    continue
                tbl_h = _cmp_song_tbl(items_h, r_h["pref_map"])
                tbl_e = _cmp_song_tbl(items_e, r_e["pref_map"])
                outer = Table(
                    [[Paragraph(f"H: {sname}", cell_s),
                      Paragraph(f"E: {sname}", cell_s)],
                     [tbl_h, tbl_e]],
                    colWidths=[93*mm, 93*mm],
                )
                outer.setStyle(TableStyle([
                    ("FONT",        (0,0), (-1,-1), font, 7),
                    ("VALIGN",      (0,0), (-1,-1), "TOP"),
                    ("LEFTPADDING", (0,0), (-1,-1), 2),
                    ("RIGHTPADDING",(0,0), (-1,-1), 2),
                    ("BOTTOMPADDING",(0,0),(-1,-1), 4),
                ]))
                story.append(outer)
            story.append(Spacer(1, 6*mm))

    doc.build(story)
    buf.seek(0)
    return buf.read()
