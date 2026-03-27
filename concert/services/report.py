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

# ── グラフ生成関数（matplotlib）────────────────────────────

_HOPE_CATS   = ["第1希望","第2希望","第3希望","補完","降り番"]
_HOPE_COLORS = ["#3C3489","#085041","#633806","#A32D2D","#AAAAAA"]
_DIST_CATS   = ["0曲","1曲","2曲","3曲以上"]
_DIST_COLORS = ["#CCCCCC","#7B9ED9","#3C5FA0","#1A2F6A"]


def _collect_bar_data(results: list[dict]) -> list[dict]:
    """resultsから希望充足内訳を集計する。"""
    rows = []
    for r in results:
        pm = r["pref_map"]
        c = {k: 0 for k in _HOPE_CATS}
        for a in r["assignments"]:
            pk   = str((a["player_id"], a["song_id"], a["part_id"]))
            pref = pm.get(pk)
            prio = pref.get("priority", 0) if pref else None
            src  = a.get("source", "")
            if prio == 1:   c["第1希望"] += 1
            elif prio == 2: c["第2希望"] += 1
            elif prio == 3: c["第3希望"] += 1
            elif prio is None or (isinstance(prio, int) and prio <= 0
                                  and src in ("fallback","swap","exact")):
                c["補完"] += 1
            else:           c["降り番"] += 1
        rows.append({"label": r["label"].split("：")[0], **c})
    return rows


def _collect_dist_data(results: list[dict]) -> list[dict]:
    """resultsから奏者ごとの担当曲数分布を集計する。"""
    rows = []
    for r in results:
        cnt: dict[str, int] = {}
        for a in r["assignments"]:
            pid = a["player_id"]
            cnt[pid] = cnt.get(pid, 0) + 1
        dist = {"0曲":0,"1曲":0,"2曲":0,"3曲以上":0}
        # 希望提出者で割当0の人
        pref_pids = {v["player_id"] for v in r["pref_map"].values()}
        for pid in pref_pids:
            if pid not in cnt:
                dist["0曲"] += 1
        for n in cnt.values():
            if n == 1:   dist["1曲"] += 1
            elif n == 2: dist["2曲"] += 1
            else:        dist["3曲以上"] += 1
        rows.append({"label": r["label"].split("：")[0], **dist})
    return rows


def make_stacked_bar(results: list[dict]) -> bytes:
    """希望充足の積み上げ横棒グラフをPNG bytesで返す。"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import io
    plt.rcParams['font.family'] = ['IPAGothic', 'Noto Sans CJK JP', 'sans-serif']

    bar_data = _collect_bar_data(results)
    labels   = [d["label"] for d in bar_data]
    n        = len(labels)
    fig, ax  = plt.subplots(figsize=(5.5, max(1.6, n * 0.55 + 0.8)))
    bottoms  = [0] * n
    fs = 7

    for cat, col in zip(_HOPE_CATS, _HOPE_COLORS):
        vals = [d.get(cat, 0) for d in bar_data]
        bars = ax.barh(labels, vals, left=bottoms, color=col, height=0.55, label=cat)
        for bar, v, b in zip(bars, vals, bottoms):
            if v > 0:
                ax.text(b + v / 2, bar.get_y() + bar.get_height() / 2,
                        str(v), ha='center', va='center',
                        fontsize=fs - 1, color='white', fontweight='bold')
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    ax.set_xlabel("割当件数", fontsize=fs)
    ax.tick_params(axis='both', labelsize=fs)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    patches = [mpatches.Patch(color=c, label=l)
               for l, c in zip(_HOPE_CATS, _HOPE_COLORS)]
    ax.legend(handles=patches, fontsize=fs - 1, loc='lower right',
              ncol=len(_HOPE_CATS), bbox_to_anchor=(1, -0.38), frameon=False)
    fig.tight_layout(pad=0.5)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def make_dist_bar(results: list[dict]) -> bytes:
    """奏者ごとの担当曲数分布棒グラフをPNG bytesで返す。"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import io
    plt.rcParams['font.family'] = ['IPAGothic', 'Noto Sans CJK JP', 'sans-serif']

    dist_data = _collect_dist_data(results)
    labels    = [d["label"] for d in dist_data]
    n_labels  = len(labels)
    n_cats    = len(_DIST_CATS)
    bar_w     = 0.18
    x         = list(range(n_labels))
    fs        = 7

    fig, ax = plt.subplots(figsize=(5.5, 2.0))
    for ci, (cat, col) in enumerate(zip(_DIST_CATS, _DIST_COLORS)):
        vals   = [d.get(cat, 0) for d in dist_data]
        offset = (ci - n_cats / 2 + 0.5) * bar_w
        bars   = ax.bar([xi + offset for xi in x], vals, width=bar_w,
                        color=col, label=cat)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.05,
                        str(v), ha='center', va='bottom', fontsize=fs - 1)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=fs)
    ax.set_ylabel("人数", fontsize=fs)
    max_v = max((d.get(c, 0) for d in dist_data for c in _DIST_CATS), default=1)
    ax.set_yticks(range(0, max_v + 2))
    ax.tick_params(axis='both', labelsize=fs)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    patches = [mpatches.Patch(color=c, label=l)
               for l, c in zip(_DIST_CATS, _DIST_COLORS)]
    ax.legend(handles=patches, fontsize=fs - 1, loc='upper right', frameon=False)
    fig.tight_layout(pad=0.5)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── グラフ生成関数（matplotlib）────────────────────────────

_HOPE_CATS   = ["第1希望","第2希望","第3希望","補完","降り番"]
_HOPE_COLORS = ["#3C3489","#085041","#633806","#A32D2D","#AAAAAA"]
_DIST_CATS   = ["0曲","1曲","2曲","3曲以上"]
_DIST_COLORS = ["#CCCCCC","#7B9ED9","#3C5FA0","#1A2F6A"]


def _collect_bar_data(results: list[dict]) -> list[dict]:
    """resultsから希望充足内訳を集計する。"""
    rows = []
    for r in results:
        pm = r["pref_map"]
        c = {k: 0 for k in _HOPE_CATS}
        for a in r["assignments"]:
            pk   = str((a["player_id"], a["song_id"], a["part_id"]))
            pref = pm.get(pk)
            prio = pref.get("priority", 0) if pref else None
            src  = a.get("source", "")
            if prio == 1:   c["第1希望"] += 1
            elif prio == 2: c["第2希望"] += 1
            elif prio == 3: c["第3希望"] += 1
            elif prio is None or (isinstance(prio, int) and prio <= 0
                                  and src in ("fallback","swap","exact")):
                c["補完"] += 1
            else:           c["降り番"] += 1
        rows.append({"label": r["label"].split("：")[0], **c})
    return rows


def _collect_dist_data(results: list[dict]) -> list[dict]:
    """resultsから奏者ごとの担当曲数分布を集計する。"""
    rows = []
    for r in results:
        cnt: dict[str, int] = {}
        for a in r["assignments"]:
            pid = a["player_id"]
            cnt[pid] = cnt.get(pid, 0) + 1
        dist = {"0曲":0,"1曲":0,"2曲":0,"3曲以上":0}
        pref_pids = {v["player_id"] for v in r["pref_map"].values()}
        for pid in pref_pids:
            if pid not in cnt:
                dist["0曲"] += 1
        for n in cnt.values():
            if n == 1:   dist["1曲"] += 1
            elif n == 2: dist["2曲"] += 1
            else:        dist["3曲以上"] += 1
        rows.append({"label": r["label"].split("：")[0], **dist})
    return rows


def make_stacked_bar(results: list[dict]) -> bytes:
    """希望充足の積み上げ横棒グラフをPNG bytesで返す。"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import io
    plt.rcParams['font.family'] = ['IPAGothic', 'Noto Sans CJK JP', 'sans-serif']

    bar_data = _collect_bar_data(results)
    labels   = [d["label"] for d in bar_data]
    n        = len(labels)
    fig, ax  = plt.subplots(figsize=(5.5, max(1.6, n * 0.55 + 0.8)))
    bottoms  = [0] * n
    fs = 7

    for cat, col in zip(_HOPE_CATS, _HOPE_COLORS):
        vals = [d.get(cat, 0) for d in bar_data]
        bars = ax.barh(labels, vals, left=bottoms, color=col, height=0.55)
        for bar, v, b in zip(bars, vals, bottoms):
            if v > 0:
                ax.text(b + v / 2, bar.get_y() + bar.get_height() / 2,
                        str(v), ha='center', va='center',
                        fontsize=fs - 1, color='white', fontweight='bold')
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    ax.set_xlabel("割当件数", fontsize=fs)
    ax.tick_params(axis='both', labelsize=fs)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    patches = [mpatches.Patch(color=c, label=l)
               for l, c in zip(_HOPE_CATS, _HOPE_COLORS)]
    ax.legend(handles=patches, fontsize=fs - 1, loc='lower right',
              ncol=len(_HOPE_CATS), bbox_to_anchor=(1, -0.38), frameon=False)
    fig.tight_layout(pad=0.5)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def make_dist_bar(results: list[dict]) -> bytes:
    """奏者ごとの担当曲数分布棒グラフをPNG bytesで返す。"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import io
    plt.rcParams['font.family'] = ['IPAGothic', 'Noto Sans CJK JP', 'sans-serif']

    dist_data = _collect_dist_data(results)
    labels    = [d["label"] for d in dist_data]
    n_labels  = len(labels)
    n_cats    = len(_DIST_CATS)
    bar_w     = 0.18
    x         = list(range(n_labels))
    fs        = 7

    fig, ax = plt.subplots(figsize=(5.5, 2.0))
    for ci, (cat, col) in enumerate(zip(_DIST_CATS, _DIST_COLORS)):
        vals   = [d.get(cat, 0) for d in dist_data]
        offset = (ci - n_cats / 2 + 0.5) * bar_w
        bars   = ax.bar([xi + offset for xi in x], vals, width=bar_w,
                        color=col, label=cat)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.05,
                        str(v), ha='center', va='bottom', fontsize=fs - 1)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=fs)
    ax.set_ylabel("人数", fontsize=fs)
    max_v = max((d.get(c, 0) for d in dist_data for c in _DIST_CATS), default=1)
    ax.set_yticks(range(0, max_v + 2))
    ax.tick_params(axis='both', labelsize=fs)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    patches = [mpatches.Patch(color=c, label=l)
               for l, c in zip(_DIST_CATS, _DIST_COLORS)]
    ax.legend(handles=patches, fontsize=fs - 1, loc='upper right', frameon=False)
    fig.tight_layout(pad=0.5)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# 候補ごとの説明文
from concert.services.score_constants import CANDIDATE_DESC as _CAND_DESC_RAW
# report.py では short 説明を使用
CANDIDATE_DESC = {k: v["short"] for k, v in _CAND_DESC_RAW.items()}

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

    # 共通採点関数（全PDFでこれだけを使う）
    try:
        from concert.services.verify_results import verify as _verify
    except ImportError:
        _verify = None

    def _v(r):
        """1つのresultをverify()で採点して返す。"""
        if _verify:
            return _verify(r["assignments"], r["pref_map"])
        s = r.get("stats", {})
        return {
            "total_score":              s.get("total_score", 0),
            "first_choice_count":       0,
            "first_choice_rate":        s.get("first_choice_rate", 0),
            "min_player_score":         s.get("min_score", 0),
            "fallback_count":           sum(1 for a in r["assignments"] if a.get("source")=="fallback"),
            "swap_count":               0,
            "assignment_count_by_player": {},
        }

    # 共通採点関数（全PDFでこれだけを使う）
    try:
        from concert.services.verify_results import verify as _verify
    except ImportError:
        _verify = None

    def _v(r):
        """1つのresultをverify()で採点して返す。verify未使用時はstatsからフォールバック。"""
        if _verify:
            return _verify(r["assignments"], r["pref_map"])
        s = r.get("stats", {})
        return {
            "total_score":        s.get("total_score", 0),
            "first_choice_count": 0,
            "first_choice_rate":  s.get("first_choice_rate", 0),
            "min_player_score":   s.get("min_score", 0),
            "fallback_count":     sum(1 for a in r["assignments"] if a.get("source")=="fallback"),
            "swap_count":         0,
        }

    # ── 表紙 ──────────────────────────────────────────────────
    story.append(Spacer(1, 20*mm))
    story.append(Paragraph("ArtéMis HARMONIA", st["subtitle"]))
    story.append(Spacer(1, 2*mm))
    if compare_results:
        story.append(Paragraph("ヒューリスティック vs 厳密解 比較", st["title"]))
    else:
        story.append(Paragraph("パート割当 候補案", st["title"]))
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph(concert_name, st["h2"]))
    story.append(Spacer(1, 4*mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC")))
    story.append(Spacer(1, 6*mm))

    # 比較モード：通常ページを全スキップ→①〜⑥のみ出力
    if compare_results:
        pass  # 以下の通常ページ生成をスキップ（後述のif not compare_results:で制御）

    if not compare_results:
        # ── 候補案サマリー比較表 ──────────────────────────────────
        story.append(Paragraph("■ 候補案 比較サマリー", st["h2"]))
    
        header = ["候補", "説明", "総スコア", "第1希望率", "最低スコア", "FB+補完"]
        rows = [header]
        for r in results:
            vr = _v(r)
            fb = vr.get("supplemental_count", vr.get("fallback_count", 0))
            desc = CANDIDATE_DESC.get(r["label"], "")
            rows.append([
                r["label"].split("：")[0],
                Paragraph(desc, st["small"]),
                f"{vr['total_score']:.1f}点",
                f"{vr['first_choice_rate']*100:.0f}%",
                f"{vr['min_player_score']:.1f}点",
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
        # ── 違いの解説ページ ──────────────────────────────────
        story.append(PageBreak())
        story.append(Paragraph("ArtéMis HARMONIA　アサイン検討 Tips", st["subtitle"]))
        story.append(Spacer(1, 2*mm))
        story.append(Paragraph("ヒューリスティック解 vs 厳密解", st["title"]))
        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC")))
        story.append(Spacer(1, 6*mm))

        diff_data = [
            ["", "ヒューリスティック解", "厳密解"],
            ["手法", "貪欲法 + 反復局所探索（ILS）", "整数計画法（MILP / HiGHSソルバー）"],
            ["速度", "◎ 瞬時（0.1秒以下）", "○ 数秒（規模による）"],
            ["最適性", "△ 局所最適（保証なし）", "◎ 数学的最適解を保証"],
            ["使いどころ",
             "速さ優先・大規模演奏会・結果をすぐ確認したい場合",
             "正確さ優先・小規模確認・H解との差を検証したい場合"],
            ["候補数", "A〜D（4案）", "A〜D（4案）"],
        ]
        diff_tbl = Table(
            [[Paragraph(str(c), st["cellb_wht"] if (ri==0 or ci==0) else st["cell"])
              for ci, c in enumerate(row)]
             for ri, row in enumerate(diff_data)],
            colWidths=[25*mm, 75*mm, 75*mm],
            repeatRows=1,
        )
        diff_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0),  colors.HexColor("#2C3E50")),
            ("BACKGROUND", (0,0), (0,-1),  colors.HexColor("#34495E")),
            ("BACKGROUND", (1,1), (-1,-1), colors.HexColor("#F8F8F8")),
            ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
            ("FONT",       (0,0), (-1,-1), font,   8),
            ("FONT",       (0,0), (-1,0),  font_b, 8),
            ("FONT",       (0,0), (0,-1),  font_b, 8),
            ("VALIGN",     (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 5),
            ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ]))
        story.append(diff_tbl)
        story.append(Spacer(1, 6*mm))

        story.append(Paragraph("■ 読み方のポイント", st["h2"]))
        story.append(Spacer(1, 2*mm))
        points = [
            "総スコアが一致 → ヒューリスティック解は最適解と同じ結果が出せている。自信を持って採用できる。",
            "総スコアに差がある → 厳密解の方が良い割当が存在する。差が小さければ（1〜2点）実務上は許容範囲。",
            "第1希望本数が一致・総スコアに差 → 第2希望以下の詰めが厳密解の方が上手い。",
            "候補C（公平性）・候補D（降り番均等）は厳密解と大きく異なる場合がある。これらを重視するなら厳密解を推奨。",
        ]
        for i, p in enumerate(points, 1):
            story.append(Paragraph(f"{i}. {p}", st["body"]))
            story.append(Spacer(1, 2*mm))

        # ── スコア比較ページ ────────────────────────────────
        story.append(PageBreak())
        story.append(Paragraph("ヒューリスティック vs 厳密解　スコア比較", st["h2"]))
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
                label_a = r_h["label"]
                # 候補ごとに適切な比較指標を選択
                is_ab  = any(x in label_a for x in ["候補A", "候補B"])
                is_c   = "候補C" in label_a
                is_d   = "候補D" in label_a
                cmp_rows = [
                    ["指標", "ヒューリスティック", "厳密解", "差"],
                    ["総スコア",
                     f"{vh['total_score']:.1f}", f"{ve['total_score']:.1f}", f"{gap:+.1f}"],
                    ["第1希望本数",
                     f"{vh['first_choice_count']}件", f"{ve['first_choice_count']}件",
                     f"{ve['first_choice_count']-vh['first_choice_count']:+d}"],
                    ["第1希望率",
                     f"{vh['first_choice_rate']:.1%}", f"{ve['first_choice_rate']:.1%}", "—"],
                    ["最低スコア（公平性）",
                     f"{vh['min_player_score']:.1f}", f"{ve['min_player_score']:.1f}",
                     f"{ve['min_player_score']-vh['min_player_score']:+.1f}"],
                ]
                # 最適解比率はA/Bのみ（総スコア最大化が目的の候補）
                if is_ab:
                    cmp_rows.append(["最適解比率（総スコア）", f"{rate:.1f}%", "100%", "—"])
                elif is_c:
                    cmp_rows.append(["主指標（最低スコア）", f"{vh['min_player_score']:.1f}",
                                     f"{ve['min_player_score']:.1f}",
                                     f"{ve['min_player_score']-vh['min_player_score']:+.1f}"])
                elif is_d:
                    h_cnt = vh.get("assignment_count_by_player", {})
                    e_cnt = ve.get("assignment_count_by_player", {})
                    h_rng = (max(h_cnt.values()) - min(h_cnt.values())) if h_cnt else 0
                    e_rng = (max(e_cnt.values()) - min(e_cnt.values())) if e_cnt else 0
                    cmp_rows.append(["主指標（割当数の範囲）",
                                     f"{h_rng}曲差", f"{e_rng}曲差", f"{e_rng-h_rng:+d}"])
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

        # ── ⑤ 曲別割当 横並び比較（候補ごとにページ分割）──────
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
            label_h   = r_h["label"]
            cand_short = label_h.split("：")[0] if "：" in label_h else label_h
            cand_name  = label_h.split("：")[1] if "：" in label_h else ""
            story.append(PageBreak())
            story.append(Paragraph("ArteMis HARMONIA  アサイン検討", st["subtitle"]))
            story.append(Spacer(1, 2*mm))
            story.append(Paragraph("曲別割当 横並び比較", st["title"]))
            story.append(Paragraph(f"{cand_short}  {cand_name}", st["subtitle"]))
            story.append(Spacer(1, 4*mm))
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=colors.HexColor("#CCCCCC")))
            story.append(Spacer(1, 4*mm))
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
                    ("FONT",         (0,0), (-1,-1), font, 7),
                    ("VALIGN",       (0,0), (-1,-1), "TOP"),
                    ("LEFTPADDING",  (0,0), (-1,-1), 2),
                    ("RIGHTPADDING", (0,0), (-1,-1), 2),
                    ("BOTTOMPADDING",(0,0), (-1,-1), 4),
                ]))
                story.append(outer)

        # ── ⑥ 総括 ────────────────────────────────────────────
        story.append(PageBreak())
        story.append(Paragraph("ArteMis HARMONIA  アサイン検討 Tips", st["subtitle"]))
        story.append(Spacer(1, 2*mm))
        story.append(Paragraph("総括", st["title"]))
        story.append(Spacer(1, 4*mm))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                color=colors.HexColor("#CCCCCC")))
        story.append(Spacer(1, 6*mm))
        story.append(Paragraph("■ ヒューリスティック解の品質評価", st["h2"]))
        story.append(Spacer(1, 2*mm))

        # 候補ごとの主目的・指標・比較結果を構築
        CAND_OBJECTIVE = {
            "候補A": "第1希望率最大（同率なら総スコア）",
            "候補B": "総スコア最大",
            "候補C": "最低スコア最大（公平性）",
            "候補D": "割当数の範囲最小（降り番均等）",
        }
        summ_rows = [["候補", "主目的", "H値", "厳密解値", "比較結果"]]
        ab_rates = []
        for r_h, r_e in zip(results, compare_results):
            vh = _v(r_h)
            ve = _v(r_e)
            lbl = r_h["label"].split("：")[0]
            obj = CAND_OBJECTIVE.get(lbl, "—")
            if lbl in ("候補A", "候補B"):
                h_val  = f"{vh['total_score']:.1f}"
                e_val  = f"{ve['total_score']:.1f}"
                rate   = (vh['total_score'] / ve['total_score'] * 100
                          if ve['total_score'] > 0 else 100.0)
                ab_rates.append(rate)
                result_str = f"{rate:.1f}%"
            elif lbl == "候補C":
                h_val  = f"{vh['min_player_score']:.1f}点"
                e_val  = f"{ve['min_player_score']:.1f}点"
                result_str = ("同等" if abs(vh['min_player_score'] - ve['min_player_score']) < 0.01
                              else "Hは厳密解未満")
            elif lbl == "候補D":
                h_cnt  = vh.get("assignment_count_by_player", {})
                e_cnt  = ve.get("assignment_count_by_player", {})
                h_rng  = (max(h_cnt.values()) - min(h_cnt.values())) if h_cnt else 0
                e_rng  = (max(e_cnt.values()) - min(e_cnt.values())) if e_cnt else 0
                h_val  = f"{h_rng}曲差"
                e_val  = f"{e_rng}曲差"
                result_str = "同等" if h_rng == e_rng else ("Hが劣る" if h_rng > e_rng else "Hが優る")
            else:
                h_val = e_val = "—"; result_str = "—"
            summ_rows.append([lbl, obj, h_val, e_val, result_str])

        summ_ps = [
            [Paragraph(str(c), st["cellb_wht"] if ri==0 else st["cell"])
             for c in row]
            for ri, row in enumerate(summ_rows)
        ]
        summ_tbl = Table(summ_ps, colWidths=[18*mm, 55*mm, 22*mm, 22*mm, 28*mm])
        summ_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0),  colors.HexColor("#2C3E50")),
            ("BACKGROUND", (0,1), (-1,-1), colors.HexColor("#F8F8F8")),
            ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#BBBBBB")),
            ("FONT",       (0,0), (-1,-1), font,   8),
            ("FONT",       (0,0), (-1,0),  font_b, 8),
            ("VALIGN",     (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ]))
        story.append(summ_tbl)
        story.append(Spacer(1, 6*mm))

        # ── 総評：A/B・C・Dを別判定して4文連結 ──────────────
        # 判定値を収集
        ab_ratio_vals = []
        c_fair_ratio  = None
        d_rest_equal  = None
        d_h_rng = d_e_rng = 0
        for r_h2, r_e2 in zip(results, compare_results):
            vh2 = _v(r_h2); ve2 = _v(r_e2)
            lbl2 = r_h2["label"].split("：")[0]
            if lbl2 in ("候補A", "候補B"):
                if ve2["total_score"] > 0:
                    ab_ratio_vals.append(vh2["total_score"] / ve2["total_score"])
            elif lbl2 == "候補C":
                if ve2["min_player_score"] > 0:
                    c_fair_ratio = vh2["min_player_score"] / ve2["min_player_score"]
                elif ve2["min_player_score"] == 0 and vh2["min_player_score"] == 0:
                    c_fair_ratio = 1.0
            elif lbl2 == "候補D":
                h_cnt2 = vh2.get("assignment_count_by_player", {})
                e_cnt2 = ve2.get("assignment_count_by_player", {})
                d_h_rng = (max(h_cnt2.values()) - min(h_cnt2.values())) if h_cnt2 else 0
                d_e_rng = (max(e_cnt2.values()) - min(e_cnt2.values())) if e_cnt2 else 0
                d_rest_equal = (d_h_rng == d_e_rng)

        avg_ab = sum(ab_ratio_vals) / len(ab_ratio_vals) if ab_ratio_vals else 0

        # 1文目：候補A/B（奏者向け・希望の通りやすさで表現）
        if avg_ab >= 0.98:
            s1 = "今回の比較では、候補A・Bについては2つの計算方法でほとんど差がなく、希望の反映という点ではどちらも近い結果になりました。"
        elif avg_ab >= 0.95:
            s1 = "今回の比較では、候補A・Bについては2つの計算方法で概ね近い結果になりましたが、わずかに差が見られました。"
        elif avg_ab >= 0.90:
            s1 = "今回の比較では、候補A・Bについては2つの計算方法である程度近い結果になりましたが、希望の反映に改善余地があります。"
        else:
            s1 = "今回の比較では、候補A・Bについて2つの計算方法で差が見られました。希望の通り方が異なる可能性があるため、各候補の内容をよく確認してください。"

        # 2文目：候補C（公平性の観点）
        if c_fair_ratio is None:
            s2 = ""
        elif c_fair_ratio >= 1.0:
            s2 = "候補Cは、できるだけ誰かひとりに不満が集中しないよう配慮した案で、2つの計算方法でほぼ同じ結果になりました。"
        elif c_fair_ratio >= 0.80:
            s2 = "候補Cは、できるだけ誰かひとりに不満が集中しないよう配慮した案で、2つの計算方法で近い結果になりました。"
        else:
            s2 = "一方、候補Cのように担当の公平さを重視した案では、2つの計算方法で割当の出方が変わる場合があります。公平さを特に重視したい場合は、候補の内容をよく確認してください。"

        # 3文目：候補D（均等性の観点）
        if d_rest_equal is None:
            s3 = ""
        elif d_rest_equal:
            s3 = "候補Dは、できるだけ担当曲数の偏りが出ないようにした案で、2つの計算方法で同じ結果になりました。ただし誰がどの曲を担当するかという内容は異なる場合があるため、曲別の一覧も合わせて確認してください。"
        elif abs(d_h_rng - d_e_rng) <= 1:
            s3 = "候補Dは、担当曲数の偏りを抑えた案で、2つの計算方法でほぼ近い結果になりました。"
        else:
            s3 = "候補Dのように担当の均等さを重視した案では、2つの計算方法で割当の出方が変わることがあります。担当の偏りを特に気にする場合は、候補の内容をよく確認してください。"

        # 4文目：読み方の案内
        if avg_ab >= 0.95 and (c_fair_ratio is None or c_fair_ratio >= 0.80) and (d_rest_equal is None or d_rest_equal):
            s4 = "通常は候補A・Bを中心に見れば十分ですが、できるだけ公平さや担当の均等さを重視したい場合は、候補C・Dも参考にしてください。"
        elif avg_ab >= 0.90:
            s4 = "候補A・Bを中心に検討しつつ、公平さや均等さを重視したい場合は候補C・Dの内容も参照することをお勧めします。"
        else:
            s4 = "各候補の内容をよく見比べながら、どの観点を重視するかに応じて選択してください。"

        full_comment = " ".join(s for s in [s1, s2, s3, s4] if s)
        story.append(Paragraph(full_comment, st["body"]))
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph("■ 候補C・Dを読む際の注意", st["h2"]))
        story.append(Spacer(1, 2*mm))
        story.append(Paragraph(
            "候補C（公平性重視）・候補D（降り番均等）は「総スコアを最大化する」候補ではありません。"
            "そのため、候補A・Bより総スコアが低くなることは設計上の意図した結果です。"
            "また主指標（最低スコアや割当数の範囲）が同じ値でも、"
            "誰がどの曲を担当するかという割当の内容は候補A・Bと大きく異なる場合があります。"
            "候補C・Dは「公平性や均等性を優先したい場合」の参考案として活用してください。",
            st["body"]
        ))


    doc.build(story)
    buf.seek(0)
    return buf.read()
