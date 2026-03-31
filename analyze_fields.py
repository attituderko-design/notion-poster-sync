"""
HARMONIA フィールド棚卸しスクリプト
使い方:
    pip install requests
    python analyze_fields.py --token YOUR_NOTION_API_TOKEN

出力:
    field_audit.csv   … DB別フィールド×コード参照の全対応表
    summary.csv       … DB別の削除候補・掃除候補サマリ
"""

import ast
import argparse
import csv
import re
import time
import requests
from pathlib import Path

NOTION_VERSION = "2022-06-28"

# ──────────────────────────────────────────
# DB名 → Notion DB ID のマッピング
# ここだけ自分のsecrets.tomlの値に合わせて書き換えてください
# ──────────────────────────────────────────
DB_MAP = {
    "ATLAS（演奏会）":                    "2704532d7d5680ab9beed2574eb2daa5",
    "PRACTICE（練習）":                   "21b4532d7d5683c0b35c81bd73f3b5a2",
    "APOLLO（演奏曲）":                   "3224532d7d56804a85dbd2eab6ac2050",
    "INSTRUMENT（楽器種別）":             "cad4532d7d5682658898819e2fa59e87",
    "PART_DEFINITION（パート定義）":      "32c4532d7d56803ba3e1c8c87d1cd0dc",
    "PERFORMER（奏者）":                  "3224532d7d568072bbb0c2cea44d67d9",
    "CONCERT_CAST（参加者）":             "3224532d7d56808e8dd0eb06c11f92db",
    "ATTENDANCE（出欠）":                 "32c4532d7d5680e6813fe67bae986c39",
    "PLAYER_INSTRUMENT（楽器アサイン）":  "32d4532d7d5680da9634cb9626808f94",
    "RENTAL（レンタル見積）":             "cf14532d7d56821fb75d81988c8373d5",
    "PREFERENCE（希望入力）":             "32c4532d7d5680b1902dce3555590db3",
    "SCHEDULE（スケジュール）":           "32d4532d7d5680f3925ad1d79eff36e2",
    "PI_MASTER（所有楽器マスタ）":        "32e4532d7d56801d846dd4811bdc724c",
    "CONCERT_EXPENSE（経費明細）":        "32e4532d7d5680dcb3bbeee6cd603b37",
    "BILLING（見積/請求）":               "3314532d7d5680fb9cdbebd1d2730e62",
    "CONCERT_SONG（演奏会×曲）":          "3324532d7d5680f38f0fccc3adae9860",
    "HARMONIA_CONCERT（ヘッダ）":         "3334532d7d5680589934fa73ed352551",
    "CONCERT_INSTRUMENT（必要楽器）":     "3334532d7d5680b48e1ced6aae5c7b40",
    "CONCERT_ASSIGNMENT（アサイン結果）": "3344532d7d5680b38ff7e510cf001cd7",
}

# ──────────────────────────────────────────
# Step1: keys.py からコードが知っているフィールド候補を収集
# ──────────────────────────────────────────

def collect_keys_from_file(path: Path) -> set[str]:
    """keys.py内のリスト定数から文字列をすべて収集する"""
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    result = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.List):
            for elt in node.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    result.add(elt.value)
    return result

# ──────────────────────────────────────────
# Step2: 各.py ファイルからリテラル文字列をすべて収集（ワイルドカード対策）
# ──────────────────────────────────────────

def collect_strings_from_files(paths: list[Path]) -> set[str]:
    """Pythonファイル群から文字列定数を全収集（ノイズは後でフィルタ）"""
    result = set()
    for path in paths:
        try:
            src = path.read_text(encoding="utf-8")
            tree = ast.parse(src)
        except Exception:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                s = node.value.strip()
                if s:
                    result.add(s)
    return result

# ──────────────────────────────────────────
# Step3: Notion API からDBスキーマを取得
# ──────────────────────────────────────────

def fetch_schema(db_id: str, token: str) -> dict[str, str]:
    """DB IDからプロパティ名→型のdictを返す"""
    # ハイフンなしIDを正規化
    raw = db_id.replace("-", "")
    if len(raw) == 32:
        db_id = f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

    url = f"https://api.notion.com/v1/databases/{db_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
    }
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                props = r.json().get("properties", {})
                return {k: v.get("type", "unknown") for k, v in props.items()}
            else:
                print(f"  ⚠ HTTP {r.status_code} for {db_id}: {r.text[:120]}")
                return {}
        except Exception as e:
            print(f"  ⚠ 接続エラー: {e}")
    return {}

# ──────────────────────────────────────────
# Step4: 差分分析＋CSV出力
# ──────────────────────────────────────────

def normalize(s: str) -> str:
    return re.sub(r"\s+", "", s).lower()

def main():
    parser = argparse.ArgumentParser(description="HARMONIA フィールド棚卸し")
    parser.add_argument("--token", required=True, help="Notion API トークン")
    parser.add_argument(
        "--src-dir", default=".",
        help="artemis-cers のルートディレクトリ (default: カレント)"
    )
    args = parser.parse_args()

    src_dir = Path(args.src_dir)

    # keys.py のパスを探す
    keys_path = src_dir / "concert" / "services" / "keys.py"
    if not keys_path.exists():
        keys_path = src_dir / "keys.py"  # フォールバック
    if not keys_path.exists():
        print(f"❌ keys.py が見つかりません: {keys_path}")
        return

    # 解析対象ファイル
    target_files = list(src_dir.rglob("*.py"))
    print(f"🔍 解析対象: {len(target_files)} ファイル ({keys_path.name} 含む)")

    # コードが知っているフィールド候補（keys.pyリスト定数）
    keys_candidates = collect_keys_from_file(keys_path)
    # コード全体に登場する文字列（ワイルドカードimport対策）
    all_code_strings = collect_strings_from_files(target_files)

    print(f"📋 keys.py 候補数: {len(keys_candidates)}")

    # 出力行を蓄積
    detail_rows = []   # field_audit.csv
    summary_rows = []  # summary.csv

    for db_label, db_id in DB_MAP.items():
        print(f"\n⏳ {db_label} ({db_id[:8]}…) を取得中…")
        schema = fetch_schema(db_id, args.token)

        if not schema:
            print(f"  → スキーマ取得失敗。スキップ。")
            summary_rows.append({
                "DB名": db_label,
                "Notionフィールド数": "取得失敗",
                "削除候補数": "-",
                "keys.py掃除候補数": "-",
            })
            continue

        notion_fields = set(schema.keys())
        notion_norm = {normalize(f): f for f in notion_fields}

        # keys.py候補のうちこのDBに対応しそうなものは全部チェック対象
        # → 正規化で一致するものを「参照あり」と判定
        code_norm = {normalize(s): s for s in keys_candidates}
        all_code_norm = {normalize(s) for s in all_code_strings}

        delete_candidates = 0
        keys_candidates_in_db = 0

        for field in sorted(notion_fields):
            fn = normalize(field)
            in_keys  = fn in code_norm            # keys.pyのリスト定数に登場
            in_code  = fn in all_code_norm        # コード全体の文字列に登場

            if in_keys:
                ref_status = "✅ keys.py参照あり"
            elif in_code:
                ref_status = "⚠ コード内直接参照（keys.py未登録）"
            else:
                ref_status = "❌ 未参照"
                delete_candidates += 1

            detail_rows.append({
                "DB名":         db_label,
                "フィールド名": field,
                "型":           schema[field],
                "参照状況":     ref_status,
                "判定":         "削除候補" if ref_status == "❌ 未参照" else "保持",
            })

        # keys.pyにあるがNotionに存在しないフィールド候補
        keys_not_in_notion = []
        for cand in sorted(keys_candidates):
            cn = normalize(cand)
            # 短すぎる汎用文字列（タイトル・備考等）は除外
            if len(cand) <= 2:
                continue
            if cn not in notion_norm:
                keys_not_in_notion.append(cand)

        for cand in keys_not_in_notion:
            detail_rows.append({
                "DB名":         db_label,
                "フィールド名": f"[keys.py候補] {cand}",
                "型":           "（Notionに存在しない）",
                "参照状況":     "⚠ keys.py掃除候補",
                "判定":         "keys.py掃除候補",
            })

        summary_rows.append({
            "DB名":               db_label,
            "Notionフィールド数": len(notion_fields),
            "削除候補数":         delete_candidates,
            "keys.py掃除候補数":  len(keys_not_in_notion),
        })

        print(f"  → {len(notion_fields)} フィールド | 削除候補: {delete_candidates} | keys.py掃除候補: {len(keys_not_in_notion)}")
        time.sleep(0.3)  # レートリミット対策

    # CSV出力
    detail_path = Path("field_audit.csv")
    with detail_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["DB名", "フィールド名", "型", "参照状況", "判定"])
        w.writeheader()
        w.writerows(detail_rows)

    summary_path = Path("summary.csv")
    with summary_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["DB名", "Notionフィールド数", "削除候補数", "keys.py掃除候補数"])
        w.writeheader()
        w.writerows(summary_rows)

    print(f"\n✅ 完了！")
    print(f"   詳細 → {detail_path.resolve()}")
    print(f"   サマリ → {summary_path.resolve()}")
    print()
    print("💡 次のステップ:")
    print("   1. field_audit.csv を Excel/スプレッドシートで開き、判定='削除候補' で絞り込む")
    print("   2. 削除候補のフィールドをNotionで手動確認してから削除")
    print("   3. keys.py掃除候補はkeys.pyから対象リスト内の候補文字列を削除")

if __name__ == "__main__":
    main()
