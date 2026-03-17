import csv
import json
from pathlib import Path
from urllib.request import urlopen


API_URL = "https://restcountries.com/v3.1/all?fields=unMember,cca2,flag,name,translations"
BASE_DIR = Path(__file__).resolve().parents[1]
OUT_CSV = BASE_DIR / "docs" / "country_master_un.csv"


def main() -> None:
    with urlopen(API_URL, timeout=20) as res:
        data = json.loads(res.read().decode("utf-8"))

    rows: list[dict] = []
    for c in data:
        if not c.get("unMember"):
            continue
        code = (c.get("cca2") or "").upper().strip()
        if len(code) != 2:
            continue
        flag = (c.get("flag") or "").strip()
        name_en = ((c.get("name") or {}).get("common") or "").strip()
        name_ja = (((c.get("translations") or {}).get("jpn") or {}).get("common") or "").strip()
        if not name_en:
            continue
        if not name_ja:
            name_ja = name_en
        rows.append(
            {
                "Name": f"{flag} {name_ja}" if flag else name_ja,
                "国コード": code,
                "国名": name_en,
                "国名_日本語": name_ja,
                "国旗": flag,
            }
        )

    rows.sort(key=lambda x: x["国名_日本語"].lower())
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["Name", "国コード", "国名", "国名_日本語", "国旗"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"written: {OUT_CSV} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
