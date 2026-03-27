"""
concert/services/score_constants.py
希望優先度→スコアの変換定数。
assign_solver.py / verify_results.py の両方から参照する。
"""
from __future__ import annotations

# 希望優先度 → スコア（希望なし・降り番は 0.0、補完は 0.5）
SCORE_MAP: dict[int, float] = {
    1: 3.0,   # 第1希望
    2: 2.0,   # 第2希望
    3: 1.0,   # 第3希望
    0: 0.0,   # 希望なし / 降り番でも可
}

# 補完割当（希望データなし or 希望なし での割当）のスコア
SUPPLEMENTAL_SCORE: float = 0.5

# NG（割当禁止）を示す優先度値
NG_PRIORITY: int = -1
