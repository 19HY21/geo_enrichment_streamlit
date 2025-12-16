"""
総務省の全国地方公共団体コード一覧を取得・整形してマスタを生成するスクリプト。
仕様:
- 取得元: https://www.soumu.go.jp/main_content/000925835.xls
- シート名は年度で変動するため、"政令" を含むシートを政令指定都市、その他を現在の団体として扱う
- 団体コードでフルジョインし、政令指定都市フラグを付与
- カラム名はNFKC正規化＋括弧統一で揃える
- クリーニング:
  * 市区町村名（漢字）がNullの行は削除
  * 政令指定都市フラグは '1' 以外を '0' に
  * 都道府県コードを団体コード先頭2桁から作成
  * 都道府県名（カナ）/市区町村名（カナ）は出力前に削除
出力: mst/mic_localgoverment_mst.xlsx
"""

import io
import unicodedata
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

SRC_URL = "https://www.soumu.go.jp/main_content/000925835.xls"
OUT_PATH = Path(r"mst/mic_localgoverment_mst.xlsx")


def normalize_col(name: str) -> str:
    """NFKCで正規化し、括弧を全角に統一、スペース類を除去。"""
    s = unicodedata.normalize("NFKC", name or "")
    s = s.replace("(", "（").replace(")", "）")
    s = s.replace(" ", "").replace("\u3000", "").replace("\n", "")
    return s


CANONICAL_COLS: Dict[str, str] = {
    "団体コード": "団体コード",
    "都道府県名（漢字）": "都道府県名（漢字）",
    "市区町村名（漢字）": "市区町村名（漢字）",
    "都道府県名（カナ）": "都道府県名（カナ）",
    "市区町村名（カナ）": "市区町村名（カナ）",
}


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for c in df.columns:
        norm = normalize_col(str(c))
        if norm in CANONICAL_COLS:
            new_cols[c] = CANONICAL_COLS[norm]
    return df.rename(columns=new_cols)


def pick_sheets(xls: Dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """政令シートと現在シートを推定して返す."""
    seirei = None
    current = None
    for name, df in xls.items():
        if "政令" in name:
            seirei = df
        else:
            current = df if current is None else current  # 最初の非政令を採用
    if seirei is None or current is None:
        raise ValueError("必要なシートが見つかりません（政令/現在）")
    return current, seirei


def load_sheets() -> tuple[pd.DataFrame, pd.DataFrame]:
    resp = requests.get(SRC_URL, timeout=30)
    resp.raise_for_status()
    with io.BytesIO(resp.content) as buf:
        xls = pd.read_excel(buf, sheet_name=None, dtype=str)
    return pick_sheets(xls)


def cleanse(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = rename_columns(df)
    # 必要カラムだけに絞る
    keep = list(CANONICAL_COLS.values())
    df = df[[c for c in keep if c in df.columns]]
    return df


def merge_and_flag(df_current: pd.DataFrame, df_seirei: pd.DataFrame) -> pd.DataFrame:
    df_current["政令指定都市フラグ"] = "0"
    df_seirei["政令指定都市フラグ"] = "1"

    merged = pd.merge(
        df_current,
        df_seirei,
        on="団体コード",
        how="outer",
        suffixes=("_cur", "_sei"),
    )

    def coalesce(row, col):
        cur = row.get(f"{col}_cur")
        sei = row.get(f"{col}_sei")
        return cur if pd.notna(cur) and str(cur).strip() else sei

    out = pd.DataFrame()
    out["団体コード"] = merged["団体コード"]
    for col in ["都道府県名（漢字）", "市区町村名（漢字）", "都道府県名（カナ）", "市区町村名（カナ）"]:
        out[col] = merged.apply(lambda r: coalesce(r, col), axis=1)

    # フラグはどちらかに '1' があれば 1
    flag_cur = merged.get("政令指定都市フラグ_cur", "0")
    flag_sei = merged.get("政令指定都市フラグ_sei", "0")
    out["政令指定都市フラグ"] = (
        (flag_cur == "1") | (flag_sei == "1")
    ).map(lambda x: "1" if x else "0")

    return out


def post_process(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 市区町村名（漢字）が Null/空 は削除
    df["市区町村名（漢字）"] = df["市区町村名（漢字）"].apply(lambda x: x if pd.notna(x) and str(x).strip() else None)
    df = df.dropna(subset=["市区町村名（漢字）"])

    # フラグを '0'/'1' で正規化
    df["政令指定都市フラグ"] = df["政令指定都市フラグ"].apply(lambda x: "1" if str(x).strip() == "1" else "0")

    # 都道府県コードを付与
    df["都道府県コード"] = df["団体コード"].apply(lambda x: str(x)[:2] if pd.notna(x) else None)

    # カナ列は削除
    df = df.drop(columns=["都道府県名（カナ）", "市区町村名（カナ）"], errors="ignore")

    # 列順整理
    cols = ["団体コード", "都道府県コード", "都道府県名（漢字）", "市区町村名（漢字）", "政令指定都市フラグ"]
    remaining = [c for c in df.columns if c not in cols]
    df = df[cols + remaining]
    return df


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_current_raw, df_seirei_raw = load_sheets()

    df_current = cleanse(df_current_raw)
    df_seirei = cleanse(df_seirei_raw)

    merged = merge_and_flag(df_current, df_seirei)
    final_df = post_process(merged)

    with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="master", index=False)

    print(f"Saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
