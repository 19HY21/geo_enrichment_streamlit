"""
日本郵政の郵便番号マスタと総務省の団体コードマスタを突合し、統合マスタを生成するスクリプト。
- 入力:
    mst/japanpost_zipcode_mst.xlsx
    mst/mic_localgoverment_mst.xlsx
- 出力:
    mst/zipcode_localgoverment_mst.xlsx
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "mst"
ZIP_FILE = DATA_DIR / "japanpost_zipcode_mst.xlsx"
MIC_FILE = DATA_DIR / "mic_localgoverment_mst.xlsx"
OUTPUT_FILE = DATA_DIR / "zipcode_localgoverment_mst.xlsx"

# カラム名の正規化マップ（括弧の半角/全角や改行・空白を吸収）
CANONICAL = {
    "団体コード": "団体コード",
    "都道府県コード": "都道府県コード",
    "都道府県名（漢字）": "都道府県名（漢字）",
    "都道府県名(漢字)": "都道府県名（漢字）",
    "市区町村名（漢字）": "市区町村名（漢字）",
    "市区町村名(漢字)": "市区町村名（漢字）",
    "都道府県名（カナ）": "都道府県名（カナ）",
    "都道府県名(カナ)": "都道府県名（カナ）",
    "市区町村名（カナ）": "市区町村名（カナ）",
    "市区町村名(カナ)": "市区町村名（カナ）",
    "郵便番号": "郵便番号",
    "町域名（漢字）": "町域名（漢字）",
    "町域名(漢字)": "町域名（漢字）",
    "小字名、丁目、番地等（漢字）": "小字名、丁目、番地等（漢字）",
    "小字名、丁目、番地等(漢字)": "小字名、丁目、番地等（漢字）",
    "大口事業所名（漢字）": "大口事業所名（漢字）",
    "大口事業所名(漢字)": "大口事業所名（漢字）",
    "事業所郵便番号フラグ": "事業所郵便番号フラグ",
    "政令指定都市フラグ": "政令指定都市フラグ",
}


def normalize_col(name: str) -> str:
    s = str(name or "")
    s = s.replace("(", "（").replace(")", "）")
    s = s.replace(" ", "").replace("\u3000", "").replace("\n", "")
    return s


def normalize_name(value: object) -> str:
    """市区町村名などの正規化（スペース除去、ヶ→ケ）。"""
    if pd.isna(value):
        return ""
    text = str(value)
    text = re.sub(r"[\s\u3000]", "", text)
    text = text.replace("ヶ", "ケ")
    return text


def load_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    zip_df = pd.read_excel(ZIP_FILE, dtype={"郵便番号": str})
    mic_df = pd.read_excel(
        MIC_FILE,
        dtype={"都道府県コード": str, "団体コード": str, "政令指定都市フラグ": str},
    )
    # カラム名を正規化
    zip_df = zip_df.rename(columns=lambda c: CANONICAL.get(normalize_col(c), c))
    mic_df = mic_df.rename(columns=lambda c: CANONICAL.get(normalize_col(c), c))
    return zip_df, mic_df


def prepare_mic(mic_df: pd.DataFrame) -> pd.DataFrame:
    mic = mic_df.copy()
    mic["pref_norm"] = mic["都道府県名（漢字）"].map(normalize_name)
    mic["city_norm"] = mic["市区町村名（漢字）"].map(normalize_name)
    mic["city_len"] = mic["city_norm"].str.len()
    return mic


def prepare_zip(zip_df: pd.DataFrame) -> pd.DataFrame:
    df = zip_df.copy()
    df["pref_norm"] = df["都道府県名（漢字）"].map(normalize_name)
    df["city_norm"] = df["市区町村名（漢字）"].map(normalize_name)
    return df


def match_city_rows(zip_df: pd.DataFrame, mic_df: pd.DataFrame) -> pd.DataFrame:
    pref_code_map = (
        mic_df.drop_duplicates(subset=["pref_norm"])[["pref_norm", "都道府県コード"]]
        .set_index("pref_norm")["都道府県コード"]
    )

    mic_groups = {
        pref: g.sort_values("city_len", ascending=False)
        for pref, g in mic_df.groupby("pref_norm")
    }

    matched_parts: list[pd.DataFrame] = []

    for pref, pref_zip in zip_df.groupby("pref_norm"):
        mic_group = mic_groups.get(pref)
        if mic_group is None:
            matched_parts.append(pref_zip)
            continue

        pref_zip = pref_zip.copy()
        matched_mask = pd.Series(False, index=pref_zip.index)

        for _, mic_row in mic_group.iterrows():
            pattern = re.escape(mic_row["city_norm"])
            mask = (~matched_mask) & pref_zip["city_norm"].str.contains(pattern, regex=True, na=False)
            if not mask.any():
                continue

            pref_zip.loc[mask, "都道府県コード"] = mic_row["都道府県コード"]
            pref_zip.loc[mask, "団体コード"] = mic_row["団体コード"]
            pref_zip.loc[mask, "政令指定都市フラグ"] = mic_row["政令指定都市フラグ"]
            matched_mask |= mask

        matched_parts.append(pref_zip)

    merged = pd.concat(matched_parts, ignore_index=True)
    merged["都道府県コード"] = merged["都道府県コード"].fillna(merged["pref_norm"].map(pref_code_map))
    merged["政令指定都市フラグ"] = merged["政令指定都市フラグ"].fillna("0")
    return merged


def export_to_excel(
    merged: pd.DataFrame,
    zip_input: pd.DataFrame,
    mic_input: pd.DataFrame,
) -> None:
    output_cols = [
        "郵便番号",
        "都道府県コード",
        "都道府県名（漢字）",
        "団体コード",
        "市区町村名（漢字）",
        "政令指定都市フラグ",
        "町域名（漢字）",
        "小字名、丁目、番地等（漢字）",
        "事業所郵便番号フラグ",
        "大口事業所名（漢字）",
    ]

    final_df = merged.copy()
    final_df = final_df[output_cols].copy()

    # 団体コード昇順（欠損は末尾）
    final_df = final_df.sort_values("団体コード", kind="stable", na_position="last")

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="zipcode_localgoverment_mst", index=False)
        zip_input.to_excel(writer, sheet_name="japanpost_zipcode_mst", index=False)
        mic_input.to_excel(writer, sheet_name="mic_localgoverment_mst", index=False)

    unmatched = final_df["団体コード"].isna().sum()
    print(f"completed: {len(final_df)} rows written; unmatched city rows: {unmatched}")


def main() -> None:
    zip_df, mic_df = load_sources()
    mic_prepared = prepare_mic(mic_df)
    zip_prepared = prepare_zip(zip_df)
    merged = match_city_rows(zip_prepared, mic_prepared)
    export_to_excel(merged, zip_df, mic_df)


if __name__ == "__main__":
    main()
