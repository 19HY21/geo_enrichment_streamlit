import io
import os
import zipfile
import unicodedata
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR
OUT_PATH = Path(r"mst/japanpost_zipcode_mst.xlsx")
ZIP_URL = "https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip"
BIZ_URL = "https://www.post.japanpost.jp/zipcode/dl/jigyosyo/zip/jigyosyo.zip"


def normalize_zip(val: Optional[str]) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if ch.isdigit())
    if not s:
        return ""
    return s.zfill(7)


def clean_town_name(val: Optional[str]) -> str:
    """
    町域名の補正:
    - 「以下に掲載がない場合」なら空（Null）扱い
    - 「霞が関（次のビルを除く）」のような括弧書きを除去
    """
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    if "以下に掲載がない場合" in s:
        return ""
    # 括弧書きを除去（全角括弧）
    s = s.split("（")[0].strip()
    return s


def download_zip(url: str) -> bytes:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content


def read_first_csv_from_zip(raw: bytes, encoding: str = "cp932") -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("zip has no entries")
        with zf.open(names[0]) as f:
            return pd.read_csv(f, dtype=str, encoding=encoding, header=None)


# フォーマットは日本郵政配布のレイアウトに依存するため、列位置で扱う
RES_COLUMNS = [
    "全国地方公共団体コード",
    "旧郵便番号",
    "郵便番号",
    "都道府県名(カナ)",
    "市区町村名(カナ)",
    "町域名(カナ)",
    "都道府県名(漢字)",
    "市区町村名(漢字)",
    "町域名(漢字)",
    "一町域が二以上の郵便番号で表される場合の表示",
    "小字毎に番地が起番されている町域の表示",
    "丁目を有する町域の場合の表示",
    "一つの郵便番号で二以上の町域を表す場合の表示",
    "更新の表示",
    "変更理由",
]

BIZ_COLUMNS = [
    "大口事業所の所在地のJISコード",  # 0
    "大口事業所名(カナ)",            # 1
    "大口事業所名(漢字)",            # 2
    "都道府県名(漢字)",             # 3
    "市区町村名(漢字)",             # 4
    "町域名(漢字)",                # 5
    "小字名、丁目、番地等(漢字)",      # 6
    "郵便番号",                     # 7
    "取扱局(漢字)",                 # 8
    "個別番号の種別の表示",            # 9
    "複数番号の有無",               # 10
    "修正コード",                  # 11
    "旧郵便番号",                  # 12 (一部年度で存在)
]


KEEP_SCHEMA = [
    "郵便番号",
    "都道府県名(漢字)",
    "市区町村名(漢字)",
    "町域名(漢字)",
    "小字名、丁目、番地等(漢字)",
    "大口事業所名(漢字)",
    "事業所郵便番号フラグ",
]


def build_residential(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.shape[1] < len(RES_COLUMNS):
        raise ValueError("Residential CSV column count unexpected")
    df.columns = RES_COLUMNS[: df.shape[1]]
    out = pd.DataFrame()
    out["郵便番号"] = df["郵便番号"].apply(normalize_zip)
    out["都道府県名(漢字)"] = df["都道府県名(漢字)"]
    out["市区町村名(漢字)"] = df["市区町村名(漢字)"]
    out["町域名(漢字)"] = df["町域名(漢字)"].apply(clean_town_name)
    out["小字名、丁目、番地等(漢字)"] = None
    out["大口事業所名(漢字)"] = None
    out["事業所郵便番号フラグ"] = "0"
    return out


def concat_addr_parts(parts: List[Optional[str]]) -> str:
    vals = []
    for p in parts:
        if p is None:
            continue
        s = str(p).strip()
        if s:
            vals.append(s)
    return "".join(vals)


def build_business(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 列数は年度で変動するため、既知の並びに切り詰めて利用
    rename_len = min(len(BIZ_COLUMNS), df.shape[1])
    df.columns = BIZ_COLUMNS[:rename_len]
    out = pd.DataFrame()
    out["郵便番号"] = df["郵便番号"].apply(normalize_zip)
    out["都道府県名(漢字)"] = df["都道府県名(漢字)"]
    out["市区町村名(漢字)"] = df["市区町村名(漢字)"]
    out["町域名(漢字)"] = df["町域名(漢字)"].apply(clean_town_name)
    out["大口事業所名(漢字)"] = df["大口事業所名(漢字)"]
    addr_parts_raw = [
        df.get("小字名、丁目、番地等(漢字)"),
        df.get("小字名"),
        df.get("丁目"),
        df.get("番地"),
        df.get("番号"),
        df.get("以下"),
        df.get("番地等"),
    ]
    addr_parts = [s for s in addr_parts_raw if s is not None]
    if addr_parts:
        out["小字名、丁目、番地等(漢字)"] = pd.DataFrame(addr_parts).T.apply(lambda row: concat_addr_parts(row.tolist()), axis=1)
    else:
        out["小字名、丁目、番地等(漢字)"] = ""
    out["事業所郵便番号フラグ"] = out["大口事業所名(漢字)"].apply(lambda x: "1" if pd.notna(x) and str(x).strip() else "0")
    return out


def main():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    res_raw = download_zip(ZIP_URL)
    biz_raw = download_zip(BIZ_URL)

    df_res_raw = read_first_csv_from_zip(res_raw)
    df_biz_raw = read_first_csv_from_zip(biz_raw)

    res_df = build_residential(df_res_raw)
    biz_df = build_business(df_biz_raw)

    master = pd.concat([res_df, biz_df], ignore_index=True)
    master = master[KEEP_SCHEMA]

    # 並び替えを明示してExcel出力
    with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
        master.to_excel(writer, sheet_name="master", index=False)

    # 参考: Parquetも必要ならコメントアウト解除
    # master.to_parquet(OUT_PATH.with_suffix('.parquet'), index=False)

    print(f"Saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
