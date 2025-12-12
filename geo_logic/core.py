# -*- coding: utf-8 -*-
"""
Core geocoding logic
- マスタ読み込み
- 郵便番号/住所突合
- ジオコーディングとキャッシュ
"""
from __future__ import annotations

import json
import math
import os
import re
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# 定数定義
BASE_DIR = os.path.dirname(__file__)
MASTER_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "zipcode_localgoverment_mst.xlsx"))
OUTPUT_SUFFIX = "_GSI"
CHUNK_SIZE = 100_000  # Nominatim負荷を抑える
CHUNK_SLEEP_SEC = 7.0
REQUEST_SLEEP_SEC = 0.2
PROGRESS_UPDATE_SEC = 60
CHECKPOINT_SAVE_EVERY = 10_000  # ジオコーディング時のキャッシュ保存間隔（ユニーク住所数ベース）
CACHE_DIR = os.path.join(BASE_DIR, "cache")
GLOBAL_CACHE_PATH = os.path.join(CACHE_DIR, "geocode_cache_global.json")
BATCH_SIZE_DEFAULT = 5000

# 地方コード・地方名マッピング（都道府県コード頭2桁で判定）
REGION_MAP = {
    "01": ("1", "北海道"),
    "02": ("2", "東北"), "03": ("2", "東北"), "04": ("2", "東北"), "05": ("2", "東北"), "06": ("2", "東北"), "07": ("2", "東北"),
    "08": ("3", "関東"), "09": ("3", "関東"), "10": ("3", "関東"), "11": ("3", "関東"), "12": ("3", "関東"), "13": ("3", "関東"), "14": ("3", "関東"),
    "15": ("4", "中部"), "16": ("4", "中部"), "17": ("4", "中部"), "18": ("4", "中部"), "19": ("4", "中部"), "20": ("4", "中部"), "21": ("4", "中部"), "22": ("4", "中部"), "23": ("4", "中部"),
    "24": ("5", "近畿"), "25": ("5", "近畿"), "26": ("5", "近畿"), "27": ("5", "近畿"), "28": ("5", "近畿"), "29": ("5", "近畿"), "30": ("5", "近畿"),
    "31": ("6", "中国"), "32": ("6", "中国"), "33": ("6", "中国"), "34": ("6", "中国"), "35": ("6", "中国"),
    "36": ("7", "四国"), "37": ("7", "四国"), "38": ("7", "四国"), "39": ("7", "四国"),
    "40": ("8", "九州・沖縄"), "41": ("8", "九州・沖縄"), "42": ("8", "九州・沖縄"), "43": ("8", "九州・沖縄"),
    "44": ("8", "九州・沖縄"), "45": ("8", "九州・沖縄"), "46": ("8", "九州・沖縄"), "47": ("8", "九州・沖縄"),
}

# マスタ列
MASTER_COLUMNS_ZIP = [
    "郵便番号",
    "地方コード",
    "地方名",
    "都道府県コード",
    "都道府県名(漢字)",
    "団体コード",
    "市区町村名(漢字)",
    "政令指定都市フラグ",
    "町域名(漢字)",
    "小字名、丁目、番地等（漢字）",
    "事業所郵便番号フラグ",
    "大口事業所名（漢字）",
]
MASTER_COLUMNS_ADDR = [
    "地方コード",
    "地方名",
    "都道府県コード",
    "都道府県名(漢字)",
    "団体コード",
    "市区町村名(漢字)",
    "政令指定都市フラグ",
    "町域名(漢字)",
]


# ユーティリティ
def safe_strip(val: Optional[str]) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ""
    return str(val).strip()


def pad_zip(val: Optional[str]) -> str:
    v = safe_strip(val)
    if v.isdigit():
        return v.zfill(7)
    return v


KANJI_DIGITS = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九"]


def _to_kanji_number(num_str: str) -> str:
    try:
        n = int(num_str)
    except Exception:
        return num_str
    # 3桁以上（100以上）は番地・号など長い数字とみなし変換しない
    if n >= 100:
        return num_str
    if n == 0:
        return KANJI_DIGITS[0]
    units = [(1000, "千"), (100, "百"), (10, "十"), (1, "")]
    out = []
    for val, sym in units:
        q, n = divmod(n, val)
        if q == 0:
            continue
        # 安全のため、想定外の桁はそのまま返す
        if q >= len(KANJI_DIGITS):
            return num_str
        if val == 1:
            out.append(KANJI_DIGITS[q])
        else:
            if q > 1:
                out.append(KANJI_DIGITS[q])
            out.append(sym)
    return "".join(out)


def normalize_address(addr: str) -> str:
    # 住所正規化（小書きカナを通常カナに、数字を漢数字に、空白類除去）
    if not addr:
        return ""
    s = str(addr)
    small_kana_map = str.maketrans({
        "ゃ": "や", "ゅ": "ゆ", "ょ": "よ",
        "ャ": "ヤ", "ュ": "ユ", "ョ": "ヨ",
        "ぁ": "あ", "ぃ": "い", "ぅ": "う", "ぇ": "え", "ぉ": "お",
        "ァ": "ア", "ィ": "イ", "ゥ": "ウ", "ェ": "エ", "ォ": "オ",
        "っ": "つ", "ッ": "ツ",
        "ゎ": "わ", "ヮ": "ワ",
        "ヶ": "ケ", "ヵ": "カ", "㌔": "ケ", "㌕": "カ",
    })
    s = s.translate(small_kana_map)
    s = re.sub(r"[0-9０-９]+", lambda m: _to_kanji_number(m.group(0)), s)
    return s.replace("\u3000", "").replace("\n", "").replace("\t", "").replace(" ", "").strip()


def _has_paren_ambiguity(addr_norm: str, pref: Optional[str], df_rows: pd.DataFrame, city_norm_override: Optional[str] = None) -> bool:
    """
    括弧付きの町域候補があり、入力が括弧前までしかない場合は曖昧とみなす。
    """
    for _, row in df_rows.iterrows():
        town = safe_strip(row["町域名(漢字)"])
        if "(" not in town and "（" not in town:
            continue
        town_prefix = re.split(r"[（(]", town)[0]
        if not town_prefix:
            continue
        city_raw = safe_strip(row["市区町村名(漢字)"])
        city_norm = city_norm_override if city_norm_override is not None else normalize_address(city_raw)
        prefix_norm = normalize_address(f"{'' if pref is None else pref}{city_raw}{town_prefix}")
        full_norm = normalize_address(f"{'' if pref is None else pref}{city_raw}{town}")
        # 入力が括弧前まで一致し、括弧以降を含まない場合は曖昧とみなす
        if addr_norm.startswith(prefix_norm) and not addr_norm.startswith(full_norm):
            return True
    return False


def find_prefecture(addr: str, prefs: List[str]) -> Optional[str]:
    # 都道府県名は通常住所先頭に付くため、前方一致で判定
    for p in prefs:
        p_norm = normalize_address(p)
        if p_norm and addr.startswith(p_norm):
            return p
    return None


def _infer_prefecture_from_city(addr_norm: str, city_groups: Dict[str, pd.DataFrame]) -> Optional[Tuple[pd.Series, str]]:
    """
    都道府県名がなく、市区町村で一意に決まる場合に都道府県を補完する。
    戻り値: (row, flag) or None
    """
    for city_norm, df_city in city_groups.items():
        if not city_norm:
            continue
        if addr_norm.startswith(city_norm):
            unique_prefs = df_city["都道府県名(漢字)"].dropna().unique().tolist()
            if len(unique_prefs) == 1:
                return df_city.iloc[0], "city_only"
    return None


def read_master() -> pd.DataFrame:
    # マスタ読み込みと地方コード付与
    df = pd.read_excel(MASTER_PATH, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    def to_region(code: str):
        # 都道府県コードから地方コード・地方名を取得
        if not code or not isinstance(code, str):
            return None, None
        pref2 = code[:2]
        rc, rn = REGION_MAP.get(pref2, (None, None))
        if rc is not None:
            rc = str(rc).zfill(2)
        return rc, rn

    region_codes = []
    region_names = []
    for code in df["都道府県コード"]:
        r_code, r_name = to_region(code)
        region_codes.append(r_code)
        region_names.append(r_name)
    df["地方コード"] = region_codes
    df["地方名"] = region_names
    return df


def match_master_address(addr: str, master_by_pref: Dict[str, pd.DataFrame], city_groups: Optional[Dict[str, pd.DataFrame]] = None) -> Optional[Tuple[Dict[str, Optional[str]], Optional[int], Optional[str]]]:
    # 住所からマスタ突合
    addr_norm = normalize_address(addr)
    if not addr_norm:
        return None

    result: Dict[str, Optional[str]] = {col: None for col in MASTER_COLUMNS_ADDR}
    idx_used: Optional[int] = None
    match_flag: Optional[str] = None

    pref = find_prefecture(addr_norm, list(master_by_pref.keys()))
    if pref:
        df_pref = master_by_pref.get(pref)
        if df_pref is None or df_pref.empty:
            return None

        base_row = df_pref.iloc[0]
        result["地方コード"] = base_row.get("地方コード")
        result["地方名"] = base_row.get("地方名")
        result["都道府県コード"] = base_row["都道府県コード"]
        result["都道府県名(漢字)"] = base_row["都道府県名(漢字)"]

        df_pref_sorted = df_pref.copy()
        df_pref_sorted["len_city_town"] = df_pref_sorted["市区町村名(漢字)"].fillna("").str.len() + df_pref_sorted["町域名(漢字)"].fillna("").str.len()
        df_pref_sorted = df_pref_sorted.sort_values("len_city_town", ascending=False)

        # 最長一致で採用。入力が短い町域候補がある場合は曖昧として町域なし。
        ambiguous_prefix = False
        paren_ambiguous = _has_paren_ambiguity(addr_norm, pref, df_pref_sorted)
        best_row = None
        best_len = 0
        for _, row in df_pref_sorted.iterrows():
            city = safe_strip(row["市区町村名(漢字)"])
            town = safe_strip(row["町域名(漢字)"])
            full_norm = normalize_address(f"{pref}{city}{town}")
            if full_norm and addr_norm.startswith(full_norm):
                if len(addr_norm) < len(full_norm):
                    ambiguous_prefix = True
                    continue
                if len(full_norm) > best_len:
                    best_len = len(full_norm)
                    best_row = row
        if best_row is not None and not paren_ambiguous:
            result.update({
                "地方コード": best_row.get("地方コード", result.get("地方コード")),
                "地方名": best_row.get("地方名", result.get("地方名")),
                "団体コード": best_row["団体コード"],
                "市区町村名(漢字)": best_row["市区町村名(漢字)"],
                "政令指定都市フラグ": best_row["政令指定都市フラグ"],
                "町域名(漢字)": best_row["町域名(漢字)"],
            })
            idx_used = best_row.name
            match_flag = "pref_city_town"
            return result, idx_used, match_flag
        if paren_ambiguous or ambiguous_prefix:
            return result, None, "pref_city"

        # 市区町村一致
        for _, row in df_pref_sorted.iterrows():
            city = safe_strip(row["市区町村名(漢字)"])
            if city and city in addr_norm:
                result.update({
                    "団体コード": row["団体コード"],
                    "市区町村名(漢字)": row["市区町村名(漢字)"],
                    "政令指定都市フラグ": row["政令指定都市フラグ"],
                    "町域名(漢字)": None,
                })
                idx_used = row.name
                match_flag = "pref_city"
                return result, idx_used, match_flag

        # 都道府県のみ
        result["町域名(漢字)"] = None
        return result, None, "pref_only"

    # 都道府県なし: 市区町村グループで判定
    if city_groups is not None:
        # 市区町村から都道府県が一意に決まるケース（この後も町域探索を続ける）
        inferred = _infer_prefecture_from_city(addr_norm, city_groups)
        if inferred:
            row, flag = inferred
            result.update({
                "地方コード": row.get("地方コード"),
                "地方名": row.get("地方名"),
                "都道府県コード": row["都道府県コード"],
                "都道府県名(漢字)": row["都道府県名(漢字)"],
                "団体コード": row["団体コード"],
                "市区町村名(漢字)": row["市区町村名(漢字)"],
                "政令指定都市フラグ": row["政令指定都市フラグ"],
                "町域名(漢字)": None,
            })
            idx_used = row.name
            match_flag = flag

        for city_norm, df_city in city_groups.items():
            # 部分一致だと「中央区」で別の都府県に誤ヒットするため前方一致のみ
            if city_norm and addr_norm.startswith(city_norm):
                ambiguous_prefix = False
                paren_ambiguous = _has_paren_ambiguity(addr_norm, None, df_city, city_norm_override=city_norm)
                best_row = None
                best_len = 0
                for _, row in df_city.iterrows():
                    town = safe_strip(row["町域名(漢字)"])
                    target = f"{city_norm}{normalize_address(town)}"
                    if target and addr_norm.startswith(target):
                        if len(addr_norm) < len(target):
                            ambiguous_prefix = True
                            continue
                        if len(target) > best_len:
                            best_len = len(target)
                            best_row = row
                if best_row is not None and not paren_ambiguous:
                    result.update({
                        "地方コード": best_row.get("地方コード"),
                        "地方名": best_row.get("地方名"),
                        "都道府県コード": best_row["都道府県コード"],
                        "都道府県名(漢字)": best_row["都道府県名(漢字)"],
                        "団体コード": best_row["団体コード"],
                        "市区町村名(漢字)": best_row["市区町村名(漢字)"],
                        "政令指定都市フラグ": best_row["政令指定都市フラグ"],
                        "町域名(漢字)": best_row["町域名(漢字)"],
                    })
                    idx_used = best_row.name
                    match_flag = "no_pref_city_town"
                    return result, idx_used, match_flag
                if paren_ambiguous or ambiguous_prefix:
                    return result, None, "no_pref_city"
                # 町域不明だが市区町村が一意
                row = df_city.iloc[0]
                result.update({
                    "地方コード": row.get("地方コード"),
                    "地方名": row.get("地方名"),
                    "都道府県コード": row["都道府県コード"],
                    "都道府県名(漢字)": row["都道府県名(漢字)"],
                    "団体コード": row["団体コード"],
                    "市区町村名(漢字)": row["市区町村名(漢字)"],
                    "政令指定都市フラグ": row["政令指定都市フラグ"],
                    "町域名(漢字)": None,
                })
                idx_used = row.name
                match_flag = "no_pref_city"
                return result, idx_used, match_flag
        # 市区町村推定だけで町域が決まらなかった場合
        if inferred:
            return result, idx_used, match_flag
    return None


def attach_master_by_zip(df: pd.DataFrame, master: pd.DataFrame, zip_cols: List[str], progress=None, used_zip_codes=None) -> pd.DataFrame:
    # 郵便番号突合
    result = df.copy()
    total = max(len(zip_cols), 1)
    done = 0
    if used_zip_codes is None:
        used_zip_codes = set()

    # 郵便番号ごとに付与内容とフラグを決める
    master_zip = master.copy()
    master_zip["郵便番号"] = master_zip["郵便番号"].apply(pad_zip)
    zip_mapping: Dict[str, Tuple[Dict[str, Optional[str]], Optional[str]]] = {}
    for zip_code, grp in master_zip.groupby("郵便番号"):
        pref_city_pairs = {(row["都道府県名(漢字)"], row["市区町村名(漢字)"]) for _, row in grp.iterrows()}
        if len(pref_city_pairs) == 1:
            base = grp.iloc[0]
            record = {col: base.get(col) for col in MASTER_COLUMNS_ZIP}
            if len(grp) == 1:
                flag = "unique_full"  # 郵便番号＋都道府県＋市区町村＋町域が一意
            else:
                # 同じ郵便番号で町域が複数 → 町域系は付与しない
                record["町域名(漢字)"] = None
                if "小字名、丁目、番地等（漢字）" in record:
                    record["小字名、丁目、番地等（漢字）"] = None
                flag = "multi_town"  # 郵便番号＋都道府県＋市区町村は一意だが町域が複数
        else:
            # 都道府県が単一で市区町村が複数 → 都道府県だけ付与
            pref_set = set(grp["都道府県名(漢字)"].dropna().unique().tolist())
            if len(pref_set) == 1:
                base = grp.iloc[0]
                record = {col: None for col in MASTER_COLUMNS_ZIP}
                record["郵便番号"] = zip_code
                record["地方コード"] = base.get("地方コード")
                record["地方名"] = base.get("地方名")
                record["都道府県コード"] = base.get("都道府県コード")
                record["都道府県名(漢字)"] = base.get("都道府県名(漢字)")
                flag = "ambiguous_pref_city"  # 県だけ確定、都市は複数
            else:
                # 都道府県も複数 → 郵便番号のみ付与
                record = {col: None for col in MASTER_COLUMNS_ZIP}
                record["郵便番号"] = zip_code
                flag = "ambiguous_pref_city"
        zip_mapping[zip_code] = (record, flag)

    for col in zip_cols:
        if col not in result.columns:
            done += 1
            if progress:
                progress(done, total, f"{col} 列なし")
            continue

        zip_flag_col = f"{col}_zip_match_flag"
        if zip_flag_col not in result.columns:
            result[zip_flag_col] = None
        for mcol in MASTER_COLUMNS_ZIP:
            new_col = f"{col}_{mcol}"
            if new_col not in result.columns:
                result[new_col] = None

        tmp_zip = result[col].apply(pad_zip)
        matched_zips_local = []
        for idx, z in tmp_zip.items():
            rec, flag = zip_mapping.get(z, ({c: None for c in MASTER_COLUMNS_ZIP}, None))
            for mcol in MASTER_COLUMNS_ZIP:
                result.at[idx, f"{col}_{mcol}"] = rec.get(mcol)
            result.at[idx, zip_flag_col] = flag
            if flag:
                matched_zips_local.append(z)
        used_zip_codes.update(matched_zips_local)

        done += 1
        if progress:
            progress(done, total, f"{col} 突合完了")
    return result


def attach_master_by_address(df: pd.DataFrame, master: pd.DataFrame, addr_cols: List[str], progress=None, used_master_idx=None) -> pd.DataFrame:
    # 住所突合
    if not addr_cols:
        return df.copy()
    result = df.copy()
    master_addr = master[master["事業所郵便番号フラグ"] == "0"].copy()
    master_by_pref: Dict[str, pd.DataFrame] = {p: g for p, g in master_addr.groupby("都道府県名(漢字)")}
    master_addr["city_norm"] = master_addr["市区町村名(漢字)"].fillna("").apply(normalize_address)
    city_groups: Dict[str, pd.DataFrame] = {k: v for k, v in master_addr.groupby("city_norm")}
    cache: Dict[str, Optional[pd.Series]] = {}
    if used_master_idx is None:
        used_master_idx = set()
    total_cols = max(len(addr_cols), 1)
    done_cols = 0
    for col in addr_cols:
        if col not in result.columns:
            continue
        unique_addrs = pd.Series(result[col].fillna("")).unique().tolist()
        for addr in unique_addrs:
            addr_norm = normalize_address(addr)
            if addr_norm not in cache:
                matched = match_master_address(addr_norm, master_by_pref, city_groups=city_groups)
                if matched is not None:
                    vals_dict, idx_val, flag = matched
                    cache[addr_norm] = (vals_dict, idx_val, flag)
                    if idx_val is not None:
                        used_master_idx.add(idx_val)
                else:
                    cache[addr_norm] = (None, None, None)
        records = []
        for addr in result[col].fillna(""):
            addr_norm = normalize_address(addr)
            records.append(cache.get(addr_norm))
        for mcol in MASTER_COLUMNS_ADDR:
            new_col = f"{col}_{mcol}"
            vals = pd.Series(
                [
                    None
                    if (rec is None or rec[0] is None)
                    else rec[0].get(mcol, None)
                    for rec in records
                ],
                index=result.index,
            )
            if new_col in result.columns:
                result[new_col] = result[new_col].fillna(vals)
            else:
                result[new_col] = vals
        flag_col = f"{col}_match_flag"
        flags = pd.Series([None if rec is None else rec[2] for rec in records], index=result.index)
        if flag_col in result.columns:
            result[flag_col] = result[flag_col].fillna(flags)
        else:
            result[flag_col] = flags
        done_cols += 1
        if progress:
            progress(col, done_cols, total_cols, f"{col} 突合完了")
    return result


def nominatim_search(query: str, user_agent: str) -> Optional[Tuple[float, float]]:
    # Nominatimジオコーディング検索
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 1}
    headers = {"User-Agent": user_agent}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list) and data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None
    return None


def generate_queries(addr: str) -> List[Tuple[str, str]]:
    # ジオコーディング用クエリ生成
    addr = normalize_address(addr)
    queries = []
    if addr:
        queries.append((addr, "full"))
        if "丁目" in addr:
            queries.append((addr.split("丁目")[0] + "丁目", "town"))
        for token in ["町", "村"]:
            if token in addr:
                queries.append((addr.split(token)[0] + token, "town"))
        for token in ["市", "区"]:
            if token in addr:
                queries.append((addr.split(token)[0] + token, "city"))
        if "省" in addr:
            queries.append((addr.split("省")[0] + "省", "pref"))
        elif "都" in addr:
            queries.append((addr.split("都")[0] + "都", "pref"))
        elif "府" in addr:
            queries.append((addr.split("府")[0] + "府", "pref"))
    return queries


def load_cache(cache_path: str) -> Dict[str, Tuple[Optional[float], Optional[float], str]]:
    # ジオコーディングキャッシュ読み込み（Parquetのみ）
    if not cache_path or not os.path.exists(cache_path):
        return {}
    try:
        df = pd.read_parquet(cache_path)
        if df.empty:
            return {}
        return {
            str(row["address"]): (row["lat"], row["lon"], row["flag"])
            for _, row in df.iterrows()
            if "address" in df.columns
        }
    except Exception:
        return {}


def save_cache(cache_path: str, cache: Dict[str, Tuple[Optional[float], Optional[float], str]]):
    # ジオコーディングキャッシュ保存（Parquetのみ）
    if not cache_path:
        return
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df = pd.DataFrame(
            [
                {"address": k, "lat": v[0], "lon": v[1], "flag": v[2]}
                for k, v in cache.items()
            ]
        )
        df.to_parquet(cache_path, index=False)
    except Exception:
        pass


def geocode_addresses(addresses: List[str], user_agent: str, cache: Dict[str, Tuple[Optional[float], Optional[float], str]], progress_cb=None, cache_save_cb=None) -> Tuple[Dict[str, Tuple[Optional[float], Optional[float], str]], int, int]:
    # 住所リストをジオコーディング
    results: Dict[str, Tuple[Optional[float], Optional[float], str]] = {}
    unique_addrs = [a for a in pd.Series(addresses).dropna().unique().tolist() if normalize_address(a)]
    total = len(unique_addrs)
    cache_hit = 0
    new_count = 0
    if progress_cb:
        progress_cb(0, total, "start")
    last_log = time.time()
    for i, addr in enumerate(unique_addrs, start=1):
        norm = normalize_address(addr)
        if norm in cache:
            results[addr] = cache[norm]
            cache_hit += 1
        else:
            found = False
            for query, flag in generate_queries(addr):
                res = nominatim_search(query, user_agent)
                if res:
                    results[addr] = (res[0], res[1], flag)
                    cache[norm] = (res[0], res[1], flag)
                    found = True
                    new_count += 1
                    break
                time.sleep(REQUEST_SLEEP_SEC)
            if not found:
                results[addr] = (None, None, "not_found")
                cache[norm] = (None, None, "not_found")
                new_count += 1
        if cache_save_cb and (i % CHECKPOINT_SAVE_EVERY == 0):
            cache_save_cb(cache)
        if i % CHUNK_SIZE == 0:
            time.sleep(CHUNK_SLEEP_SEC)
        if progress_cb:
            now = time.time()
            if now - last_log >= PROGRESS_UPDATE_SEC:
                progress_cb(i, total, "tick")
                last_log = now
    if progress_cb:
        progress_cb(total, total, "done")
    return results, cache_hit, new_count


def add_geocode_columns(df: pd.DataFrame, addr_cols: List[str], results: Dict[str, Tuple[Optional[float], Optional[float], str]]) -> pd.DataFrame:
    out = df.copy()
    # ジオコーディング結果を各住所列に付与
    for col in addr_cols:
        lat_col = f"{col}_lat"
        lon_col = f"{col}_lon"
        flag_col = f"{col}_geocode_flag"
        if lat_col not in out.columns:
            out[lat_col] = None
        if lon_col not in out.columns:
            out[lon_col] = None
        if flag_col not in out.columns:
            out[flag_col] = None
        for idx, addr in out[col].items():
            res = results.get(addr)
            if res:
                out.at[idx, lat_col] = res[0]
                out.at[idx, lon_col] = res[1]
                out.at[idx, flag_col] = res[2]
    return out


