# -*- coding: utf-8 -*-
"""
Core geocoding logic (UI非依存)
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

# Streamlit がある場合のみデバッグ出力に利用する
try:
    import streamlit as st
except ImportError:  # API経由などStreamlit無しでも動くように
    st = None


# ログ出力は抑止（必要時はこの関数内のreturnを外すなどして一時的に有効化）
DEBUG_LOG = False


def _debug(msg: str):
    if not DEBUG_LOG:
        return
    if st is not None:
        st.write(msg)
    else:
        print(msg)

# 定数定義
BASE_DIR = os.path.dirname(__file__)
MASTER_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "zipcode_localgoverment_mst.xlsx"))
OUTPUT_SUFFIX = "_GSI"
CHUNK_SIZE = 100_000  # Nominatim負荷を考慮
CHUNK_SLEEP_SEC = 7.0  # Nominatim負荷を考慮
REQUEST_SLEEP_SEC = 0.2  # Nominatim負荷を考慮しつつ高速化
PROGRESS_UPDATE_SEC = 60  # 進捗ログ更新間隔（秒）
CHECKPOINT_SAVE_EVERY = 10_000  # ジオコーディング時のキャッシュ保存間隔（ユニーク住所数ベース）
CACHE_DIR = os.path.join(BASE_DIR, "cache")
GLOBAL_CACHE_PATH = os.path.join(CACHE_DIR, "geocode_cache_global.json")
BATCH_SIZE_DEFAULT = 5000

# 地方コード・地方名マッピング（都道府県コード先頭2桁で判定）
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


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    s = str(addr)
    # 小書きカナを正書きに揃える（ヶ/ヵ/ｹ → ケ など）
    small_kana_map = str.maketrans({
        "ゃ": "や", "ゅ": "ゆ", "ょ": "よ",
        "ャ": "ヤ", "ュ": "ユ", "ョ": "ヨ",
        "ァ": "ア", "ィ": "イ", "ゥ": "ウ", "ェ": "エ", "ォ": "オ",
        "ぁ": "あ", "ぃ": "い", "ぅ": "う", "ぇ": "え", "ぉ": "お",
        "っ": "つ", "ッ": "ツ",
        "ゎ": "わ", "ヮ": "ワ",
        "ヶ": "ケ", "ヵ": "カ", "ｹ": "ケ", "ｶ": "カ",
    })
    s = s.translate(small_kana_map)

    # 数字の表記ゆれを補正（連続数字を漢数字に変換: 19 -> 十九）
    def _to_kanji_number(num_str: str) -> str:
        try:
            n = int(num_str)
        except Exception:
            return num_str
        if n == 0:
            return "零"
        units = [(1000, "千"), (100, "百"), (10, "十"), (1, "")]
        out = []
        for val, sym in units:
            q, n = divmod(n, val)
            if q == 0:
                continue
            if val == 1:
                out.append("零" if q == 0 else "一" if q == 1 else "二" if q == 2 else "三" if q == 3 else "四" if q == 4 else "五" if q == 5 else "六" if q == 6 else "七" if q == 7 else "八" if q == 8 else "九")
            else:
                if q > 1:
                    out.append("一" if q == 1 else "二" if q == 2 else "三" if q == 3 else "四" if q == 4 else "五" if q == 5 else "六" if q == 6 else "七" if q == 7 else "八" if q == 8 else "九")
                out.append(sym)
        return "".join(out)

    s = re.sub(r"[0-9０-９]+", lambda m: _to_kanji_number(m.group(0)), s)
    return s.replace("\u3000", "").replace("\n", "").replace("\t", "").replace(" ", "").strip()


def _has_paren_ambiguity(addr_norm: str, pref: Optional[str], df_rows: pd.DataFrame, city_norm_override: Optional[str] = None) -> bool:
    """
    括弧付きの町域候補が存在し、入力が括弧の前までしか含まない場合は曖昧とみなす。
    pref: 都道府県名（None の場合は市区町村のみで判定）
    city_norm_override: 市区町村の正規化済み文字列を直接渡す場合に使用
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
        if addr_norm.startswith(prefix_norm) and len(addr_norm) < len(full_norm):
            return True
    return False



def find_prefecture(addr: str, prefs: List[str]) -> Optional[str]:
    # 都道府県名は通常住所先頭に付くため、前方一致で判定する（部分一致だと「東京都府中市」で京都府を誤検出する）
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
            # city_norm が住所先頭にあり、都道府県が一意なら補完
            unique_prefs = df_city["都道府県名(漢字)"].dropna().unique().tolist()
            if len(unique_prefs) == 1:
                return df_city.iloc[0], "city_only"
    return None


def read_master() -> pd.DataFrame:
    df = pd.read_excel(MASTER_PATH, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    def to_region(code: str):
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
    addr_norm = normalize_address(addr)
    if not addr_norm:
        return None

    # デバッグ用に正規化後の住所をログ出力
    _debug(f"[addr_match_debug] addr_norm={addr_norm}")

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

        # 入力に含まれる町域の最長一致を採用（入力が短い町域候補が存在する場合は曖昧として町域なし）
        ambiguous_prefix = False
        paren_ambiguous = _has_paren_ambiguity(addr_norm, pref, df_pref_sorted)
        best_row = None
        best_len = 0
        for _, row in df_pref_sorted.iterrows():
            city = safe_strip(row["市区町村名(漢字)"])
            town = safe_strip(row["町域名(漢字)"])
            full_norm = normalize_address(f"{pref}{city}{town}")
            # デバッグ: 特定住所のマスタ候補確認
            if any(key in addr_norm for key in ["箕沖町", "長崎", "荒井町新浜"]):
                _debug(f"[addr_match_debug] cand_pref addr_norm={addr_norm} full_norm={full_norm} len(addr)={len(addr_norm)} len(full)={len(full_norm)}")
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
        else:
            _debug(f"[addr_match_debug] town_not_found_pref addr_norm={addr_norm}")
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
        # まず市区町村から都道府県が一意に決まるケース（この後も町域探索を続ける）
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
                best_row = None
                best_len = 0
                for _, row in df_city.iterrows():
                    town = safe_strip(row["町域名(漢字)"])
                    target = f"{city_norm}{normalize_address(town)}"
                    if any(key in addr_norm for key in ["箕沖町", "長崎", "荒井町新浜"]):
                        _debug(f"[addr_match_debug] cand_no_pref addr_norm={addr_norm} target={target} len(addr)={len(addr_norm)} len(target)={len(target)}")
                    if target and addr_norm.startswith(target):
                        if len(addr_norm) < len(target):
                            ambiguous_prefix = True
                            continue
                        if len(target) > best_len:
                            best_len = len(target)
                            best_row = row
                if best_row is not None:
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
                else:
                    # デバッグ: 町域が見つからなかった（都道府県なし系）
                    _debug(f"[addr_match_debug] town_not_found_no_pref addr_norm={addr_norm}")
                if ambiguous_prefix:
                    return result, None, "no_pref_city"
                # 町域は不明だが市区町村が一意
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
            # 都道府県・市区町村の組み合わせも複数 → 郵便番号のみ付与
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
        if "県" in addr:
            queries.append((addr.split("県")[0] + "県", "pref"))
        elif "都" in addr:
            queries.append((addr.split("都")[0] + "都", "pref"))
        elif "府" in addr:
            queries.append((addr.split("府")[0] + "府", "pref"))
    return queries


def load_cache(cache_path: str) -> Dict[str, Tuple[Optional[float], Optional[float], str]]:
    if not cache_path or not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: (v[0], v[1], v[2]) for k, v in data.items()}
    except Exception:
        return {}


def save_cache(cache_path: str, cache: Dict[str, Tuple[Optional[float], Optional[float], str]]):
    if not cache_path:
        return
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        serializable = {k: [v[0], v[1], v[2]] for k, v in cache.items()}
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False)
    except Exception:
        pass


def geocode_addresses(addresses: List[str], user_agent: str, cache: Dict[str, Tuple[Optional[float], Optional[float], str]], progress_cb=None, cache_save_cb=None) -> Tuple[Dict[str, Tuple[Optional[float], Optional[float], str]], int, int]:
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
