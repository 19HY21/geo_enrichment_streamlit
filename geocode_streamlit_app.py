# -*- coding: utf-8 -*-
"""
Geo Enrichment Tool (Streamlit)
- geo_logic/core 繧貞茜逕ｨ縺励√ヶ繝ｩ繧ｦ繧ｶ縺九ｉ驛ｵ萓ｿ逡ｪ蜿ｷ/菴乗園遯∝粋縺ｨ繧ｸ繧ｪ繧ｳ繝ｼ繝・ぅ繝ｳ繧ｰ繧貞ｮ溯｡・- Streamlit Cloud 縺ｧ繧ょ虚縺上ｈ縺・↓譛蟆城剞縺ｮ繝｡繝｢繝ｪ縺ｧ蜍穂ｽ懊☆繧九ｈ縺・ｪｿ謨ｴ
"""

import io
import json
import os
import sys
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import streamlit as st

BASE_DIR = os.path.dirname(__file__)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from geo_logic import core as logic  # noqa: E402

# 繝槭せ繧ｿ繝代せ繧偵Μ繝昴ず繝医Μ蜀・data 縺ｫ蝗ｺ螳・logic.MASTER_PATH = os.path.join(BASE_DIR, "data", "zipcode_localgoverment_mst.xlsx")

# 蛻ｩ逕ｨ髢｢謨ｰ
CACHE_DIR = logic.CACHE_DIR
OUTPUT_SUFFIX = logic.OUTPUT_SUFFIX
BATCH_SIZE_DEFAULT = 5_000
attach_master_by_address = logic.attach_master_by_address
attach_master_by_zip = logic.attach_master_by_zip
geocode_addresses = logic.geocode_addresses
load_cache = logic.load_cache
read_master = logic.read_master
save_cache = logic.save_cache
add_geocode_columns = logic.add_geocode_columns
normalize_address = logic.normalize_address


def _log(log_box, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    logs = st.session_state.setdefault("logs", [])
    logs.append(f"[{ts}] {msg}")
    log_box.write("\n".join(logs))


def _run_pipeline(
    df_input: pd.DataFrame,
    sheet_name: str,
    file_kind: str,
    zip_cols: List[str],
    addr_cols: List[str],
    xls_for_copy: pd.ExcelFile,
    log_box,
    addr_dl_box,
    geo_dl_box,
    base_name: str,
    batch_size: int,
    geocode_enabled: bool = True,
    uploaded_cache: dict | None = None,
    process_mask: pd.Series | None = None,
    chunk_offset: int = 0,
):
    st.session_state["addr_chunk_downloads"] = []
    st.session_state["geo_chunk_downloads"] = []
    st.session_state["logs"] = []
    progress = st.progress(0)
    status = st.empty()

    # 繝輔ぉ繝ｼ繧ｺ縺ｮ驥阪∩
    weights = {"zip": 20, "addr": 20, "geo": 55, "out": 5}
    enabled_phases = ["zip"]
    if addr_cols:
        enabled_phases.append("addr")
    if addr_cols and geocode_enabled:
        enabled_phases.append("geo")
    enabled_phases.append("out")
    total_weight = sum(weights[p] for p in enabled_phases)

    def set_progress(phase: str, pct: float, text: str):
        offset = 0
        for p in ["zip", "addr", "geo", "out"]:
            if p == phase:
                break
            if p in enabled_phases:
                offset += weights[p]
        if phase not in enabled_phases:
            return
        w = weights[phase]
        overall = (offset + w * (pct / 100.0)) / total_weight * 100.0
        progress.progress(min(max(int(overall), 0), 100))
        status.write(text)

    def prog_bar(phase, pct, text):
        set_progress(phase, pct, text)

    _log(log_box, "繝槭せ繧ｿ隱ｭ霎ｼ髢句ｧ・)
    master_df = read_master()
    _log(log_box, "繝槭せ繧ｿ隱ｭ霎ｼ螳御ｺ・)
    total_rows_all = len(df_input)
    df_proc_in = df_input.copy() if process_mask is None else df_input.loc[process_mask].copy()
    _log(log_box, f"蜈･蜉帑ｻｶ謨ｰ: {total_rows_all} / 蟇ｾ雎｡莉ｶ謨ｰ: {len(df_proc_in)}") # 蜈ｨ菴謎ｻｶ謨ｰ縺ｨ蜃ｦ逅・ｯｾ雎｡莉ｶ謨ｰ

    # 蠢・ｦ∝・縺ｮ縺ｿ謚ｽ蜃ｺ
    cols_needed = list(dict.fromkeys(zip_cols + addr_cols))
    df_work = df_proc_in[cols_needed].copy() if cols_needed else df_proc_in.copy()
    used_zip_codes = set()
    used_master_idx = set()

    # 蜈ｨ陦後′Parquet逕ｱ譚･縺ｪ縺ｩ縺ｧ蜃ｦ逅・ｯｾ雎｡縺檎┌縺・ｴ蜷医・縺昴・縺ｾ縺ｾ蜃ｺ蜉帙ｒ霑斐☆
    if df_work.empty:
        _log(log_box, "蜃ｦ逅・ｯｾ雎｡縺ｮ陦後′縺ゅｊ縺ｾ縺帙ｓ・亥・莉ｶParquet逕ｱ譚･縺ｪ縺ｩ・峨・)
        out_base = base_name or "output"
        df_out_merge = df_input.copy()
        for helper_col in ["__merge_key", "__is_parquet"]:
            if helper_col in df_out_merge.columns:
                df_out_merge = df_out_merge.drop(columns=[helper_col])
        df_input_clean = df_input.drop(columns=["__merge_key", "__is_parquet"], errors="ignore")
        if file_kind == "excel" and xls_for_copy is not None:
            buf = _build_excel_output(
                xls_for_copy, sheet_name, df_input_clean, df_out_merge, read_master(), set(), set()
            )
            fname = f"{out_base}{OUTPUT_SUFFIX}.xlsx"
        else:
            buf = _build_csv_output(df_out_merge)
            fname = f"{out_base}{OUTPUT_SUFFIX}.csv"
        return buf, fname, df_out_merge, os.path.join(CACHE_DIR, "streamlit_local_cache.json")

    # 驛ｵ萓ｿ逡ｪ蜿ｷ遯∝粋
    if zip_cols:
        _log(log_box, "驛ｵ萓ｿ逡ｪ蜿ｷ遯∝粋髢句ｧ・)

        def zip_prog(done, total, detail):
            pct = done / max(total, 1) * 100
            prog_bar("zip", pct, f"[驛ｵ萓ｿ逡ｪ蜿ｷ] {detail}")

        df_work = attach_master_by_zip(
            df_work, master_df, zip_cols, progress=zip_prog, used_zip_codes=used_zip_codes
        )
        prog_bar("zip", 100, "[驛ｵ萓ｿ逡ｪ蜿ｷ] 螳御ｺ・)
        _log(log_box, f"驛ｵ萓ｿ逡ｪ蜿ｷ遯∝粋螳御ｺ・菴ｿ逕ｨ驛ｵ萓ｿ逡ｪ蜿ｷ: {len(used_zip_codes)}莉ｶ")

    # 菴乗園遯∝粋・医メ繝｣繝ｳ繧ｯ・九が繝ｳ繝・ぅ繧ｹ繧ｯ菫晏ｭ假ｼ・    if addr_cols:
        _log(log_box, "菴乗園遯∝粋髢句ｧ・)
        prog_bar("addr", 0, "[addr] 蜃ｦ逅・幕蟋・)
        chunk_size = 1000
        addr_chunks = []
        total_rows = len(df_work)
        processed = 0
        chunk_dir = os.path.join(CACHE_DIR, "addr_chunks")
        os.makedirs(chunk_dir, exist_ok=True)

        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            chunk = df_work.iloc[start:end].copy()
            chunk = attach_master_by_address(
                chunk, master_df, addr_cols, progress=None, used_master_idx=used_master_idx
            )
            addr_chunks.append(chunk)
            chunk_fname = f"{base_name or 'output'}_addr_chunk_{start+1+chunk_offset}_{end+chunk_offset}.parquet"
            chunk_path = os.path.join(chunk_dir, chunk_fname)
            chunk.to_parquet(chunk_path, index=False)
            try:
                buf = io.BytesIO()
                chunk.to_parquet(buf, index=False)
                buf.seek(0)
                st.session_state["addr_chunk_downloads"].append(
                    {
                        "label": f"菴乗園繝√Ε繝ｳ繧ｯ {start+1+chunk_offset}-{end+chunk_offset} 繧偵ム繧ｦ繝ｳ繝ｭ繝ｼ繝・(Parquet)",
                        "data": buf.getvalue(),
                        "name": chunk_fname,
                    }
                )
                addr_dl_box.download_button(
                    label=f"菴乗園繝√Ε繝ｳ繧ｯ {start+1+chunk_offset}-{end+chunk_offset} 繧偵ム繧ｦ繝ｳ繝ｭ繝ｼ繝・(Parquet)",
                    data=buf.getvalue(),
                    file_name=chunk_fname,
                    mime="application/octet-stream",
                    key=f"addr_chunk_live_{start}_{end}_{chunk_offset}",
                )
            except Exception:
                pass
            processed = end
            pct = processed / max(total_rows, 1) * 100
            prog_bar("addr", pct, f"[addr] {processed}/{total_rows} ({pct:.1f}%)")
            _log(log_box, f"[addr] {processed}/{total_rows} ({pct:.1f}%) chunk {start+1}-{end} 菫晏ｭ・ {chunk_path}")

        df_work = pd.concat(addr_chunks).sort_index()
        prog_bar("addr", 100, "[addr] 螳御ｺ・)
        _log(log_box, f"菴乗園遯∝粋螳御ｺ・菴ｿ逕ｨ陦・ {len(used_master_idx)}莉ｶ")

    # 繧ｸ繧ｪ繧ｳ繝ｼ繝・ぅ繝ｳ繧ｰ
    os.makedirs(CACHE_DIR, exist_ok=True)
    local_cache_path = os.path.join(CACHE_DIR, "streamlit_local_cache.parquet")
    cache = load_cache(local_cache_path)
    if uploaded_cache:
        cache.update(uploaded_cache)
    _log(log_box, f"繧ｭ繝｣繝・す繝･隱ｭ霎ｼ: 繝ｭ繝ｼ繧ｫ繝ｫ{len(cache)}莉ｶ")

    geo_results = {}
    if addr_cols and geocode_enabled:
        _log(log_box, "繧ｸ繧ｪ繧ｳ繝ｼ繝・ぅ繝ｳ繧ｰ髢句ｧ具ｼ医Θ繝九・繧ｯ菴乗園繝吶・繧ｹ・・)
        prog_bar("geo", 0, "[geo] 蜃ｦ逅・幕蟋・)
        all_addrs = []
        for col in addr_cols:
            all_addrs.extend(df_work[col].dropna().tolist())
        unique_addrs = [a for a in pd.Series(all_addrs).dropna().unique().tolist() if normalize_address(a)]
        total_unique = len(unique_addrs)
        geo_chunk_size = 1000
        _log(log_box, f"繝ｦ繝九・繧ｯ菴乗園謨ｰ: {total_unique}莉ｶ / 繧ｸ繧ｪ繧ｳ繝ｼ繝峨メ繝｣繝ｳ繧ｯ繧ｵ繧､繧ｺ: {geo_chunk_size}")

        overall_done = 0
        for start in range(0, total_unique, geo_chunk_size):
            end = min(start + geo_chunk_size, total_unique)
            chunk = unique_addrs[start:end]

            def geo_prog(done, total, kind):
                now_done = overall_done + done
                pct = now_done / max(total_unique, 1) * 100
                prog_bar("geo", pct, f"[geo] {now_done}/{total_unique} ({pct:.1f}%)")

            def geo_cache_save(c):
                save_cache(local_cache_path, c)

            _log(log_box, f"繝舌ャ繝∝・逅・ {start+1}縲悳end}莉ｶ逶ｮ")
            chunk_results, cache_hit, new_count = geocode_addresses(
                chunk,
                user_agent="GeoGUI_streamlit",
                cache=cache,
                progress_cb=geo_prog,
                cache_save_cb=geo_cache_save,
            )
            geo_results.update(chunk_results)
            save_cache(local_cache_path, cache)
            # 繧ｸ繧ｪ繧ｳ繝ｼ繝臥ｵ先棡繝√Ε繝ｳ繧ｯ繧単arquet縺ｧ繝繧ｦ繝ｳ繝ｭ繝ｼ繝牙庄閭ｽ縺ｫ縺吶ｋ
            try:
                geo_df = pd.DataFrame(
                    [
                        {"address": k, "lat": v[0], "lon": v[1], "flag": v[2]}
                        for k, v in chunk_results.items()
                    ]
                )
                geo_chunk_fname = f"{base_name or 'output'}_geo_chunk_{start+1+chunk_offset}_{end+chunk_offset}.parquet"
                geo_bytes = io.BytesIO()
                geo_df.to_parquet(geo_bytes, index=False)
                geo_bytes.seek(0)
                st.session_state["geo_chunk_downloads"].append(
                    {
                        "label": f"繧ｸ繧ｪ繧ｳ繝ｼ繝峨メ繝｣繝ｳ繧ｯ {start+1+chunk_offset}-{end+chunk_offset} 繧偵ム繧ｦ繝ｳ繝ｭ繝ｼ繝・(Parquet)",
                        "data": geo_bytes.getvalue(),
                        "name": geo_chunk_fname,
                    }
                )
                geo_dl_box.download_button(
                    label=f"繧ｸ繧ｪ繧ｳ繝ｼ繝峨メ繝｣繝ｳ繧ｯ {start+1+chunk_offset}-{end+chunk_offset} 繧偵ム繧ｦ繝ｳ繝ｭ繝ｼ繝・(Parquet)",
                    data=geo_bytes.getvalue(),
                    file_name=geo_chunk_fname,
                    mime="application/octet-stream",
                    key=f"geo_chunk_live_{start}_{end}_{chunk_offset}",
                )
            except Exception:
                pass
            overall_done = end
            _log(log_box, f"繝舌ャ繝∝ｮ御ｺ・cache_hit={cache_hit} 譁ｰ隕・{new_count} 邏ｯ險・{overall_done}/{total_unique}")

    df_work = add_geocode_columns(df_work, addr_cols, geo_results)
    if not (addr_cols and geocode_enabled):
        _log(log_box, "繧ｸ繧ｪ繧ｳ繝ｼ繝・ぅ繝ｳ繧ｰ縺ｯ繧ｹ繧ｭ繝・・・井ｽ乗園譛ｪ驕ｸ謚槭∪縺溘・邱ｯ蠎ｦ邨悟ｺｦ莉倅ｸ弱が繝包ｼ・)

    if geocode_enabled and addr_cols:
        save_cache(local_cache_path, cache)

    # 蜃ｺ蜉帷函謌撰ｼ亥・繝・・繧ｿ縺ｫ遯∝粋邨先棡繧偵・繝ｼ繧ｸ・・    out_base = base_name or "output"
    df_out_merge = df_input.copy()
    for col in df_work.columns:
        df_out_merge.loc[df_work.index, col] = df_work[col]
    for helper_col in ["__merge_key", "__is_parquet"]:
        if helper_col in df_out_merge.columns:
            df_out_merge = df_out_merge.drop(columns=[helper_col])

    df_input_clean = df_input.drop(columns=["__merge_key", "__is_parquet"], errors="ignore")

    if file_kind == "excel":
        buf = _build_excel_output(
            xls_for_copy, sheet_name, df_input_clean, df_out_merge, master_df, used_zip_codes, used_master_idx
        )
        fname = f"{out_base}{OUTPUT_SUFFIX}.xlsx"
    else:
        buf = _build_csv_output(df_out_merge)
        fname = f"{out_base}{OUTPUT_SUFFIX}.csv"

    prog_bar("out", 50, "[out] 逕滓・荳ｭ")
    _log(log_box, f"蜃ｺ蜉帷函謌仙ｮ御ｺ・ {fname}")
    prog_bar("out", 100, "螳御ｺ・)
    return buf, fname, df_out_merge, local_cache_path


def _build_excel_output(
    xls: pd.ExcelFile,
    sheet_name: str,
    original_df: pd.DataFrame,
    processed_df: pd.DataFrame,
    master_df: pd.DataFrame,
    used_zip_codes: set,
    used_master_idx: set,
):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name in xls.sheet_names:
            df_sheet = xls.parse(name, dtype=str)
            df_sheet.to_excel(writer, sheet_name=name, index=False)
        original_df.to_excel(writer, sheet_name=sheet_name, index=False)
        processed_df.to_excel(writer, sheet_name=f"{sheet_name}{OUTPUT_SUFFIX}", index=False)
        master_df.to_excel(writer, sheet_name="master", index=False)
        try:
            from openpyxl.styles import PatternFill

            ws = writer.book["master"]
            highlight_idx = set(used_master_idx) if used_master_idx else set()
            if used_zip_codes:
                matched = master_df[master_df["驛ｵ萓ｿ逡ｪ蜿ｷ"].isin(used_zip_codes)]
                highlight_idx.update(matched.index.tolist())
            if highlight_idx:
                fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                for idx in highlight_idx:
                    row_num = idx + 2
                    for col_num in range(1, len(master_df.columns) + 1):
                        ws.cell(row=row_num, column=col_num).fill = fill
        except Exception:
            pass
    buf.seek(0)
    return buf


def _build_csv_output(processed_df: pd.DataFrame):
    buf = io.BytesIO()
    processed_df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)
    return buf


def _load_input(uploaded_file) -> Tuple[str, pd.DataFrame, pd.ExcelFile, str, str]:
    file_bytes = uploaded_file.read()
    file_kind = "excel" if uploaded_file.name.lower().endswith(("xlsx", "xls")) else "csv"
    if file_kind == "excel":
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        sheet = xls.sheet_names[0]
        df = xls.parse(sheet, dtype=str)
        sheet_name = sheet
    else:
        xls = None
        sheet_name = "data"
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)
    base_name = os.path.splitext(uploaded_file.name)[0]
    return file_kind, df, xls, sheet_name, base_name


def main():
    st.set_page_config(page_title="Geo Enrichment Tool", layout="wide")
    st.title("Geo Enrichment Tool")
    st.caption(
        "菴乗園繝ｻ驛ｵ萓ｿ逡ｪ蜿ｷ縺九ｉ縲∵律譛ｬ驛ｵ謾ｿ繝槭せ繧ｿ繧堤畑縺・※驛ｽ驕灘ｺ懃恁繝ｻ蟶ょ玄逕ｺ譚代・謾ｿ莉､謖・ｮ夐・蟶ゅ↑縺ｩ縺ｮ蝨ｰ蝓滓ュ蝣ｱ縺ｨ邱ｯ蠎ｦ邨悟ｺｦ繧剃ｻ倅ｸ弱＠縲√ョ繝ｼ繧ｿ繧堤函謌舌☆繧九い繝励Μ縺ｧ縺吶・
    )

    uploaded = st.file_uploader("蜈･蜉帙ヵ繧｡繧､繝ｫ繧帝∈謚・(Excel/CSV)", type=["csv", "xlsx", "xls"])
    parquet_uploader = st.file_uploader(
        "遯∝粋貂医∩Parquet繧偵い繝・・繝ｭ繝ｼ繝会ｼ井ｻｻ諢上・隍・焚蜿ｯ・・,
        type=["parquet"],
        key="parquet_uploader",
        accept_multiple_files=True,
    )
    result_placeholder = st.empty()
    download_cache_placeholder = st.empty()

    addr_dl_box = st.container()
    geo_dl_box = st.container()

    st.session_state.setdefault("addr_chunk_downloads", [])
    st.session_state.setdefault("geo_chunk_downloads", [])
    st.session_state.setdefault("result_file", None)
    st.session_state.setdefault("cache_file", None)

    if uploaded or parquet_uploader:
        df_parquet = None
        parquet_base_name = None
        parquet_files = parquet_uploader if parquet_uploader else []
        if parquet_files:
            pq_frames = []
            pq_names = []
            for f in parquet_files:
                pq_names.append(f.name)
                pq_frames.append(pd.read_parquet(io.BytesIO(f.read())))
            df_parquet = pd.concat(pq_frames, ignore_index=True) if pq_frames else None
            parquet_base_name = os.path.splitext(parquet_files[0].name)[0]
        parquet_keys_set = set()
        process_mask = None
        chunk_offset = 0
        if uploaded:
            file_kind, df_input, xls_for_copy, sheet_name, base_name = _load_input(uploaded)
            # 繧ｷ繝ｼ繝亥錐驕ｸ謚橸ｼ・xcel縺ｮ縺ｿ・・            if file_kind == "excel" and xls_for_copy is not None:
                sheet_name = st.selectbox("繧ｷ繝ｼ繝亥錐繧帝∈謚・, options=xls_for_copy.sheet_names, index=0)
                df_input = xls_for_copy.parse(sheet_name, dtype=str)
        elif df_parquet is not None:
            df_input = df_parquet
            file_kind = "parquet"
            xls_for_copy = None
            sheet_name = "data"
            base_name = parquet_base_name or "parquet_input"

        zip_cols = st.multiselect("驛ｵ萓ｿ逡ｪ蜿ｷ蛻励ｒ驕ｸ謚・, options=df_input.columns.tolist())
        addr_cols = st.multiselect("菴乗園蛻励ｒ驕ｸ謚・, options=df_input.columns.tolist())
        geocode_enabled = st.checkbox("邱ｯ蠎ｦ邨悟ｺｦ繧剃ｻ倅ｸ弱☆繧・, value=False)
        cache_uploader = st.file_uploader("繧ｭ繝｣繝・す繝･Parquet繧偵い繝・・繝ｭ繝ｼ繝会ｼ井ｻｻ諢擾ｼ・, type=["parquet"])

        uploaded_cache = None
        if cache_uploader:
            try:
                cache_df = pd.read_parquet(io.BytesIO(cache_uploader.read()))
                # 諠ｳ螳壹き繝ｩ繝: address, lat, lon, flag
                required_cols = {"address", "lat", "lon", "flag"}
                if not required_cols.issubset(set(cache_df.columns)):
                    raise ValueError("蠢・ｦ√↑繧ｫ繝ｩ繝(address, lat, lon, flag)縺瑚ｦ九▽縺九ｊ縺ｾ縺帙ｓ")
                uploaded_cache = {
                    str(row["address"]): (row["lat"], row["lon"], row["flag"])
                    for _, row in cache_df.iterrows()
                    if pd.notna(row.get("address"))
                }
            except Exception as e:
                st.warning(f"繧ｭ繝｣繝・す繝･Parquet縺ｮ隱ｭ霎ｼ縺ｫ螟ｱ謨励＠縺ｾ縺励◆: {e}")

        # Parquet縺後≠繧句ｴ蜷医・菴乗園蛻励く繝ｼ縺ｧParquet繧貞━蜈医・繝ｼ繧ｸ
        if df_parquet is not None and addr_cols:
            missing_base = [c for c in addr_cols if c not in df_input.columns]
            missing_parquet = [c for c in addr_cols if c not in df_parquet.columns]
            if missing_base or missing_parquet:
                st.warning(
                    f"Parquet蜆ｪ蜈医・繝ｼ繧ｸ繧偵せ繧ｭ繝・・縺励∪縺励◆縲よｬ謳榊・: "
                    f"繝吶・繧ｹ={missing_base or '縺ｪ縺・}, Parquet={missing_parquet or '縺ｪ縺・}"
                )
            else:
                def _build_key(df):
                    def _to_key(val):
                        if pd.isna(val):
                            return ""
                        if isinstance(val, float) and val.is_integer():
                            val = int(val)
                        return normalize_address(str(val))

                    parts = [df[col].apply(_to_key) for col in addr_cols]
                    key = parts[0]
                    for p in parts[1:]:
                        key = key + "||" + p
                    return key

                base_df = df_input.copy()
                pq_df = df_parquet.copy()
                base_df["_merge_key"] = _build_key(base_df)
                pq_df["_merge_key"] = _build_key(pq_df)

                # 遨ｺ繧ｭ繝ｼ髯､螟厄ｼ・arquet蛛ｴ縺ｯ驥崎､・く繝ｼ縺ｯ蜈磯ｭ縺縺代ｒ谿九☆
                pq_df = pq_df[pq_df["_merge_key"] != ""]
                pq_df = pq_df.drop_duplicates("_merge_key", keep="first")

                # Parquet蛛ｴ縺ｫ縺ゅｋ蛻励ｒ繝吶・繧ｹ縺ｫ繧りｿｽ蜉縺励※縺翫￥・育ｪ∝粋貂医∩蛻励ｒ關ｽ縺ｨ縺輔↑縺・◆繧・ｼ・                for col in pq_df.columns:
                    if col not in base_df.columns:
                        base_df[col] = None

                base_df = base_df.set_index("_merge_key")
                pq_df = pq_df.set_index("_merge_key")

                base_keys = set(base_df.index[base_df.index != ""])
                pq_keys = set(pq_df.index[pq_df.index != ""])
                common_keys = base_keys & pq_keys
                base_only = base_keys - pq_keys
                pq_only = pq_keys - base_keys

                # Parquet縺ｮ繧ｭ繝ｼ縺御ｸ閾ｴ縺吶ｋ繝吶・繧ｹ陦後ｒ荳頑嶌縺搾ｼ郁｡後・蠅励ｄ縺輔↑縺・ｼ・                aligned = pq_df.reindex(base_df.index)
                base_df.update(aligned)

                base_df["__is_parquet"] = base_df.index.isin(common_keys)
                df_input = base_df.reset_index().rename(columns={"index": "__merge_key"})

                st.info(
                    f"繧ｭ繝ｼ莉ｶ謨ｰ 繝吶・繧ｹ={len(base_keys)} / Parquet={len(pq_keys)} / 蜈ｱ騾・{len(common_keys)} / "
                    f"繝吶・繧ｹ縺ｮ縺ｿ={len(base_only)} / Parquet縺ｮ縺ｿ={len(pq_only)}"
                )

                # Parquet縺ｧ荳頑嶌縺阪＠縺溯｡後・蜃ｦ逅・せ繧ｭ繝・・縲∵悴繝槭ャ繝√・縺ｿ遯∝粋
                process_mask = ~df_input["__is_parquet"]
                chunk_offset = int(df_input["__is_parquet"].sum())

        run_clicked = st.button("螳溯｡・/ 蜀榊ｮ溯｡・, type="primary")

        if run_clicked:
            log_box = st.empty()
            buf, fname, df_out, cache_path = _run_pipeline(
                df_input=df_input,
                sheet_name=sheet_name,
                file_kind=file_kind,
                zip_cols=zip_cols,
                addr_cols=addr_cols,
                xls_for_copy=xls_for_copy,
                log_box=log_box,
                addr_dl_box=addr_dl_box,
                geo_dl_box=geo_dl_box,
                base_name=base_name,
                batch_size=BATCH_SIZE_DEFAULT,
                geocode_enabled=geocode_enabled,
                uploaded_cache=uploaded_cache,
                process_mask=process_mask,
                chunk_offset=chunk_offset,
            )

            st.session_state["result_file"] = {
                "data": buf.getvalue(),
                "name": fname,
            }
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, "rb") as f:
                        st.session_state["cache_file"] = {
                            "data": f.read(),
                            "name": os.path.basename(cache_path),
                        }
                except Exception:
                    pass

        # 繝励Ο繧ｰ繝ｬ繧ｹ繝舌・縺ｮ荳九↓縺ｾ縺ｨ繧√※驟咲ｽｮ
        download_section = st.container()
        with download_section:
            if st.session_state.get("addr_chunk_downloads"):
                st.subheader("菴乗園遯∝粋繝√Ε繝ｳ繧ｯ縺ｮ繝繧ｦ繝ｳ繝ｭ繝ｼ繝・)
                for i, item in enumerate(st.session_state["addr_chunk_downloads"]):
                    st.download_button(
                        label=item["label"],
                        data=item["data"],
                        file_name=item["name"],
                        mime="application/octet-stream",
                        key=f"addr_chunk_{i}",
                    )

            if st.session_state.get("geo_chunk_downloads"):
                st.subheader("繧ｸ繧ｪ繧ｳ繝ｼ繝・ぅ繝ｳ繧ｰ繝√Ε繝ｳ繧ｯ縺ｮ繝繧ｦ繝ｳ繝ｭ繝ｼ繝・)
                for i, item in enumerate(st.session_state["geo_chunk_downloads"]):
                    st.download_button(
                        label=item["label"],
                        data=item["data"],
                        file_name=item["name"],
                        mime="application/octet-stream",
                        key=f"geo_chunk_{i}",
                    )

            if st.session_state.get("result_file"):
                result_placeholder.download_button(
                    label="邨先棡繝・・繧ｿ繧偵ム繧ｦ繝ｳ繝ｭ繝ｼ繝・,
                    data=st.session_state["result_file"]["data"],
                    file_name=st.session_state["result_file"]["name"],
                    mime="application/octet-stream",
                    key="result_download",
                )

            if st.session_state.get("cache_file"):
                download_cache_placeholder.download_button(
                    label="繧ｭ繝｣繝・す繝･Parquet繧偵ム繧ｦ繝ｳ繝ｭ繝ｼ繝会ｼ域ｬ｡蝗槫・蛻ｩ逕ｨ逕ｨ・・,
                    data=st.session_state["cache_file"]["data"],
                    file_name=st.session_state["cache_file"]["name"],
                    mime="application/octet-stream",
                    key="cache_download",
                )

if __name__ == "__main__":
    main()



