# -*- coding: utf-8 -*-
"""
Geo Enrichment Tool (Streamlit版)
- geocode_gui_ver2 のロジックを利用し、ブラウザから郵便番号/住所突合とジオコーディングを実行
- GitHub 配置想定:
    - geo_enrichment_streamlit/data/zipcode_localgoverment_mst.xlsx
    - geo_enrichment_streamlit/geo_logic/geocode_gui_ver2.py
    - エントリーポイント: geo_enrichment_streamlit/geocode_streamlit_app.py
"""

import io
import os
import sys
import tempfile
import zipfile
from typing import List, Tuple

import pandas as pd
import streamlit as st

# パス設定（geo_logic 配下の geocode_gui_ver2 を読み込む）
BASE_DIR = os.path.dirname(__file__)
LOGIC_DIR = os.path.join(BASE_DIR, "geo_logic")
if LOGIC_DIR not in sys.path:
    sys.path.insert(0, LOGIC_DIR)

import geocode_gui_ver2 as logic  # noqa: E402

# マスタパスをGitHub内の data に差し替える
logic.MASTER_PATH = os.path.join(BASE_DIR, "data", "zipcode_localgoverment_mst.xlsx")

# 利用する関数・定数を束ねる
CACHE_DIR = logic.CACHE_DIR
GLOBAL_CACHE_PATH = logic.GLOBAL_CACHE_PATH
OUTPUT_SUFFIX = logic.OUTPUT_SUFFIX
attach_master_by_address = logic.attach_master_by_address
attach_master_by_zip = logic.attach_master_by_zip
create_map_html = logic.create_map_html
geocode_addresses = logic.geocode_addresses
load_cache = logic.load_cache
read_master = logic.read_master
save_cache = logic.save_cache
add_geocode_columns = logic.add_geocode_columns


def _log(log_box, msg: str):
    """ログをテキストで追記表示する。"""
    logs = st.session_state.setdefault("logs", [])
    logs.append(msg)
    log_box.write("\n".join(logs))


def _run_pipeline(
    df_input: pd.DataFrame,
    sheet_name: str,
    file_kind: str,
    zip_cols: List[str],
    addr_cols: List[str],
    xls_for_copy: pd.ExcelFile,
    log_box,
    base_name: str,
):
    """突合〜ジオコーディング〜出力データ生成を実行。"""
    st.session_state["logs"] = []
    progress = st.progress(0)
    status = st.empty()

    def prog_bar(pct, text):
        progress.progress(min(max(int(pct), 0), 100))
        status.write(text)

    _log(log_box, "マスタ読込開始")
    master_df = read_master()
    _log(log_box, f"マスタ読込完了 shape={master_df.shape}")

    df_work = df_input.copy()
    used_zip_codes = set()
    used_master_idx = set()

    # 郵便番号突合
    if zip_cols:
        _log(log_box, f"郵便番号突合開始: {zip_cols}")

        def zip_prog(done, total, detail):
            pct = done / max(total, 1) * 100
            prog_bar(pct, f"[zip] {detail}")

        df_work = attach_master_by_zip(
            df_work, master_df, zip_cols, progress=zip_prog, used_zip_codes=used_zip_codes
        )
        prog_bar(100, "[zip] 完了")
        _log(log_box, f"郵便番号突合完了 使用郵便番号: {len(used_zip_codes)}件")

    # 住所突合
    if addr_cols:
        _log(log_box, f"住所突合開始: {addr_cols}")

        def addr_prog(col, done, total, detail):
            pct = done / max(total, 1) * 100
            prog_bar(pct, f"[addr] {detail}")

        df_work = attach_master_by_address(
            df_work, master_df, addr_cols, progress=addr_prog, used_master_idx=used_master_idx
        )
        prog_bar(100, "[addr] 完了")
        _log(log_box, f"住所突合完了 使用行: {len(used_master_idx)}件")

    # ジオコーディング（キャッシュは毎回グローバルも参照）
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_global = load_cache(GLOBAL_CACHE_PATH)
    local_cache_path = os.path.join(CACHE_DIR, "streamlit_local_cache.json")
    cache_local = load_cache(local_cache_path)
    cache = {**cache_global, **cache_local}
    map_bytes = None
    map_name = None
    _log(log_box, f"キャッシュ読込: グローバル{len(cache_global)}件 / ローカル{len(cache_local)}件")

    if addr_cols:
        _log(log_box, "ジオコーディング開始（ユニーク住所ベース）")
        all_addrs = []
        for col in addr_cols:
            all_addrs.extend(df_work[col].dropna().tolist())

        def geo_prog(done, total, kind):
            pct = done / max(total, 1) * 100
            prog_bar(pct, f"[geo] {done}/{total} ({pct:.1f}%)")

        def geo_cache_save(c):
            save_cache(local_cache_path, c)

        geo_results, cache_hit, new_count = geocode_addresses(
            all_addrs,
            user_agent="GeoGUI_streamlit",
            cache=cache,
            progress_cb=geo_prog,
            cache_save_cb=geo_cache_save,
        )
        _log(log_box, f"ジオコーディング完了 cache_hit={cache_hit} 新規={new_count}")
        merged_cache = {**cache, **geo_results}
        save_cache(GLOBAL_CACHE_PATH, merged_cache)
        save_cache(local_cache_path, merged_cache)
        df_work = add_geocode_columns(df_work, addr_cols, geo_results)
    else:
        _log(log_box, "住所列が未選択のためジオコーディングはスキップ")

    # 出力生成
    out_base = base_name or "output"
    if file_kind == "excel":
        buf = _build_excel_output(
            xls_for_copy, sheet_name, df_input, df_work, master_df, used_zip_codes, used_master_idx
        )
        fname = f"{out_base}{OUTPUT_SUFFIX}.xlsx"
    else:
        buf = _build_csv_output(df_work)
        fname = f"{out_base}{OUTPUT_SUFFIX}.csv"

    _log(log_box, f"出力生成完了: {fname}")
    prog_bar(100, "完了")

    # 地図HTML
    if addr_cols:
        with tempfile.TemporaryDirectory() as td:
            html_path = os.path.join(td, f"{out_base}{OUTPUT_SUFFIX}_map.html")
            map_path = create_map_html(df_work, addr_cols, html_path)
            if map_path and os.path.exists(map_path):
                with open(map_path, "rb") as f:
                    map_bytes = f.read()
                    map_name = os.path.basename(map_path)

    # ローカルキャッシュは完了時に削除
    try:
        if os.path.exists(local_cache_path):
            os.remove(local_cache_path)
    except Exception:
        pass

    return buf, fname, map_bytes, map_name, df_work


def _build_excel_output(
    xls: pd.ExcelFile,
    sheet_name: str,
    original_df: pd.DataFrame,
    processed_df: pd.DataFrame,
    master_df: pd.DataFrame,
    used_zip_codes: set,
    used_master_idx: set,
):
    """Excel出力をBytesIOに作成。"""
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
                matched = master_df[master_df["郵便番号"].isin(used_zip_codes)]
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
    """アップロードファイルを読み込み、種別/df/xls/シート名を返す。"""
    name = uploaded_file.name
    lower = name.lower()
    if lower.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str)
        return name, df, None, "csv", "data"
    xls = pd.ExcelFile(uploaded_file)
    sheet = st.selectbox("Excelシートを選択", xls.sheet_names)
    df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
    return name, df, xls, "excel", sheet


def main():
    st.set_page_config(page_title="Geo Enrichment Tool (Streamlit)", layout="wide")
    st.title("Geo Enrichment Tool (Streamlit)")
    st.caption(
        "アップロードしたExcel/CSVにマスタ情報と緯度経度を付与し、結果データと地図HTMLをダウンロードできます。"
        "郵便番号・住所突合とジオコーディングをブラウザから実行します。"
    )
    st.write(
        "※ Streamlit の統計送信を無効にする場合は `%USERPROFILE%\\.streamlit\\config.toml` に "
        "`[browser]\\ngatherUsageStats = false` を設定してください。メール入力プロンプトは空のまま Enter でスキップできます。"
    )

    uploaded = st.file_uploader("入力ファイルを選択 (Excel/CSV)", type=["csv", "xlsx", "xls"])
    if uploaded is None:
        st.info("入力ファイルを選択してください。")
        st.stop()

    file_name, df_input, xls, kind, sheet_name = _load_input(uploaded)
    base_name = os.path.splitext(file_name)[0]
    st.success(f"読み込み完了: {file_name} / {df_input.shape}")
    st.dataframe(df_input.head())

    cols = df_input.columns.tolist()
    zip_cols = st.multiselect("郵便番号列を選択（複数可）", cols)
    addr_cols = st.multiselect("住所列を選択（複数可）", cols)

    st.markdown("#### ログ")
    log_box = st.empty()

    if st.button("実行"):
        with st.spinner("処理中..."):
            buf, fname, map_bytes, map_name, df_out = _run_pipeline(
                df_input, sheet_name, kind, zip_cols, addr_cols, xls, log_box, base_name
            )
            st.session_state["last_output"] = {
                "buf": buf.getvalue(),
                "fname": fname,
                "map_bytes": map_bytes,
                "map_name": map_name,
                "df_head": df_out.head(),
            }
        st.success("処理完了")
        st.dataframe(df_out.head())

    # 前回実行結果を常に表示し、ダウンロードも保持（チェックボックス方式）
    if "last_output" in st.session_state:
        lo = st.session_state["last_output"]
        st.markdown("#### 前回の結果")
        if lo.get("df_head") is not None:
            st.dataframe(lo["df_head"])

        st.write("ダウンロードする内容を選択してください（チェック後に下のボタンで保存）。")
        sel_data = st.checkbox("結果データ", value=True, key="dl_data_chk")
        has_map = bool(lo.get("map_bytes") and lo.get("map_name"))
        sel_map = st.checkbox("地図HTML", value=has_map, key="dl_map_chk", disabled=not has_map)

        download_bytes = None
        download_name = None
        download_mime = "application/octet-stream"

        if sel_data and sel_map and has_map:
            buf_zip = io.BytesIO()
            with zipfile.ZipFile(buf_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(lo["fname"], lo["buf"])
                zf.writestr(lo["map_name"], lo["map_bytes"])
            buf_zip.seek(0)
            download_bytes = buf_zip.getvalue()
            download_name = "geo_results.zip"
            download_mime = "application/zip"
        elif sel_data:
            download_bytes = lo["buf"]
            download_name = lo["fname"]
        elif sel_map and has_map:
            download_bytes = lo["map_bytes"]
            download_name = lo["map_name"]
            download_mime = "text/html"

        if download_bytes and download_name:
            st.download_button(
                "選択したファイルをダウンロード",
                data=download_bytes,
                file_name=download_name,
                mime=download_mime,
                key="download_selected",
            )
        else:
            st.info("ダウンロード対象をチェックしてください。")


if __name__ == "__main__":
    main()
