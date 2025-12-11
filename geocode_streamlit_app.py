# -*- coding: utf-8 -*-
"""
Geo Enrichment Tool (Streamlit版)
- geo_logic/core のロジックを利用し、ブラウザから郵便番号/住所突合とジオコーディングを実行
- GitHub 配置想定:
    - geo_enrichment_streamlit/data/zipcode_localgoverment_mst.xlsx
    - geo_enrichment_streamlit/geo_logic/core.py
    - エントリーポイント: geo_enrichment_streamlit/geocode_streamlit_app.py
"""

import io
import json
import os
import sys
from typing import List, Tuple

import pandas as pd
import streamlit as st

# パス設定（geo_logic 配下の core を読み込む）
BASE_DIR = os.path.dirname(__file__)
LOGIC_DIR = os.path.join(BASE_DIR, "geo_logic")
if LOGIC_DIR not in sys.path:
    sys.path.insert(0, LOGIC_DIR)

import core as logic  # noqa: E402

# マスタパスをGitHub内の data に差し替える
logic.MASTER_PATH = os.path.join(BASE_DIR, "data", "zipcode_localgoverment_mst.xlsx")

# 利用する関数・定数を束ねる
CACHE_DIR = logic.CACHE_DIR
OUTPUT_SUFFIX = logic.OUTPUT_SUFFIX
BATCH_SIZE_DEFAULT = 5000
attach_master_by_address = logic.attach_master_by_address
attach_master_by_zip = logic.attach_master_by_zip
geocode_addresses = logic.geocode_addresses
load_cache = logic.load_cache
read_master = logic.read_master
save_cache = logic.save_cache
add_geocode_columns = logic.add_geocode_columns
normalize_address = logic.normalize_address


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
    batch_size: int,
    geocode_enabled: bool = True,
    uploaded_cache: dict | None = None,
):
    """突合〜ジオコーディング〜出力データ生成を実行。"""
    st.session_state["logs"] = []
    progress = st.progress(0)
    status = st.empty()

    # フェーズ別の重みを設定（有効なフェーズのみで正規化）
    weights = {
        "zip": 20,
        "addr": 20,
        "geo": 55,
        "out": 5,
    }
    enabled_phases = []
    enabled_phases.append("zip")
    if addr_cols:
        enabled_phases.append("addr")
    if addr_cols and geocode_enabled:
        enabled_phases.append("geo")
    enabled_phases.append("out")
    total_weight = sum(weights[p] for p in enabled_phases)

    def set_progress(phase: str, pct: float, text: str):
        # pct: 0-100
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
            prog_bar("zip", pct, f"[郵便番号] {detail}")

        df_work = attach_master_by_zip(
            df_work, master_df, zip_cols, progress=zip_prog, used_zip_codes=used_zip_codes
        )
        prog_bar("zip", 100, "[郵便番号] 完了")
        _log(log_box, f"郵便番号突合完了 使用郵便番号: {len(used_zip_codes)}件")

    # 住所突合
    if addr_cols:
        _log(log_box, f"住所突合開始: {addr_cols}")
        prog_bar("addr", 0, "[addr] 処理開始")

        def addr_prog(col, done, total, detail):
            pct = done / max(total, 1) * 100
            prog_bar("addr", pct, f"[addr] {detail}")

        df_work = attach_master_by_address(
            df_work, master_df, addr_cols, progress=addr_prog, used_master_idx=used_master_idx
        )
        prog_bar("addr", 100, "[addr] 完了")
        _log(log_box, f"住所突合完了 使用行: {len(used_master_idx)}件")

    # ジオコーディング（キャッシュはローカル使用）
    os.makedirs(CACHE_DIR, exist_ok=True)
    local_cache_path = os.path.join(CACHE_DIR, "streamlit_local_cache.json")
    cache = load_cache(local_cache_path)
    if uploaded_cache:
        cache.update(uploaded_cache)
    _log(log_box, f"キャッシュ読込: ローカル{len(cache)}件")

    if addr_cols and geocode_enabled:
        _log(log_box, "ジオコーディング開始（ユニーク住所ベース）")
        prog_bar("geo", 0, "[geo] 処理開始")
        all_addrs = []
        for col in addr_cols:
            all_addrs.extend(df_work[col].dropna().tolist())
        unique_addrs = [a for a in pd.Series(all_addrs).dropna().unique().tolist() if normalize_address(a)]
        total_unique = len(unique_addrs)
        _log(log_box, f"ユニーク住所数: {total_unique}件 / バッチサイズ: {batch_size}")

        geo_results = {}
        overall_done = 0

        for start in range(0, total_unique, batch_size):
            end = min(start + batch_size, total_unique)
            chunk = unique_addrs[start:end]

            def geo_prog(done, total, kind):
                now_done = overall_done + done
                pct = now_done / max(total_unique, 1) * 100
                prog_bar("geo", pct, f"[geo] {now_done}/{total_unique} ({pct:.1f}%)")

            def geo_cache_save(c):
                save_cache(local_cache_path, c)

            _log(log_box, f"バッチ処理: {start+1}〜{end}件目")
            chunk_results, cache_hit, new_count = geocode_addresses(
                chunk,
                user_agent="GeoGUI_streamlit",
                cache=cache,
                progress_cb=geo_prog,
                cache_save_cb=geo_cache_save,
            )
            geo_results.update(chunk_results)
            save_cache(local_cache_path, cache)
            overall_done = end
            _log(log_box, f"バッチ完了 cache_hit={cache_hit} 新規={new_count} 累計={overall_done}/{total_unique}")

        df_work = add_geocode_columns(df_work, addr_cols, geo_results)
    else:
        _log(log_box, "ジオコーディングはスキップ（住所未選択または緯度経度付与オフ）")

    # ジオコーディングを実行した場合のみキャッシュを保存（ダウンロード可）
    if geocode_enabled and addr_cols:
        save_cache(local_cache_path, cache)

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

    prog_bar("out", 50, "[out] 生成中")
    _log(log_box, f"出力生成完了: {fname}")
    prog_bar("out", 100, "完了")
    return buf, fname, df_work, local_cache_path


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
    st.set_page_config(page_title="Geo Enrichment Tool", layout="wide")
    st.title("Geo Enrichment Tool")
    st.caption(
        "住所・郵便番号から、日本郵政マスタを用いて都道府県・市区町村・政令指定都市などの地域情報と緯度経度を付与し、結果データを生成するアプリです。"
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
    geocode_enabled = st.checkbox("緯度経度を付与する", value=True)
    cache_upload = st.file_uploader("キャッシュJSONをアップロード（任意）", type=["json"])

    st.markdown("#### ログ")
    log_box = st.empty()

    if st.button("実行"):
        with st.spinner("処理中..."):
            uploaded_cache = None
            if cache_upload is not None:
                try:
                    raw = json.load(cache_upload)
                    uploaded_cache = {k: tuple(v) if isinstance(v, list) else tuple(v) for k, v in raw.items()}
                except Exception:
                    st.warning("キャッシュJSONの読み込みに失敗しました。無視して続行します。")
            buf, fname, df_out, cache_path = _run_pipeline(
                df_input,
                sheet_name,
                kind,
                zip_cols,
                addr_cols,
                xls,
                log_box,
                base_name,
                batch_size=BATCH_SIZE_DEFAULT,
                geocode_enabled=geocode_enabled,
                uploaded_cache=uploaded_cache,
            )
            st.session_state["last_output"] = {
                "buf": buf.getvalue(),
                "fname": fname,
                "df_head": df_out.head(),
                "cache_path": cache_path,
            }
        st.success("処理完了")

    # 前回実行結果を常に表示し、ダウンロードも保持（チェックボックス方式）
    if "last_output" in st.session_state:
        lo = st.session_state["last_output"]
        st.markdown("#### 出力データサンプル")
        if lo.get("df_head") is not None:
            st.dataframe(lo["df_head"])

        st.download_button(
            "結果データをダウンロード",
            data=lo["buf"],
            file_name=lo["fname"],
            mime="application/octet-stream",
            key="download_data",
        )

        if lo.get("cache_path") and os.path.exists(lo["cache_path"]):
            with open(lo["cache_path"], "rb") as f:
                cache_bytes = f.read()
            st.download_button(
                "キャッシュJSONをダウンロード（次回再利用用）",
                data=cache_bytes,
                file_name=os.path.basename(lo["cache_path"]),
                mime="application/json",
                key="download_cache",
            )


if __name__ == "__main__":
    main()
