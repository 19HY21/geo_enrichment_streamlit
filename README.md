# geo_enrichment_streamlit

住所・郵便番号から、日本郵政マスタを用いて  
**都道府県・市区町村・政令指定都市などの地域情報と緯度経度を付与するツール群** です。

- ブラウザUI: `geocode_streamlit_app.py`（Streamlit）
- API: `api/main.py`（FastAPI ＋ 簡易ジョブキュー）
- コアロジック: `geo_logic/core.py`（UI非依存）
- バッチ実行ヘルパ: `geo_logic/tasks.py`
- ジョブキュー: `geo_logic/job_queue.py`

> Streamlit Cloud のような一時コンテナ環境では、キャッシュはコンテナ内の `geo_logic/cache/` に書かれ、再起動で消えます。必要ならキャッシュJSONをダウンロードし、次回アップロードしてください。

---

## 主要機能

- 入力ファイル：CSV / Excel（複数シート対応）
- 郵便番号・住所によるマスタ突合
- 住所からのジオコーディング（Nominatim / キャッシュ利用、5,000件バッチ）
- 結果CSV/Excelのダウンロード
- キャッシュJSONのアップロード／ダウンロード

---

## 実行方法

### Streamlit（ブラウザUI）
```bash
streamlit run geocode_streamlit_app.py
```

### FastAPI（API）
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```
- ジョブ投入: `POST /jobs`（file, zip_cols, addr_cols, cache_file など）
- 進捗確認: `GET /jobs/{job_id}`
- 出力取得: `GET /jobs/{job_id}/result`
- キャッシュ取得: `GET /jobs/{job_id}/cache`

---

## キャッシュについて
- パス: `geo_logic/cache/streamlit_local_cache.json`（ジョブID付きで別名保存）
- Streamlit Cloud ではコンテナ再起動で消えます。必要ならキャッシュJSONをダウンロード→次回アップロードしてください。

---

## フォルダ構成

```text
geo_enrichment_streamlit/
├─ geocode_streamlit_app.py      # Streamlit アプリ入口（UIのみ）
├─ api/
│   └─ main.py                   # FastAPI エントリ（ジョブ投入/進捗/結果取得）
├─ geo_logic/
│   ├─ core.py                   # 住所/郵便番号突合・ジオコーディング純粋ロジック
│   ├─ tasks.py                  # バッチ実行ヘルパ（コアを呼ぶ）
│   ├─ job_queue.py              # 簡易ジョブキュー（スレッドワーカー）
│   ├─ cache/                    # キャッシュ保存先（コンテナ内、非永続）
│   └─ archive/                  # 旧ファイル置き場
├─ data/
│   └─ zipcode_localgoverment_mst.xlsx  # 日本郵政ベースのマスタ（任意）
├─ .streamlit/
│   └─ config.toml               # 使用統計オフなどの設定（任意）
├─ .gitignore
└─ README.md
```
