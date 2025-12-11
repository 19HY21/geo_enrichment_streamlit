# geo_enrichment_streamlit

住所・郵便番号を日本郵政マスタに突合し、地域情報と緯度経度を付与するツール群です。

- ブラウザUI: `geocode_streamlit_app.py`（Streamlit）
- API: `api/main.py`（FastAPI＋簡易ジョブキュー）
- コアロジック: `geo_logic/core.py`（UI非依存）
- バッチ実行ヘルパー: `geo_logic/tasks.py`
- ジョブキュー: `geo_logic/job_queue.py`

> Streamlit Cloud のような一時コンテナ環境ではキャッシュはコンテナ内 (`geo_logic/cache/`) に保存され、再起動で消えます。永続させたい場合はキャッシュJSONをダウンロードし、次回アップロードしてください。

---

## 主な機能
- CSV / Excel 入力（複数シート対応）
- 郵便番号突合  
  - 重複郵便番号（町域が複数）の場合は町域系を空にし、`*_zip_match_flag` に状態を記録
- 住所突合  
  - 数字ゆれ（半角/全角/漢数字）を正規化。ただし 2桁までを漢数字に変換（番地等の長い数字は変換しない）
  - 括弧付き町域がマスタに存在し、入力が括弧前までしかない場合は曖昧として町域を付与しない
  - 既存値があれば空セルのみ埋める
  - フラグは `*_match_flag`
- ジオコーディング（Nominatim + キャッシュ、5,000件バッチ想定）  
  - UIでON/OFF選択可。フラグは `*_geocode_flag`
- 出力のダウンロード（CSV/Excel）
- キャッシュJSONのアップロード／ダウンロード（ジオコーディング実行時のみ保存）

---

## 実行方法
### Streamlit（ブラウザUI）
```bash
streamlit run geocode_streamlit_app.py
```
- 郵便番号列・住所列を選択し、必要に応じて「緯度経度を付与する」をON/OFF
- キャッシュJSONを持っていればアップロード可能。ジオコーディング実行時のみ新しいキャッシュを保存・ダウンロードできます

### FastAPI（API）
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```
- ジョブ投入: `POST /jobs`（file, zip_cols, addr_cols, cache_file などを送信）
- 進捗確認: `GET /jobs/{job_id}`
- 出力取得: `GET /jobs/{job_id}/result`
- キャッシュ取得: `GET /jobs/{job_id}/cache`

---

## キャッシュについて
- パス例: `geo_logic/cache/streamlit_local_cache.json`（ジョブID付きで別名保存）
- Streamlit Cloud では再起動で消えます。必要ならダウンロードし、次回アップロードしてください。

---

## フォルダ構成
```text
geo_enrichment_streamlit/
├─ geocode_streamlit_app.py      # Streamlit アプリ入口
├─ api/
│  └─ main.py                    # FastAPI エントリ。ジョブ投入/進捗/結果取得
├─ geo_logic/
│  ├─ core.py                    # 住所/郵便番号突合・ジオコーディングの純粋ロジック
│  ├─ tasks.py                   # バッチ実行ヘルパー（コア呼び出し）
│  ├─ job_queue.py               # 簡易ジョブキュー（スレッドワーカー）
│  ├─ cache/                     # キャッシュ保存（一時コンテナでは非永続）
│  └─ archive/                   # 不要ファイルの退避先
├─ data/
│  └─ zipcode_localgoverment_mst.xlsx  # 日本郵政ベースのマスタ
└─ README.md
```

---

## フラグの意味
- `*_zip_match_flag`（郵便番号突合）
  - `unique_full`: 郵便番号＋都道府県＋市区町村＋町域が一意で付与
  - `multi_town`: 郵便番号＋都道府県＋市区町村は一意だが町域が複数（町域系は空）
  - `ambiguous_pref_city`: 都道府県・市区町村の組み合わせも複数（郵便番号のみ付与）
  - `None`: マスタ未一致
- `*_match_flag`（住所突合）
  - `pref_city_town`: 都道府県＋市区町村＋町域で一致
  - `pref_city`: 都道府県＋市区町村まで一致、町域は空
  - `no_pref_city_town`: 住所に都道府県が無いが市区町村＋町域で一致
  - `no_pref_city`: 住所に都道府県なし、市区町村のみ一致、町域は空
  - `city_only`: 市区町村の先頭一致で都道府県を一意に補完できた場合（町域は空）
- `*_geocode_flag`（ジオコーディング）
  - `full`: フルマッチ
  - `town`: 町レベル
  - `city`: 市区町村レベル
  - `pref`: 都道府県レベル
  - `not_found`: 未取得
