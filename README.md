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
  - 郵便番号重複に対応（町域重複は町域を空に、都道府県・市区町村も曖昧なら郵便番号のみ）し、`*_zip_match_flag` を付与
  - 住所突合は数字表記ゆれ（半角/全角→漢数字、例: 19→十九）を正規化し、既存列があれば空セルのみ埋める。結果フラグは `*_match_flag`
- 住所からのジオコーディング（Nominatim / キャッシュ利用、5,000件バッチ、UIでON/OFF選択可、結果フラグは `*_geocode_flag`）
- 結果CSV/Excelのダウンロード
- キャッシュJSONのアップロード／ダウンロード（ジオコーディング実行時のみ保存）

---

## 実行方法

### Streamlit（ブラウザUI）
```bash
streamlit run geocode_streamlit_app.py
```
- 郵便番号列・住所列を選択し、必要に応じて「緯度経度を付与する」をON/OFF
- キャッシュJSONを持っていればアップロード可能。ジオコーディング実行時のみ新しいキャッシュを保存・ダウンロードできます。

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

---

## 付与フラグの意味
- `*_zip_match_flag`: 郵便番号突合の状態  
  - `unique_full`: 郵便番号＋都道府県＋市区町村＋町域が一意でフル付与  
  - `multi_town`: 郵便番号＋都道府県＋市区町村は一意だが町域が複数（町域系は空）  
  - `ambiguous_pref_city`: 都道府県・市区町村の組み合わせも複数（郵便番号のみ付与）  
  - `None`: マスタ未一致
- `*_match_flag`: 住所突合の状態  
  - `pref_city_town`: 都道府県＋市区町村＋町域で一致  
  - `pref_city`: 都道府県＋市区町村まで一致（町域は空）  
  - `no_pref_city_town`: 住所に都道府県なしだが市区町村＋町域で一致  
  - `no_pref_city`: 住所に都道府県なし、市区町村のみ一致（町域は空）  
  - `city_only`: 市区町村先頭一致で都道府県を一意に補完できた場合（町域は空）
- `*_geocode_flag`: ジオコーディング結果  
  - `full`: フルマッチ  
  - `town`: 町レベルまで  
  - `city`: 市区町村レベルまで  
  - `pref`: 都道府県レベルまで  
  - `not_found`: 未取得
```
