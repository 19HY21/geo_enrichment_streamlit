# Geo Enrichment Streamlit

日本郵政マスタを使って郵便番号・住所の突合と緯度経度付与を行うツールです。ブラウザUI（Streamlit）とAPIを同梱しています。

## 構成
- `geocode_streamlit_app.py`: Streamlit アプリ
- `geo_logic/`: 突合・ジオコーディングのコアロジック
- `api/main.py`: FastAPI エントリ
- `data/zipcode_localgoverment_mst.xlsx`: 郵便番号／住所マスタ

## 現行仕様（Streamlit）
- 入力: Excel/CSV
- 突合済みParquet（任意）: 住所キーで元データに上書き。未一致のみマスタ突合。
- キャッシュParquet（任意）: `address,lat,lon,flag` をキャッシュに取り込み、ヒットしない住所だけジオコーディング。
- 郵便番号: 元データとマスタを突合。
- 住所: 突合済みParquetで一致→上書き、未一致のみマスタ突合。
- ジオコーディング: キャッシュParquetでヒット→流用、未ヒットのみAPIで取得。
- 出力: 元データ + 突合/ジオコード列。住所/ジオコードは1000件ごとParquetでダウンロード可。キャッシュもParquetでダウンロード可。
- キャッシュファイルはParquetのみを使用。

## 使い方（UI）
1. `streamlit run geocode_streamlit_app.py`
2. Excel/CSV をアップロード。必要に応じて突合済みParquet、キャッシュParquetをアップロード。
3. 郵便番号列・住所列を選択し、必要なら「緯度経度を付与する」をON。
4. 実行後、プログレス下に住所チャンク、ジオコードチャンク、結果データ、キャッシュParquetのダウンロードボタンが表示されます。

## API起動
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

## フラグの意味
- `*_zip_match_flag`（郵便番号突合）
  - `unique_full`: 郵便番号＋都道府県＋市区町村＋町域が一意で一致
  - `multi_town`: 郵便番号＋都道府県＋市区町村は一意だが町域が複数（町域は空）
  - `ambiguous_pref_city`: 都道府県は一意だが市区町村が複数候補（都道府県だけ付与）
  - `None`: マスタ未一致
- `*_match_flag`（住所突合）
  - `pref_city_town`: 都道府県＋市区町村＋町域で一致
  - `pref_city`: 都道府県＋市区町村まで一致、町域は空
  - `no_pref_city_town`: 都道府県なしで市区町村＋町域が一致
  - `no_pref_city`: 都道府県なしで市区町村のみ一致、町域は空
  - `city_only`: 市区町村の先頭一致から都道府県を一意に補完（町域は空）
  - `None`: 未一致
- `*_geocode_flag`（ジオコード精度）
  - `full`: フルマッチ
  - `town`: 町レベル
  - `city`: 市区町村レベル
  - `pref`: 都道府県レベル
  - `not_found`: 未取得
