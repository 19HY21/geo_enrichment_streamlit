# Geo Enrichment Streamlit

日本郵政マスタを用いて、郵便番号・住所の突合および緯度経度付与（ジオコーディング）を行うツールです。
**ブラウザUI（Streamlit）** と **API（FastAPI）** を同梱し、業務データの地理情報付与・可視化・再利用可能なキャッシュ運用を想定しています。

---

## 目的 / 背景（Why）

- 住所データには表記ゆれ（全角/半角、丁目表記、漢数字など）や欠損が多く、そのままでは地理分析・可視化・商圏分析に利用しづらい。
- 郵便番号と住所の不整合も多く、突合精度の把握が重要。
- 本ツールは、突合精度やジオコード精度を段階的に判定し、キャッシュを活用して負荷を抑えつつ UI/API から再利用できる基盤を提供します。

---

## 特徴

- 郵便番号・住所の段階的突合ロジック
- Parquet を用いた突合済みデータ / ジオコードキャッシュ
- 未一致データのみを対象とする効率的処理
- チャンク単位（1000件）での結果出力
- Streamlit UI と FastAPI を分離した構成
- GitHub Actions による日本郵政マスタ・統合マスタの自動更新

---

## 構成（全フォルダ・主要ファイル）※archive 配下は省略

```
.
├── geocode_streamlit_app.py # Streamlitアプリ本体
├── api/
│ └── main.py # FastAPI エントリ
├── geo_logic/
│ ├── core.py # 郵便番号/住所突合・ジオコーディングのコアロジック
│ └── init.py
├── mst_logic/ # マスタ生成スクリプト
│ ├── zip_mst_build.py # 日本郵政データ取得
│ ├── mic_localgov_build.py # 総務省 団体コード取得
│ └── zipcode_localgov_build.py # 上記マスタ統合
├── mst/ # 生成済みマスタ配置先
├── data/ # サンプル・入力データ
├── .github/workflows/
│ └── build_mst.yml # マスタ自動更新（GitHub Actions）
├── .devcontainer/ # 開発コンテナ設定
└── requirements.txt
```

---

## 処理フロー概要

1. 入力データ（Excel / CSV）を読み込み
2. 郵便番号を正規化（ハイフン除去・7桁化）
3. 郵便番号マスタと突合
4. 住所キーで突合済みParquetを参照し、ヒット分を上書き
5. 未一致住所のみマスタ突合を実施
6. ジオコードはキャッシュParquetを優先利用
7. 未ヒットのみ外部ジオコーディング API を利用
8. 結果をチャンク単位で Parquet 出力
9. 更新されたキャッシュを次回処理用に保存

---

## 現行仕様（Streamlit）

- **入力**: Excel / CSV（任意で住所チャンク、ジオコードチャンク）
- **郵便番号突合**: 日本郵政マスタと突合（ハイフン・全角対応で7桁化）
- **住所突合**: 住所チャンクで一致 → 上書き、未一致のみマスタ突合
- **ジオコーディング**: ジオコーディングチャンクでヒット → 流用、未ヒットのみ API 問い合わせ
- **出力**: 入力データ + 突合/ジオコード結果。住所/ジオコードは 1000 件ごと Parquet ダウンロード可。
- **キャッシュ形式**: Parquetのみ

---

## 使い方（UI）

```bash
streamlit run geocode_streamlit_app.py
```

1. Excel/CSV をアップロード
2. 必要に応じて突合済みParquet、キャッシュParquetを追加
3. 郵便番号列・住所列を選択
4. 必要なら「緯度経度を付与する」をON
5. 実行後、画面下部に各種ダウンロードボタンが表示

---

## 使い方（API）

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

例: `POST /geocode`
Request:

```json
{ "address": "東京都千代田区千代田1-1" }
```

Response:

```json
{ "lat": 35.685175, "lon": 139.752799, "geocode_flag": "full" }
```

※ 詳細は `/docs`（OpenAPI）を参照

---

## マスタ生成（ローカル実行）

```bash
python mst_logic/zip_mst_build.py
python mst_logic/mic_localgov_build.py
python mst_logic/zipcode_localgov_build.py
```

出力:

- `mst/japanpost_zipcode_mst.xlsx`
- `mst/mic_localgoverment_mst.xlsx`
- `mst/zipcode_localgoverment_mst.xlsx`

---

## GitHub Actions（月1回自動更新）

- `.github/workflows/build_mst.yml`
- 毎月1日 05:00 UTC に以下を実行（更新があればコミット）
  - 日本郵政マスタ取得 (`zip_mst_build.py`)
  - 総務省団体コード取得 (`mic_localgov_build.py`)
  - 統合マスタ生成 (`zipcode_localgov_build.py`)

---

## フラグ定義

### `*_zip_match_flag`（郵便番号突合）

- `unique_full`: 都道府県/市区町村/町域が一意
- `multi_town`: 町域が複数候補
- `ambiguous_pref_city`: 都道府県のみ一意
- `None`: 未一致

### `*_match_flag`（住所突合）

- `pref_city_town`
- `pref_city`
- `no_pref_city_town`
- `no_pref_city`
- `city_only`
- `None`

### `*_geocode_flag`（ジオコード精度）

- `full`
- `town`
- `city`
- `pref`
- `not_found`

---

## Known Limitations / Trade-offs

- Streamlit Community Cloud は CPU/メモリ/実行時間に制約があり、1リクエストあたり約30分で強制終了することがあります。
- 数十万件規模では強制終了の可能性があるため、Parquetキャッシュで再計算を抑制し、未一致のみ処理する設計。
- 大規模データや長時間バッチは、サーバー/コンテナ環境や外部スケジューラ（GitHub Actions, Cloud Run Jobs, Lambda 等）での運用を推奨。

---

## 本番構成例（想定）

- UI: Streamlit
- API: FastAPI + Uvicorn
- バッチ: GitHub Actions / Cloud Run Jobs / Lambda など
- マスタ・キャッシュ: S3 / GCS（Parquet）
- ジオコーディング: 非同期 API + レート制御

---

## 想定ユースケース

- 顧客・拠点データへの地理情報付与
- 地図可視化・商圏分析の前処理
- 住所品質チェック・補完
- ジオコードキャッシュ基盤の構築
