# geo_enrichment_streamlit

住所・郵便番号から、日本郵政マスタを用いて  
**都道府県・市区町村・政令指定都市などの地域情報と緯度経度を付与し、  
結果データと地図HTMLを生成する Streamlit アプリ** です。

ブラウザから Excel / CSV をアップロードするだけで、
- 郵便番号・住所のマスタ突合
- ジオコーディング（緯度・経度付与）
- 結果CSV/Excelのダウンロード
- 地図HTMLのダウンロード

まで実行できます。

---

## 機能概要

- 入力ファイル：CSV / Excel（複数シート対応）
- 郵便番号マスタ：日本郵政の公開データを使用（同梱 or 別取得）
- 郵便番号・住所によるマスタ突合
- 住所からのジオコーディング（Nominatim / キャッシュ利用）
- 結果データ＋地図HTMLのダウンロード
- Streamlit によるブラウザ UI

※ 実装本体は `geocode_streamlit.py` にあります。

---

## フォルダ構成（予定）

```text
geo_enrichment_streamlit/
├─ geocode_streamlit.py          # Streamlit アプリ入口
├─ geocode_gui_ver2.py           # 住所/郵便番号突合・ジオコーディングロジック
├─ data/
│   └─ zipcode_localgoverment_mst.xlsx  # 日本郵政ベースのマスタ（任意）
├─ .streamlit/
│   └─ config.toml               # 使用統計オフなどの設定（任意）
├─ .gitignore
├─ .gitattributes                # Git LFS 設定（必要な場合）
└─ README.md
