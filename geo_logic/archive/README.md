# archive ディレクトリについて

このディレクトリには、現行のアプリでは使用していない旧ファイルを退避しています。

- `geocode_gui.py`: 旧GUI実装
- `geocode_streamlit.py`: 旧Streamlit実装
- `geocode_gui_ver2.py`: 旧ロジック統合版（現行は `geo_logic/core.py` に置き換え）

必要に応じて参照してくださいが、通常の利用では `geo_logic/core.py` / `geo_logic/tasks.py` / `api/main.py` を使用します。
