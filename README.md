# 🌙 ArtéMis CERS

**Cultural Experience Record System**

> Notion-based personal tracker for films, dramas, anime, concerts, exhibitions, live shows, books, manga, music albums, games, and musical scores — with metadata & cover fetching.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://artemis-cers.streamlit.app)

---

## 📘 User Guide

操作マニュアル（非技術者向け）は以下を参照してください。

- [`docs/USER_GUIDE.md`](docs/USER_GUIDE.md)

アプリ画面のサイドバー `📘 操作ガイド` からも閲覧できます。

---

## ✨ Features / 機能

- 媒体別メタデータ検索  
  `TMDB / AniList / Rakuten Books / OpenLibrary / iTunes / IGDB / MusicBrainz / Wikipedia`
- Notionカバー更新、Google Driveバックアップ
- 登録リスト（カート）による一括登録
- ロケーション検索（Nominatim）とNotion `place` 保存
- 既存データのリフレッシュ同期（全件/個別）
- 失敗ログからの個別再実行
- 演奏会（出演）⇔ 演奏曲のリレーション連携
- Driveデータスキップモード（ネットワーク制限時テスト用）

---

## 🛠 Tech Stack / 技術スタック

| Layer | Technology |
|---|---|
| Frontend | [Streamlit](https://streamlit.io/) |
| Database | [Notion API](https://developers.notion.com/) |
| Film & Drama | [TMDB API](https://www.themoviedb.org/documentation/api) |
| Anime | [AniList GraphQL API](https://anilist.gitbook.io/anilist-apiv2-docs/) |
| Books & Manga | [Rakuten Books API](https://webservice.rakuten.co.jp/) / [OpenLibrary API](https://openlibrary.org/developers/api) |
| Music Albums | [iTunes Search API](https://developer.apple.com/library/archive/documentation/AudioVideo/Conceptual/iTuneSearchAPI/) |
| Games | [IGDB API](https://api-docs.igdb.com/) (via Twitch OAuth2) |
| Musical Scores | [MusicBrainz API](https://musicbrainz.org/doc/MusicBrainz_API) |
| Composer Portraits | [Wikipedia API](https://www.mediawiki.org/wiki/API:Main_page) |
| Location | [Nominatim / OpenStreetMap](https://nominatim.org/) |
| Image Storage | [Google Drive API](https://developers.google.com/drive/api) |
| Hosting | [Streamlit Community Cloud](https://streamlit.io/cloud) |

---

## 🗂 Notion Database Schema / Notionデータベース構成

最低限、以下のプロパティを用意してください。

| Field name | Type | Notes |
|---|---|---|
| `タイトル` | Title | 必須 |
| `International Title` | Rich text | 英語タイトル |
| `媒体` | Multi-select | 映画 / ドラマ / アニメ / 演奏会（鑑賞） / 演奏会（出演） / 展示会 / ライブ/ショー / 書籍 / 漫画 / 音楽アルバム / ゲーム / 演奏曲 |
| `鑑賞日` | Date | 任意 |
| `リリース日` | Date | 任意 |
| `クリエイター` | Rich text | 監督 / 著者 / アーティスト / 開発元 / 作曲者など |
| `キャスト・関係者` | Rich text | 任意 |
| `ジャンル` | Multi-select | 任意 |
| `評価` | Select | `★`〜`★★★★★` |
| `WLflg` | Checkbox | Watchlist |
| `ロケーション` | Place | Notionの場所型 |
| `メモ` | Rich text | 任意 |
| `ISBN` | Rich text | 書籍/漫画で使用 |
| `TMDB_ID` | Number | 映画/ドラマで使用 |
| `AniList_ID` | Number | アニメで使用 |
| `IGDB_ID` | Number | ゲームで使用 |
| `iTunes_ID` | Number | 音楽アルバムで使用 |
| `TMDB_score` | Number | 任意 |
| `演奏曲` | Relation | 推奨（演奏会（出演）連携） |
| `出演履歴` | Relation | 推奨（演奏曲連携） |

### Schema Notes

- 現行仕様は `媒体` を正とします。
- `MEDIA_TYPE` は旧仕様です。現行では不要です。

---

## ⚙️ Setup / セットアップ

### 1. Install

```bash
pip install -r requirements.txt
```

### 2. Prepare Secrets

`.streamlit/secrets.toml` に以下を設定します。

```toml
NOTION_API_KEY = "..."
NOTION_DB_ID = "..."
TMDB_API_KEY = "..."
RAKUTEN_APP_ID = "..."
DRIVE_FOLDER_ID = "..."
GOOGLE_REFRESH_TOKEN = "..."
GOOGLE_CLIENT_ID = "..."
GOOGLE_CLIENT_SECRET = "..."
IGDB_CLIENT_ID = "..."
IGDB_CLIENT_SECRET = "..."
```

### 3. Run

```bash
streamlit run app.py
```

---

## 📁 Repository Structure

```text
New project/
├── app.py
├── requirements.txt
├── README.md
└── docs/
    └── USER_GUIDE.md
```

---

## 📝 License

MIT

---

> _ArtéMis — named after the goddess of the hunt and the moon. She keeps track of everything you've ever experienced._
