# 🌙 ArtéMis CERS

**Cultural Experience Recording System**

> Notion-based personal tracker for films, dramas, concerts, exhibitions, live shows, books, manga, music albums, games, and musical scores — with automatic metadata & poster fetching.
>
> Notionをバックエンドにした、映画・ドラマ・演奏会・展示会・ライブ/ショー・書籍・漫画・音楽アルバム・ゲーム・演奏曲の鑑賞記録管理システム。メタデータとポスター画像を自動取得します。

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://artemis-cers.streamlit.app)

---

## ✨ Features / 機能一覧

| Feature | Description |
|---|---|
| 🔍 Multi-source search | Fetch metadata from TMDB / Rakuten Books / iTunes / IGDB / MusicBrainz by media type |
| 🖼️ Auto poster fetch | Automatically retrieve cover images and save to Google Drive (films & dramas) |
| 🛒 Cart registration | Add multiple titles to a cart, edit per-item details, then bulk-register in one go |
| 📍 Location tagging | Search venues via Nominatim and write to Notion's place field (mini-map enabled) |
| 🔄 Refresh sync | Re-sync metadata and icons for existing Notion records |
| 🎵 Track listing | Append album track lists to notes (music albums) |
| 🔁 Duplicate detection | Detect duplicates via TMDB ID and auto-fetch existing Notion records |
| 🎼 Composer search | Search composers and works via MusicBrainz; auto-fetch portrait images |

---

## 📸 Screenshots / スクリーンショット

![ArtéMis CERS Screenshot](assets/SS.png)

---

## 🛠 Tech Stack / 技術スタック

| Layer | Technology |
|---|---|
| Frontend | [Streamlit](https://streamlit.io/) |
| Database | [Notion API](https://developers.notion.com/) |
| Film & Drama | [TMDB API](https://www.themoviedb.org/documentation/api) |
| Books & Manga | [Rakuten Books API](https://webservice.rakuten.co.jp/) |
| Music Albums | [iTunes Search API](https://developer.apple.com/library/archive/documentation/AudioVideo/Conceptual/iTuneSearchAPI/) |
| Games | [IGDB API](https://api-docs.igdb.com/) (via Twitch OAuth2) |
| Musical Scores | [MusicBrainz API](https://musicbrainz.org/doc/MusicBrainz_API) |
| Composer Portraits | [Wikipedia API](https://www.mediawiki.org/wiki/API:Main_page) |
| Location | [Nominatim / OpenStreetMap](https://nominatim.org/) |
| Image Storage | [Google Drive API](https://developers.google.com/drive/api) |
| Hosting | [Streamlit Cloud](https://streamlit.io/cloud) |

---

## 🗂 Notion Database Schema / Notionデータベース構成

The following fields are required in your Notion database:
以下のフィールドをNotionデータベースに用意してください。

| Field name | Type | Notes |
|---|---|---|
| `タイトル` | Title | — |
| `International Title` | Rich text | English title |
| `媒体` | Multi-select | 映画 / ドラマ / 演奏会 / 展示会 / ライブ/ショー / 書籍 / 漫画 / 音楽アルバム / ゲーム / 演奏曲 |
| `MEDIA_TYPE` | Multi-select | movie / tv / event / book / manga / album / game / score |
| `鑑賞日` | Date | — |
| `リリース日` | Date | Release date (end date used for exhibitions) |
| `クリエイター` | Rich text | Director / Author / Artist / Developer / Composer |
| `キャスト・関係者` | Rich text | Cast / Publisher |
| `ジャンル` | Multi-select | — |
| `評価` | Select | ★ / ★★ / ★★★ / ★★★★ / ★★★★★ |
| `WLflg` | Checkbox | Watchlist flag |
| `ロケーション` | Place | Venue / purchase location (mini-map enabled) |
| `メモ` | Rich text | Track lists, notes, etc. |
| `ISBN` | Rich text | Books & manga |
| `TMDB_ID` | Number | Films & dramas (for duplicate detection) |
| `TMDB_score` | Number | TMDB user score |
| `年代` | Formula | Auto-calculated from release date (do not write) |
| `メディアリンク` | Formula | Auto-generated link (do not write) |

---

## ⚙️ Setup / セットアップ手順

### 1. Clone the repository

```bash
git clone https://github.com/attituderko-design/artemis-cers.git
cd artemis-cers
pip install -r requirements.txt
```

### 2. Notion

1. Create a new Notion integration at https://www.notion.so/my-integrations
2. Connect the integration to your database
3. Note your **API Key** and **Database ID**

### 3. TMDB

1. Create an account at https://www.themoviedb.org/
2. Go to Settings → API → Request an API key
3. Note your **API Key (v3 auth)**

### 4. Rakuten Books

1. Register at https://webservice.rakuten.co.jp/
2. Create an app and note your **Application ID** and **Access Key**

### 5. IGDB (Games)

1. Create a Twitch Developer account at https://dev.twitch.tv/
2. Register an application and note your **Client ID** and **Client Secret**

### 6. Google Drive

1. Create a project in [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the Google Drive API
3. Create OAuth2 credentials and obtain a **refresh token**
4. Create a folder in Google Drive and note the **Folder ID**

### 7. Configure Streamlit secrets

Create `.streamlit/secrets.toml` (local) or set via Streamlit Cloud dashboard.
See `.streamlit/secrets.toml.example` for the full template.

### 8. Run locally

```bash
streamlit run app.py
```

---

## 📁 Repository Structure / リポジトリ構成

```
artemis-cers/
├── app.py                  # Main application
├── requirements.txt
├── README.md
├── .gitignore
├── .streamlit/
│   └── secrets.toml.example
└── assets/
    ├── logo.png
    ├── favicon.png
    ├── SS.png
    └── icons/
        ├── camera-reels.svg      # 映画
        ├── display.svg           # ドラマ
        ├── music-note-beamed.svg # 演奏会
        ├── exhibition.svg        # 展示会
        ├── mic.svg               # ライブ/ショー
        ├── book.svg              # 書籍
        ├── book-manga.svg        # 漫画
        ├── disc.svg              # 音楽アルバム
        ├── controller.svg        # ゲーム
        └── music-score.svg       # 演奏曲
```

---

## 📝 License

MIT

---

> _ArtéMis — named after the goddess of the hunt and the moon. She keeps track of everything you've ever experienced._
