# 🌙 ArtéMis CERS

**Cultural Experience Recording System**

> Notion-based personal tracker for films, dramas, concerts, exhibitions, live shows, books, manga, music albums, and games — with automatic metadata & poster fetching.
>
> Notionをバックエンドにした、映画・ドラマ・演奏会・展示会・ライブ/ショー・書籍・漫画・音楽アルバム・ゲームの鑑賞記録管理システム。メタデータとポスター画像を自動取得します。

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://notion-poster-sync-5wr4mgqdksey3z8tttbk9u.streamlit.app)

---

## ✨ Features / 機能一覧

| Feature | Description |
|---|---|
| 🔍 Multi-source search | Fetch metadata from TMDB / Rakuten Books / iTunes / IGDB by media type |
| 🖼️ Auto poster fetch | Automatically retrieve cover images and save to Google Drive (films & dramas) |
| 📝 Bulk registration | Register multiple titles at once from search results |
| 🔄 Refresh sync | Re-sync metadata and icons for existing Notion records |
| 🎵 Track listing | Append album track lists to notes (music albums) |
| 🔁 Duplicate detection | Detect duplicates via TMDB ID and auto-fetch existing Notion records |

---

## 📸 Screenshots / スクリーンショット

<!-- TODO: Add screenshots here -->
> _Screenshots coming soon_

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
| `MEDIA_TYPE` | Select | 映画 / ドラマ / 演奏会 / 展示会 / ライブ/ショー / 書籍 / 漫画 / 音楽アルバム / ゲーム |
| `鑑賞日` | Date | — |
| `クリエイター` | Rich text | Director / Author / Artist / Developer |
| `キャスト・関係者` | Rich text | Cast / Publisher |
| `リリース日` | Rich text | — |
| `ジャンル` | Multi-select | — |
| `評価` | Select | — |
| `メモ` | Rich text | Track lists, notes, etc. |
| `ISBN` | Rich text | Books & manga |
| `TMDB_ID` | Rich text | Films & dramas (for duplicate detection) |
| `カバー` | Files & media | Poster image (auto-set via Google Drive URL) |
| `アイコン` | Files & media | Media type icon (auto-set via GitHub raw URL) |

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

Create `.streamlit/secrets.toml` (local) or set via Streamlit Cloud dashboard:

```toml
NOTION_API_KEY       = "secret_xxxx"
NOTION_DB_ID         = "xxxx"
TMDB_API_KEY         = "xxxx"
RAKUTEN_APP_ID       = "xxxx"
RAKUTEN_ACCESS_KEY   = "pk_xxxx"
RAKUTEN_AFFILIATE_ID = "xxxx"        # optional
IGDB_CLIENT_ID       = "xxxx"
IGDB_CLIENT_SECRET   = "xxxx"
DRIVE_FOLDER_ID      = "xxxx"

[gcp_service_account]
# Google OAuth2 credentials
type                        = "authorized_user"
client_id                   = "xxxx"
client_secret               = "xxxx"
refresh_token               = "xxxx"
```

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
    └── icons/
        ├── camera-reels.svg   # 映画
        ├── display.svg        # ドラマ
        ├── music-note-beamed.svg # 演奏会
        ├── exhibition.svg     # 展示会
        ├── mic.svg            # ライブ/ショー
        ├── book.svg           # 書籍
        ├── book-manga.svg     # 漫画
        ├── disc.svg           # 音楽アルバム
        └── controller.svg     # ゲーム
```

---

## 📝 License

MIT

---

> _ArtéMis — named after the goddess of the hunt and the moon. She keeps track of everything you've ever experienced._
