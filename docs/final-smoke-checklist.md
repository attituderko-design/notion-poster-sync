# HARMONIA 最終スモークチェック（本番）

## 0. 反映
1. `cd /home/ubuntu/harmonia_form`
2. `git pull origin main`
3. `sudo systemctl restart harmonia`
4. `sudo systemctl status harmonia --no-pager`

期待値:
- `active (running)`
- 500 が出ない

---

## 1. 共通導線（全ロール）
1. `https://artemis-harmonia.com` にアクセス
2. ログイン→演奏会選択→メニュー到達
3. 右上 `?` / `🔔` モーダルが開閉できる
4. 通知の「確認しました」で履歴に詳細が残る

期待値:
- モーダル表示崩れなし
- 通知履歴に本文が展開表示される

---

## 2. Player チェック
1. 出欠入力→保存
2. 楽器・パート希望入力→保存
3.（Percussionのみ）所有楽器入力→保存
4. 「資料」タブ表示（PDFリンク/楽譜リンク）

期待値:
- 保存後に値が保持される
- 権限外メニューが見えない

---

## 3. Leader チェック
1. パート内の `出欠 / メンバー / 所有楽器 / アサイン / 資料` が表示
2. ToDo が条件に応じて表示・遷移
3. 直近練習情報モーダル（タイムライン）表示

期待値:
- 自パート範囲で正しく絞り込み
- コメント表示/モーダル動作が正常

---

## 4. Manager チェック（重要）
1. `進行表` タブ
   - 追加 / 編集 / 削除
   - 並び替え → 「並び替えを保存」 → 反映
2. `練習日程` タブ（PRACTICE CRUD）
3. `所有楽器` タブ（ownmap）
   - 練習日切替
   - 持参見込み（必要/○/△/不足/レンタル）表示
   - 不足分の「レンタル登録」実行

期待値:
- 並び替え後にDBの `表示順` と時刻が整合
- ownmap の必要数は同一(曲,楽器)で合算される
- レンタル登録で RENTAL DB に upsert される

---

## 5. アサイン導線
1. 厳密解生成（A〜D）
2. 案提示
3. Player 側で 賛同/異議
4. Leader/Manager 側で応答集計確認
5. 確定

期待値:
- 案提示中: 「あなたへのアサイン案」
- 確定後: 「あなたのアサイン状況」
- 応答が `CONCERT_ASSIGN_RESPONSE` に記録

---

## 6. PWA
1. ブラウザで `manifest`/`sw` が 200
   - `https://artemis-harmonia.com/manifest.webmanifest`
   - `https://artemis-harmonia.com/sw.js`
2. ホーム画面追加（iOS/Android/PC）
3. 起動後、画面遷移が可能

期待値:
- 旧ドメインアプリと混同しない
- アイコン起動でメニュー到達

---

## 7. 監視（最低限）
1. `sudo journalctl -u harmonia -n 200 --no-pager`
2. `sudo journalctl -u harmonia -f`

確認ポイント:
- `ReadTimeout` 連発していない
- 500 トレースバックが出ていない

---

## 8. ロールバック（緊急時）
1. `git log --oneline -n 5`
2. 直前安定コミットへ戻す
   - `git checkout <stable_commit> -- vps_patch`
   - `sudo systemctl restart harmonia`

※ destructive 操作（`reset --hard`）は使わない。
