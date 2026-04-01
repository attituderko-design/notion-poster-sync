"""
concert/services/keys.py
HARMONIA 全ページ共通のプロパティキー定数。
このファイルを唯一の真実源とし、各ページはここからインポートする。
"""

# ── ATLAS（演奏会）DB ────────────────────────────────────────
CONCERT_NAME_KEYS         = ["名称", "タイトル", "演奏会名", "PK名称"]
CONCERT_DATE_KEYS         = ["日時", "日付", "出演日", "体験日", "リリース日"]
CONCERT_MEDIA_KEYS        = ["媒体", "MEDIA_TYPE", "メディア", "種類"]
CONCERT_VENUE_KEYS        = ["会場", "場所", "会場名", "Venue"]
CONCERT_ADDRESS_KEYS      = ["住所", "Address"]
CONCERT_MEMO_KEYS         = ["メモ", "備考", "Memo"]
CONCERT_KEY_KEYS          = ["concert_key", "ConcertKey", "演奏会キー", "PK演奏会キー"]
CONCERT_CAST_MEMBERS_KEYS = ["キャスト・関係者", "キャスト", "関係者", "cast_members", "organizer"]

# ── PRACTICE（練習）DB ───────────────────────────────────────
PRACTICE_NAME_KEYS        = ["練習名", "タイトル", "PK練習名"]
PRACTICE_DATE_KEYS        = ["日時", "日付"]
PRACTICE_CONCERT_REL_KEYS = ["演奏会", "出演", "FK演奏会"]
PRACTICE_CONCERT_DAY_KEYS = ["本番日", "演奏会当日フラグ", "本番フラグ"]
PRACTICE_VENUE_KEYS       = ["会場名", "会場", "場所", "Venue"]
PRACTICE_ADDRESS_KEYS     = ["会場住所", "住所", "Address"]
PRACTICE_MEMO_KEYS        = ["メモ", "備考"]
PRACTICE_KEY_KEYS         = ["practice_key", "PracticeKey", "練習キー", "PK練習キー"]
PRACTICE_SONG_REL_KEYS    = ["演奏曲", "楽曲", "FK楽曲"]
PRACTICE_PERCUSSION_OFF_KEYS = ["打楽器休み", "打楽器不要フラグ", "打楽器不要", "打楽器休みフラグ"]
PRACTICE_DATE_CONFIRM_KEYS = ["練習日確定", "practice_date_confirmed", "practice_confirmed", "practice_fixed"]

# ── PERFORMER（出演者）DB ────────────────────────────────────
PLAYER_NAME_KEYS          = ["氏名", "名前", "表示名", "タイトル"]
PLAYER_EMAIL_KEYS         = ["メールアドレス", "Email", "email"]
PLAYER_HN_KEYS            = ["H.N.", "ハンドルネーム", "HN"]
PLAYER_PHONE_KEYS         = ["電話番号", "Phone", "Tel"]
PLAYER_LINE_KEYS          = ["LINE ID", "LINE", "Line"]
PLAYER_RECEIVE_KEYS       = ["受信", "メール受信", "前日共有受信"]
PLAYER_MEMO_KEYS          = ["メモ", "備考"]
PLAYER_KEY_KEYS           = ["player_key", "PlayerKey", "奏者キー", "PK奏者キー"]
PLAYER_PASSWORD_HASH_KEYS = ["password_hash", "パスワードハッシュ"]

# ── CONCERT_CAST（演奏会参加者）DB ───────────────────────────
PARTICIPANT_RECORD_KEYS      = ["タイトル", "participant_key", "PK参加者"]
PARTICIPANT_PLAYER_REL_KEYS  = ["出演者", "奏者", "FK奏者"]
PARTICIPANT_CONCERT_REL_KEYS = ["出演", "演奏会", "FK演奏会"]
PARTICIPANT_INST_KEYS        = ["担当楽器", "楽器"]
PARTICIPANT_NOTE_KEYS        = ["備考", "メモ"]

# ── ATTENDANCE（出欠）DB ─────────────────────────────────────
ATT_RECORD_KEYS           = ["attendance_key", "タイトル", "PK出欠"]
ATT_PLAYER_REL_KEYS       = ["演奏会参加者", "奏者", "出演者", "FK奏者"]
ATT_PRACTICE_REL_KEYS     = ["練習", "FK練習"]
ATT_STATUS_KEYS           = ["参加可否", "出欠", "ステータス", "Status"]
ATT_NOTE_KEYS             = ["コメント", "備考", "メモ"]
ATTENDANCE_KEY_KEYS       = ["attendance_key", "AttendanceKey", "出欠キー", "PK出欠キー"]

# ── APOLLO（演奏曲）DB ───────────────────────────────────────
SONG_NAME_KEYS            = ["曲名", "タイトル", "PK曲名", "作品名"]
SONG_MOVEMENT_REL_KEYS    = ["作品楽章マスタ", "作品楽章", "楽章マスタ", "Movement"]
SONG_WORK_REL_KEYS        = ["作品マスタ", "作品", "Work"]
SONG_COMPOSER_KEYS        = ["作曲者", "Composer"]
SONG_DURATION_KEYS        = ["演奏時間", "Duration"]
SONG_NOTE_KEYS            = ["メモ", "備考"]
SONG_KEY_KEYS             = ["song_key", "SongKey", "PK曲キー", "曲キー"]
SONG_CONCERT_REL_KEYS     = ["演奏会", "出演", "FK演奏会"]
SONG_SCORE_URL_KEYS       = ["楽譜URL", "score_url", "ScoreURL"]

# ── MOVEMENT（楽章）DB ───────────────────────────────────────
MOVEMENT_KEY_KEYS         = ["movement_key", "タイトル"]
MOVEMENT_NAME_KEYS        = ["楽章名", "movement_name", "name"]
MOVEMENT_NO_KEYS          = ["楽章番号", "movement_no", "number"]
MOVEMENT_ORDER_KEYS       = ["表示順", "movement_order", "order"]
MOVEMENT_ROMAN_KEYS       = ["ローマ数字表示", "roman", "roman_numeral"]
MOVEMENT_WORK_REL_KEYS    = ["作品マスタ", "作品", "Work"]

# ── INSTRUMENT（楽器種別）DB ─────────────────────────────────
INSTRUMENT_NAME_KEYS      = ["楽器名", "タイトル", "PK楽器名"]
INSTRUMENT_CATEGORY_KEYS  = ["カテゴリ", "分類", "Category"]
INSTRUMENT_MEMO_KEYS      = ["メモ", "備考"]
INSTRUMENT_KEY_KEYS       = ["instrument_key", "InstrumentKey", "PK楽器キー", "楽器キー"]

# ── PART_DEFINITION（パート定義）DB ─────────────────────────
PARTDEF_RECORD_KEYS       = ["part_key", "タイトル"]
PARTDEF_CONCERT_REL_KEYS  = ["演奏会", "出演", "FK演奏会"]
PARTDEF_SONG_REL_KEYS     = ["演奏曲", "楽曲", "FK楽曲", "作品楽章", "作品マスタ"]
PARTDEF_INST_REL_KEYS     = ["必要楽器", "楽器種別", "楽器", "FK楽器種別", "担当楽器"]
PARTDEF_PART_REL_KEYS     = ["パート区分", "パートマスタ", "part_type", "part_master"]
PARTDEF_NAME_KEYS         = ["パート名", "名称", "表示名"]
PARTDEF_DISPLAY_NAME_KEYS = ["表示パート名", "display_part_name", "パート表示名"]
PARTDEF_NOTE_KEYS         = ["備考", "メモ", "注記"]
PARTDEF_KEY_KEYS          = ["part_key", "PartKey", "PKパートキー", "パートキー"]
PARTDEF_SCORE_URL_KEYS    = ["楽譜URL", "score_url", "ScoreURL"]

# ── PREFERENCE（希望入力）DB ─────────────────────────────────
PREF_PLAYER_REL_KEYS      = ["演奏会参加者", "奏者", "出演者", "FK奏者"]
PREF_PART_REL_KEYS        = ["パート定義", "パート", "FKパート"]
PREF_SONG_REL_KEYS        = ["演奏曲", "楽曲", "FK楽曲", "作品楽章", "作品マスタ"]
PREF_INST_REL_KEYS        = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PREF_PRIORITY_KEYS        = ["希望順位", "優先度", "希望", "希望区分"]
PREFERENCE_KEY_KEYS       = ["preference_key", "PreferenceKey", "希望キー", "PK希望キー"]

# ── PLAYER_INSTRUMENT（楽器アサイン）DB ─────────────────────
PI_RECORD_KEYS            = ["assign_key", "レコード名", "タイトル"]
PI_PLAYER_REL_KEYS        = ["奏者", "出演者", "FK奏者"]
PI_INST_REL_KEYS          = ["楽器種別", "楽器", "担当楽器", "FK楽器種別"]
PI_CONCERT_REL_KEYS       = ["演奏会", "出演", "FK演奏会"]
PI_PARTICIPANT_REL_KEYS   = ["演奏会参加者", "参加者", "FK参加者"]
PI_ASSIGN_KEYS            = ["担当フラグ", "担当", "担当有無"]
PI_BRING_KEYS             = ["持参可フラグ", "持参可", "持参"]
PI_OWN_COUNT_KEYS         = ["所有台数", "持参台数", "持参数"]
PI_BRING_COUNT_KEYS       = ["持参台数", "持参数"]
PI_BRING_ASSIGN_KEYS      = ["持参担当", "持参担当フラグ"]
PI_PRACTICE_REL_KEYS      = ["練習", "FK練習", "Practice"]
PI_PART_REL_KEYS          = ["パート定義", "パート", "FKパート", "担当パート"]
PI_SONG_REL_KEYS          = ["演奏曲", "楽曲", "FK楽曲"]
PI_NOTE_KEYS              = ["備考", "メモ"]
ASSIGN_KEY_KEYS           = ["assign_key", "assignment_key", "AssignmentKey", "割当キー", "PK割当キー"]

# ── RENTAL（レンタル見積）DB ─────────────────────────────────
# 表示用のレコード名（見積キーとは分離）
RENTAL_RECORD_KEYS        = ["レコード名", "タイトル", "見積名", "名称"]
RENTAL_PRACTICE_REL_KEYS  = ["練習", "FK練習"]
RENTAL_INST_REL_KEYS      = ["楽器種別", "楽器", "FK楽器種別"]
RENTAL_ITEM_NAME_KEYS     = ["品目名", "item_name", "品目"]
RENTAL_VENDOR_KEYS        = ["業者名", "業者", "Vendor"]
RENTAL_QTY_KEYS           = ["台数", "数量", "Qty"]
RENTAL_UNIT_PRICE_KEYS    = ["単価", "単価（円）", "UnitPrice"]
RENTAL_CONFIRMED_KEYS     = ["確定フラグ", "確定", "Confirmed"]
RENTAL_NOTE_KEYS          = ["備考", "メモ"]
RENTAL_COST_TYPE_KEYS     = ["費用種別", "費用区分", "CostType"]
RENTAL_KEY_KEYS           = ["rental_key", "RentalKey", "見積キー", "PK見積キー"]

# ── PREFERENCE DB 追加キー ────────────────────────────────────
PREF_INSTR_REL_KEYS       = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PREF_CAN_BRING_KEYS       = ["持参可", "持参可フラグ", "持参"]
PREF_CONCERT_REL_KEYS     = ["演奏会", "出演", "FK演奏会"]

# ── PART_DEFINITION 別名キー（assign_solver用）────────────────
PART_SONG_REL_KEYS        = ["楽曲", "演奏曲", "FK楽曲", "作品楽章", "作品マスタ"]
PART_INST_REL_KEYS        = ["楽器", "楽器種別", "FK楽器種別", "担当楽器"]
PART_NAME_KEYS            = ["パート名", "名称", "タイトル", "表示名"]
PART_COUNT_KEYS           = ["必要人数", "必要台数", "台数", "人数"]
PART_CONCERT_REL_KEYS     = ["演奏会", "出演", "FK演奏会"]

# ── SCHEDULE（タイムスケジュール）DB ─────────────────────────
SCHEDULE_KEY_KEYS         = ["schedule_key", "タイトル"]
SCHEDULE_PRACTICE_REL_KEYS = ["練習", "FK練習"]
SCHEDULE_START_KEYS       = ["開始時刻", "開始", "Start"]
SCHEDULE_END_KEYS         = ["終了時刻", "終了", "End"]
SCHEDULE_TYPE_KEYS        = ["種別", "Type", "区分"]
SCHEDULE_CONTENT_KEYS     = ["内容", "メモ", "Content"]
SCHEDULE_SONG_REL_KEYS    = ["演奏曲", "楽曲", "FK楽曲"]
SCHEDULE_ORDER_KEYS       = ["表示順", "順番", "Order"]

SCHEDULE_TYPE_OPTIONS     = ["練習", "休憩", "開場", "搬入", "搬出", "退館", "その他"]

# ── PLAYER_INSTRUMENT_MASTER（所有楽器マスタ）DB ─────────────
MASTER_KEY_KEYS           = ["master_key", "タイトル"]
MASTER_PLAYER_REL_KEYS    = ["奏者", "FK奏者", "Player"]
MASTER_INST_REL_KEYS      = ["楽器種別", "楽器", "FK楽器種別"]
MASTER_OWN_COUNT_KEYS     = ["所有台数", "台数", "Count"]
MASTER_NOTE_KEYS          = ["備考", "メモ", "Note"]

# ── CONCERT_EXPENSE（経費明細）DB ────────────────────────────
EXPENSE_KEY_KEYS          = ["expense_key", "expence_key", "ExpenseKey", "経費キー", "PK経費キー", "タイトル"]
EXPENSE_CONCERT_REL_KEYS  = ["演奏会", "FK演奏会"]
EXPENSE_TYPE_KEYS         = ["種別", "Type"]
EXPENSE_CONTENT_KEYS      = ["内容", "Content"]
EXPENSE_AMOUNT_KEYS       = ["金額", "Amount"]
EXPENSE_CONFIRMED_KEYS    = ["確定", "Confirmed"]
EXPENSE_NOTE_KEYS         = ["備考", "Note"]

EXPENSE_TYPE_OPTIONS = [
    "会場費", "楽器レンタル", "楽譜レンタル",
    "印刷物・プログラム", "フライヤー", "謝礼", "その他"
]

# ── BILLING（見積/請求）DB ───────────────────────────────────
BILLING_KEY_KEYS          = ["billing_key", "BillingKey", "請求キー", "見積キー", "PK請求キー", "タイトル"]
BILLING_CONCERT_REL_KEYS  = ["演奏会", "FK演奏会", "出演"]
BILLING_DOC_TYPE_KEYS     = ["書類種別", "種別", "DocType", "区分"]
BILLING_ISSUE_DATE_KEYS   = ["発行日", "IssueDate"]
BILLING_DUE_DATE_KEYS     = ["支払期限", "DueDate", "期限"]
BILLING_MEMBER_COUNT_KEYS = ["参加者数", "人数", "MemberCount"]
BILLING_PRACTICE_COUNT_KEYS = ["練習回数", "PracticeCount"]
BILLING_OPTION_KEYS       = ["オプション実費", "OptionActual", "オプション"]
BILLING_DISCOUNT_KEYS     = ["出精値引き", "DedicationDiscount", "値引き"]
BILLING_TAX_RATE_KEYS     = ["税率", "TaxRate"]
BILLING_SUBTOTAL_KEYS     = ["税抜小計", "Subtotal"]
BILLING_TAX_KEYS          = ["消費税", "Tax"]
BILLING_TOTAL_KEYS        = ["税込合計", "Total"]
BILLING_MODE_KEYS         = ["算出モード", "Mode", "連動モード"]
BILLING_NOTE_KEYS         = ["備考", "メモ", "Note"]

# ── CONCERT_CAST 追加フィールド ───────────────────────────────
PARTICIPANT_PART_KEYS     = ["パート", "Part"]          # 旧Select型（互換用に残す）
PARTICIPANT_PART_REL_KEYS = ["パート", "Part"]          # Relation型（PART_MASTERへ）
PARTICIPANT_ROLE_KEYS     = ["役職_音楽", "役職", "Role"]
PARTICIPANT_ROLE_OPS_KEYS = ["役職_運営", "RoleOps"]
PARTICIPANT_SYSTEM_ROLE_KEYS = ["システムロール", "system_role", "SystemRole"]
PARTICIPANT_FEE_KEYS      = ["参加費", "Fee"]
PARTICIPANT_PAID_KEYS     = ["入金済", "Paid"]
PARTICIPANT_OWN_CONFIRM_KEYS = ["所有楽器確定", "ownership_confirmed", "own_confirmed"]

# ── PART_MASTER（パートマスタ）DB ────────────────────────────
PARTMASTER_NAME_KEYS      = ["パート名", "名称", "タイトル", "Name"]
PARTMASTER_TYPE_KEYS      = ["種別", "type", "category", "カテゴリ"]

# ── ATLAS（CONCERT DB）追加フィールド ────────────────────────
CONCERT_CONFIRMED_FEE_KEYS = ["確定参加費", "ConfirmedFee"]

CONCERT_CONDUCTOR_KEYS    = ["クリエイター", "指揮者", "Conductor"]
CONCERT_SOLOIST_KEYS      = ["ソリスト", "Soloist"]
SONG_CREATOR_KEYS         = ["クリエイター", "作曲家", "Composer", "作曲者"]

# ── CONCERT_SONG（演奏会×曲）DB ─────────────────────────────
CONCERT_SONG_KEY_KEYS         = ["concert_song_key", "CONCERT_SONG_KEY", "key"]
CONCERT_SONG_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
CONCERT_SONG_SONG_REL_KEYS    = ["曲", "楽曲", "演奏曲", "song"]
CONCERT_SONG_ORDER_KEYS       = ["曲順", "順番", "order"]
CONCERT_SONG_DONE_KEYS        = ["定義完了", "definition_done", "完了"]
CONCERT_SONG_NOTE_KEYS        = ["備考", "メモ", "note"]


# ── HARMONIA_CONCERT（演奏会ヘッダ）DB ─────────────────────
HARMONIA_CONCERT_KEY_KEYS = ["concert_key", "harmonia_concert_key", "タイトル"]
HARMONIA_CONCERT_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
HARMONIA_CONCERT_MANAGED_KEYS = ["管理開始", "managed", "management_started"]
HARMONIA_CONCERT_SONG_INFO_KEYS = ["楽曲情報確定", "song_info_confirmed"]
HARMONIA_CONCERT_PRACTICE_INFO_KEYS = ["練習情報確定", "practice_info_confirmed"]
HARMONIA_CONCERT_PRACTICE_DATE_KEYS = ["練習日確定", "practice_dates_confirmed", "practice_date_confirmed"]
HARMONIA_CONCERT_REQUIRED_INST_KEYS = ["必要楽器確定", "required_instruments_confirmed"]
HARMONIA_CONCERT_PARTDEF_KEYS = ["パート定義確定", "part_definition_confirmed"]
HARMONIA_CONCERT_PLAYER_INFO_KEYS = ["奏者情報確定", "participant_confirmed"]
HARMONIA_CONCERT_OWN_INST_KEYS = ["所有楽器確定", "ownership_confirmed"]
HARMONIA_CONCERT_ATTENDANCE_KEYS = ["出欠確定", "attendance_confirmed"]
HARMONIA_CONCERT_PREFERENCE_KEYS = ["希望入力確定", "preference_confirmed"]
HARMONIA_CONCERT_BRING_KEYS = ["持参楽器確定", "bring_confirmed"]
HARMONIA_CONCERT_PLAN_KEYS = ["案提示", "proposal_presented"]
HARMONIA_CONCERT_ASSIGN_KEYS = ["アサイン確定", "assign_confirmed"]
HARMONIA_CONCERT_FINANCE_KEYS = ["収支確定", "finance_confirmed"]
HARMONIA_CONCERT_INVITE_CODE_KEYS = ["招待コード", "invite_code"]

# ── CONCERT_INSTRUMENT（演奏会必要楽器）DB ──────────────────
CONCERT_INST_KEY_KEYS         = ["concert_instrument_key", "タイトル"]
CONCERT_INST_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
CONCERT_INST_SONG_REL_KEYS    = ["演奏曲", "CONCERT_SONG", "曲", "FK演奏曲"]
CONCERT_INST_INST_REL_KEYS    = ["楽器", "楽器種別", "FK楽器", "instrument"]
CONCERT_INST_COUNT_KEYS       = ["必要台数", "台数", "count", "required_count"]
CONCERT_INST_NOTE_KEYS        = ["備考", "メモ", "note"]

# ── CONCERT_ASSIGNMENT（アサイン結果）DB ────────────────────
ASSIGNMENT_KEY_KEYS         = ["assignment_key", "タイトル"]
ASSIGNMENT_CONCERT_REL_KEYS = ["演奏会", "FK演奏会", "concert"]
ASSIGNMENT_PLAYER_REL_KEYS  = ["奏者", "出演者", "FK奏者", "player"]
ASSIGNMENT_PARTDEF_REL_KEYS = ["パート定義", "パート", "FKパート", "part"]
ASSIGNMENT_SONG_REL_KEYS    = ["演奏曲", "楽曲", "FK楽曲", "song"]
ASSIGNMENT_INST_REL_KEYS    = ["楽器種別", "楽器", "FK楽器", "instrument"]
ASSIGNMENT_FLAG_KEYS        = ["担当フラグ", "担当", "assigned"]
ASSIGNMENT_NOTE_KEYS        = ["備考", "メモ", "note"]
