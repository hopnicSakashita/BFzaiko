# -*- coding: utf-8 -*-
"""
在庫管理システム - 定数定義ファイル
各種固定値を一元管理するための定数定義モジュール
"""

# =============================================================================
# アプリケーション設定定数
# =============================================================================
class AppConstants:
    """アプリケーション全般の定数"""
    # デフォルト値
    DEFAULT_SECRET_KEY = 'dev_key_hj37d9a8h3d98ah398dhj'  # 環境変数SECRET_KEYが設定されていない場合のフォールバック値
    DEFAULT_PORT = 5000  # 環境変数PORTが設定されていない場合のフォールバック値
    DEFAULT_DEBUG = True  # 環境変数DEBUGが設定されていない場合のフォールバック値
    
    # システム名・バージョン
    SYSTEM_NAME = '在庫管理システム'
    VERSION = '1.0.0'

# =============================================================================
# データベース関連定数
# =============================================================================
class DatabaseConstants:
    """データベース関連の定数"""
    # コート区分
    PROC_NON_COAT = 0      # ノンコート
    PROC_HARD_COAT = 1     # ハードコート
    
    # ステータスフラグ
    STATUS_ACTIVE = 0      # 有効
    STATUS_DELETED = 1     # 削除済み
    STATUS_COMPLETED = 2   # 完了
    
    # 在庫フラグ
    BPDD_FLG_NOT_SHIPPED = 0 # 未出荷
    BPDD_FLG_SHIPPED = 1     # 出荷済
    
    # 受注フラグ
    BRCP_FLG_NOT_SHIPPED = 0 # 未出荷
    BRCP_FLG_SHIPPED = 1     # 出荷済
    
    # 出荷フラグ
    BSHK_FLG_NOT_SHIPPED = 0 # 未出荷
    BSHK_FLG_SHIPPED = 1     # 出荷済み
    BSHK_FLG_PROCESSED = 2   # 加工済
    BSHK_FLG_DELIVERED = 3   # 納品書発行済
    BSHK_FLG_INVOICED = 4   # 請求書発行済
    
    # 出荷先ID
    SHIPMENT_TO_PROCESS = 601  # 加工宛
    ORDER_CMP_COLUMBUS = 501   # コロンバス
    ORDER_CMP_DALLAS = 502     # ダラス  
    ORDER_CMP_YOUNGER = 503    # ヤンガー
    ORDER_CMP_YOUNGER_EU = 504    # ヤンガーヨーロッパ
    ORDER_CMP_MISSING = 999    # 欠損
    
    # 委託先区分（CZTR_KBN）
    CZTR_KBN_CUSTOMER = 1        # 得意先
    CZTR_KBN_PROCESS_COMPANY = 2 # 加工会社
    CZTR_KBN_OTHER = 3 # その他
    
    # 委託先タイプ（CZTR_TYP）
    CZTR_TYPE_COMMON = 1 # 一般
    CZTR_TYPE_BF = 2 # BF一般兼用
    
    # 出荷区分（CSHK_KBN）
    CSHK_KBN_SHIPMENT = 0    # 出荷
    CSHK_KBN_PROCESS = 1     # 加工
    CSHK_KBN_LOSS = 2        # 欠損
    
    # グラデーション関連
    GPRR_REQ_TO_CONVEX = 1   # コンベックス
    GPRR_REQ_TO_NIDEC = 2    # ニデック
    GSHK_TO_NIDEC = 2        # ニデック出荷先
    GSHK_TO_CONVEX = 3       # コンベックス出荷先
    
    # 数値制限
    QUANTITY_MAX = 99999     # 数量最大値
    SHIPMENT_TO_MAX = 999    # 出荷先最大値
    PROCESS_ID_MAX = 99999   # 加工ID最大値（5桁対応）
    
    # フラグ値
    FLG_ACTIVE = 0           # 有効フラグ
    FLG_INACTIVE = 1         # 無効フラグ
    FLG_DELETED = 9          # 論理削除フラグ
    
    # ユーザーフラグ
    USER_FLG_ACTIVE = 0      # ユーザー有効フラグ

    # 商品分類 1:製造レンズ、2:BF、3:基材、4:加工レンズ、5:その他
    PRD_KBN_PRD = 1 # 製造レンズ
    PRD_KBN_BF = 2 # BF
    PRD_KBN_BASE = 3 # 基材
    PRD_KBN_PROC = 4 # 加工レンズ
    PRD_KBN_OTHER = 5 # その他

    # バーコード区分（BBCD_KBN）
    BBCD_KBN_BF = 1        # BF
    BBCD_KBN_COMMON = 2    # 一般
    BBCD_KBN_PACKING = 3   # 箱詰
    BBCD_KBN_INSPECTION = 4 # 検品

    # 製品マスタ文字数制限
    PRD_ID_MAX_LENGTH = 5      # 製品ID最大文字数
    PRD_NAME_MAX_LENGTH = 40   # 呼び名最大文字数
    PRD_MONOMER_MAX_LENGTH = 30 # モノマー最大文字数
    PRD_LOWER_DIE_MAX_LENGTH = 20 # 下型最大文字数
    PRD_UPPER_DIE_MAX_LENGTH = 20 # 上型最大文字数
    PRD_FILM_COLOR_MAX_LENGTH = 20 # 膜カラー最大文字数
    PRD_DSP_NM_MAX_LENGTH = 70 # 表示名最大文字数

    SHIPMENT_STATUS_LABELS = {
        BSHK_FLG_NOT_SHIPPED: "未出荷",
        BSHK_FLG_SHIPPED: "出荷済",
        BSHK_FLG_PROCESSED: "加工済",
        BSHK_FLG_DELIVERED: "納品済",
        BSHK_FLG_INVOICED: "請求済"
    }

# =============================================================================
# 区分マスタ関連定数
# =============================================================================
class KbnConstants:
    """区分マスタ関連の定数"""
    # 区分ID定数（実際に使用されているもののみ）
    KBN_ID_RANK = 'RANK'           # ランク区分
    KBN_ID_GSPEC = 'GSPEC'         # グラデーション仕様区分
    KBN_ID_GCOLOR = 'GCOLOR'       # グラデーション色区分
    
    # 有効フラグ
    KBN_FLG_ACTIVE = 0             # 有効
    KBN_FLG_INACTIVE = 1           # 無効
    
    # ランク名定数
    RANK_NAME_A = 'A'              # ランクA
    RANK_NAME_B = 'B'              # ランクB
    RANK_NAME_C = 'C'              # ランクC
    RANK_NAME_D = 'D'              # ランクD
    RANK_NAME_UNKNOWN = '不明'     # 不明ランク
    
    # 選択肢パターン定数
    CHOICE_PATTERN_NORMAL = 0      # 通常の選択肢（空の選択肢なし）
    CHOICE_PATTERN_ALL = 1         # 「全て」の選択肢を追加
    CHOICE_PATTERN_UNSELECTED = 2  # 「未選択」の選択肢を追加

# =============================================================================
# Excel関連定数
# =============================================================================
class ExcelConstants:
    """Excel処理関連の定数"""
    # シート名
    TEMPLATE_SHEET_NAME = '原紙'
    
    # データ書き込み設定
    WRITE_START_ROW = 5      # 書き込み開始行
    MAX_ROWS_PER_SHEET = 20  # 1シートあたりの最大行数
    
    # 列位置定義
    COLUMN_BASKET_NO = 'A'   # カゴ番号
    COLUMN_LOT = 'U'         # LOT
    COLUMN_BASE = 'B'        # ベース
    COLUMN_ADP = 'C'         # 加入度数
    COLUMN_LR = 'D'          # L/R
    COLUMN_COLOR = 'E'       # 色
    COLUMN_QUANTITY = 'F'    # 数量
    COLUMN_PASS_QTY = 'T'    # 合格数量
    COLUMN_BSHK_ID = 'V'     # 出荷ID
    COLUMN_COAT_DATE = 'G'   # コート日
    
    # ファイル名テンプレート
    HARDCOAT_FILENAME_TEMPLATE = 'ハードコート指図_{}.xlsx'

# =============================================================================
# CSV関連定数
# =============================================================================
class CsvConstants:
    """CSV処理関連の定数"""
    # ノンコートCSVの列数
    CSV_NON_COAT_COLUMN_COUNT = 75

# =============================================================================
# PDF関連定数
# =============================================================================
class PdfConstants:
    """PDF出力関連の定数"""
    # マージン設定（mm単位）
    MARGIN_TOP = 20
    MARGIN_BOTTOM = 20
    MARGIN_LEFT = 20
    MARGIN_RIGHT = 20
    
    # 列幅（mm単位）
    COL_WIDTH_SHIPMENT_DATE = 22
    COL_WIDTH_DESTINATION = 25
    COL_WIDTH_ORDER_NUMBER = 25
    COL_WIDTH_LOT = 18
    COL_WIDTH_BASE = 15
    COL_WIDTH_ADP = 15
    COL_WIDTH_LR = 10
    COL_WIDTH_COLOR = 10
    COL_WIDTH_QUANTITY = 15
    COL_WIDTH_COATING_DATE = 20
    COL_WIDTH_PROC_TYPE = 15
    
    # PDF出力用検索条件ラベル
    LABEL_BASE = 'ベース'
    LABEL_ADP = '加入度数'
    LABEL_LR = 'L/R'
    LABEL_COLOR = '色'
    LABEL_COATING = 'コーティング'
    LABEL_SHIPMENT_DATE = '出荷日'
    LABEL_DESTINATION = '出荷先'
    LABEL_ORDER_NO = '注文番号'
    LABEL_SHIPMENT_STATUS = '出荷状況'

# =============================================================================
# フォーム選択肢定数
# =============================================================================
class FormChoiceConstants:
    """フォームの選択肢定数"""
    # バーコード区分選択肢
    BBCD_KBN_CHOICES = [
        ('1', 'BF'),
        ('2', '一般'),
        ('3', '箱入'),
        ('4', '検品')
    ]
    
    # ベース選択肢
    BASE_CHOICES = [
        ('', '全て'),
        ('2', '2'),
        ('4', '4'),
        ('6', '6'),
        ('8', '8')
    ]
    
    # 加入度数選択肢
    ADP_CHOICES = [
        ('', '全て'),
        ('150', '150'),
        ('175', '175'),
        ('200', '200'),
        ('225', '225'),
        ('250', '250'),
        ('275', '275'),
        ('300', '300')
    ]
    
    # L/R選択肢
    LR_CHOICES = [
        ('', '全て'),
        ('L', 'L'),
        ('R', 'R')
    ]
    
    # 色選択肢
    COLOR_CHOICES = [
        ('', '全て'),
        ('BR', 'BR'),
        ('SG', 'SG')
    ]
    
    # 加工区分選択肢
    PROC_CHOICES = [
        ('', '全て'),
        ('0', 'ノンコート'),
        ('1', 'ハードコート')
    ]
    
    # コーティング選択肢
    COATING_CHOICES = [
        ('', '全て'),
        ('NC', 'NC'),
        ('HC', 'HC')
    ]
    
    # 受注残選択肢
    ORDER_REMAIN_CHOICES = [
        ('', '全て'),
        ('0', 'あり'),
        ('1', 'なし')
    ]
    
    # 出荷先選択肢
    SHIPMENT_STATUS_CHOICES = [
        ('', '全て'),
        ('0', '未出荷'),
        ('1', '出荷済'),
        ('2', '加工済'),
        ('3', '納品書発行済'),
        ('4', '請求書発行済')
    ]

# =============================================================================
# HTML表示用定数
# =============================================================================
class HtmlConstants:
    """HTMLテンプレートで使用する定数"""
    # 出荷ステータス表示用
    SHIPMENT_STATUS_DISPLAY = {
        DatabaseConstants.BSHK_FLG_NOT_SHIPPED: "未出荷",
        DatabaseConstants.BSHK_FLG_SHIPPED: "出荷済",
        DatabaseConstants.BSHK_FLG_PROCESSED: "加工済",
        DatabaseConstants.BSHK_FLG_DELIVERED: "納品済",
        DatabaseConstants.BSHK_FLG_INVOICED: "請求済"
    }
    
    # 加工区分表示用
    PROC_TYPE_DISPLAY = {
        DatabaseConstants.PROC_NON_COAT: "ノンコート",
        DatabaseConstants.PROC_HARD_COAT: "ハードコート"
    }
    
    # 委託先区分表示用
    CZTR_KBN_DISPLAY = {
        DatabaseConstants.CZTR_KBN_CUSTOMER: "得意先",
        DatabaseConstants.CZTR_KBN_PROCESS_COMPANY: "加工会社",
        DatabaseConstants.CZTR_KBN_OTHER: "その他"
    }
    
    # 出荷区分表示用
    CSHK_KBN_DISPLAY = {
        DatabaseConstants.CSHK_KBN_SHIPMENT: "出荷",
        DatabaseConstants.CSHK_KBN_PROCESS: "加工",
        DatabaseConstants.CSHK_KBN_LOSS: "欠損"
    }
    
    # 委託先タイプ表示用
    CZTR_TYPE_DISPLAY = {
        DatabaseConstants.CZTR_TYPE_COMMON: "一般",
        DatabaseConstants.CZTR_TYPE_BF: "BF一般兼用"
    }
    
    # 商品分類表示用
    PRD_KBN_DISPLAY = {
        DatabaseConstants.PRD_KBN_PRD: "製造レンズ",
        DatabaseConstants.PRD_KBN_BF: "BF",
        DatabaseConstants.PRD_KBN_BASE: "基材",
        DatabaseConstants.PRD_KBN_PROC: "加工レンズ",
        DatabaseConstants.PRD_KBN_OTHER: "その他"
    }
    
    # フラグ表示用
    FLAG_DISPLAY = {
        DatabaseConstants.FLG_ACTIVE: "有効",
        DatabaseConstants.FLG_INACTIVE: "無効"
    }
    
    # フラグ表示用（バッジ付き）
    FLAG_DISPLAY_BADGE = {
        DatabaseConstants.FLG_ACTIVE: '<span class="badge bg-success">有効</span>',
        DatabaseConstants.FLG_INACTIVE: '<span class="badge bg-secondary">無効</span>',
        DatabaseConstants.FLG_DELETED: '<span class="badge bg-danger">削除済</span>'
    }

# =============================================================================
# エラーメッセージ定数
# =============================================================================
class ErrorMessages:
    """エラーメッセージ定数"""
    # データベース関連エラー
    DB_SESSION_ERROR = "データベースセッションの作成に失敗しました"
    DB_CONFIG_INCOMPLETE = "データベース接続情報が不完全です。環境変数を確認してください。"
    
    # ファイル関連エラー
    FILE_NOT_FOUND = "テンプレートファイルが見つかりません。システム管理者に連絡してください。"
    FILE_PERMISSION_DENIED = "ファイルへのアクセス権限がありません。"
    FILE_INVALID_FORMAT = "テンプレートファイルのフォーマットが不正です。システム管理者に連絡してください。"
    FILE_CREATION_FAILED = "ファイルの作成に失敗しました。"
    
    # Excel関連エラー
    EXCEL_MEMORY_ERROR = "メモリ不足が発生しました。出力データを減らしてください。"
    EXCEL_NUMBER_CONVERSION_ERROR = "数値の変換に失敗しました。入力データを確認してください。"

# =============================================================================
# 成功メッセージ定数
# =============================================================================
class SuccessMessages:
    """成功メッセージ定数"""
    DATA_SAVED = "データが正常に保存されました。"
    FILE_CREATED = "ファイルが正常に作成されました。"
    EXCEL_EXPORTED = "Excelファイルが正常に出力されました。"
    PDF_EXPORTED = "PDFファイルが正常に出力されました。"
    DATA_IMPORTED = "データの取り込みが完了しました。"
    LOGIN_SUCCESS = "ログインしました。"
    LOGOUT_SUCCESS = "ログアウトしました。" 