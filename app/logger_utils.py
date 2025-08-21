import logging
import traceback
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True  # 既存の設定を強制的に上書き
)

def log_error(error_message, exception=None, include_traceback=True):
    """エラーメッセージをログに記録する
    
    Args:
        error_message (str): エラーメッセージ
        exception (Exception, optional): 例外オブジェクト
        include_traceback (bool): トレースバックを含めるかどうか
    """
    if exception:
        if include_traceback:
            logging.error(f"{error_message}: {str(exception)}\n{traceback.format_exc()}")
        else:
            logging.error(f"{error_message}: {str(exception)}")
    else:
        logging.error(error_message)

def log_info(info_message):
    """情報メッセージをログに記録する"""
    logging.info(info_message)

def log_debug(debug_message):
    """デバッグメッセージをログに記録する"""
    logging.debug(debug_message)

def handle_database_error(exception, operation_name="データベース操作"):
    """データベースエラーを処理し、適切なエラーメッセージを返す
    
    Args:
        exception (Exception): 発生した例外
        operation_name (str): 実行していた操作の名前
        
    Returns:
        str: ユーザー向けエラーメッセージ
    """
    if isinstance(exception, OperationalError):
        error_msg = f"{operation_name}でデータベースへの接続に失敗しました。システム管理者に連絡してください。"
        log_error(f"{operation_name} - データベース接続エラー", exception)
    elif isinstance(exception, SQLAlchemyError):
        error_msg = f"{operation_name}でデータベースの操作中にエラーが発生しました。"
        log_error(f"{operation_name} - SQLエラー", exception)
    else:
        error_msg = f"{operation_name}で予期せぬエラーが発生しました。"
        log_error(f"{operation_name} - 予期せぬエラー", exception)
    
    return error_msg

def handle_value_error(exception, operation_name="データ処理"):
    """ValueErrorを処理し、適切なエラーメッセージを返す
    
    Args:
        exception (ValueError): 発生したValueError
        operation_name (str): 実行していた操作の名前
        
    Returns:
        str: ユーザー向けエラーメッセージ
    """
    error_msg = str(exception)
    log_error(f"{operation_name} - バリデーションエラー", exception, include_traceback=False)
    return error_msg 