import os
import sys
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from app.constants import ErrorMessages

# 環境変数を読み込む
if getattr(sys, 'frozen', False):
    base_dir = os.path.dirname(sys.executable)
    env_path = os.path.join(base_dir, '.env')
    load_dotenv(env_path)
else:
    load_dotenv()

# データベース接続設定
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

# 必要な設定が揃っているか確認
if not all([server, database, username, password]):
    raise ValueError(ErrorMessages.DB_CONFIG_INCOMPLETE)

# SQLAlchemy用のベースクラス
Base = declarative_base()

# SQLAlchemy接続エンジンの作成
connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"

engine = create_engine(
    connection_string,
    echo=False,
    pool_size=5, 
    max_overflow=10,
    pool_recycle=3600,
    connect_args={'charset': 'UTF-8', 'autocommit': False}
)

# セッションファクトリの作成
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def get_db_session():
    """データベースセッションを取得する"""
    try:
        session = Session()
        return session
    except Exception as e:
        logging.error(f"データベースセッション作成エラー: {str(e)}")
        Session.remove()
        raise Exception(f"{ErrorMessages.DB_SESSION_ERROR}: {str(e)}")

def get_db_connection():
    """従来のpymssql接続と互換性のある関数（非推奨）"""
    return engine.raw_connection() 