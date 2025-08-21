from functools import wraps
from flask import session, redirect, url_for, flash
from app.database import get_db_session
from sqlalchemy import text
import traceback
from app.models import log_error
from werkzeug.security import check_password_hash

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('ログインが必要です。', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def verify_user(user_id, password):
    """ユーザー認証を行う"""
    session = get_db_session()
    try:
        # ユーザー情報を取得
        result = session.execute(
            text("SELECT USER_ID, USER_NM, USER_PW FROM USER_MST WHERE USER_ID = :user_id"),
            {"user_id": user_id}
        ).fetchone()
        
        if result and check_password_hash(result.USER_PW, password):
            return {
                'user_id': result.USER_ID,
                'user_name': result.USER_NM
            }
        return None
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"ユーザー認証でエラーが発生しました: {str(e)}\n{tb}")
        flash(f'認証エラー: {str(e)}', 'error')
        return None
    finally:
        session.close()

def get_user_info(user_id):
    """ユーザー情報を取得する"""
    session = get_db_session()
    try:
        result = session.execute(
            text("SELECT USER_ID, USER_NM FROM USER_MST WHERE USER_ID = :user_id"),
            {"user_id": user_id}
        ).fetchone()
        
        if result:
            return {
                'user_id': result.USER_ID,
                'user_name': result.USER_NM
            }
        return None
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"ユーザー情報取得でエラーが発生しました: {str(e)}\n{tb}")
        flash(f'ユーザー情報取得エラー: {str(e)}', 'error')
        return None
    finally:
        session.close() 