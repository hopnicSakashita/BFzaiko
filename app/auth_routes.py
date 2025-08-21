from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from app.auth import verify_user, login_required
import traceback
from app.models import log_error

auth = Blueprint('auth', __name__, url_prefix='/auth')

@auth.route('/login', methods=['GET', 'POST'])
def login():
    """ログイン画面を表示し、ログイン処理を行う"""
    # ログイン済みの場合はトップページにリダイレクト
    if 'user_id' in session:
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        try:
            user_id = request.form.get('user_id')
            password = request.form.get('password')
            
            if not user_id or not password:
                flash('ユーザーIDとパスワードを入力してください。', 'error')
                return render_template('login.html')
            
            user = verify_user(user_id, password)
            if user:
                session['user_id'] = user['user_id']
                session['user_name'] = user['user_name']
                flash('ログインしました。', 'success')
                return redirect(url_for('index'))
            else:
                flash('ユーザーIDまたはパスワードが正しくありません。', 'error')
        except Exception as e:
            tb = traceback.format_exc()
            log_error(f"ログイン処理でエラーが発生しました: {str(e)}\n{tb}")
            flash(f'ログイン処理でエラーが発生しました: {str(e)}', 'error')
    
    return render_template('login.html')

@auth.route('/logout')
@login_required
def logout():
    """ログアウト処理を行う"""
    try:
        session.clear()
        flash('ログアウトしました。', 'success')
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"ログアウト処理でエラーが発生しました: {str(e)}\n{tb}")
        flash(f'ログアウト処理でエラーが発生しました: {str(e)}', 'error')
    
    return redirect(url_for('auth.login')) 