from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
from app.auth_routes import auth
from app.constants import AppConstants
from app.logger_utils import log_error, log_info
from app.constants import DatabaseConstants, KbnConstants, FormChoiceConstants, AppConstants, HtmlConstants

# テンプレートフォルダのパスを明示的に指定
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'static'))

app = Flask(__name__,
            template_folder='templates',  # テンプレートフォルダの位置を指定
            static_folder='static')       # 静的ファイルフォルダの位置を指定

# デバッグモードを有効化
app.config['DEBUG'] = True

# シークレットキーを設定（環境変数SECRET_KEYから取得、設定されていない場合はDEFAULT_SECRET_KEYを使用）
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', AppConstants.DEFAULT_SECRET_KEY)

# アップロードフォルダの設定
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp')

# アップロードフォルダが存在しない場合は作成
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# グローバルテンプレート関数を追加
@app.context_processor
def inject_constants():
    """HTMLテンプレートで定数を直接使用できるようにする"""
    return {
        'DatabaseConstants': DatabaseConstants,
        'KbnConstants': KbnConstants,
        'FormChoiceConstants': FormChoiceConstants,
        'AppConstants': AppConstants,
        'HtmlConstants': HtmlConstants
    }

# 認証Blueprintを登録
app.register_blueprint(auth)

# 一般Blueprintを登録
from app.routes_common import common_bp
app.register_blueprint(common_bp)

# 加工集計Blueprintを登録
from app.routes_total import total_bp
app.register_blueprint(total_bp)

# ルートのインポート
from app import routes
from app import routes_gradation
from app import routes_master

