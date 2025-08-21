import os
from app import app
from app.constants import AppConstants

if __name__ == '__main__':
    # 環境変数PORTから取得、設定されていない場合はDEFAULT_PORTを使用
    port = int(os.environ.get('PORT', AppConstants.DEFAULT_PORT))
    app.run(debug=False, host='0.0.0.0', port=port) 