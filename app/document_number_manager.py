from datetime import datetime
import json
import os
import threading
from app.models import log_error

class DocumentNumberManager:
    """ドキュメント番号管理クラス"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """初期化処理"""
        self.json_path = os.path.join(os.path.dirname(__file__), 'data', 'document_numbers.json')
        # dataディレクトリが存在しない場合は作成
        os.makedirs(os.path.dirname(self.json_path), exist_ok=True)
        self._ensure_json_file()
    
    def _ensure_json_file(self):
        """JSONファイルの存在確認と初期化"""
        if not os.path.exists(self.json_path):
            default_data = {
                'process_request': 0,
                'shipment': 0,
                'stock': 0
            }
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(default_data, f, indent=2)
    
    def get_next_number(self, document_type):
        """次の番号を取得して更新する
        
        Args:
            document_type (str): ドキュメントタイプ ('process_request', 'shipment', 'stock')
            
        Returns:
            str: 生成された6桁の番号 (例: '000001')
            
        Raises:
            Exception: ファイル操作エラーなど
        """
        with self._lock:
            try:
                # JSONファイルを読み込む
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if document_type not in data:
                    data[document_type] = 0
                
                # 現在の番号を取得して増分
                current_number = data[document_type]
                next_number = current_number + 1
                
                # JSONファイルを更新
                data[document_type] = next_number
                with open(self.json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                
                # 6桁の連番
                return f'{next_number:06d}'
                
            except Exception as e:
                log_error(f"ドキュメント番号の生成に失敗しました: {str(e)}")
                raise
    
    def reset_numbers(self):
        """番号をリセットする（テスト用）"""
        with self._lock:
            self._ensure_json_file() 