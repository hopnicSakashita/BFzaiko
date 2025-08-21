from sqlalchemy import text
import traceback
from app.database import get_db_session
from app.models import log_error

class BarcodeGenerator:
    """バーコード生成を担当するクラス"""
    
    @staticmethod
    def make_barcode_y(product_id, lot, coating_date):
        """バーコードY生成（MakeCode_Y相当）
        
        Args:
            product_id (str): 製品ID
            lot (str): ロット番号
            coating_date (str): コーティング日付
            
        Returns:
            str: 生成されたバーコード、またはエラーメッセージ
        """
        try:
            session = get_db_session()
            # バーコードマスタから値を取得
            result = session.execute(text("""
                SELECT 
                    BFSP_Y_BCD as bcd,
                    BFSP_Y_GTIN as gtin
                FROM BFSP_MST
                WHERE BFSP_PRD_ID = :product_id
            """), {'product_id': product_id}).first()
            
            if result:
                # バーコード生成
                return f"01{result.gtin}11{coating_date}42239210{lot}{coating_date}240{result.bcd}"
            return "該当品番なし"
        except Exception as e:
            tb = traceback.format_exc()
            log_error(f"バーコードY生成でエラーが発生しました: {str(e)}\n{tb}")
            return None
        finally:
            session.close()

    @staticmethod
    def make_barcode_s(product_id, date_str, coating_type):
        """バーコードS生成（MakeCode_S相当）
        
        Args:
            product_id (str): 製品ID
            date_str (str): 日付文字列（YYMMDD形式）
            coating_type (int): コーティングタイプ（1: HC, 0: NC）
            
        Returns:
            str: 生成されたバーコード、またはエラーメッセージ
        """
        try:
            session = get_db_session()
            # バーコードマスタから値を取得
            result = session.execute(text("""
                SELECT 
                    BFSP_S_NC as nc,
                    BFSP_S_HC as hc
                FROM BFSP_MST
                WHERE BFSP_PRD_ID = :product_id
            """), {'product_id': product_id}).first()
            
            if result:
                # 日付の分解
                year = date_str[0:2]
                month = int(date_str[2:4])
                day = date_str[4:6]
                
                # 月のアルファベット変換
                month_chars = 'ABCDEFGHIJKL'
                month_char = month_chars[month - 1] if 1 <= month <= 12 else ''
                
                # バーコード値の選択
                barcode_val = result.hc if int(coating_type) == 1 else result.nc
                
                # バーコード生成
                return f"10{day}{month_char}{year}240{barcode_val}"
            return "該当品番なし"
        except Exception as e:
            tb = traceback.format_exc()
            log_error(f"バーコードS生成でエラーが発生しました: {str(e)}\n{tb}")
            return None
        finally:
            session.close() 
            
    @staticmethod
    def make_barcode_s_shipment(barcode_val, date_str):
        """出荷用バーコードを生成する
        
        Args:
            barcode_val (str): バーコード値
            date_str (str): 日付文字列（YYMMDD形式）
            
        Returns:
            str: 生成されたバーコード、またはNone（エラー時）
        """
        try:
            try:
                # 日付の分解と検証
                year = int(date_str[0:2])
                month = int(date_str[2:4])
                day = int(date_str[4:6])
                
                if not (0 <= year <= 99 and 1 <= month <= 12 and 1 <= day <= 31):
                    log_error(f"バーコード生成エラー: 日付の値が範囲外です: {date_str}")
                    return None
                    
                # 月のアルファベット変換
                month_chars = 'ABCDEFGHIJKL'
                month_char = month_chars[month - 1]
                
                # バーコード生成
                return f"10{day:02d}{month_char}{year:02d}240{barcode_val}"
                
            except ValueError as e:
                log_error(f"バーコード生成エラー: 日付の数値変換に失敗: {str(e)}")
                return None
                
        except Exception as e:
            log_error(f"バーコード生成エラー: {str(e)}\n{traceback.format_exc()}")
            return None
