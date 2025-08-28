from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from app.database import get_db_session
from app.models import BrcpDat, PrdDat, log_error
from app.constants import DatabaseConstants, KbnConstants
import traceback

from app.models_common import CprdDatModel, CshkDatModel

class ShipmentCommon:

    @staticmethod
    def create_shipment(cpdd_id, qty, to, dt):
        """入庫データから出荷データを作成する"""
        session = get_db_session()
        try:
            from datetime import datetime
            
            # 入力値の検証
            if not all([cpdd_id, qty, to, dt]):
                raise ValueError('必須項目を入力してください。')
            
            try:
                cpdd_id = int(cpdd_id)
                qty = int(qty)
                to = int(to)
                # 日付文字列をdatetimeに変換
                if isinstance(dt, str):
                    dt = datetime.strptime(dt, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError('数値項目は正しい数値で入力してください。')
            
            # 桁数チェック
            if qty <= 0 or qty > DatabaseConstants.QUANTITY_MAX:
                raise ValueError(f'出荷数量は1から{DatabaseConstants.QUANTITY_MAX}の範囲で入力してください。')
            if to <= 0 or to > DatabaseConstants.SHIPMENT_TO_MAX:
                raise ValueError(f'出荷先IDは1から{DatabaseConstants.SHIPMENT_TO_MAX}の範囲で入力してください。')
            
            # 入庫データの存在確認
            cprd = session.query(CprdDatModel).filter(CprdDatModel.CPDD_ID == cpdd_id).first()
            if not cprd:
                raise ValueError('指定された入庫データが見つかりません。')
            
            # 在庫残数量のチェック
            zaiko_zan = CprdDatModel.get_zaiko_zan(cpdd_id)
            if qty > zaiko_zan:
                raise ValueError(f'出荷数量が在庫残数量（{zaiko_zan}）を超えています。')
            
            # 新規出荷データを作成
            new_cshk = CshkDatModel(
                CSHK_KBN=1,  # 出荷区分（1=通常出荷）
                CSHK_TO=to,
                CSHK_PRC_ID=None,  # 加工IDは空
                CSHK_PRD_ID=None,  # 製品IDは空（後で設定可能）
                CSHK_DT=dt,
                CSHK_PDD_ID=cpdd_id,  # 入庫IDを製造IDとして設定
                CSHK_RCP_ID=None,  # 受注IDは空
                CSHK_QTY=qty,
                CSHK_FLG=0  # フラグ（0=正常）
            )
            session.add(new_cshk)
            session.commit()
            
            return new_cshk.CSHK_ID

        except Exception as e:
            session.rollback()
            log_error(f"出荷データの登録中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_by_cpdd_id(cpdd_id):
        """指定された入庫IDの出荷データ一覧を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CSHK_ID,
                    CSHK_KBN,
                    CSHK_TO,
                    CSHK_PRC_ID,
                    CSHK_PRD_ID,
                    CSHK_DT,
                    CSHK_ORD_DT,
                    CSHK_PDD_ID,
                    CSHK_RCP_ID,
                    CSHK_QTY,
                    CSHK_FLG
                FROM CSHK_DAT
                WHERE CSHK_PDD_ID = :cpdd_id
                ORDER BY CSHK_ID DESC
            """)
            
            results = session.execute(sql, {'cpdd_id': cpdd_id}).fetchall()
            
            result = []
            for r in results:
                cshk = {
                    'CSHK_ID': r.CSHK_ID,
                    'CSHK_KBN': r.CSHK_KBN,
                    'CSHK_TO': r.CSHK_TO,
                    'CSHK_PRC_ID': r.CSHK_PRC_ID,
                    'CSHK_PRD_ID': r.CSHK_PRD_ID,
                    'CSHK_DT': r.CSHK_DT.strftime('%Y-%m-%d') if r.CSHK_DT else '',
                    'CSHK_ORD_DT': r.CSHK_ORD_DT.strftime('%Y-%m-%d') if r.CSHK_ORD_DT else '',
                    'CSHK_PDD_ID': r.CSHK_PDD_ID,
                    'CSHK_RCP_ID': r.CSHK_RCP_ID,
                    'CSHK_QTY': r.CSHK_QTY,
                    'CSHK_FLG': r.CSHK_FLG
                }
                result.append(cshk)
            return result
            
        except Exception as e:
            log_error(f"出荷データ一覧の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_by_cpdd_id_with_details(cpdd_id):
        """指定された入庫IDの出荷データ一覧を詳細情報付きで取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    c.CSHK_ID,
                    c.CSHK_KBN,
                    c.CSHK_TO,
                    c.CSHK_PRC_ID,
                    c.CSHK_PRD_ID,
                    c.CSHK_DT,
                    c.CSHK_ORD_DT,
                    c.CSHK_PDD_ID,
                    c.CSHK_RCP_ID,
                    c.CSHK_QTY,
                    c.CSHK_FLG,
                    z.CZTR_NM,
                    p.CPRC_NM
                FROM CSHK_DAT c
                LEFT JOIN CZTR_MST z ON c.CSHK_TO = z.CZTR_ID
                LEFT JOIN CPRC_MST p ON c.CSHK_PRC_ID = p.CPRC_ID
                WHERE c.CSHK_PDD_ID = :cpdd_id
                ORDER BY c.CSHK_ID DESC
            """)
            
            results = session.execute(sql, {'cpdd_id': cpdd_id}).fetchall()
            
            result = []
            for r in results:
                cshk = {
                    'CSHK_ID': r.CSHK_ID,
                    'CSHK_KBN': r.CSHK_KBN,
                    'CSHK_TO': r.CSHK_TO,
                    'CSHK_PRC_ID': r.CSHK_PRC_ID,
                    'CSHK_PRD_ID': r.CSHK_PRD_ID,
                    'CSHK_DT': r.CSHK_DT.strftime('%Y-%m-%d') if r.CSHK_DT else '',
                    'CSHK_ORD_DT': r.CSHK_ORD_DT.strftime('%Y-%m-%d') if r.CSHK_ORD_DT else '',
                    'CSHK_PDD_ID': r.CSHK_PDD_ID,
                    'CSHK_RCP_ID': r.CSHK_RCP_ID,
                    'CSHK_QTY': r.CSHK_QTY,
                    'CSHK_FLG': r.CSHK_FLG,
                    'CZTR_NM': r.CZTR_NM,
                    'CPRC_NM': r.CPRC_NM
                }
                result.append(cshk)
            return result
            
        except Exception as e:
            log_error(f"出荷データ詳細一覧の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def update_shipment(cshk_id, cshk_kbn, qty, cshk_dt, cshk_ord_dt, cshk_to, cshk_prc_id):
        """出荷データを更新する（SQL直接実行）"""
        session = get_db_session()
        try:
            # 基本バリデーション
            if not all([cshk_id, cshk_kbn is not None, qty, cshk_dt]):
                error_msg = '必須項目を入力してください。'
                log_error(f"基本バリデーションエラー: {error_msg}")
                raise ValueError(error_msg)
            
            # 既存データの存在確認と情報取得
            log_error(f"既存データ取得開始: CSHK_ID={cshk_id}")
            check_sql = "SELECT CSHK_PDD_ID, CSHK_QTY FROM CSHK_DAT WHERE CSHK_ID = :cshk_id"
            result = session.execute(text(check_sql), {'cshk_id': cshk_id}).fetchone()
            
            if not result:
                error_msg = '指定された出荷データが見つかりません。'
                log_error(f"データ存在エラー: {error_msg} (ID={cshk_id})")
                raise ValueError(error_msg)
            
            cpdd_id = result.CSHK_PDD_ID
            existing_qty = int(result.CSHK_QTY) if result.CSHK_QTY is not None else 0
            log_error(f"既存データ確認完了: 入庫ID={cpdd_id}, 既存数量={existing_qty}")
            
            # 在庫残数量のチェック
            current_zaiko_zan = CprdDatModel.get_zaiko_zan(cpdd_id)
            current_zaiko_zan_int = int(current_zaiko_zan) if current_zaiko_zan is not None else 0
            available_qty = current_zaiko_zan_int + existing_qty  # 既存の出荷数量を戻す
            
            log_error(f"在庫チェック: 現在残数={current_zaiko_zan_int}, 既存出荷数={existing_qty}, 利用可能数={available_qty}, 新数量={qty}")
            
            if qty > available_qty:
                error_msg = f'出荷数量が在庫残数量（{available_qty}）を超えています。'
                log_error(f"在庫不足エラー: {error_msg}")
                raise ValueError(error_msg)
            
            # SQLで直接更新
            log_error("SQL直接更新開始")
            update_sql = """
                UPDATE CSHK_DAT SET 
                    CSHK_KBN = :cshk_kbn,
                    CSHK_QTY = :qty,
                    CSHK_DT = :cshk_dt,
                    CSHK_ORD_DT = :cshk_ord_dt,
                    CSHK_TO = :cshk_to,
                    CSHK_PRC_ID = :cshk_prc_id
                WHERE CSHK_ID = :cshk_id
            """
            
            params = {
                'cshk_id': cshk_id,
                'cshk_kbn': cshk_kbn,
                'qty': qty,
                'cshk_dt': cshk_dt,
                'cshk_ord_dt': cshk_ord_dt,
                'cshk_to': cshk_to,
                'cshk_prc_id': cshk_prc_id
            }
            
            log_error(f"SQLパラメータ: {params}")
            
            session.execute(text(update_sql), params)
            session.commit()
            log_error("SQLによるデータベース更新・コミット完了")
            
            return True

        except Exception as e:
            session.rollback()
            log_error(f"出荷データの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def delete_shipment(cshk_id):
        """出荷データを削除する"""
        session = get_db_session()
        try:
            # 既存データを取得
            cshk = session.query(CshkDatModel).filter(CshkDatModel.CSHK_ID == cshk_id).first()
            if not cshk:
                raise ValueError('指定された出荷データが見つかりません。')
            
            # フラグが0以外の場合は削除不可
            if cshk.CSHK_FLG != 0:
                raise ValueError('このデータはフラグが0以外のため、削除できません。')
            
            # データを削除
            session.delete(cshk)
            session.commit()
            
            return True

        except Exception as e:
            session.rollback()
            log_error(f"出荷データの削除中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def create_shipment_new(cpdd_id, cshk_kbn, qty, cshk_dt, cshk_ord_dt, cshk_to=None, cshk_prc_id=None):
        """新仕様での出荷データ作成"""
        session = get_db_session()
        try:
            from datetime import datetime
            from app.constants import DatabaseConstants
            
            # 入力値の検証
            if not all([cpdd_id, cshk_kbn is not None, qty]):
                raise ValueError('必須項目を入力してください。')
            
            try:
                cpdd_id = int(cpdd_id)
                cshk_kbn = int(cshk_kbn)
                qty = int(qty)
                if cshk_to is not None:
                    cshk_to = int(cshk_to)
                if cshk_prc_id is not None:
                    cshk_prc_id = int(cshk_prc_id)
                
                # 日付文字列をdatetimeに変換
                if isinstance(cshk_dt, str):
                    cshk_dt = datetime.strptime(cshk_dt, '%Y-%m-%d')
                if isinstance(cshk_ord_dt, str):
                    cshk_ord_dt = datetime.strptime(cshk_ord_dt, '%Y-%m-%d')
                    
            except ValueError:
                raise ValueError('数値項目は正しい数値で入力してください。')
            
            # 桁数チェック
            if qty <= 0 or qty > DatabaseConstants.QUANTITY_MAX:
                raise ValueError(f'出荷数量は1から{DatabaseConstants.QUANTITY_MAX}の範囲で入力してください。')
            
            # 出荷区分の検証
            if cshk_kbn not in [DatabaseConstants.CSHK_KBN_SHIPMENT, 
                               DatabaseConstants.CSHK_KBN_PROCESS, 
                               DatabaseConstants.CSHK_KBN_LOSS]:
                raise ValueError('出荷区分が無効です。')
            
            # 入庫データの存在確認と製品IDの取得
            cprd = session.query(CprdDatModel).filter(CprdDatModel.CPDD_ID == cpdd_id).first()
            if not cprd:
                raise ValueError('指定された入庫データが見つかりません。')
            
            # 在庫残数量のチェック
            zaiko_zan = CprdDatModel.get_zaiko_zan(cpdd_id)
            if qty > zaiko_zan:
                raise ValueError(f'出荷数量が在庫残数量（{zaiko_zan}）を超えています。')
            
            # 出荷区分別の検証
            if cshk_kbn == DatabaseConstants.CSHK_KBN_SHIPMENT:  # 出荷
                if not cshk_to:
                    raise ValueError('出荷先を指定してください。')
                if cshk_to <= 0 or cshk_to > DatabaseConstants.SHIPMENT_TO_MAX:
                    raise ValueError(f'出荷先IDは1から{DatabaseConstants.SHIPMENT_TO_MAX}の範囲で入力してください。')
            elif cshk_kbn == DatabaseConstants.CSHK_KBN_PROCESS:  # 加工
                if not cshk_prc_id:
                    raise ValueError('加工IDを指定してください。')
                if cshk_prc_id <= 0 or cshk_prc_id > DatabaseConstants.PROCESS_ID_MAX:
                    raise ValueError(f'加工IDは1から{DatabaseConstants.PROCESS_ID_MAX}の範囲で入力してください。')
            
            # 新規出荷データを作成
            new_cshk = CshkDatModel(
                CSHK_KBN=Decimal(str(cshk_kbn)),
                CSHK_TO=Decimal(str(cshk_to)) if cshk_to is not None else None,
                CSHK_PRC_ID=Decimal(str(cshk_prc_id)) if cshk_prc_id is not None else None,
                CSHK_PRD_ID=cprd.CPDD_PRD_ID,  # 入庫データの製品IDを設定
                CSHK_DT=cshk_dt,
                CSHK_ORD_DT=cshk_ord_dt,  # 注文日を設定
                CSHK_PDD_ID=Decimal(str(cpdd_id)),  # 入庫IDを設定
                CSHK_RCP_ID=None,  # 受注IDは空
                CSHK_QTY=Decimal(str(qty)),
                CSHK_FLG=Decimal('0')  # フラグ（0=正常）
            )
            
            session.add(new_cshk)
            session.commit()
            
            return new_cshk.CSHK_ID

        except Exception as e:
            session.rollback()
            log_error(f"出荷データの登録中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_process_request_list(date_from=None, date_to=None, prd_id=None, prc_id=None, cztr_id=None, return_status=None):
        """加工依頼一覧を取得する（CSHK_KBN=1のデータ）- 画面表示用（CSHK_ID含む）"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    c.CSHK_ID,
                    c.CSHK_KBN,
                    c.CSHK_TO,
                    c.CSHK_PRC_ID,
                    c.CSHK_PRD_ID,
                    c.CSHK_DT,
                    c.CSHK_ORD_DT,
                    cprd.CPDD_LOT,
                    c.CSHK_QTY,
                    c.CSHK_FLG,
                    cprd.CPDD_PRD_ID,
                    cm.CPRC_NM,
                    cm.CPRC_PRD_NM,
                    cztr.CZTR_NM as PRC_TO_NAME,
                    cztr.CZTR_FULL_NM as PRC_TO_FULL_NM,
                    cztr.CZTR_TANTO_NM as PRC_TO_TANTO_NM,
                    dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) as PRC_ZAN_QTY
                FROM CSHK_DAT c
                LEFT JOIN CPRD_DAT cprd ON c.CSHK_PDD_ID = cprd.CPDD_ID
                LEFT JOIN CPRC_MST cm ON c.CSHK_PRC_ID = cm.CPRC_ID
                LEFT JOIN CZTR_MST cztr ON cm.CPRC_TO = cztr.CZTR_ID
                WHERE c.CSHK_KBN = :cshk_kbn
            """
            
            params = {
                'cshk_kbn': DatabaseConstants.CSHK_KBN_PROCESS
            }
            
            # 検索条件を追加
            if date_from:
                sql += " AND c.CSHK_DT >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND c.CSHK_DT <= :date_to"
                params['date_to'] = date_to
            if prd_id:
                sql += " AND cprd.CPDD_PRD_ID LIKE :prd_id"
                params['prd_id'] = f'%{prd_id}%'
            if prc_id:
                sql += " AND c.CSHK_PRC_ID = :prc_id"
                params['prc_id'] = prc_id
            if cztr_id:
                sql += " AND cztr.CZTR_ID = :cztr_id"
                params['cztr_id'] = cztr_id
            
            # 戻り残数の検索条件を追加
            if return_status is not None:
                if return_status == 1:  # 戻り残数あり
                    sql += " AND dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) > 0"
                elif return_status == 0:  # 戻り残数なし
                    sql += " AND (dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) = 0 OR dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) IS NULL)"
            
            # 並び順を設定
            sql += " ORDER BY c.CSHK_DT DESC, c.CSHK_ID DESC"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                process_request = {
                    'CSHK_ID': r.CSHK_ID,
                    'CSHK_PRD_ID': r.CSHK_PRD_ID,
                    'CSHK_DT': r.CSHK_DT.strftime('%Y-%m-%d') if r.CSHK_DT else '',
                    'CSHK_QTY': r.CSHK_QTY,
                    'CSHK_FLG': r.CSHK_FLG,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPRC_NM': r.CPRC_NM,
                    'CPRC_PRD_NM': r.CPRC_PRD_NM,
                    'PRC_NAME': r.CPRC_NM,  # 画面で使用されている項目名
                    'PRC_TO_NAME': r.PRC_TO_NAME,
                    'PRC_TO_FULL_NM': r.PRC_TO_FULL_NM,
                    'PRC_TO_TANTO_NM': r.PRC_TO_TANTO_NM,
                    'PRC_ZAN_QTY': r.PRC_ZAN_QTY or 0
                }
                result.append(process_request)
            return result
            
        except Exception as e:
            log_error(f"加工依頼一覧の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_process_request_list_for_pdf(date_from=None, date_to=None, prd_id=None, prc_id=None, cztr_id=None, return_status=None):
        """加工依頼一覧を取得する（CSHK_KBN=1のデータ）- PDF出力用（GROUP BY使用）"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    c.CSHK_KBN,
                    c.CSHK_TO,
                    c.CSHK_PRC_ID,
                    c.CSHK_PRD_ID,
                    c.CSHK_DT,
                    c.CSHK_ORD_DT,
                    cprd.CPDD_LOT,
                    SUM(c.CSHK_QTY) as CSHK_QTY,
                    MIN(c.CSHK_FLG) as CSHK_FLG,
                    MIN(cprd.CPDD_PRD_ID) as CPDD_PRD_ID,
                    MIN(cm.CPRC_NM) as CPRC_NM,
                    MIN(cm.CPRC_PRD_NM) as CPRC_PRD_NM,
                    MIN(cztr.CZTR_NM) as PRC_TO_NAME,
                    MIN(cztr.CZTR_FULL_NM) as PRC_TO_FULL_NM,
                    MIN(cztr.CZTR_TANTO_NM) as PRC_TO_TANTO_NM,
                    SUM(dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID)) as PRC_ZAN_QTY
                FROM CSHK_DAT c
                LEFT JOIN CPRD_DAT cprd ON c.CSHK_PDD_ID = cprd.CPDD_ID
                LEFT JOIN CPRC_MST cm ON c.CSHK_PRC_ID = cm.CPRC_ID
                LEFT JOIN CZTR_MST cztr ON cm.CPRC_TO = cztr.CZTR_ID
                WHERE c.CSHK_KBN = :cshk_kbn
            """
            
            params = {
                'cshk_kbn': DatabaseConstants.CSHK_KBN_PROCESS
            }
            
            # 検索条件を追加
            if date_from:
                sql += " AND c.CSHK_DT >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND c.CSHK_DT <= :date_to"
                params['date_to'] = date_to
            if prd_id:
                sql += " AND cprd.CPDD_PRD_ID LIKE :prd_id"
                params['prd_id'] = f'%{prd_id}%'
            if prc_id:
                sql += " AND c.CSHK_PRC_ID = :prc_id"
                params['prc_id'] = prc_id
            if cztr_id:
                sql += " AND cztr.CZTR_ID = :cztr_id"
                params['cztr_id'] = cztr_id
            
            # 戻り残数の検索条件を追加
            if return_status is not None:
                if return_status == 1:  # 戻り残数あり
                    sql += " AND dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) > 0"
                elif return_status == 0:  # 戻り残数なし
                    sql += " AND (dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) = 0 OR dbo.Get_CSHK_PRC_ZAN_Qty(c.CSHK_ID) IS NULL)"
            
            # GROUP BYを検索条件の後に配置
            sql += " GROUP BY c.CSHK_KBN, c.CSHK_TO, c.CSHK_PRC_ID, c.CSHK_PRD_ID, c.CSHK_DT, c.CSHK_ORD_DT, cprd.CPDD_LOT"
            
            # 並び順を設定
            sql += " ORDER BY c.CSHK_DT DESC, c.CSHK_PRD_ID"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                process_request = {
                    'CSHK_PRD_ID': r.CSHK_PRD_ID,
                    'CSHK_DT': r.CSHK_DT.strftime('%Y-%m-%d') if r.CSHK_DT else '',
                    'CSHK_QTY': r.CSHK_QTY,
                    'CSHK_FLG': r.CSHK_FLG,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPRC_NM': r.CPRC_NM,
                    'CPRC_PRD_NM': r.CPRC_PRD_NM,
                    'PRC_NAME': r.CPRC_NM,  # 画面で使用されている項目名
                    'PRC_TO_NAME': r.PRC_TO_NAME,
                    'PRC_TO_FULL_NM': r.PRC_TO_FULL_NM,
                    'PRC_TO_TANTO_NM': r.PRC_TO_TANTO_NM,
                    'PRC_ZAN_QTY': r.PRC_ZAN_QTY or 0
                }
                result.append(process_request)
            return result
            
        except Exception as e:
            log_error(f"加工依頼一覧の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_by_cshk_id_with_details(cshk_id):
        """指定された出荷IDの出荷データを詳細情報付きで取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    c.CSHK_ID,
                    c.CSHK_KBN,
                    c.CSHK_TO,
                    c.CSHK_PRC_ID,
                    c.CSHK_PRD_ID,
                    c.CSHK_DT,
                    c.CSHK_ORD_DT,
                    c.CSHK_PDD_ID,
                    c.CSHK_RCP_ID,
                    c.CSHK_QTY,
                    c.CSHK_FLG,
                    z.CZTR_NM,
                    p.CPRC_NM,
                    prd_mst.PRD_DSP_NM,
                    prd.CPDD_LOT,
                    r.KBN_NM as RANK_NAME
                FROM CSHK_DAT c
                LEFT JOIN CZTR_MST z ON c.CSHK_TO = z.CZTR_ID
                LEFT JOIN CPRC_MST p ON c.CSHK_PRC_ID = p.CPRC_ID
                LEFT JOIN CPRD_DAT prd ON c.CSHK_PDD_ID = prd.CPDD_ID
                LEFT JOIN PRD_MST prd_mst ON prd.CPDD_PRD_ID = prd_mst.PRD_ID
                LEFT JOIN KBN_MST r ON r.KBN_ID = :rank_kbn_id AND r.KBN_NO = prd.CPDD_RANK AND r.KBN_FLG = :kbn_flg_active
                WHERE c.CSHK_ID = :cshk_id
            """)
            
            params = {
                'cshk_id': cshk_id,
                'rank_kbn_id': KbnConstants.KBN_ID_RANK,
                'kbn_flg_active': KbnConstants.KBN_FLG_ACTIVE
            }
            
            result = session.execute(sql, params).fetchone()
            
            if result:
                cshk = {
                    'CSHK_ID': result.CSHK_ID,
                    'CSHK_KBN': result.CSHK_KBN,
                    'CSHK_TO': result.CSHK_TO,
                    'CSHK_PRC_ID': result.CSHK_PRC_ID,
                    'CSHK_PRD_ID': result.CSHK_PRD_ID,
                    'CSHK_DT': result.CSHK_DT.strftime('%Y-%m-%d') if result.CSHK_DT else '',
                    'CSHK_ORD_DT': result.CSHK_ORD_DT.strftime('%Y-%m-%d') if result.CSHK_ORD_DT else '',
                    'CSHK_PDD_ID': result.CSHK_PDD_ID,
                    'CSHK_RCP_ID': result.CSHK_RCP_ID,
                    'CSHK_QTY': result.CSHK_QTY,
                    'CSHK_FLG': result.CSHK_FLG,
                    'CZTR_NM': result.CZTR_NM,
                    'CPRC_NM': result.CPRC_NM,
                    'PRD_DSP_NM': result.PRD_DSP_NM,
                    'CPDD_LOT': result.CPDD_LOT,
                    'RANK_NAME': result.RANK_NAME
                }
                return cshk
            else:
                return None
            
        except Exception as e:
            log_error(f"出荷データ詳細の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_prc_zan_qty(cshk_id):
        """指定された出荷IDの加工から戻ってきていない残数を取得する"""
        session = get_db_session()
        try:
            sql = text("SELECT dbo.Get_CSHK_PRC_ZAN_Qty(:cshk_id) as PRC_ZAN_QTY")
            result = session.execute(sql, {'cshk_id': cshk_id}).fetchone()
            
            if result:
                return result.PRC_ZAN_QTY or 0
            else:
                return 0
                
        except Exception as e:
            log_error(f"加工残数の取得中にエラーが発生: {str(e)}")
            return 0
        finally:
            session.close()
    
    @staticmethod
    def delete_shipment(cshk_id):
        """出荷データを削除する（出荷区分または加工区分で加工依頼残が0の場合のみ）"""
        session = get_db_session()
        try:
            # 既存データを取得
            cshk = session.query(CshkDatModel).filter(CshkDatModel.CSHK_ID == cshk_id).first()
            if not cshk:
                raise ValueError('指定された出荷データが見つかりません。')
            
            # フラグが0以外の場合は削除不可
            if cshk.CSHK_FLG != DatabaseConstants.FLG_ACTIVE:
                raise ValueError('このデータはフラグが0以外のため、削除できません。')
            
            # 出荷区分の場合
            if cshk.CSHK_KBN == DatabaseConstants.CSHK_KBN_SHIPMENT:
                # 出荷区分の場合は削除可能
                pass
            # 加工区分の場合
            elif cshk.CSHK_KBN == DatabaseConstants.CSHK_KBN_PROCESS:
                # 加工依頼数と加工依頼残をチェック
                prc_zan_qty = ShipmentCommon.get_prc_zan_qty(cshk_id)
                if prc_zan_qty != cshk.CSHK_QTY:
                    raise ValueError(f'加工依頼数({cshk.CSHK_QTY})と加工依頼残数({prc_zan_qty})が一致していないため、削除できません。')
            # その他の区分の場合
            else:
                raise ValueError('出荷区分または加工区分でないデータは削除できません。')
            
            # データを削除
            session.delete(cshk)
            session.commit()
            
            return True

        except Exception as e:
            session.rollback()
            log_error(f"出荷データの削除中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_shipment_list(date_from=None, date_to=None, ord_date_from=None, ord_date_to=None, cshk_to=None):
        """出荷一覧を取得する（CSHK_KBN=0のデータ）"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    c.CSHK_ID,
                    c.CSHK_KBN,
                    c.CSHK_TO,
                    c.CSHK_PRC_ID,
                    c.CSHK_PRD_ID,
                    c.CSHK_DT,
                    c.CSHK_ORD_DT,
                    c.CSHK_PDD_ID,
                    c.CSHK_RCP_ID,
                    c.CSHK_QTY,
                    c.CSHK_FLG,
                    cprd.CPDD_LOT,
                    cprd.CPDD_SPRIT1,
                    cprd.CPDD_SPRIT2,
                    cprd.CPDD_RANK,
                    cprd.CPDD_QTY as CPDD_QTY,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME,
                    cztr.CZTR_NM as SHIPMENT_TO_NAME
                FROM CSHK_DAT c
                LEFT JOIN CPRD_DAT cprd ON c.CSHK_PDD_ID = cprd.CPDD_ID
                LEFT JOIN PRD_MST p ON cprd.CPDD_PRD_ID = p.PRD_ID
                LEFT JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id 
                                   AND k.KBN_NO = cprd.CPDD_RANK 
                                   AND k.KBN_FLG = 0
                LEFT JOIN CZTR_MST cztr ON c.CSHK_TO = cztr.CZTR_ID
                WHERE c.CSHK_KBN = :cshk_kbn
            """
            
            params = {
                'rank_kbn_id': KbnConstants.KBN_ID_RANK,
                'cshk_kbn': DatabaseConstants.CSHK_KBN_SHIPMENT
            }
            
            # 検索条件を追加
            if date_from:
                sql += " AND c.CSHK_DT >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND c.CSHK_DT <= :date_to"
                params['date_to'] = date_to
            if ord_date_from:
                sql += " AND c.CSHK_ORD_DT >= :ord_date_from"
                params['ord_date_from'] = ord_date_from
            if ord_date_to:
                sql += " AND c.CSHK_ORD_DT <= :ord_date_to"
                params['ord_date_to'] = ord_date_to
            if cshk_to:
                sql += " AND c.CSHK_TO = :cshk_to"
                params['cshk_to'] = cshk_to
            
            # 並び順を設定
            sql += " ORDER BY c.CSHK_DT DESC, c.CSHK_ID DESC"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                shipment = {
                    'CSHK_ID': r.CSHK_ID,
                    'CSHK_KBN': r.CSHK_KBN,
                    'CSHK_TO': r.CSHK_TO,
                    'CSHK_PRC_ID': r.CSHK_PRC_ID,
                    'CSHK_PRD_ID': r.CSHK_PRD_ID,
                    'CSHK_DT': r.CSHK_DT.strftime('%Y-%m-%d') if r.CSHK_DT else '',
                    'CSHK_ORD_DT': r.CSHK_ORD_DT.strftime('%Y-%m-%d') if r.CSHK_ORD_DT else '',
                    'CSHK_PDD_ID': r.CSHK_PDD_ID,
                    'CSHK_RCP_ID': r.CSHK_RCP_ID,
                    'CSHK_QTY': r.CSHK_QTY,
                    'CSHK_FLG': r.CSHK_FLG,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME,
                    'SHIPMENT_TO_NAME': r.SHIPMENT_TO_NAME
                }
                result.append(shipment)
            return result
            
        except Exception as e:
            log_error(f"出荷一覧の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()

