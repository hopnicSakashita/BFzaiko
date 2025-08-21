from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from app.database import get_db_session
from app.models import BrcpDat, PrdDat, log_error
from app.constants import DatabaseConstants
import traceback

class Shipment:
    """出荷データのモデルクラス"""
    
    @staticmethod
    def search(base=None, adp=None, lr=None, color=None, proc_type=None, 
               shipment_date=None, destination=None, order_no=None, shipment_status=None, order_date=None   ):
        """出荷データを検索する"""
        session = get_db_session()
        try:
            # デバッグログ
            print(f"Shipment.search: base={base}, adp={adp}, lr={lr}, color={color}, proc_type={proc_type}, shipment_date={shipment_date}, destination={destination}")
            
            # 出荷データを検索
            query = """
                SELECT 
                    CAST(BSHK.BSHK_ID as VARCHAR) as id,
                    FORMAT(BSHK.BSHK_DT, 'yy/MM/dd') as shipment_date,
                    CZTR.CZTR_NM as destination,
                    BRCP.BRCP_ORDER_NO as order_number,
                    BPRD.BPDD_LOT as lot,
                    BFSP.BFSP_BASE as base,
                    BFSP.BFSP_ADP as adp,
                    BFSP.BFSP_LR as lr,
                    BFSP.BFSP_CLR as color,
                    BSHK.BSHK_QTY as quantity,
                    CONVERT(VARCHAR(10), BPRD.BPDD_CRT, 120) as coating_date,
                    FORMAT(BSHK.BSHK_ORD_DT, 'yy/MM/dd') as order_date,
                    CASE 
                        WHEN BRCP.BRCP_PROC = :proc_noncoat THEN 'NC'
                        WHEN BRCP.BRCP_PROC = :proc_hardcoat THEN 'HC'
                        ELSE ''
                    END as proc_type,
                    BSHK.BSHK_FLG as shipment_status_code
                FROM BSHK_DAT AS BSHK
                INNER JOIN BPRD_DAT AS BPRD
                    ON BSHK.BSHK_PDD_ID = BPRD.BPDD_ID
                INNER JOIN BFSP_MST AS BFSP
                    ON BPRD.BPDD_PRD_ID = BFSP.BFSP_PRD_ID
                LEFT OUTER JOIN BRCP_DAT AS BRCP
                    ON BSHK.BSHK_RCP_ID = BRCP.BRCP_ID
                LEFT OUTER JOIN CZTR_MST AS CZTR ON BSHK.BSHK_TO = CZTR.CZTR_ID
                WHERE 1=1
            """
            
            params = {
                'proc_noncoat': DatabaseConstants.PROC_NON_COAT,
                'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT
            }
            
            if base:
                query += " AND BFSP.BFSP_BASE = :base"
                params['base'] = base
                
            if adp:
                query += " AND BFSP.BFSP_ADP = :adp"
                params['adp'] = adp
                
            if lr:
                query += " AND BFSP.BFSP_LR = :lr"
                params['lr'] = lr
                
            if color:
                query += " AND BFSP.BFSP_CLR = :color"
                params['color'] = color
                
            if proc_type:
                if proc_type == 'NC':
                    query += " AND BRCP.BRCP_PROC = :filter_proc_noncoat"
                    params['filter_proc_noncoat'] = DatabaseConstants.PROC_NON_COAT
                elif proc_type == 'HC':
                    query += " AND BRCP.BRCP_PROC = :filter_proc_hardcoat"
                    params['filter_proc_hardcoat'] = DatabaseConstants.PROC_HARD_COAT
                    
            if shipment_date:
                query += " AND CAST(BSHK.BSHK_DT AS DATE) = :shipment_date"
                params['shipment_date'] = shipment_date
                
            if destination:
                query += " AND CZTR.CZTR_ID = :destination"
                params['destination'] = destination
                
            if order_no:
                query += " AND BRCP.BRCP_ORDER_NO = :order_no"
                params['order_no'] = order_no
                
            if shipment_status:
                query += " AND BSHK.BSHK_FLG = :shipment_status"
                params['shipment_status'] = shipment_status
                
            if order_date:
                query += " AND BSHK.BSHK_ORD_DT = :order_date"
                params['order_date'] = order_date
                
            query += " ORDER BY BSHK.BSHK_DT DESC"
            
            # デバッグログ
            print(f"SQLクエリ: {query}")
            print(f"パラメータ: {params}")
            
            results = session.execute(text(query), params).fetchall()
            
            # デバッグログ
            print(f"SQL実行結果: {len(results)}件")
            
            # 結果を整形（すべての値を文字列に変換）
            shipments = []
            for row in results:
                print(f"Processing row: {row}")  # デバッグ出力
                
                # shipment_status_codeを日本語に変換
                status_code = row.shipment_status_code if hasattr(row, 'shipment_status_code') else None
                shipment_status = DatabaseConstants.SHIPMENT_STATUS_LABELS.get(status_code, '不明') if status_code is not None else ''
                
                shipment = {
                    'id': row.id,
                    'shipment_date': str(row.shipment_date) if row.shipment_date else '',
                    'destination': str(row.destination) if row.destination else '',
                    'order_number': str(row.order_number) if row.order_number else '',
                    'lot': str(row.lot) if row.lot else '',
                    'base': str(row.base) if row.base else '',
                    'adp': str(row.adp) if row.adp else '',
                    'lr': str(row.lr) if row.lr else '',
                    'color': str(row.color) if row.color else '',
                    'quantity': str(row.quantity) if row.quantity else '0',
                    'coating_date': str(row.coating_date) if row.coating_date else '',
                    'proc_type': str(row.proc_type) if row.proc_type else '',
                    'shipment_status': shipment_status,
                    'order_date': str(row.order_date) if row.order_date else ''
                }
                print(f"Mapped shipment: {shipment}")  # デバッグ出力
                shipments.append(shipment)
            
            return shipments
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            log_error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            log_error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except ValueError as e:
            error_msg = f"入力値が不正です: {str(e)}"
            log_error(f"バリデーションエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            log_error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
            
    @staticmethod
    def get_by_dt_CT(shipment_date):
        """出荷データを取得する"""
        session = get_db_session()
        try:
            print(f"get_by_dt_CT: 検索日付 = {shipment_date}")
            
            # 出荷データを検索
            query = """
                SELECT 
                    BPRD.BPDD_LOT as lot,
                    BFSP.BFSP_BASE as base,
                    BFSP.BFSP_ADP as adp,
                    BFSP.BFSP_LR as lr,
                    BFSP.BFSP_CLR as color,
                    BSHK.BSHK_QTY as quantity,
                    BFSP.BFSP_SORT as sort,
                    BSHK.BSHK_ID as bshk_id
                FROM BSHK_DAT AS BSHK
                INNER JOIN BPRD_DAT AS BPRD
                    ON BSHK.BSHK_PDD_ID = BPRD.BPDD_ID
                INNER JOIN BFSP_MST AS BFSP
                    ON BPRD.BPDD_PRD_ID = BFSP.BFSP_PRD_ID
                LEFT OUTER JOIN BRCP_DAT AS BRCP
                    ON BSHK.BSHK_RCP_ID = BRCP.BRCP_ID
                LEFT OUTER JOIN CZTR_MST AS CZTR ON BSHK.BSHK_TO = CZTR.CZTR_ID
                WHERE BSHK.BSHK_FLG = :bshk_flg_not_shipped
                AND CAST(BSHK.BSHK_DT AS DATE) = CAST(:shipment_date AS DATE)
                AND BSHK.BSHK_TO = :shipment_to_process
                ORDER BY BFSP.BFSP_SORT
            """
            
            params = {
                'shipment_date': shipment_date,
                'bshk_flg_not_shipped': DatabaseConstants.BSHK_FLG_NOT_SHIPPED,
                'shipment_to_process': DatabaseConstants.SHIPMENT_TO_PROCESS
            }
            print(f"SQL実行パラメータ: {params}")
            
            results = session.execute(text(query), params).fetchall()
            print(f"取得した出荷データ数: {len(results)}")
            
            if len(results) == 0:
                print("出荷データが見つかりませんでした")
                return []
            
            return results
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            log_error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            log_error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except ValueError as e:
            error_msg = f"入力値が不正です: {str(e)}"
            log_error(f"バリデーションエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            log_error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()

    """出荷データのモデルクラス"""
    
    @staticmethod
    def get_stock_info(stock_id):
        """在庫情報を取得する"""
        session = get_db_session()
        try:
            # 在庫情報を取得
            stock = session.execute(text("""
                SELECT 
                    BPDD_ID,
                    BPDD_LOT,
                    BPDD_PRD_ID,
                    BFSP_BASE,
                    BFSP_ADP,
                    BFSP_LR,
                    BFSP_CLR,
                    ISNULL(dbo.Get_Zaiko_Qty_BF(BPDD_ID), 0) as stock_qty,
                    BPDD_CRT
                FROM BPRD_DAT AS BPRD
                INNER JOIN BFSP_MST AS BFSP
                    ON BPDD_PRD_ID = BFSP_PRD_ID
                WHERE BPDD_ID = :stock_id
            """), {'stock_id': stock_id}).first()
            
            return stock
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            log_error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            log_error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except ValueError as e:
            error_msg = f"入力値が不正です: {str(e)}"
            log_error(f"バリデーションエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            log_error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
    
    @staticmethod
    def get_noncoat_orders(stock_id):
        """非コート受注残を取得する"""
        session = get_db_session()
        try:
            # 非コート受注残を取得
            nc_orders = session.execute(text("""
                SELECT 
                    BRCP_DT,
                    BRCP_ORDER_NO,
                    CZTR_NM,
                    ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID), 0) as remaining_qty,
                    BRCP.BRCP_ID
                FROM BPRD_DAT AS BPRD
                INNER JOIN BRCP_DAT AS BRCP
                    ON BPRD.BPDD_PRD_ID = BRCP.BRCP_PRD_ID
                INNER JOIN CZTR_MST AS CZTR ON BRCP.BRCP_ORDER_CMP = CZTR.CZTR_ID
                WHERE BPRD.BPDD_ID = :stock_id
                    AND BRCP.BRCP_PROC = :brcp_proc_noncoat
                    AND BRCP.BRCP_FLG = :brcp_flg_not_shipped
                    AND ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID), 0) > 0
                ORDER BY BRCP_DT, BRCP_ORDER_NO
            """), {
                'stock_id': stock_id,
                'brcp_proc_noncoat': DatabaseConstants.PROC_NON_COAT,
                'brcp_flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
            }).fetchall()
            
            return nc_orders
        except Exception as e:
            log_error(f"非コート受注残取得中にエラーが発生しました: {str(e)}")
            raise Exception(f"非コート受注残の取得に失敗しました: {str(e)}")
        finally:
            session.close()
    
    @staticmethod
    def get_hardcoat_orders(stock_id):
        """ハードコート受注残を取得する"""
        session = get_db_session()
        try:
            # ハードコート受注残を取得
            hc_orders = session.execute(text("""
                SELECT 
                    BRCP_DT,
                    BRCP_ORDER_NO,
                    CZTR_NM,
                    ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID), 0) as remaining_qty,
                    BRCP.BRCP_ID
                FROM BPRD_DAT AS BPRD
                INNER JOIN BRCP_DAT AS BRCP
                    ON BPRD.BPDD_PRD_ID = BRCP.BRCP_PRD_ID
                INNER JOIN CZTR_MST AS CZTR ON BRCP.BRCP_ORDER_CMP = CZTR.CZTR_ID
                WHERE BPRD.BPDD_ID = :stock_id
                    AND BRCP.BRCP_PROC = :brcp_proc_hardcoat
                    AND BRCP.BRCP_FLG = :brcp_flg_not_shipped
                    AND ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID), 0) > 0
                ORDER BY BRCP_DT, BRCP_ORDER_NO
            """), {
                'stock_id': stock_id,
                'brcp_proc_hardcoat': DatabaseConstants.PROC_HARD_COAT,
                'brcp_flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
            }).fetchall()
            
            return hc_orders
        except Exception as e:
            log_error(f"ハードコート受注残取得中にエラーが発生しました: {str(e)}")
            raise Exception(f"ハードコート受注残の取得に失敗しました: {str(e)}")
        finally:
            session.close()
    
    @staticmethod
    def get_shipping_destinations():
        """出荷先リストを取得する"""
        session = get_db_session()
        try:
            # 出荷先リストを取得
            shipping_destinations = session.execute(text("""
                SELECT CZTR_ID, CZTR_NM 
                FROM CZTR_MST 
                WHERE CZTR_TYP = :cztr_type_bf
                ORDER BY CZTR_ID
            """), {'cztr_type_bf': DatabaseConstants.CZTR_TYPE_BF}).fetchall()
            
            return shipping_destinations
        except Exception as e:
            log_error(f"出荷先リスト取得中にエラーが発生しました: {str(e)}")
            raise Exception(f"出荷先リストの取得に失敗しました: {str(e)}")
        finally:
            session.close()
    
    @staticmethod
    def save(stock_id, shipments, proc_type = DatabaseConstants.PROC_NON_COAT):
        """出荷データを保存する"""
        session = get_db_session()
        try:
            # 在庫情報を取得
            stock = session.execute(text("""
                SELECT 
                    BPDD_ID,
                    BPDD_LOT,
                    BPDD_PRD_ID,
                    ISNULL(dbo.Get_Zaiko_Qty_BF(BPDD_ID), 0) as stock_qty
                FROM BPRD_DAT
                WHERE BPDD_ID = :stock_id
            """), {'stock_id': stock_id}).first()
            
            if not stock:
                return {'success': False, 'error': '在庫が見つかりません。'}
                
            # 合計出荷数量をチェック
            total_qty = sum(int(shipment['quantity']) for shipment in shipments)
            if total_qty > stock.stock_qty:
                return {'success': False, 'error': '出荷数量が在庫数量を超えています。'}
                
            try:
                # 出荷データを登録
                for shipment in shipments:
                    remaining_qty = int(shipment['quantity'])
                    process = DatabaseConstants.FLG_ACTIVE
                    ship_to = shipment['ship_to']
                    ship_date = shipment['ship_date']
                    order_date = shipment['order_date']  # 手配日を取得
                    
                    # 出荷先が加工の場合は出荷データを登録
                    if ship_to == str(DatabaseConstants.SHIPMENT_TO_PROCESS) or ship_to == str(DatabaseConstants.ORDER_CMP_MISSING) or ship_to == str(DatabaseConstants.SHIPMENT_TO_PROCESS):
                        # 出荷データを登録
                        ship_sql = text("""
                            INSERT INTO BSHK_DAT (
                                BSHK_TO,
                                BSHK_PDD_ID,
                                BSHK_RCP_ID,
                                BSHK_DT,
                                BSHK_QTY,
                                BSHK_FLG,
                                BSHK_ORD_DT    
                            ) VALUES (
                                :ship_to,
                                :pdd_id,
                                :rcp_id,
                                :ship_date,
                                :qty,
                                :bshk_flg_initial,
                                :order_date
                            )
                        """)
                        
                        session.execute(ship_sql, {
                            'ship_to': ship_to,
                            'pdd_id': stock.BPDD_ID,
                            'rcp_id': 0,
                            'ship_date': ship_date,
                            'qty': remaining_qty,
                            'bshk_flg_initial': DatabaseConstants.BSHK_FLG_NOT_SHIPPED,
                            'order_date': order_date
                        })
                    # 出荷先が1:加工以外の場合は受注データを取得
                    else:
                        # 対象の受注データを古い順に取得
                        orders = session.execute(text("""
                            SELECT 
                                BRCP_ID,
                                ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP_ID), 0) as remaining_qty
                            FROM BRCP_DAT
                            WHERE BRCP_ORDER_CMP = :ship_to
                                AND BRCP_PRD_ID = :prd_id
                                AND BRCP_PROC = :proc_type
                                AND BRCP_FLG = :brcp_flg_not_shipped
                                AND ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP_ID), 0) > 0
                            ORDER BY BRCP_DT
                        """), {
                            'ship_to': ship_to,
                            'prd_id': stock.BPDD_PRD_ID,
                            'proc_type': proc_type,
                            'brcp_flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
                        }).fetchall()
                        
                        # 受注データに対して出荷数を割り当て
                        for order in orders:
                            if remaining_qty <= 0:
                                break
                                
                            ship_qty = min(remaining_qty, order.remaining_qty)
                            
                            # 出荷データを登録
                            ship_sql = text("""
                                INSERT INTO BSHK_DAT (
                                    BSHK_TO,
                                    BSHK_PDD_ID,
                                    BSHK_RCP_ID,
                                    BSHK_DT,
                                    BSHK_QTY,
                                    BSHK_FLG,
                                    BSHK_ORD_DT    
                                ) VALUES (
                                    :ship_to,
                                    :pdd_id,
                                    :rcp_id,
                                    :ship_date,
                                    :qty,
                                    :bshk_flg_initial,
                                    :order_date
                                )
                            """)
                            
                            session.execute(ship_sql, {
                                'ship_to': ship_to,
                                'pdd_id': stock.BPDD_ID,
                                'rcp_id': order.BRCP_ID,
                                'ship_date': ship_date,
                                'qty': ship_qty,
                                'bshk_flg_initial': DatabaseConstants.BSHK_FLG_NOT_SHIPPED,
                                'order_date': order_date
                            })
                            
                            # 出荷残が0以上の場合は受注のフラグを1にする
                            if remaining_qty >= order.remaining_qty:
                                BrcpDat.set_flg(order.BRCP_ID, DatabaseConstants.BRCP_FLG_SHIPPED)
                                
                            # 出荷残を減算
                            remaining_qty -= ship_qty
                                
                        if remaining_qty > 0:
                            raise ValueError('受注残より出荷数量が多いです。')
                        
                # 出荷数量が在庫数量と同じ場合は在庫のフラグを1にする
                if total_qty == stock.stock_qty:
                    PrdDat.set_flg(stock.BPDD_ID, DatabaseConstants.BPDD_FLG_SHIPPED)
                
                session.commit()
                return {'success': True}
                
            except Exception as e:
                session.rollback()
                return {'success': False, 'error': str(e)}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}
        finally:
            session.close()
            
    @staticmethod
    def get_shipment_by_prd_id(prd_id, lot):
        """加工データを取得する"""
        session = get_db_session()
        try:
            # 加工データを取得
            sql = text("""
                SELECT 
                    BSHK_ID
                FROM BSHK_DAT
                INNER JOIN BPRD_DAT
                    ON BSHK_DAT.BSHK_PDD_ID = BPRD_DAT.BPDD_ID
                WHERE BPRD_DAT.BPDD_PRD_ID = :prd_id
                    AND BPRD_DAT.BPDD_LOT = :lot
                    AND BPRD_DAT.BPDD_PROC = :proc_type
                    AND BSHK_DAT.BSHK_TO = :ship_to

            """)
            
            results = session.execute(sql, {
                'prd_id': prd_id,
                'lot': lot,
                'proc_type': DatabaseConstants.PROC_NON_COAT,
                'ship_to': DatabaseConstants.SHIPMENT_TO_PROCESS
            }).fetchall()
            
            return results 
        
        except Exception as e:
            log_error(f"加工データ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_proc_order(prd_id):
        """加工指示書を取得する"""
        session = get_db_session()
        try:
            # 加工指示書を取得
            sql = text("""
                SELECT 
                    BSHK_ID,
                    BSHK_DT,
                    BSHK_QTY,
                    BPDD_LOT
                FROM BSHK_DAT
                INNER JOIN BPRD_DAT
                ON BSHK_DAT.BSHK_PDD_ID = BPRD_DAT.BPDD_ID
                WHERE BPRD_DAT.BPDD_PRD_ID = :prd_id
                AND BSHK_DAT.BSHK_TO = :ship_to
                AND BSHK_DAT.BSHK_FLG = :bshk_flg_not_shipped
            """)    

            results = session.execute(sql, {
                'prd_id': prd_id,
                'ship_to': DatabaseConstants.SHIPMENT_TO_PROCESS,
                'bshk_flg_not_shipped': DatabaseConstants.BSHK_FLG_NOT_SHIPPED
            }).fetchall()

            return results

        except Exception as e:
            log_error(f"加工指示書取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete_shipment(shipment_id):
        """出荷データを削除する"""
        session = get_db_session()
        try:
            # 出荷データと関連する受注データを取得
            shipment = session.execute(text("""
                SELECT 
                    BSHK.BSHK_ID,
                    BSHK.BSHK_RCP_ID,
                    BSHK.BSHK_PDD_ID,
                    BSHK.BSHK_QTY,
                    BRCP.BRCP_QTY,
                    BRCP.BRCP_FLG,
                    BPRD.BPDD_FLG,
                    BSHK.BSHK_FLG
                FROM BSHK_DAT AS BSHK
                LEFT JOIN BRCP_DAT AS BRCP
                    ON BSHK.BSHK_RCP_ID = BRCP.BRCP_ID
                LEFT JOIN BPRD_DAT AS BPRD
                    ON BSHK.BSHK_PDD_ID = BPRD.BPDD_ID
                WHERE BSHK.BSHK_ID = :shipment_id
            """), {'shipment_id': shipment_id}).first()

            if not shipment:
                raise ValueError('出荷データが見つかりません。')
            
            if shipment.BSHK_FLG == DatabaseConstants.BSHK_FLG_PROCESSED:
                raise ValueError('加工済みデータのため削除できません。')
            
            if shipment.BSHK_FLG >= DatabaseConstants.BSHK_FLG_DELIVERED:
                raise ValueError('納品書・請求書が発行されているため削除できません。')

            # 出荷データを削除
            delete_shipment_sql = text("""
                DELETE FROM BSHK_DAT
                WHERE BSHK_ID = :shipment_id
            """)
            session.execute(delete_shipment_sql, {'shipment_id': shipment_id})

            # 受注データが存在し、フラグが1の場合は0に戻す
            if shipment.BSHK_RCP_ID and shipment.BRCP_FLG == DatabaseConstants.BRCP_FLG_SHIPPED:
                # 受注残を計算
                remaining_qty = session.execute(text("""
                    SELECT ISNULL(dbo.Get_ODR_ZAN_Qty_BF(:rcp_id), 0) as remaining_qty
                """), {'rcp_id': shipment.BSHK_RCP_ID}).first()

                # 受注残がある場合はフラグを0に戻す
                if remaining_qty.remaining_qty > 0:
                    update_order_sql = text("""
                        UPDATE BRCP_DAT
                        SET BRCP_FLG = :brcp_flg_not_shipped
                        WHERE BRCP_ID = :rcp_id
                    """)
                    session.execute(update_order_sql, {
                        'rcp_id': shipment.BSHK_RCP_ID,
                        'brcp_flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
                    })

            # 在庫フラグを0に戻す
            if shipment.BSHK_PDD_ID and shipment.BPDD_FLG == DatabaseConstants.BPDD_FLG_SHIPPED:
                update_stock_sql = text("""
                    UPDATE BPRD_DAT
                    SET BPDD_FLG = :bpdd_flg_not_shipped
                    WHERE BPDD_ID = :pdd_id
                """)
                session.execute(update_stock_sql, {
                    'pdd_id': shipment.BSHK_PDD_ID,
                    'bpdd_flg_not_shipped': DatabaseConstants.BPDD_FLG_NOT_SHIPPED
                })

            session.commit()
            return {'success': True}
        except Exception as e:
            session.rollback()
            log_error(f"出荷データ削除中にエラーが発生しました: {str(e)}")
            return {'success': False, 'error': str(e)}
        finally:
            session.close()

    @staticmethod
    def auto_shipping_hardcoat(base, adp, lr, clr, ship_to, order_date, ship_date, quantity):
        """ハードコート自動出荷処理を実行する"""
        session = get_db_session()
        try:
            # 製品IDを取得
            product = session.execute(text("""
                SELECT BFSP_PRD_ID
                FROM BFSP_MST
                WHERE BFSP_BASE = :base
                    AND BFSP_ADP = :adp
                    AND BFSP_LR = :lr
                    AND BFSP_CLR = :clr
            """), {
                'base': base,
                'adp': adp,
                'lr': lr,
                'clr': clr
            }).first()
            
            if not product:
                return {'success': False, 'error': '指定された製品が見つかりません。'}
            
            # 在庫データをBPDD_LOTの小さい順で取得
            stocks = session.execute(text("""
                SELECT 
                    BPDD_ID,
                    BPDD_LOT,
                    BPDD_QTY,
                    BPDD_CRT,
                    ISNULL(dbo.Get_Zaiko_Qty_BF(BPDD_ID), 0) as stock_qty
                FROM BPRD_DAT
                WHERE BPDD_PRD_ID = :prd_id
                    AND BPDD_PROC = :proc_hardcoat
                    AND BPDD_FLG = :flg_not_shipped
                    AND ISNULL(dbo.Get_Zaiko_Qty_BF(BPDD_ID), 0) > 0
                ORDER BY BPDD_LOT
            """), {
                'prd_id': product.BFSP_PRD_ID,
                'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT,
                'flg_not_shipped': DatabaseConstants.BPDD_FLG_NOT_SHIPPED
            }).fetchall()
            
            if not stocks:
                return {'success': False, 'error': '利用可能な在庫が見つかりません。'}
            
            # 総在庫数を再チェック
            total_stock = sum(stock.stock_qty for stock in stocks)
            if quantity > total_stock:
                return {'success': False, 'error': f'出荷数({quantity})が利用可能在庫数({total_stock})を超えています。'}
            
            # 出荷処理
            remaining_qty = quantity
            shipped_records = []
            
            # 出荷先が「加工」または「欠損」以外の場合は受注データを取得
            if int(ship_to) not in [DatabaseConstants.SHIPMENT_TO_PROCESS, DatabaseConstants.ORDER_CMP_MISSING]:
                # 対象の受注データを古い順に取得
                orders = session.execute(text("""
                    SELECT 
                        BRCP_ID,
                        ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP_ID), 0) as remaining_qty
                    FROM BRCP_DAT
                    WHERE BRCP_ORDER_CMP = :ship_to
                        AND BRCP_PRD_ID = :prd_id
                        AND BRCP_PROC = :proc_hardcoat
                        AND BRCP_FLG = :brcp_flg_not_shipped
                        AND ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP_ID), 0) > 0
                    ORDER BY BRCP_DT
                """), {
                    'ship_to': ship_to,
                    'prd_id': product.BFSP_PRD_ID,
                    'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT,
                    'brcp_flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
                }).fetchall()
                
                if not orders:
                    return {'success': False, 'error': f'指定された出荷先に対する受注データが見つかりません。'}
                
                # 受注残の合計をチェック
                total_order_qty = sum(order.remaining_qty for order in orders)
                if quantity > total_order_qty:
                    return {'success': False, 'error': f'出荷数({quantity})が受注残数({total_order_qty})を超えています。'}
            else:
                orders = []
            
            # 在庫から出荷処理
            current_order_index = 0
            current_order_remaining = orders[current_order_index].remaining_qty if orders else 0
            
            for stock in stocks:
                if remaining_qty <= 0:
                    break
                
                # この在庫から出荷する数量を決定
                ship_qty = min(remaining_qty, stock.stock_qty)
                
                # 出荷先が「加工」または「欠損」の場合
                if int(ship_to) in [DatabaseConstants.SHIPMENT_TO_PROCESS, DatabaseConstants.ORDER_CMP_MISSING]:
                    # 出荷データを登録（受注IDは0）
                    ship_sql = text("""
                        INSERT INTO BSHK_DAT (
                            BSHK_TO,
                            BSHK_PDD_ID,
                            BSHK_RCP_ID,
                            BSHK_DT,
                            BSHK_QTY,
                            BSHK_FLG,
                            BSHK_ORD_DT    
                        ) VALUES (
                            :ship_to,
                            :pdd_id,
                            :rcp_id,
                            :ship_date,
                            :qty,
                            :bshk_flg_initial,
                            :order_date
                        )
                    """)
                    
                    session.execute(ship_sql, {
                        'ship_to': ship_to,
                        'pdd_id': stock.BPDD_ID,
                        'rcp_id': 0,
                        'ship_date': ship_date,
                        'qty': ship_qty,
                        'bshk_flg_initial': DatabaseConstants.BSHK_FLG_NOT_SHIPPED,
                        'order_date': order_date
                    })
                else:
                    # 受注データに対して出荷数を割り当て
                    stock_remaining = ship_qty
                    
                    while stock_remaining > 0 and current_order_index < len(orders):
                        if current_order_remaining <= 0:
                            current_order_index += 1
                            if current_order_index < len(orders):
                                current_order_remaining = orders[current_order_index].remaining_qty
                            continue
                        
                        # この受注に対する出荷数量を決定
                        order_ship_qty = min(stock_remaining, current_order_remaining)
                        
                        # 出荷データを登録
                        ship_sql = text("""
                            INSERT INTO BSHK_DAT (
                                BSHK_TO,
                                BSHK_PDD_ID,
                                BSHK_RCP_ID,
                                BSHK_DT,
                                BSHK_QTY,
                                BSHK_FLG,
                                BSHK_ORD_DT    
                            ) VALUES (
                                :ship_to,
                                :pdd_id,
                                :rcp_id,
                                :ship_date,
                                :qty,
                                :bshk_flg_initial,
                                :order_date
                            )
                        """)
                        
                        session.execute(ship_sql, {
                            'ship_to': ship_to,
                            'pdd_id': stock.BPDD_ID,
                            'rcp_id': orders[current_order_index].BRCP_ID,
                            'ship_date': ship_date,
                            'qty': order_ship_qty,
                            'bshk_flg_initial': DatabaseConstants.BSHK_FLG_NOT_SHIPPED,
                            'order_date': order_date
                        })
                        
                        # 受注完了の場合はフラグを更新
                        if order_ship_qty >= current_order_remaining:
                            BrcpDat.set_flg(orders[current_order_index].BRCP_ID, DatabaseConstants.BRCP_FLG_SHIPPED)
                        
                        # 残量を更新
                        stock_remaining -= order_ship_qty
                        current_order_remaining -= order_ship_qty
                
                # 出荷記録を保存
                shipped_records.append({
                    'bpdd_id': stock.BPDD_ID,
                    'bpdd_lot': stock.BPDD_LOT,
                    'bpdd_crt': str(stock.BPDD_CRT) if stock.BPDD_CRT else '',
                    'shipped_qty': ship_qty,
                    'stock_qty': stock.stock_qty
                })
                
                # 在庫が完全に出荷された場合はフラグを更新
                if ship_qty >= stock.stock_qty:
                    update_stock_sql = text("""
                        UPDATE BPRD_DAT
                        SET BPDD_FLG = :bpdd_flg_shipped
                        WHERE BPDD_ID = :pdd_id
                    """)
                    session.execute(update_stock_sql, {
                        'pdd_id': stock.BPDD_ID,
                        'bpdd_flg_shipped': DatabaseConstants.BPDD_FLG_SHIPPED
                    })
                
                remaining_qty -= ship_qty
            
            session.commit()
            
            return {
                'success': True,
                'message': f'自動出荷が完了しました。出荷数量: {quantity}',
                'total_shipped': quantity,
                'shipped_records': shipped_records
            }
            
        except Exception as e:
            session.rollback()
            log_error(f"自動出荷処理中にエラーが発生しました: {str(e)}")
            return {'success': False, 'error': str(e)}
        finally:
            session.close()
