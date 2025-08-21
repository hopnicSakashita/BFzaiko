import logging
from sqlalchemy import Column, String, Integer, ForeignKey, Float, DateTime, Boolean, Text, inspect, text, case
from sqlalchemy.types import Numeric
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from datetime import datetime
from sqlalchemy.orm import relationship
from flask import jsonify
import pandas as pd
from io import BytesIO
import unicodedata
import traceback

from app.database import Base, get_db_session, get_db_connection
from app.constants import DatabaseConstants
from app.models_common import CprdDatModel
from app.logger_utils import log_error, log_info

# デバッグモードの設定
DEBUG_MODE = False

# SQLServerの日本語データを適切に処理するヘルパー関数
def process_text_to_db(value):
    """
    データベースに保存する前にテキストデータを適切に処理する
    UTF-8でデータベースに登録するための準備をする
    """
    if value is None or not isinstance(value, str):
        return value
    
    if value == '':
        return value
    
    # UTF-8として正規化して返す
    try:
        # NFDとNFCの正規化形式の違いを吸収
        normalized = unicodedata.normalize('NFC', value)
        
        # UTF-8エンコーディングが可能か確認
        normalized.encode('utf-8')
        
        return normalized
    except Exception as e:
        log_error(f"テキスト正規化中にエラー: {str(e)}")
        return value

class PrdDatModel(Base):
    """BPRD_DATテーブルのSQLAlchemyモデル"""
    __tablename__ = 'BPRD_DAT'
    
    BPDD_ID = Column(Integer, primary_key=True, autoincrement=True)
    BPDD_PROC = Column(Numeric(1, 0))
    BPDD_PRD_ID = Column(String(4, collation='Japanese_CI_AS'), ForeignKey('BFSP_MST.BFSP_PRD_ID'))
    BPDD_LOT = Column(Numeric(6, 0))
    BPDD_QTY = Column(Numeric(5, 0))
    BPDD_FLG = Column(Numeric(1, 0))
    BPDD_CRT = Column(Numeric(6, 0))
    
    # リレーションシップ
    bfsp_mst = relationship("BfspMstModel", back_populates="bprd_dat")
    
class BprdMeiModel(Base):
    """製造明細テーブルのSQLAlchemyモデル"""
    __tablename__ = 'BPRD_MEI'
    
    BPDM_ID = Column(Integer, primary_key=True, autoincrement=True)
    BPDM_PRD_ID = Column(String(4, collation='Japanese_CI_AS'))
    BPDM_LOT = Column(Numeric(6, 0))
    BPDM_NO = Column(Numeric(4, 0))
    BPDM_QTY = Column(Numeric(5, 0))


class BfspMstModel(Base):
    """BF規格マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'BFSP_MST'
    
    BFSP_PRD_ID = Column(String(4, collation='Japanese_CI_AS'), primary_key=True)
    BFSP_MONO = Column(Numeric(2, 0))  # モノマー
    BFSP_BASE = Column(Numeric(1, 0))  # ベース
    BFSP_ADP = Column(Numeric(3, 0))   # 加入度数
    BFSP_LR = Column(String(1, collation='Japanese_CI_AS'))  # L/R
    BFSP_CLR = Column(String(2, collation='Japanese_CI_AS')) # 色
    BFSP_SORT = Column(Numeric(3, 0))  # ソート順
    BFSP_S_NC = Column(String(15, collation='Japanese_CI_AS')) # サンレーNC
    BFSP_S_HC = Column(String(15, collation='Japanese_CI_AS')) # サンレーHC
    BFSP_Y_BCD = Column(String(15, collation='Japanese_CI_AS')) # ヤンガーBCD
    BFSP_Y_GTIN = Column(String(15, collation='Japanese_CI_AS')) # ヤンガーGTIN

    # リレーションシップ
    bprd_dat = relationship("PrdDatModel", back_populates="bfsp_mst")

class BfspMst:
    """BF規格マスタテーブルのモデルクラス"""
    
    @staticmethod
    def get_all():
        """すべてのBF規格マスタデータを取得する"""
        session = None
        try:
            session = get_db_session()
            bfsp_data = session.query(BfspMstModel).order_by(BfspMstModel.BFSP_SORT).all()
            
            result = []
            for b in bfsp_data:
                bfsp = {
                    'BFSP_PRD_ID': b.BFSP_PRD_ID,
                    'BFSP_MONO': b.BFSP_MONO,
                    'BFSP_BASE': b.BFSP_BASE,
                    'BFSP_ADP': b.BFSP_ADP,
                    'BFSP_LR': b.BFSP_LR,
                    'BFSP_CLR': b.BFSP_CLR,
                    'BFSP_SORT': b.BFSP_SORT
                }
                result.append(bfsp)
            
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            log_error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            log_error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            log_error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            if session:
                session.close()
                
    # 選択肢を取得する共通関数
    def get_choices(column_name):
        session = get_db_session()
        try:
            choices = session.execute(text(f"""
                SELECT DISTINCT {column_name}
                FROM BFSP_MST 
                WHERE {column_name} IS NOT NULL 
                ORDER BY {column_name}
            """)).fetchall()
            return [('', '全て')] + [(str(r[0]), str(r[0])) for r in choices]
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            log_error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            log_error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            log_error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            if session:
                session.close()


class BrcpDatModel(Base):
    """受注データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'BRCP_DAT'
    
    BRCP_ID = Column(Integer, primary_key=True, autoincrement=True)
    BRCP_DT = Column(DateTime)
    BRCP_PRD_ID = Column(String(4, collation='Japanese_CI_AS'))
    BRCP_PROC = Column(Numeric(1, 0))
    BRCP_ORDER_CMP = Column(Numeric(1, 0))
    BRCP_ORDER_NO = Column(Numeric(10, 0))
    BRCP_QTY = Column(Numeric(5, 0))
    BRCP_FLG = Column(Numeric(1, 0))
    
    # リレーションシップ
    bshk_dat = relationship("BshkDatModel", back_populates="brcp_dat")

class BshkDatModel(Base):
    """出荷データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'BSHK_DAT'
    
    BSHK_ID = Column(Integer, primary_key=True, autoincrement=True)
    BSHK_TO = Column(Numeric(1, 0))
    BSHK_PDD_ID = Column(Numeric(8, 0))
    BSHK_RCP_ID = Column(Numeric(10, 0), ForeignKey('BRCP_DAT.BRCP_ID'))
    BSHK_DT = Column(DateTime)
    BSHK_QTY = Column(Numeric(5, 0))    
    BSHK_FLG = Column(Numeric(1, 0))
    BSHK_ORD_DT = Column(DateTime)
    
    # リレーションシップ
    brcp_dat = relationship("BrcpDatModel", back_populates="bshk_dat")

class BrcpDat:
    """受注データのモデルクラス"""
    
    @staticmethod
    def set_flg(id, flg, session=None):
        """受注データのフラグを更新する"""
        session_flg = False
        if session is None:
            session = get_db_session()
            session_flg = True
        try:

            session.execute(text("""
                UPDATE BRCP_DAT 
                SET BRCP_FLG = :flg 
                WHERE BRCP_ID = :id
            """), {'id': id, 'flg': flg})

            if session_flg:
                session.commit()
        except Exception as e:
            if session_flg:
                session.rollback()
            log_error(f"受注データのフラグ更新中にエラーが発生: {str(e)}")
            raise
        finally:
            if session_flg:
                session.close()

    
    @staticmethod
    def search(data):
        """受注データを検索する"""
        session = get_db_session()
        try:
            # 受注データを検索
            sql = text("""
                SELECT BRCP_ID, BRCP_PRD_ID, BRCP_QTY 
                FROM BRCP_DAT 
                WHERE BRCP_DT = :order_date 
                AND BRCP_ORDER_NO = :customer_order_no 
                AND BRCP_PROC = :process
                AND BRCP_ORDER_CMP = :ship_to
                AND BRCP_FLG = :flg_not_shipped
            """)
            
            results = session.execute(sql, {
                'order_date': data['orderDate'],
                'customer_order_no': data['customerOrderNo'],
                'process': data['process'],
                'ship_to': data['shipTo'],
                'flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
            }).fetchall()
            
            return {
                'exists': len(results) > 0,
                'details': [{'BRCP_PRD_ID': r.BRCP_PRD_ID, 'BRCP_QTY': r.BRCP_QTY} for r in results]
            }
        except Exception as e:
            log_error(f"受注データ検索中にエラーが発生しました: {str(e)}")
            return {'error': str(e)}
        finally:
            session.close()
    
    @staticmethod
    def save(data):
        """受注データを保存する"""
        session = get_db_session()
        try:
            # トランザクション開始
            session.begin()
            
            # 既存データを更新
            for detail in data['details']:
                # 既存データを検索
                sql = text("""
                    SELECT BRCP_ID 
                    ,BRCP_QTY
                    ,dbo.Get_ODR_ZAN_Qty_BF(BRCP_ID) as ZAN_QTY
                    FROM BRCP_DAT 
                    WHERE BRCP_DT = :order_date 
                    AND BRCP_ORDER_NO = :customer_order_no 
                    AND BRCP_PROC = :process
                    AND BRCP_PRD_ID = :prd_id
                """)
                
                existing = session.execute(sql, {
                    'order_date': data['orderDate'],
                    'customer_order_no': data['customerOrderNo'],
                    'process': data['process'],
                    'prd_id': detail['BRCP_PRD_ID']
                }).first()
                
                if existing:
                    if detail['BRCP_QTY'] > 0:
                        sql = text("""
                            UPDATE BRCP_DAT 
                            SET BRCP_QTY = :qty 
                            WHERE BRCP_ID = :id
                        """)
                        session.execute(sql, {
                            'qty': detail['BRCP_QTY'],
                            'id': existing.BRCP_ID
                        })
                    else:
                        sql = text("""
                            DELETE FROM BRCP_DAT 
                            WHERE BRCP_ID = :id
                        """)
                        session.execute(sql, {'id': existing.BRCP_ID})
                else:
                    # 新規データを登録
                    if detail['BRCP_QTY'] > 0:
                        sql = text("""
                            INSERT INTO BRCP_DAT 
                            (BRCP_DT, BRCP_PRD_ID, BRCP_PROC, BRCP_ORDER_NO, BRCP_ORDER_CMP, BRCP_QTY, BRCP_FLG) 
                            VALUES (:order_date, :prd_id, :process, :customer_order_no, :ship_to, :qty, :flg_not_shipped)
                        """)
                        session.execute(sql, {
                            'order_date': data['orderDate'],
                            'prd_id': detail['BRCP_PRD_ID'],
                            'process': data['process'],
                            'customer_order_no': data['customerOrderNo'],
                            'ship_to': data['shipTo'],
                            'qty': detail['BRCP_QTY'],
                            'flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
                        })
            
            # コミット
            session.commit()
            return {'success': True}
        except Exception as e:
            session.rollback()
            log_error(f"受注データ保存中にエラーが発生しました: {str(e)}")
            return {'error': str(e)}
        finally:
            session.close()

    @staticmethod
    def get_order_summary():
        """取引先・加工IDごとの受注残と在庫数を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                WITH OrderSummary AS (
                    SELECT 
                        BRCP.BRCP_PRD_ID,
                        BRCP.BRCP_PROC,
                        BRCP.BRCP_ORDER_CMP,
                        SUM(ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID), 0)) as remaining_qty
                    FROM BRCP_DAT BRCP
                    WHERE BRCP.BRCP_FLG = :flg_not_shipped
                    GROUP BY 
                        BRCP.BRCP_PRD_ID,
                        BRCP.BRCP_PROC,
                        BRCP.BRCP_ORDER_CMP
                    HAVING SUM(ISNULL(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID), 0)) > 0
                ),
                StockSummary AS (
                    SELECT 
                        BPRD.BPDD_PRD_ID,
                        BPRD.BPDD_PROC,
                        SUM(ISNULL(dbo.Get_Zaiko_Qty_BF(BPRD.BPDD_ID), 0)) as stock_qty
                    FROM BPRD_DAT BPRD
                    WHERE BPRD.BPDD_FLG = :flg_not_shipped
                    GROUP BY 
                        BPRD.BPDD_PRD_ID,
                        BPRD.BPDD_PROC
                    HAVING SUM(ISNULL(dbo.Get_Zaiko_Qty_BF(BPRD.BPDD_ID), 0)) > 0
                )
                SELECT 
                    BFSP_PRD_ID as prd_id,
                    BFSP_BASE as base,
                    BFSP_ADP as adp,
                    BFSP_LR as lr,
                    BFSP_CLR as clr,
                    BFSP_SORT as sort,
                    SUM(nc_c) as nc_c,
                    SUM(nc_d) as nc_d,
                    SUM(hc_c) as hc_c,
                    SUM(hc_d) as hc_d,
                    SUM(hc_y) as hc_y,
                    SUM(hc_y_eu) as hc_y_eu,
                    SUM(nc_stock) as nc_stock,
                    SUM(hc_stock) as hc_stock
                FROM (
                    -- OrderSummaryのデータ
                    SELECT 
                        OrderSummary.BRCP_PRD_ID as BFSP_PRD_ID,
                        BFSP.BFSP_BASE,
                        BFSP.BFSP_ADP,
                        BFSP.BFSP_LR,
                        BFSP.BFSP_CLR,
                        BFSP.BFSP_SORT,
                        case when OrderSummary.BRCP_ORDER_CMP = :order_cmp_columbus and OrderSummary.BRCP_PROC = :proc_noncoat then ISNULL(OrderSummary.remaining_qty, 0) else 0 end as nc_c,
                        case when OrderSummary.BRCP_ORDER_CMP = :order_cmp_dallas and OrderSummary.BRCP_PROC = :proc_noncoat then ISNULL(OrderSummary.remaining_qty, 0) else 0 end as nc_d,
                        case when OrderSummary.BRCP_ORDER_CMP = :order_cmp_columbus and OrderSummary.BRCP_PROC = :proc_hardcoat then ISNULL(OrderSummary.remaining_qty, 0) else 0 end as hc_c,
                        case when OrderSummary.BRCP_ORDER_CMP = :order_cmp_dallas and OrderSummary.BRCP_PROC = :proc_hardcoat then ISNULL(OrderSummary.remaining_qty, 0) else 0 end as hc_d,
                        case when OrderSummary.BRCP_ORDER_CMP = :order_cmp_younger and OrderSummary.BRCP_PROC = :proc_hardcoat then ISNULL(OrderSummary.remaining_qty, 0) else 0 end as hc_y,
                        case when OrderSummary.BRCP_ORDER_CMP = :order_cmp_younger_eu and OrderSummary.BRCP_PROC = :proc_hardcoat then ISNULL(OrderSummary.remaining_qty, 0) else 0 end as hc_y_eu,
                        0 as nc_stock, 0 as hc_stock
                    FROM OrderSummary
                    INNER JOIN BFSP_MST BFSP ON OrderSummary.BRCP_PRD_ID = BFSP.BFSP_PRD_ID
                    
                    UNION ALL
                    
                    -- StockSummaryのデータ
                    SELECT 
                        StockSummary.BPDD_PRD_ID as BFSP_PRD_ID,
                        BFSP.BFSP_BASE,
                        BFSP.BFSP_ADP,
                        BFSP.BFSP_LR,
                        BFSP.BFSP_CLR,
                        BFSP.BFSP_SORT,
                        0 as nc_c, 0 as nc_d, 0 as hc_c, 0 as hc_d, 0 as hc_y, 0 as hc_y_eu,
                        case when StockSummary.BPDD_PROC = :proc_noncoat then ISNULL(StockSummary.stock_qty, 0) else 0 end as nc_stock,
                        case when StockSummary.BPDD_PROC = :proc_hardcoat then ISNULL(StockSummary.stock_qty, 0) else 0 end as hc_stock
                    FROM StockSummary
                    INNER JOIN BFSP_MST BFSP ON StockSummary.BPDD_PRD_ID = BFSP.BFSP_PRD_ID
                ) AS CombinedData
                GROUP BY 
                    BFSP_PRD_ID,
                    BFSP_BASE,
                    BFSP_ADP,
                    BFSP_LR,
                    BFSP_CLR,
                    BFSP_SORT
                ORDER BY 
                    BFSP_SORT
            """)
            
            results = session.execute(sql, {
                'order_cmp_columbus': DatabaseConstants.ORDER_CMP_COLUMBUS,
                'order_cmp_dallas': DatabaseConstants.ORDER_CMP_DALLAS,
                'order_cmp_younger': DatabaseConstants.ORDER_CMP_YOUNGER,
                'order_cmp_younger_eu': DatabaseConstants.ORDER_CMP_YOUNGER_EU,
                'proc_noncoat': DatabaseConstants.PROC_NON_COAT,
                'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT,
                'flg_not_shipped': DatabaseConstants.BRCP_FLG_NOT_SHIPPED
            }).fetchall()
            
            # 結果を整形
            formatted_results = []
            for row in results:
                formatted_results.append({
                    'sort': row.sort,
                    'base': row.base,
                    'adp': row.adp,
                    'lr': row.lr,
                    'clr': row.clr,
                    'prd_id': row.prd_id,
                    'nc_c': row.nc_c,
                    'nc_d': row.nc_d,
                    'hc_c': row.hc_c,
                    'hc_d': row.hc_d,
                    'hc_y': row.hc_y,
                    'hc_y_eu': row.hc_y_eu,
                    'nc_stock': row.nc_stock,
                    'hc_stock': row.hc_stock
                })
            
            return formatted_results
        except Exception as e:
            log_error(f"受注残サマリー取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def search_orders(order_date=None, product_id=None, base=None, adp=None, lr=None, clr=None, proc=None, order_company=None, order_no=None, zan_select=None):
        """受注データを検索"""
        session = get_db_session()
        try:
            # SQLクエリの構築
            query = """
                SELECT 
                    b.BRCP_ID,
                    b.BRCP_DT,
                    b.BRCP_PRD_ID,
                    case when b.BRCP_PROC = :proc_noncoat then 'NC' when b.BRCP_PROC = :proc_hardcoat then 'HC' end as BRCP_PROC,
                    b.BRCP_ORDER_CMP,
                    b.BRCP_ORDER_NO,
                    b.BRCP_QTY,
                    b.BRCP_FLG,
                    m.BFSP_BASE,
                    m.BFSP_ADP,
                    m.BFSP_LR,
                    m.BFSP_CLR,
                    m.BFSP_SORT,
                    z.CZTR_NM,
                    dbo.Get_ODR_ZAN_Qty_BF(b.BRCP_ID) as ZAN_QTY
                FROM BRCP_DAT b
                LEFT JOIN BFSP_MST m ON b.BRCP_PRD_ID = m.BFSP_PRD_ID
                LEFT JOIN CZTR_MST z ON b.BRCP_ORDER_CMP = z.CZTR_ID
                WHERE 1=1
            """
            params = {
                'proc_noncoat': DatabaseConstants.PROC_NON_COAT,
                'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT
            }
            
            # 検索条件の追加
            if order_date:
                query += " AND CONVERT(date, b.BRCP_DT) = :order_date"
                params['order_date'] = order_date
            if product_id:
                query += " AND b.BRCP_PRD_ID LIKE :product_id"
                params['product_id'] = f"%{product_id}%"
            if base:
                query += " AND m.BFSP_BASE = :base"
                params['base'] = base
            if adp:
                query += " AND m.BFSP_ADP = :adp"
                params['adp'] = adp
            if lr:
                query += " AND m.BFSP_LR = :lr"
                params['lr'] = lr
            if clr:
                query += " AND m.BFSP_CLR = :clr"
                params['clr'] = clr
            if proc:
                query += " AND b.BRCP_PROC = :proc"
                params['proc'] = proc
            if order_company:
                query += " AND b.BRCP_ORDER_CMP = :order_company"
                params['order_company'] = order_company
            if order_no:
                query += " AND b.BRCP_ORDER_NO = :order_no"
                params['order_no'] = order_no
            if zan_select:
                if zan_select == '0':
                    query += " AND dbo.Get_ODR_ZAN_Qty_BF(b.BRCP_ID) > :zan_qty_zero"
                    params['zan_qty_zero'] = 0
                elif zan_select == '1':
                    query += " AND dbo.Get_ODR_ZAN_Qty_BF(b.BRCP_ID) = :zan_qty_zero"
                    params['zan_qty_zero'] = 0
                params['zan_select'] = zan_select
                
            # 並び順
            query += " ORDER BY b.BRCP_DT ,b.BRCP_PROC, b.BRCP_ORDER_CMP, b.BRCP_PRD_ID"
            
            # クエリ実行
            result = session.execute(text(query), params)
            return result.fetchall()
            
        except Exception as e:
            raise e
        finally:
            session.close()

class BprdMei:
    """製造明細テーブルのモデルクラス"""
    
    @staticmethod
    def get_by_prd_id(prd_id):
        """製品IDに基づいて製造明細を取得する"""
        session = None
        try:
            session = get_db_session()
            records = session.query(BprdMeiModel).filter(
                BprdMeiModel.BPDM_PRD_ID == prd_id
            ).all()
            
            result = []
            for record in records:
                mei = {
                    'BPDM_ID': record.BPDM_ID,
                    'BPDM_PRD_ID': record.BPDM_PRD_ID,
                    'BPDM_LOT': record.BPDM_LOT,
                    'BPDM_NO': record.BPDM_NO,
                    'BPDM_QTY': record.BPDM_QTY
                }
                result.append(mei)
            
            return result
            
        except Exception as e:
            error_msg = f"製造明細の取得中にエラーが発生しました: {str(e)}"
            log_error(error_msg)
            raise Exception(error_msg)
        finally:
            if session:
                session.close()

    @staticmethod
    def create(prd_id, lot, no, qty):
        """製造明細を新規作成する"""
        session = None
        try:
            session = get_db_session()

            # 入力値の検証
            if not all([prd_id, lot, no, qty]):
                raise ValueError('すべての項目を入力してください。')

            try:
                lot = int(lot)
                no = int(no)
                qty = int(qty)
            except ValueError:
                raise ValueError('ロット、No、数量は数値で入力してください。')

            # 桁数チェック
            if lot < 0 or lot > 999999:
                raise ValueError('ロットは0から999999の範囲で入力してください。')
            if no < 0 or no > 9999:
                raise ValueError('Noは0から9999の範囲で入力してください。')
            if qty < 0 or qty > 99999:
                raise ValueError('数量は0から99999の範囲で入力してください。')

            # 製品IDの存在確認
            prd = session.query(BfspMstModel).filter(BfspMstModel.BFSP_PRD_ID == prd_id).first()
            if not prd:
                raise ValueError('指定された製品IDは存在しません。')

            # 重複チェック
            existing = session.query(BprdMeiModel).filter(
                BprdMeiModel.BPDM_PRD_ID == prd_id,
                BprdMeiModel.BPDM_LOT == lot,
                BprdMeiModel.BPDM_NO == no
            ).first()

            if existing:
                raise ValueError('同じ製品ID、ロット、Noの組み合わせが既に存在します。')

            # 新規データを作成
            new_mei = BprdMeiModel(
                BPDM_PRD_ID=prd_id,
                BPDM_LOT=lot,
                BPDM_NO=no,
                BPDM_QTY=qty
            )
            session.add(new_mei)

            # PrdDatModelも更新
            prd_dat = session.query(PrdDatModel).filter(
                PrdDatModel.BPDD_PRD_ID == prd_id,
                PrdDatModel.BPDD_LOT == lot
            ).first()

            if prd_dat:
                prd_dat.BPDD_QTY += qty
            else:
                new_prd_dat = PrdDatModel(
                    BPDD_PRD_ID=prd_id,
                    BPDD_LOT=lot,
                    BPDD_QTY=qty,
                    BPDD_FLG=DatabaseConstants.BPDD_FLG_NOT_SHIPPED,
                    BPDD_PROC=DatabaseConstants.PROC_NON_COAT
                )
                session.add(new_prd_dat)

            session.commit()
            log_error(f"製造明細を登録しました: 製品ID={prd_id}, ロット={lot}, No={no}, 数量={qty}")
            return True

        except Exception as e:
            if session:
                session.rollback()
            log_error(f"製造明細の登録中にエラーが発生: {str(e)}")
            raise

        finally:
            if session:
                session.close()


class PrdDat:
    """PRD_DATテーブルのモデルクラス"""
    
    @staticmethod
    def set_flg(id, flg, session=None):
        """PRD_DATテーブルのフラグを更新する"""
        session_flg = False
        if session is None:
            session = get_db_session()
            session_flg = True
        try:

            session.query(PrdDatModel).filter(PrdDatModel.BPDD_ID == id).update({'BPDD_FLG': flg})

            if session_flg:
                session.commit()
        except Exception as e:
            if session_flg:
                session.rollback()
            log_error(f"PRD_DATテーブルのフラグ更新中にエラーが発生: {str(e)}")
            raise
        finally:
            if session_flg:
                session.close()
    
    @staticmethod
    def search_noncoat_stock(product_id=None, lot=None, base=None, adp=None, lr=None, clr=None):
        """ノンコート在庫を検索する"""
        session = get_db_session()
        try:
            query = """
                SELECT 
                    BPDD_ID,
                    BPDD_LOT,
                    BFSP_BASE,
                    BFSP_ADP,
                    BFSP_LR,
                    BFSP_CLR,
                    ISNULL(dbo.Get_Zaiko_Qty_BF(BPDD_ID), 0) as stock_qty,
                    ISNULL(SUM(case when BRCP.BRCP_PROC = :proc_noncoat then dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID) else 0 end), 0) as order_remaining,
                    ISNULL(SUM(case when BRCP.BRCP_PROC = :proc_hardcoat then dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID) else 0 end), 0) as order_remaining2
                FROM BPRD_DAT AS BPRD
                INNER JOIN BFSP_MST AS BFSP
                    ON BPDD_PRD_ID = BFSP_PRD_ID
                LEFT OUTER JOIN BRCP_DAT AS BRCP
                    ON BPDD_PRD_ID = BRCP.BRCP_PRD_ID
                WHERE dbo.Get_Zaiko_Qty_BF(BPDD_ID) > 0
                    AND BPDD_PROC = :proc_noncoat
                    AND BPDD_FLG = :flg_not_shipped
            """
            
            params = {
                'proc_noncoat': DatabaseConstants.PROC_NON_COAT,
                'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT,
                'flg_not_shipped': DatabaseConstants.BPDD_FLG_NOT_SHIPPED
            }
            
            if product_id:
                query += " AND BPDD_PRD_ID = :product_id"
                params['product_id'] = product_id
                
            if lot:
                query += " AND BPDD_LOT = :lot"
                params['lot'] = lot

            if base:
                query += " AND BFSP_BASE = :base"
                params['base'] = base

            if adp:
                query += " AND BFSP_ADP = :adp"
                params['adp'] = adp

            if lr:
                query += " AND BFSP_LR = :lr"
                params['lr'] = lr

            if clr:
                query += " AND BFSP_CLR = :clr"
                params['clr'] = clr
                
            query += " GROUP BY BPDD_ID, BPDD_LOT, BFSP_BASE, BFSP_ADP, BFSP_LR, BFSP_CLR"
            query += " ORDER BY BFSP_CLR, BFSP_BASE, BFSP_ADP, BFSP_LR, BPDD_LOT"
            
            return session.execute(text(query), params).fetchall()
        except Exception as e:
            log_error(f"ノンコート在庫検索中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def search_hardcoat_stock(product_id=None, lot=None, base=None, adp=None, lr=None, clr=None):
        """ハードコート在庫を検索する"""
        session = get_db_session()
        try:
            query = """
                SELECT 
                    BPDD_ID,
                    BPDD_LOT,
                    BFSP_BASE,
                    BFSP_ADP,
                    BFSP_LR,
                    BFSP_CLR,
                    ISNULL(dbo.Get_Zaiko_Qty_BF(BPDD_ID), 0) as stock_qty,
                    ISNULL(SUM(dbo.Get_ODR_ZAN_Qty_BF(BRCP.BRCP_ID)), 0) as order_remaining,
                    BPDD_CRT
                FROM BPRD_DAT AS BPRD
                INNER JOIN BFSP_MST AS BFSP
                    ON BPDD_PRD_ID = BFSP_PRD_ID
                LEFT OUTER JOIN BRCP_DAT AS BRCP
                    ON BPDD_PRD_ID = BRCP.BRCP_PRD_ID
                    AND BRCP.BRCP_PROC = :proc_hardcoat
                WHERE dbo.Get_Zaiko_Qty_BF(BPDD_ID) > 0
                    AND BPDD_PROC = :proc_hardcoat
                    AND BPDD_FLG = :flg_not_shipped
            """
            
            params = {
                'proc_hardcoat': DatabaseConstants.PROC_HARD_COAT,
                'flg_not_shipped': DatabaseConstants.BPDD_FLG_NOT_SHIPPED
            }
            
            if product_id:
                query += " AND BPDD_PRD_ID = :product_id"
                params['product_id'] = product_id
                
            if lot:
                query += " AND BPDD_LOT = :lot"
                params['lot'] = lot

            if base:
                query += " AND BFSP_BASE = :base"
                params['base'] = base

            if adp:
                query += " AND BFSP_ADP = :adp"
                
                params['adp'] = adp

            if lr:
                query += " AND BFSP_LR = :lr"
                params['lr'] = lr
                
            if clr:
                query += " AND BFSP_CLR = :clr"
                params['clr'] = clr
                
            query += " GROUP BY BPDD_ID, BPDD_LOT, BFSP_BASE, BFSP_ADP, BFSP_LR, BFSP_CLR, BPDD_CRT"
            query += " ORDER BY BFSP_CLR, BFSP_BASE, BFSP_ADP, BFSP_LR, BPDD_LOT"
            
            return session.execute(text(query), params).fetchall()
        except Exception as e:
            log_error(f"ハードコート在庫検索中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()
                    
class BBcdDat(Base):
    """バーコードデータテーブル"""
    __tablename__ = 'BBCD_DAT'
    
    BBCD_ID = Column(String(20), primary_key=True, nullable=False, comment='ID')
    BBCD_NO = Column(String(60), nullable=True, comment='バーコード')
    BBCD_NM = Column(String(30), nullable=True, comment='バーコード名')
    BBCD_KBN = Column(Integer, primary_key=True, nullable=False, comment='区分')
    
    def __repr__(self):
        return f"<BBcdDat(BBCD_ID='{self.BBCD_ID}', BBCD_NO='{self.BBCD_NO}', BBCD_NM='{self.BBCD_NM}', BBCD_KBN='{self.BBCD_KBN}')>"

