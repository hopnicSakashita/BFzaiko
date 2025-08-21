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
from app.constants import DatabaseConstants, KbnConstants
from app.logger_utils import log_error


class CprgMstModel(Base):
    """加工集計グループマスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CPRG_MST'
    
    CPRG_ID = Column(String(5), primary_key=True)  # グループID
    CPRG_PRD_ID = Column(String(5), primary_key=True)  # 製品ID
    CPRG_PRC_ID = Column(Numeric(5, 0), primary_key=True)  # 加工ID
    CPRG_G_NM = Column(String(20, collation='Japanese_CI_AS'))  # グループ名
    CPRG_COL_NM = Column(String(20, collation='Japanese_CI_AS'))  # 列名
    CPRG_ROW_NM = Column(String(20, collation='Japanese_CI_AS'))  # 行名
    CPRG_AF_PRD_ID = Column(String(5))  # 加工後製品ID
    CPRG_COL_KEY = Column(Numeric(2, 0))  # 列キー
    CPRG_ROW_KEY = Column(Numeric(2, 0))  # 行キー
    
    @staticmethod
    def get_processing_matrix_data(cprg_id):
        """加工集計グループマスタをベースにしたマトリックスデータを取得する"""
        session = get_db_session()
        try:
            # 提供されたSQLクエリを実行
            sql = text("""
                SELECT CPRG_COL_KEY,CPRG_ROW_KEY,isnull(sum( dbo.Get_CPRD_ZAN_Qty(CPDD_ID)),0) as zaiko,0 as kakozan,0 as kakougozaiko
                FROM CPRG_MST
                LEFT OUTER JOIN CPRD_DAT ON CPDD_PRD_ID = CPRG_PRD_ID
                AND dbo.Get_CPRD_ZAN_Qty(CPDD_ID) > 0
                WHERE CPRG_ID = :cprg_id
                GROUP BY CPRG_COL_KEY,CPRG_ROW_KEY
                UNION ALL 
                SELECT CPRG_COL_KEY,CPRG_ROW_KEY,0  as zaiko,isnull(dbo.Get_CSHK_PRC_ZAN_Qty(CSHK_ID),0) as kakozan,0 as kakougozaiko
                FROM CPRG_MST 
                LEFT OUTER JOIN CSHK_DAT ON CSHK_PRD_ID = CPRG_PRD_ID
                AND CSHK_PRC_ID = CPRG_PRC_ID
                AND dbo.Get_CSHK_PRC_ZAN_Qty(CSHK_ID) > 0
                WHERE CPRG_ID = :cprg_id
                UNION ALL 
                SELECT CPRG_COL_KEY,CPRG_ROW_KEY,0  as zaiko, 0 as kakozan,isnull(dbo.Get_CPRD_ZAN_Qty(CPDD_ID),0) as kakougozaiko
                FROM CPRG_MST 
                LEFT OUTER JOIN CPRD_DAT ON CPDD_PRD_ID = CPRG_AF_PRD_ID
                AND dbo.Get_CPRD_ZAN_Qty(CPDD_ID) > 0
                WHERE CPRG_ID = :cprg_id
            """)
            
            results = session.execute(sql, {'cprg_id': cprg_id}).fetchall()
            
            # データを集計してマトリックス形式に変換
            matrix_data = {}
            for row in results:
                key = (row.CPRG_COL_KEY, row.CPRG_ROW_KEY)
                if key not in matrix_data:
                    matrix_data[key] = {
                        'zaiko': 0,
                        'kakozan': 0,
                        'kakougozaiko': 0
                    }
                
                matrix_data[key]['zaiko'] += int(row.zaiko) if row.zaiko else 0
                matrix_data[key]['kakozan'] += int(row.kakozan) if row.kakozan else 0
                matrix_data[key]['kakougozaiko'] += int(row.kakougozaiko) if row.kakougozaiko else 0
            
            return matrix_data
            
        except Exception as e:
            log_error(f"加工集計マトリックスデータ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_cprg_groups():
        """加工集計グループ一覧を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT CPRG_ID, CPRG_G_NM
                FROM CPRG_MST
                ORDER BY CPRG_ID
            """)
            
            results = session.execute(sql).fetchall()
            return results
            
        except Exception as e:
            log_error(f"加工集計グループ一覧取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_cprg_details(cprg_id):
        """指定されたグループIDの詳細情報を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CPRG_ID,
                    CPRG_PRD_ID,
                    CPRG_PRC_ID,
                    CPRG_G_NM,
                    CPRG_COL_NM,
                    CPRG_ROW_NM,
                    CPRG_AF_PRD_ID,
                    CPRG_COL_KEY,
                    CPRG_ROW_KEY,
                    PRD.PRD_DSP_NM AS PRD_NAME,
                    PRC.CPRC_NM AS PRC_NAME,
                    AF_PRD.PRD_DSP_NM AS AF_PRD_NAME
                FROM CPRG_MST CPRG
                LEFT JOIN PRD_MST PRD ON CPRG.CPRG_PRD_ID = PRD.PRD_ID
                LEFT JOIN CPRC_MST PRC ON CPRG.CPRG_PRC_ID = PRC.CPRC_ID
                LEFT JOIN PRD_MST AF_PRD ON CPRG.CPRG_AF_PRD_ID = AF_PRD.PRD_ID
                WHERE CPRG.CPRG_ID = :cprg_id
                ORDER BY CPRG.CPRG_PRD_ID, CPRG.CPRG_PRC_ID
            """)
            
            results = session.execute(sql, {'cprg_id': cprg_id}).fetchall()
            return results
            
        except Exception as e:
            log_error(f"加工集計グループ詳細取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    # 以下、CRUD操作のためのメソッドを追加
    @staticmethod
    def get_all():
        """すべての加工集計グループマスタリストを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CPRG_ID,
                    CPRG_PRD_ID,
                    CPRG_PRC_ID,
                    CPRG_G_NM,
                    CPRG_COL_NM,
                    CPRG_ROW_NM,
                    CPRG_AF_PRD_ID,
                    CPRG_COL_KEY,
                    CPRG_ROW_KEY,
                    PRD.PRD_DSP_NM AS PRD_NAME,
                    PRC.CPRC_NM AS PRC_NAME,
                    AF_PRD.PRD_DSP_NM AS AF_PRD_NAME
                FROM CPRG_MST CPRG
                LEFT JOIN PRD_MST PRD ON CPRG.CPRG_PRD_ID = PRD.PRD_ID
                LEFT JOIN CPRC_MST PRC ON CPRG.CPRG_PRC_ID = PRC.CPRC_ID
                LEFT JOIN PRD_MST AF_PRD ON CPRG.CPRG_AF_PRD_ID = AF_PRD.PRD_ID
                ORDER BY CPRG.CPRG_ID, CPRG.CPRG_PRD_ID, CPRG.CPRG_PRC_ID
            """)
            
            results = session.execute(sql).fetchall()
            
            result = []
            for r in results:
                cprg_data = {
                    'CPRG_ID': r.CPRG_ID,
                    'CPRG_PRD_ID': r.CPRG_PRD_ID,
                    'CPRG_PRC_ID': r.CPRG_PRC_ID,
                    'CPRG_G_NM': r.CPRG_G_NM,
                    'CPRG_COL_NM': r.CPRG_COL_NM,
                    'CPRG_ROW_NM': r.CPRG_ROW_NM,
                    'CPRG_AF_PRD_ID': r.CPRG_AF_PRD_ID,
                    'CPRG_COL_KEY': r.CPRG_COL_KEY,
                    'CPRG_ROW_KEY': r.CPRG_ROW_KEY,
                    'PRD_NAME': r.PRD_NAME,
                    'PRC_NAME': r.PRC_NAME,
                    'AF_PRD_NAME': r.AF_PRD_NAME
                }
                result.append(cprg_data)
            return result
        except Exception as e:
            log_error(f"加工集計グループマスタ一覧取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_by_id(cprg_id, cprg_prd_id, cprg_prc_id):
        """指定されたIDの加工集計グループマスタを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CPRG_ID,
                    CPRG_PRD_ID,
                    CPRG_PRC_ID,
                    CPRG_G_NM,
                    CPRG_COL_NM,
                    CPRG_ROW_NM,
                    CPRG_AF_PRD_ID,
                    CPRG_COL_KEY,
                    CPRG_ROW_KEY
                FROM CPRG_MST 
                WHERE CPRG_ID = :cprg_id 
                    AND CPRG_PRD_ID = :cprg_prd_id 
                    AND CPRG_PRC_ID = :cprg_prc_id
            """)
            
            result = session.execute(sql, {
                'cprg_id': cprg_id,
                'cprg_prd_id': cprg_prd_id,
                'cprg_prc_id': cprg_prc_id
            }).first()
            
            if result:
                return {
                    'CPRG_ID': result.CPRG_ID,
                    'CPRG_PRD_ID': result.CPRG_PRD_ID,
                    'CPRG_PRC_ID': result.CPRG_PRC_ID,
                    'CPRG_G_NM': result.CPRG_G_NM,
                    'CPRG_COL_NM': result.CPRG_COL_NM,
                    'CPRG_ROW_NM': result.CPRG_ROW_NM,
                    'CPRG_AF_PRD_ID': result.CPRG_AF_PRD_ID,
                    'CPRG_COL_KEY': result.CPRG_COL_KEY,
                    'CPRG_ROW_KEY': result.CPRG_ROW_KEY
                }
            return None
        except Exception as e:
            log_error(f"加工集計グループマスタ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def create(cprg_data):
        """加工集計グループマスタを新規作成する"""
        session = get_db_session()
        try:
            sql = text("""
                INSERT INTO CPRG_MST (
                    CPRG_ID, CPRG_PRD_ID, CPRG_PRC_ID, CPRG_G_NM, 
                    CPRG_COL_NM, CPRG_ROW_NM, CPRG_AF_PRD_ID, 
                    CPRG_COL_KEY, CPRG_ROW_KEY
                ) VALUES (
                    :cprg_id, :cprg_prd_id, :cprg_prc_id, :cprg_g_nm,
                    :cprg_col_nm, :cprg_row_nm, :cprg_af_prd_id,
                    :cprg_col_key, :cprg_row_key
                )
            """)
            
            session.execute(sql, cprg_data)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"加工集計グループマスタ作成中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update(cprg_id, cprg_prd_id, cprg_prc_id, cprg_data):
        """加工集計グループマスタを更新する"""
        session = get_db_session()
        try:
            sql = text("""
                UPDATE CPRG_MST 
                SET CPRG_G_NM = :cprg_g_nm,
                    CPRG_COL_NM = :cprg_col_nm,
                    CPRG_ROW_NM = :cprg_row_nm,
                    CPRG_AF_PRD_ID = :cprg_af_prd_id,
                    CPRG_COL_KEY = :cprg_col_key,
                    CPRG_ROW_KEY = :cprg_row_key
                WHERE CPRG_ID = :cprg_id 
                    AND CPRG_PRD_ID = :cprg_prd_id 
                    AND CPRG_PRC_ID = :cprg_prc_id
            """)
            
            cprg_data.update({
                'cprg_id': cprg_id,
                'cprg_prd_id': cprg_prd_id,
                'cprg_prc_id': cprg_prc_id
            })
            
            session.execute(sql, cprg_data)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"加工集計グループマスタ更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete(cprg_id, cprg_prd_id, cprg_prc_id):
        """加工集計グループマスタを削除する"""
        session = get_db_session()
        try:
            sql = text("""
                DELETE FROM CPRG_MST 
                WHERE CPRG_ID = :cprg_id 
                    AND CPRG_PRD_ID = :cprg_prd_id 
                    AND CPRG_PRC_ID = :cprg_prc_id
            """)
            
            session.execute(sql, {
                'cprg_id': cprg_id,
                'cprg_prd_id': cprg_prd_id,
                'cprg_prc_id': cprg_prc_id
            })
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"加工集計グループマスタ削除中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_group_choices():
        """加工集計グループIDの選択肢を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT CPRG_ID, CPRG_G_NM
                FROM CPRG_MST
                ORDER BY CPRG_ID
            """)
            
            results = session.execute(sql).fetchall()
            return [('', '選択してください')] + [(row.CPRG_ID, f"{row.CPRG_ID} - {row.CPRG_G_NM}") for row in results]
        except Exception as e:
            log_error(f"加工集計グループ選択肢取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_product_choices():
        """製品IDの選択肢を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT PRD_ID, PRD_DSP_NM
                FROM PRD_MST
                ORDER BY PRD_ID
            """)
            
            results = session.execute(sql).fetchall()
            return [('', '選択してください')] + [(row.PRD_ID, f"{row.PRD_ID} - {row.PRD_DSP_NM}") for row in results]
        except Exception as e:
            log_error(f"製品選択肢取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_process_choices():
        """加工IDの選択肢を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT CPRC_ID, CPRC_NM
                FROM CPRC_MST
                ORDER BY CPRC_ID
            """)
            
            results = session.execute(sql).fetchall()
            return [('', '選択してください')] + [(row.CPRC_ID, f"{row.CPRC_ID} - {row.CPRC_NM}") for row in results]
        except Exception as e:
            log_error(f"加工選択肢取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_process_choices_by_product(prd_id):
        """製品IDに紐づく加工IDの選択肢を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT CPRC_ID, CPRC_NM
                FROM CPRC_MST
                WHERE CPRC_PRD_ID = :prd_id
                ORDER BY CPRC_ID
            """)
            
            results = session.execute(sql, {'prd_id': prd_id}).fetchall()
            return [('', '選択してください')] + [(row.CPRC_ID, f"{row.CPRC_ID} - {row.CPRC_NM}") for row in results]
        except Exception as e:
            log_error(f"製品別加工選択肢取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

