import logging
from decimal import Decimal
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
from app.models_master import PrdMstModel
from app.logger_utils import log_error, log_info

class CprdDatModel(Base):
    """入庫データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CPRD_DAT'
    
    CPDD_ID = Column(Numeric(10, 0), primary_key=True, autoincrement=True)  # 製造ID
    CPDD_PRD_ID = Column(String(5))  # 製品ID
    CPDD_LOT = Column(Numeric(6, 0))  # LOT
    CPDD_SPRIT1 = Column(Numeric(2, 0))  # 分割番号1
    CPDD_SPRIT2 = Column(Numeric(2, 0))  # 分割番号2
    CPDD_RANK = Column(Numeric(2, 0))  # ランク
    CPDD_QTY = Column(Numeric(5, 0))  # 数量
    CPDD_FLG = Column(Numeric(1, 0))  # フラグ
    CPDD_PCD_ID = Column(Numeric(10, 0))  # 加工ID
    
    @staticmethod
    def get_all():
        """すべての入庫データを取得する"""
        session = get_db_session()
        try:
            sql = text(f"""
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    c.CPDD_PCD_ID,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME
                FROM CPRD_DAT c
                LEFT OUTER JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT OUTER JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id AND k.KBN_NO = c.CPDD_RANK
                ORDER BY c.CPDD_ID DESC
            """)
            
            params = {
                'rank_kbn_id': KbnConstants.KBN_ID_RANK
            }
            
            results = session.execute(sql, params).fetchall()
            
            result = []
            for r in results:
                cprd = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'CPDD_FLG': r.CPDD_FLG,
                    'CPDD_PCD_ID': r.CPDD_PCD_ID,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME
                }
                result.append(cprd)
            return result
        except OperationalError as e:
            log_error("データベースへの接続に失敗しました", e)
            raise Exception("データベースへの接続に失敗しました。システム管理者に連絡してください。")
        except SQLAlchemyError as e:
            log_error("データベースの操作中にエラーが発生しました", e)
            raise Exception("データベースの操作中にエラーが発生しました。")
        except Exception as e:
            log_error("予期せぬエラーが発生しました", e)
            raise Exception("予期せぬエラーが発生しました。")
        finally:
            session.close()
    
    @staticmethod
    def get_by_id(cpdd_id):
        """IDで入庫データを取得"""
        session = get_db_session()
        try:
            result = session.query(CprdDatModel).filter(CprdDatModel.CPDD_ID == cpdd_id).first()
            return result
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
    
    @staticmethod
    def get_by_id_with_details(cpdd_id):
        """IDで入庫データを詳細情報（製品名、ランク名、在庫残数）付きで取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME,
                    dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) as ZAIKO_ZAN
                FROM CPRD_DAT c
                LEFT JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id 
                                   AND k.KBN_NO = c.CPDD_RANK 
                                   AND k.KBN_FLG = :kbn_flg_active
                WHERE c.CPDD_ID = :cpdd_id
            """)
            
            params = {
                'cpdd_id': cpdd_id,
                'rank_kbn_id': KbnConstants.KBN_ID_RANK,
                'kbn_flg_active': KbnConstants.KBN_FLG_ACTIVE
            }
            
            result = session.execute(sql, params).fetchone()
            
            if result:
                cprd_data = {
                    'CPDD_ID': result.CPDD_ID,
                    'CPDD_PRD_ID': result.CPDD_PRD_ID,
                    'CPDD_LOT': result.CPDD_LOT,
                    'CPDD_SPRIT1': result.CPDD_SPRIT1,
                    'CPDD_SPRIT2': result.CPDD_SPRIT2,
                    'CPDD_RANK': result.CPDD_RANK,
                    'CPDD_QTY': result.CPDD_QTY,
                    'CPDD_FLG': result.CPDD_FLG,
                    'PRD_DSP_NM': result.PRD_DSP_NM,
                    'RANK_NAME': result.RANK_NAME,
                    'ZAIKO_ZAN': result.ZAIKO_ZAN or 0
                }
                return cprd_data
            
            return None
            
        except Exception as e:
            logging.error(f"入庫データの詳細取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def create(prd_id, lot, sprit1, sprit2, rank, qty, flg=0):
        """入庫データを新規作成する"""
        session = get_db_session()
        try:
            # 入力値の検証
            if not all([prd_id, lot is not None, rank is not None, qty is not None]):
                raise ValueError('必須項目を入力してください。')

            try:
                lot = int(lot)
                # 分割番号は空の場合は0
                sprit1 = int(sprit1) if sprit1 else 0
                sprit2 = int(sprit2) if sprit2 else 0
                rank = int(rank)
                qty = int(qty)
                # フラグは常に0
                flg = DatabaseConstants.FLG_ACTIVE
            except ValueError:
                raise ValueError('数値項目は正しい数値で入力してください。')

            # 桁数チェック
            if len(prd_id) > 5:
                raise ValueError('製品IDは5文字以内で入力してください。')
            if lot < 0 or lot > 999999:
                raise ValueError('LOTは0から999999の範囲で入力してください。')
            if sprit1 < 0 or sprit1 > 99:
                raise ValueError('分割番号1は0から99の範囲で入力してください。')
            if sprit2 < 0 or sprit2 > 99:
                raise ValueError('分割番号2は0から99の範囲で入力してください。')
            if rank < 0 or rank > 99:
                raise ValueError('ランクは0から99の範囲で入力してください。')
            if qty < 0 or qty > 99999:
                raise ValueError('数量は0から99999の範囲で入力してください。')
            if flg < 0 or flg > 9:
                raise ValueError('フラグは0から9の範囲で入力してください。')

            # 製品IDの存在確認（PRD_MSTテーブルがある場合）
            prd = session.query(PrdMstModel).filter(PrdMstModel.PRD_ID == prd_id).first()
            if not prd:
                logging.warning(f"製品ID {prd_id} はPRD_MSTに存在しませんが、データを登録します。")

            # 新規データを作成
            new_cprd = CprdDatModel(
                CPDD_PRD_ID=prd_id,
                CPDD_LOT=lot,
                CPDD_SPRIT1=sprit1,
                CPDD_SPRIT2=sprit2,
                CPDD_RANK=rank,
                CPDD_QTY=qty,
                CPDD_FLG=flg,
                CPDD_PCD_ID=0
            )
            session.add(new_cprd)
            session.commit()
            
            logging.info(f"入庫データを登録しました: 製品ID={prd_id}, LOT={lot}, 分割1={sprit1}, 分割2={sprit2}, ランク={rank}, 数量={qty}")
            return new_cprd.CPDD_ID

        except Exception as e:
            session.rollback()
            logging.error(f"入庫データの登録中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def update(cpdd_id, prd_id, lot, sprit1, sprit2, rank, qty, flg=0):
        """入庫データを更新する"""
        session = get_db_session()
        try:
            # 既存データを取得
            cprd = session.query(CprdDatModel).filter(CprdDatModel.CPDD_ID == cpdd_id).first()
            if not cprd:
                raise ValueError('指定された入庫データが見つかりません。')

            # フラグが0以外の場合は編集不可
            if cprd.CPDD_FLG != DatabaseConstants.FLG_ACTIVE:
                raise ValueError('このデータは編集できません。（フラグが0以外）')

            # 入力値の検証
            if not all([prd_id, lot is not None, rank is not None, qty is not None]):
                raise ValueError('必須項目を入力してください。')

            try:
                lot = int(lot)
                # 分割番号は空の場合は0
                sprit1 = int(sprit1) if sprit1 else 0
                sprit2 = int(sprit2) if sprit2 else 0
                rank = int(rank)
                qty = int(qty)
                # フラグは既存値を維持（通常は0）
                flg = cprd.CPDD_FLG
            except ValueError:
                raise ValueError('数値項目は正しい数値で入力してください。')

            # 桁数チェック
            if len(prd_id) > 5:
                raise ValueError('製品IDは5文字以内で入力してください。')
            if lot < 0 or lot > 999999:
                raise ValueError('LOTは0から999999の範囲で入力してください。')
            if sprit1 < 0 or sprit1 > 99:
                raise ValueError('分割番号1は0から99の範囲で入力してください。')
            if sprit2 < 0 or sprit2 > 99:
                raise ValueError('分割番号2は0から99の範囲で入力してください。')
            if rank < 0 or rank > 99:
                raise ValueError('ランクは0から99の範囲で入力してください。')
            if qty < 0 or qty > 99999:
                raise ValueError('数量は0から99999の範囲で入力してください。')
            if flg < 0 or flg > 9:
                raise ValueError('フラグは0から9の範囲で入力してください。')

            # データを更新
            cprd.CPDD_PRD_ID = prd_id
            cprd.CPDD_LOT = lot
            cprd.CPDD_SPRIT1 = sprit1
            cprd.CPDD_SPRIT2 = sprit2
            cprd.CPDD_RANK = rank
            cprd.CPDD_QTY = qty
            cprd.CPDD_FLG = flg
            # CPDD_PCD_IDは既存値を維持（更新しない）

            session.commit()
            logging.info(f"入庫データを更新しました: ID={cpdd_id}")
            return True

        except Exception as e:
            session.rollback()
            logging.error(f"入庫データの更新中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def delete(cpdd_id):
        """入庫データを削除する"""
        session = get_db_session()
        try:
            # 既存データを取得
            cprd = session.query(CprdDatModel).filter(CprdDatModel.CPDD_ID == cpdd_id).first()
            if not cprd:
                raise ValueError('指定された入庫データが見つかりません。')

            # フラグが0以外の場合は削除不可
            if cprd.CPDD_FLG != DatabaseConstants.FLG_ACTIVE:
                raise ValueError('このデータは削除できません。（フラグが0以外）')

            # データを削除
            session.delete(cprd)
            session.commit()
            logging.info(f"入庫データを削除しました: ID={cpdd_id}")
            return True

        except Exception as e:
            session.rollback()
            logging.error(f"入庫データの削除中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def search(prd_id=None, lot=None, rank=None, flg=None):
        """入庫データを検索する"""
        session = get_db_session()
        try:
            sql = f"""
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    c.CPDD_PCD_ID,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME
                FROM CPRD_DAT c
                LEFT OUTER JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT OUTER JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id AND k.KBN_NO = c.CPDD_RANK
                WHERE 1=1
            """
            
            params = {}
            params['rank_kbn_id'] = KbnConstants.KBN_ID_RANK
            
            # 検索条件を適用
            if prd_id:
                sql += " AND c.CPDD_PRD_ID LIKE :prd_id"
                params['prd_id'] = f'%{prd_id}%'
            if lot is not None:
                sql += " AND c.CPDD_LOT = :lot"
                params['lot'] = lot
            if rank is not None:
                sql += " AND c.CPDD_RANK = :rank"
                params['rank'] = rank
            if flg is not None:
                sql += " AND c.CPDD_FLG = :flg"
                params['flg'] = flg
            
            # 並び順を設定
            sql += " ORDER BY c.CPDD_ID DESC"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                cprd = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'CPDD_FLG': r.CPDD_FLG,
                    'CPDD_PCD_ID': r.CPDD_PCD_ID,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME
                }
                result.append(cprd)
            return result
            
        except Exception as e:
            logging.error(f"入庫データの検索中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_cprd_id_list():
        """入庫IDと製品名のリストを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    p.PRD_DSP_NM
                FROM CPRD_DAT c
                LEFT OUTER JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                ORDER BY c.CPDD_ID DESC
            """)
            
            results = session.execute(sql).fetchall()
            
            result = []
            for r in results:
                cprd = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID or '',
                    'PRD_DSP_NM': r.PRD_DSP_NM or ''
                }
                result.append(cprd)
            return result
            
        except Exception as e:
            logging.error(f"入庫IDリストの取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def search_with_cprd_id(cprd_id=None, prd_id=None, lot=None, rank=None):
        """入庫データを検索する（入庫ID指定対応）"""
        session = get_db_session()
        try:
            sql = f"""
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    c.CPDD_PCD_ID,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME
                FROM CPRD_DAT c
                LEFT OUTER JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT OUTER JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id AND k.KBN_NO = c.CPDD_RANK
                WHERE 1=1
            """
            
            params = {}
            params['rank_kbn_id'] = KbnConstants.KBN_ID_RANK
            
            # 検索条件を適用
            if cprd_id is not None:
                sql += " AND c.CPDD_ID = :cprd_id"
                params['cprd_id'] = cprd_id
            if prd_id:
                sql += " AND c.CPDD_PRD_ID LIKE :prd_id"
                params['prd_id'] = f'%{prd_id}%'
            if lot is not None:
                sql += " AND c.CPDD_LOT = :lot"
                params['lot'] = lot
            if rank is not None:
                sql += " AND c.CPDD_RANK = :rank"
                params['rank'] = rank
            
            # 並び順を設定
            sql += " ORDER BY c.CPDD_ID DESC"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                cprd = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'CPDD_FLG': r.CPDD_FLG,
                    'CPDD_PCD_ID': r.CPDD_PCD_ID,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME
                }
                result.append(cprd)
            return result
            
        except Exception as e:
            logging.error(f"入庫データの検索中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_zaiko_zan(cpdd_id):
        """指定された入庫IDの在庫残数量を取得する"""
        session = get_db_session()
        try:
            sql = text("SELECT dbo.Get_CPRD_ZAN_Qty(:cpdd_id) AS zaiko_zan")
            result = session.execute(sql, {'cpdd_id': cpdd_id}).fetchone()
            return result.zaiko_zan if result else 0
        except Exception as e:
            logging.error(f"在庫残数量の取得中にエラーが発生: {str(e)}")
            return 0
        finally:
            session.close()
    
    @staticmethod
    def get_all_with_zaiko_zan():
        """すべての入庫データを在庫残数量付きで取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    c.CPDD_PCD_ID,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME,
                    dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) AS ZAIKO_ZAN
                FROM CPRD_DAT c
                LEFT OUTER JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT OUTER JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id AND k.KBN_NO = c.CPDD_RANK
                ORDER BY c.CPDD_ID DESC
            """)
            
            params = {
                'rank_kbn_id': KbnConstants.KBN_ID_RANK
            }
            
            results = session.execute(sql, params).fetchall()
            
            result = []
            for r in results:
                cprd = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'CPDD_FLG': r.CPDD_FLG,
                    'CPDD_PCD_ID': r.CPDD_PCD_ID,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME,
                    'ZAIKO_ZAN': r.ZAIKO_ZAN if r.ZAIKO_ZAN is not None else 0
                }
                result.append(cprd)
            return result
        except OperationalError as e:
            log_error("データベースへの接続に失敗しました", e)
            raise Exception("データベースへの接続に失敗しました。システム管理者に連絡してください。")
        except SQLAlchemyError as e:
            log_error("データベースの操作中にエラーが発生しました", e)
            raise Exception("データベースの操作中にエラーが発生しました。")
        except Exception as e:
            log_error("予期せぬエラーが発生しました", e)
            raise Exception("予期せぬエラーが発生しました。")
        finally:
            session.close()
    
    @staticmethod
    def search_with_zaiko_zan(prd_id=None, prd_name=None, lot=None, rank=None, stock_status=None, flg=None):
        """入庫データを在庫残数量付きで検索する"""
        session = get_db_session()
        try:
            sql = f"""
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    c.CPDD_PCD_ID,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME,
                    dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) AS ZAIKO_ZAN
                FROM CPRD_DAT c
                LEFT OUTER JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT OUTER JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id AND k.KBN_NO = c.CPDD_RANK
                WHERE 1=1
            """
            
            params = {}
            params['rank_kbn_id'] = KbnConstants.KBN_ID_RANK
            
            # 検索条件を適用
            if prd_id:
                sql += " AND c.CPDD_PRD_ID LIKE :prd_id"
                params['prd_id'] = f'%{prd_id}%'
            if prd_name:
                sql += " AND p.PRD_DSP_NM LIKE :prd_name"
                params['prd_name'] = f'%{prd_name}%'
            if lot is not None:
                sql += " AND c.CPDD_LOT = :lot"
                params['lot'] = lot
            if rank is not None:
                sql += " AND c.CPDD_RANK = :rank"
                params['rank'] = rank
            if flg is not None:
                sql += " AND c.CPDD_FLG = :flg"
                params['flg'] = flg
            
            # 在庫状況の検索条件を適用
            if stock_status is not None:
                if stock_status == 1:  # 在庫あり
                    sql += " AND dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) > 0"
                elif stock_status == 0:  # 在庫なし
                    sql += " AND (dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) = 0 OR dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) IS NULL)"
            
            # 並び順を設定
            sql += " ORDER BY c.CPDD_ID DESC"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                cprd = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'CPDD_FLG': r.CPDD_FLG,
                    'CPDD_PCD_ID': r.CPDD_PCD_ID,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME,
                    'ZAIKO_ZAN': r.ZAIKO_ZAN if r.ZAIKO_ZAN is not None else 0
                }
                result.append(cprd)
            return result
            
        except Exception as e:
            log_error("入庫データの検索中にエラーが発生", e)
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_stock_detail(prd_id, rank):
        """指定された製品IDとランクの在庫詳細データを取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    c.CPDD_ID,
                    c.CPDD_PRD_ID,
                    c.CPDD_LOT,
                    c.CPDD_SPRIT1,
                    c.CPDD_SPRIT2,
                    c.CPDD_RANK,
                    c.CPDD_QTY,
                    c.CPDD_FLG,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME,
                    dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) as ZAIKO_ZAN
                FROM CPRD_DAT c
                LEFT JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id 
                                   AND k.KBN_NO = c.CPDD_RANK 
                                   AND k.KBN_FLG = :kbn_flg_active
                WHERE c.CPDD_FLG = :flg_active
                  AND c.CPDD_PRD_ID = :prd_id
                  AND c.CPDD_RANK = :rank
                  AND dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID) > 0
                ORDER BY c.CPDD_ID DESC
            """
            
            params = {
                'rank_kbn_id': KbnConstants.KBN_ID_RANK,
                'kbn_flg_active': KbnConstants.KBN_FLG_ACTIVE,
                'flg_active': DatabaseConstants.FLG_ACTIVE,
                'prd_id': prd_id,
                'rank': rank
            }
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                stock_detail = {
                    'CPDD_ID': r.CPDD_ID,
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_LOT': r.CPDD_LOT,
                    'CPDD_SPRIT1': r.CPDD_SPRIT1,
                    'CPDD_SPRIT2': r.CPDD_SPRIT2,
                    'CPDD_RANK': r.CPDD_RANK,
                    'CPDD_QTY': r.CPDD_QTY,
                    'CPDD_FLG': r.CPDD_FLG,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME,
                    'ZAIKO_ZAN': r.ZAIKO_ZAN or 0
                }
                result.append(stock_detail)
            return result
            
        except Exception as e:
            logging.error(f"在庫詳細の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_stock_summary(prd_id=None, rank=None):
        """製品IDとランクで在庫を集計した一覧を取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    c.CPDD_PRD_ID,
                    c.CPDD_RANK,
                    p.PRD_DSP_NM,
                    k.KBN_NM as RANK_NAME,
                    COUNT(c.CPDD_ID) as STOCK_COUNT,
                    SUM(c.CPDD_QTY) as TOTAL_IN_QTY,
                    SUM(ISNULL(dbo.Get_CPRD_ZAN_Qty(c.CPDD_ID), 0)) as TOTAL_ZAIKO_ZAN
                FROM CPRD_DAT c
                LEFT JOIN PRD_MST p ON c.CPDD_PRD_ID = p.PRD_ID
                LEFT JOIN KBN_MST k ON k.KBN_ID = :rank_kbn_id 
                                   AND k.KBN_NO = c.CPDD_RANK 
                                   AND k.KBN_FLG = :kbn_flg_active
                WHERE c.CPDD_FLG = :flg_active
            """
            
            params = {
                'rank_kbn_id': KbnConstants.KBN_ID_RANK,
                'kbn_flg_active': KbnConstants.KBN_FLG_ACTIVE,
                'flg_active': DatabaseConstants.FLG_ACTIVE
            }
            
            # 検索条件を追加
            if prd_id:
                sql += " AND c.CPDD_PRD_ID LIKE :prd_id"
                params['prd_id'] = f'%{prd_id}%'
            if rank is not None:
                sql += " AND c.CPDD_RANK = :rank"
                params['rank'] = rank
            
            # グループ化と並び順を設定
            sql += " GROUP BY c.CPDD_PRD_ID, c.CPDD_RANK, p.PRD_DSP_NM, k.KBN_NM"
            sql += " ORDER BY c.CPDD_PRD_ID, c.CPDD_RANK"
            
            results = session.execute(text(sql), params).fetchall()
            
            result = []
            for r in results:
                stock_summary = {
                    'CPDD_PRD_ID': r.CPDD_PRD_ID,
                    'CPDD_RANK': r.CPDD_RANK,
                    'PRD_DSP_NM': r.PRD_DSP_NM,
                    'RANK_NAME': r.RANK_NAME,
                    'STOCK_COUNT': r.STOCK_COUNT,
                    'TOTAL_IN_QTY': r.TOTAL_IN_QTY,
                    'TOTAL_ZAIKO_ZAN': r.TOTAL_ZAIKO_ZAN or 0
                }
                result.append(stock_summary)
            return result
            
        except Exception as e:
            logging.error(f"在庫集計の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()


class CshkDatModel(Base):
    """出荷データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CSHK_DAT'
    
    CSHK_ID = Column(Numeric(10, 0), primary_key=True, autoincrement=True)  # 出荷ID
    CSHK_KBN = Column(Numeric(1, 0))  # 出荷区分
    CSHK_TO = Column(Numeric(3, 0))  # 出荷先ID
    CSHK_PRC_ID = Column(Numeric(10, 0))  # 加工ID
    CSHK_PRD_ID = Column(String(5))  # 製品ID
    CSHK_DT = Column(DateTime)  # 出荷日
    CSHK_ORD_DT = Column(DateTime)  # 注文日
    CSHK_PDD_ID = Column(Numeric(10, 0))  # 製造ID
    CSHK_RCP_ID = Column(Numeric(10, 0))  # 受注ID
    CSHK_QTY = Column(Numeric(5, 0))  # 数量
    CSHK_FLG = Column(Numeric(1, 0))  # フラグ
    

class CprcDatModel(Base):
    """加工データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CPRC_DAT'
    
    CPCD_ID = Column(Numeric(10, 0), primary_key=True, autoincrement=True)  # ID
    CPCD_SHK_ID = Column(Numeric(10, 0))  # 出荷ID
    CPCD_DATE = Column(DateTime)  # 戻り日
    CPCD_QTY = Column(Numeric(5, 0))  # 戻り数
    CPCD_RET_NG_QTY = Column(Numeric(5, 0))  # 戻り不良数
    CPCD_INS_NG_QTY = Column(Numeric(5, 0))  # 検品不良数
    CPCD_PASS_QTY = Column(Numeric(5, 0))  # 合格数
    
    @staticmethod
    def get_all():
        """すべての加工データを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    c.CPCD_ID,
                    c.CPCD_SHK_ID,
                    c.CPCD_DATE,
                    c.CPCD_QTY,
                    c.CPCD_RET_NG_QTY,
                    c.CPCD_INS_NG_QTY,
                    c.CPCD_PASS_QTY,
                    s.CSHK_PRD_ID,
                    s.CSHK_PDD_ID
                FROM CPRC_DAT c
                LEFT OUTER JOIN CSHK_DAT s ON c.CPCD_SHK_ID = s.CSHK_ID
                ORDER BY c.CPCD_ID DESC
            """)
            
            results = session.execute(sql).fetchall()
            
            result = []
            for r in results:
                cprc = {
                    'CPCD_ID': r.CPCD_ID,
                    'CPCD_SHK_ID': r.CPCD_SHK_ID,
                    'CPCD_DATE': r.CPCD_DATE.strftime('%Y-%m-%d') if r.CPCD_DATE else '',
                    'CPCD_QTY': r.CPCD_QTY,
                    'CPCD_RET_NG_QTY': r.CPCD_RET_NG_QTY,
                    'CPCD_INS_NG_QTY': r.CPCD_INS_NG_QTY,
                    'CPCD_PASS_QTY': r.CPCD_PASS_QTY,
                    'CSHK_PRD_ID': r.CSHK_PRD_ID,
                    'CSHK_PDD_ID': r.CSHK_PDD_ID
                }
                result.append(cprc)
            return result
        except Exception as e:
            logging.error(f"加工データの取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def create(shk_id, date, qty, ret_ng_qty, ins_ng_qty, pass_qty):
        """加工データを新規作成する"""
        session = get_db_session()
        try:
            
            # 入力値の検証
            if not all([shk_id, date, qty is not None]):
                error_msg = '必須項目を入力してください。'
                log_error(error_msg)
                raise ValueError(error_msg)

            try:
                shk_id = int(shk_id)
                qty = int(qty)
                ret_ng_qty = int(ret_ng_qty) if ret_ng_qty else 0
                ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
                pass_qty = int(pass_qty) if pass_qty else 0
                # 日付文字列をdatetimeに変換
                if isinstance(date, str):
                    date = datetime.strptime(date, '%Y-%m-%d')
            except ValueError as e:
                error_msg = '数値項目は正しい数値で入力してください。'
                log_error(error_msg)
                raise ValueError(error_msg)

            # 桁数チェック
            if qty <= 0 or qty > 99999:
                error_msg = '戻り数は1から99999の範囲で入力してください。'
                log_error(error_msg)
                raise ValueError(error_msg)
            if ret_ng_qty < 0 or ret_ng_qty > 99999:
                error_msg = '戻り不良数は0から99999の範囲で入力してください。'
                log_error(error_msg)
                raise ValueError(error_msg)
            if ins_ng_qty < 0 or ins_ng_qty > 99999:
                error_msg = '検品不良数は0から99999の範囲で入力してください。'
                log_error(error_msg)
                raise ValueError(error_msg)
            if pass_qty < 0 or pass_qty > 99999:
                error_msg = '合格数は0から99999の範囲で入力してください。'
                log_error(error_msg)
                raise ValueError(error_msg)

            # 出荷データの存在確認
            cshk = session.query(CshkDatModel).filter(CshkDatModel.CSHK_ID == shk_id).first()
            if not cshk:
                error_msg = '指定された出荷データが見つかりません。'
                log_error(error_msg)
                raise ValueError(error_msg)

            # SQL直接実行で新規データを作成
            insert_sql = text("""
                INSERT INTO CPRC_DAT (CPCD_SHK_ID, CPCD_DATE, CPCD_QTY, CPCD_RET_NG_QTY, CPCD_INS_NG_QTY, CPCD_PASS_QTY)
                VALUES (:shk_id, :date, :qty, :ret_ng_qty, :ins_ng_qty, :pass_qty)
            """)
            
            insert_params = {
                'shk_id': shk_id,
                'date': date,
                'qty': qty,
                'ret_ng_qty': ret_ng_qty,
                'ins_ng_qty': ins_ng_qty,
                'pass_qty': pass_qty
            }
            
            session.execute(insert_sql, insert_params)
            
            # 作成されたIDを取得
            id_sql = text("SELECT IDENT_CURRENT('CPRC_DAT') as CPCD_ID")
            id_result = session.execute(id_sql).fetchone()
            new_cprc_id = int(id_result.CPCD_ID)
            
            # 合格数が0より大きい場合、CPRD_DATを作成
            if pass_qty > 0:
                
                # 出荷データから元の製造データの情報を取得（同じセッション内で実行）
                cshk_detail_sql = text("""
                    SELECT 
                        c.CSHK_ID, c.CSHK_KBN, c.CSHK_TO, c.CSHK_PRC_ID, c.CSHK_PRD_ID, 
                        c.CSHK_DT, c.CSHK_ORD_DT, c.CSHK_PDD_ID, c.CSHK_RCP_ID, c.CSHK_QTY, c.CSHK_FLG,
                        p.PRD_DSP_NM, pr.CPRC_NM, pd.CPDD_LOT, pd.CPDD_RANK,
                        CASE pd.CPDD_RANK 
                            WHEN 1 THEN 'A'
                            WHEN 2 THEN 'B'
                            WHEN 3 THEN 'C'
                            WHEN 4 THEN 'D'
                            ELSE '不明'
                        END as RANK_NAME
                    FROM CSHK_DAT c
                    LEFT JOIN PRD_MST p ON c.CSHK_PRD_ID = p.PRD_ID
                    LEFT JOIN CPRC_MST pr ON c.CSHK_PRC_ID = pr.CPRC_ID
                    LEFT JOIN CPRD_DAT pd ON c.CSHK_PDD_ID = pd.CPDD_ID
                    WHERE c.CSHK_ID = :cshk_id
                """)
                cshk_detail_result = session.execute(cshk_detail_sql, {'cshk_id': shk_id}).fetchone()
                
                if cshk_detail_result:
                    cshk_detail = {
                        'CSHK_ID': cshk_detail_result.CSHK_ID,
                        'CSHK_KBN': cshk_detail_result.CSHK_KBN,
                        'CSHK_TO': cshk_detail_result.CSHK_TO,
                        'CSHK_PRC_ID': cshk_detail_result.CSHK_PRC_ID,
                        'CSHK_PRD_ID': cshk_detail_result.CSHK_PRD_ID,
                        'CSHK_DT': cshk_detail_result.CSHK_DT,
                        'CSHK_ORD_DT': cshk_detail_result.CSHK_ORD_DT,
                        'CSHK_PDD_ID': cshk_detail_result.CSHK_PDD_ID,
                        'CSHK_RCP_ID': cshk_detail_result.CSHK_RCP_ID,
                        'CSHK_QTY': cshk_detail_result.CSHK_QTY,
                        'CSHK_FLG': cshk_detail_result.CSHK_FLG,
                        'PRD_DSP_NM': cshk_detail_result.PRD_DSP_NM,
                        'CPRC_NM': cshk_detail_result.CPRC_NM,
                        'CPDD_LOT': cshk_detail_result.CPDD_LOT,
                        'CPDD_RANK': cshk_detail_result.CPDD_RANK,
                        'RANK_NAME': cshk_detail_result.RANK_NAME
                    }
                else:
                    error_msg = f"出荷データの詳細が取得できません: CSHK_ID={shk_id}"
                    log_error(error_msg)
                    raise ValueError(error_msg)
                
                if cshk_detail and cshk_detail.get('CSHK_PDD_ID'):
                    # 加工マスタから加工後製品IDを取得
                    cprc_sql = text("SELECT CPRC_AF_PRD_ID, CPRC_NM FROM CPRC_MST WHERE CPRC_ID = :cprc_id")
                    cprc_result = session.execute(cprc_sql, {'cprc_id': cshk_detail['CSHK_PRC_ID']}).fetchone()
                    
                    if not cprc_result:
                        error_msg = f"加工マスタが見つかりません: CPRC_ID={cshk_detail['CSHK_PRC_ID']}"
                        log_error(error_msg)
                        raise ValueError(error_msg)
                    
                    if not cprc_result.CPRC_AF_PRD_ID:
                        # 加工後製品IDが設定されていない場合はエラー
                        error_msg = f"加工マスタ（CPRC_ID={cshk_detail['CSHK_PRC_ID']}, 加工名={cprc_result.CPRC_NM}）に加工後製品IDが設定されていません。"
                        log_error(error_msg)
                        raise ValueError(error_msg)
                    else:
                        # 加工後製品IDが設定されている場合
                        
                        # 元の製造データからランクを取得
                        cprd_sql = text("SELECT CPDD_RANK FROM CPRD_DAT WHERE CPDD_ID = :cpdd_id")
                        cprd_result = session.execute(cprd_sql, {'cpdd_id': cshk_detail['CSHK_PDD_ID']}).fetchone()
                        
                        if cprd_result:
                            # 戻り日をYYMMDD形式に変換
                            lot_number = int(date.strftime('%y%m%d'))
                            
                            # CPRD_DATを作成
                            cprd_insert_sql = text("""
                                INSERT INTO CPRD_DAT (CPDD_PRD_ID, CPDD_LOT, CPDD_SPRIT1, CPDD_SPRIT2, CPDD_RANK, CPDD_QTY, CPDD_FLG, CPDD_PCD_ID)
                                VALUES (:prd_id, :lot, :sprit1, :sprit2, :rank, :qty, :flg, :pcd_id)
                            """)
                            
                            cprd_params = {
                                'prd_id': cprc_result.CPRC_AF_PRD_ID,
                                'lot': lot_number,
                                'sprit1': 0,
                                'sprit2': 0,
                                'rank': cprd_result.CPDD_RANK,
                                'qty': pass_qty,
                                'flg': 0,
                                'pcd_id': new_cprc_id
                            }
                            
                            session.execute(cprd_insert_sql, cprd_params)
                        else:
                            error_msg = f"製造データが見つかりません: CPDD_ID={cshk_detail['CSHK_PDD_ID']}"
                            logging.error(error_msg)
                            raise ValueError(error_msg)
                else:
                    error_msg = f"出荷データの製造データIDが取得できません: CSHK_ID={shk_id}"
                    log_error(error_msg)
                    raise ValueError(error_msg)
            session.commit()
            return new_cprc_id

        except Exception as e:
            log_error(f"加工データの登録中にエラーが発生: {str(e)}")
            try:
                session.rollback()
            except Exception as rollback_error:
                log_error(f"ロールバック中にエラーが発生: {str(rollback_error)}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_by_shk_id(shk_id):
        """指定された出荷IDの加工データ一覧を取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CPCD_ID,
                    CPCD_SHK_ID,
                    CPCD_DATE,
                    CPCD_QTY,
                    CPCD_RET_NG_QTY,
                    CPCD_INS_NG_QTY,
                    CPCD_PASS_QTY
                FROM CPRC_DAT
                WHERE CPCD_SHK_ID = :shk_id
                ORDER BY CPCD_ID DESC
            """)
            
            results = session.execute(sql, {'shk_id': shk_id}).fetchall()
            
            result = []
            for r in results:
                cprc = {
                    'CPCD_ID': r.CPCD_ID,
                    'CPCD_SHK_ID': r.CPCD_SHK_ID,
                    'CPCD_DATE': r.CPCD_DATE.strftime('%Y-%m-%d') if r.CPCD_DATE else '',
                    'CPCD_QTY': r.CPCD_QTY,
                    'CPCD_RET_NG_QTY': r.CPCD_RET_NG_QTY,
                    'CPCD_INS_NG_QTY': r.CPCD_INS_NG_QTY,
                    'CPCD_PASS_QTY': r.CPCD_PASS_QTY
                }
                result.append(cprc)
            return result
            
        except Exception as e:
            logging.error(f"加工データ一覧の取得中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update(cprc_id, date, qty, ret_ng_qty, ins_ng_qty, pass_qty):
        """加工データを更新する"""
        session = get_db_session()
        try:
            
            # 入力値の検証
            if not all([cprc_id, date, qty is not None]):
                raise ValueError('必須項目を入力してください。')

            try:
                cprc_id = int(cprc_id)
                qty = int(qty)
                ret_ng_qty = int(ret_ng_qty) if ret_ng_qty else 0
                ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
                pass_qty = int(pass_qty) if pass_qty else 0
                # 日付文字列をdatetimeに変換
                if isinstance(date, str):
                    date = datetime.strptime(date, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError('数値項目は正しい数値で入力してください。')

            # 桁数チェック
            if qty <= 0 or qty > 99999:
                raise ValueError('戻り数は1から99999の範囲で入力してください。')
            if ret_ng_qty < 0 or ret_ng_qty > 99999:
                raise ValueError('戻り不良数は0から99999の範囲で入力してください。')
            if ins_ng_qty < 0 or ins_ng_qty > 99999:
                raise ValueError('検品不良数は0から99999の範囲で入力してください。')
            if pass_qty < 0 or pass_qty > 99999:
                raise ValueError('合格数は0から99999の範囲で入力してください。')

            # 更新するデータの存在確認
            existing_cprc = session.query(CprcDatModel).filter(CprcDatModel.CPCD_ID == cprc_id).first()
            if not existing_cprc:
                raise ValueError('指定された加工データが見つかりません。')

            # SQL直接実行による更新
            update_sql = text("""
                UPDATE CPRC_DAT 
                SET CPCD_DATE = :date,
                    CPCD_QTY = :qty,
                    CPCD_RET_NG_QTY = :ret_ng_qty,
                    CPCD_INS_NG_QTY = :ins_ng_qty,
                    CPCD_PASS_QTY = :pass_qty
                WHERE CPCD_ID = :cprc_id
            """)
            
            params = {
                'date': date,
                'qty': qty,
                'ret_ng_qty': ret_ng_qty,
                'ins_ng_qty': ins_ng_qty,
                'pass_qty': pass_qty,
                'cprc_id': cprc_id
            }
            
            session.execute(update_sql, params)
            session.commit()
            
            logging.info(f"加工データを更新しました: ID={cprc_id}, 戻り数={qty}, 合格数={pass_qty}")

        except Exception as e:
            session.rollback()
            logging.error(f"加工データの更新中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete(cprc_id):
        """加工データを削除する"""
        session = get_db_session()
        try:
            # 削除するデータの存在確認
            existing_cprc = session.query(CprcDatModel).filter(CprcDatModel.CPCD_ID == cprc_id).first()
            if not existing_cprc:
                raise ValueError('指定された加工データが見つかりません。')

            # SQL直接実行による削除
            delete_sql = text("DELETE FROM CPRC_DAT WHERE CPCD_ID = :cprc_id")
            session.execute(delete_sql, {'cprc_id': cprc_id})
            session.commit()
            
            logging.info(f"加工データを削除しました: ID={cprc_id}")

        except Exception as e:
            session.rollback()
            logging.error(f"加工データの削除中にエラーが発生: {str(e)}")
            raise
        finally:
            session.close()


