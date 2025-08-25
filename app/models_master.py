import logging
from sqlalchemy import Column, String, text
from sqlalchemy.types import Numeric
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import traceback

from app.database import Base, get_db_session
from app.constants import DatabaseConstants, KbnConstants

class KbnMstModel(Base):
    """区分マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'KBN_MST'
    
    KBN_ID = Column(String(10, collation='Japanese_CI_AS'), primary_key=True)
    KBN_NO = Column(Numeric(2, 0))  # 区分番号
    KBN_NM = Column(String(50, collation='Japanese_CI_AS'))  # 区分名
    KBN_FLG = Column(Numeric(1, 0))  # フラグ
    
    @staticmethod
    def get_kbn_list(kbn_id, only_active=True):
        """区分リストを取得する（汎用版）
        
        Args:
            kbn_id (str): 区分ID
            only_active (bool): 有効なもののみを取得するかどうか（デフォルト: True）
            
        Returns:
            list: 区分データリスト
        """
        session = get_db_session()
        try:
            # SQLクエリの構築
            where_clause = "WHERE KBN_ID = :kbn_id"
            if only_active:
                where_clause += " AND KBN_FLG = :kbn_flg"
            
            sql = text(f"""
                SELECT 
                    KBN_NO,
                    KBN_NM,
                    KBN_FLG
                FROM KBN_MST 
                {where_clause}
                ORDER BY KBN_NO
            """)
            
            # パラメータの設定
            params = {'kbn_id': kbn_id}
            if only_active:
                params['kbn_flg'] = KbnConstants.KBN_FLG_ACTIVE
            
            results = session.execute(sql, params).fetchall()
            
            result = []
            for r in results:
                kbn_data = {
                    'KBN_NO': r.KBN_NO,
                    'KBN_NM': r.KBN_NM,
                    'KBN_FLG': r.KBN_FLG
                }
                result.append(kbn_data)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
    
    @staticmethod
    def get_rank_list():
        """ランクリストを取得する（後方互換性のために残存）"""
        return KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
    
    @staticmethod
    def get_gspec_list():
        """グラデーション仕様リストを取得する"""
        return KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_GSPEC)
    
    @staticmethod
    def get_gcolor_list():
        """グラデーション色リストを取得する"""
        return KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_GCOLOR)

class CprcMstModel(Base):
    """加工マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CPRC_MST'
    
    CPRC_ID = Column(Numeric(5, 0), primary_key=True, autoincrement=True)  # 加工ID
    CPRC_NM = Column(String(40, collation='Japanese_CI_AS'))  # 加工名
    CPRC_PRD_NM = Column(String(40, collation='Japanese_CI_AS'))  # 加工前製品名
    CPRC_TO = Column(Numeric(3, 0))  # 加工依頼先
    CPRC_TIME = Column(Numeric(2, 0))  # 加工日数
    CPRC_FLG = Column(Numeric(1, 0))  # フラグ
    CPRC_PRD_ID = Column(String(5))  # 製品ID
    CPRC_AF_PRD_ID = Column(String(5))  # 加工後製品ID
    
    @staticmethod
    def get_cprc_list_by_prd_id(prd_id):
        """製品IDに関連する加工マスタリストを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CPRC_ID,
                    CPRC_NM,
                    CPRC_PRD_NM,
                    CPRC_TO,
                    CPRC_TIME,
                    CPRC_FLG,
                    CPRC_PRD_ID,
                    CPRC_AF_PRD_ID
                FROM CPRC_MST 
                WHERE CPRC_PRD_ID = :prd_id
                    AND CPRC_FLG = :cprc_flg_active
                ORDER BY CPRC_ID
            """)
            
            results = session.execute(sql, {
                'prd_id': prd_id,
                'cprc_flg_active': DatabaseConstants.FLG_ACTIVE
            }).fetchall()
            
            result = []
            for r in results:
                cprc_data = {
                    'CPRC_ID': r.CPRC_ID,
                    'CPRC_NM': r.CPRC_NM,
                    'CPRC_PRD_NM': r.CPRC_PRD_NM,
                    'CPRC_TO': r.CPRC_TO,
                    'CPRC_TIME': r.CPRC_TIME,
                    'CPRC_FLG': r.CPRC_FLG,
                    'CPRC_PRD_ID': r.CPRC_PRD_ID,
                    'CPRC_AF_PRD_ID': r.CPRC_AF_PRD_ID
                }
                result.append(cprc_data)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
    
    @staticmethod
    def get_all():
        """すべての加工マスタリストを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CPRC_ID,
                    CPRC_NM,
                    CPRC_PRD_NM,
                    CPRC_TO,
                    CPRC_TIME,
                    CPRC_FLG,
                    CPRC_PRD_ID,
                    CPRC_AF_PRD_ID
                FROM CPRC_MST 
                WHERE CPRC_FLG = :cprc_flg_active
                ORDER BY CPRC_ID
            """)
            
            results = session.execute(sql, {'cprc_flg_active': DatabaseConstants.FLG_ACTIVE}).fetchall()
            
            result = []
            for r in results:
                cprc_data = {
                    'CPRC_ID': r.CPRC_ID,
                    'CPRC_NM': r.CPRC_NM,
                    'CPRC_PRD_NM': r.CPRC_PRD_NM,
                    'CPRC_TO': r.CPRC_TO,
                    'CPRC_TIME': r.CPRC_TIME,
                    'CPRC_FLG': r.CPRC_FLG,
                    'CPRC_PRD_ID': r.CPRC_PRD_ID,
                    'CPRC_AF_PRD_ID': r.CPRC_AF_PRD_ID
                }
                result.append(cprc_data)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()

class CtpdMstModel(Base):
    """取引先製品マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CTPD_MST'
    
    CTPD_ID = Column(Numeric(10, 0), primary_key=True, autoincrement=True)  # 取引先製品ID
    CTPD_ZTR_ID = Column(Numeric(3, 0))  # 取引先ID
    CTPD_PRD_ID = Column(String(5))  # 製品ID
    CTPD_RANK = Column(Numeric(2, 0))  # ランク
    CTPD_NM = Column(String(100, collation='Japanese_CI_AS'))  # 製品名
    CTPD_SPC = Column(String(100, collation='Japanese_CI_AS'))  # 規格
    CTPD_FRG = Column(Numeric(1, 0))  # フラグ
    
    @staticmethod
    def get_all():
        """すべての取引先製品マスタデータを取得する（有効なもののみ）"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CTPD_ID,
                    CTPD_ZTR_ID,
                    CTPD_PRD_ID,
                    CTPD_RANK,
                    CTPD_NM,
                    CTPD_SPC,
                    CTPD_FRG
                FROM CTPD_MST 
                WHERE CTPD_FRG = :ctpd_frg_active
                ORDER BY CTPD_ID
            """)
            
            results = session.execute(sql, {'ctpd_frg_active': DatabaseConstants.FLG_ACTIVE}).fetchall()
            
            result = []
            for r in results:
                ctpd_data = {
                    'CTPD_ID': r.CTPD_ID,
                    'CTPD_ZTR_ID': r.CTPD_ZTR_ID,
                    'CTPD_PRD_ID': r.CTPD_PRD_ID,
                    'CTPD_RANK': r.CTPD_RANK,
                    'CTPD_NM': r.CTPD_NM,
                    'CTPD_SPC': r.CTPD_SPC,
                    'CTPD_FRG': r.CTPD_FRG
                }
                result.append(ctpd_data)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()

class CztrMstModel(Base):
    """取引先マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CZTR_MST'
    
    CZTR_ID = Column(Numeric(3, 0), primary_key=True)  # 取引先ID
    CZTR_NM = Column(String(40, collation='Japanese_CI_AS'))  # 取引先名
    CZTR_FULL_NM = Column(String(80, collation='Japanese_CI_AS'))  # 取引先名正式名称
    CZTR_TANTO_NM = Column(String(20, collation='Japanese_CI_AS'))  # 担当者名
    CZTR_KBN = Column(Numeric(2, 0))  # 区分
    CZTR_FLG = Column(Numeric(1, 0))  # フラグ
    CZTR_TYP = Column(Numeric(2, 0))  # タイプ
    
    @staticmethod
    def get_by_kbn(cztr_kbn):
        """区分で委託先リストを取得する（共通メソッド）"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CZTR_ID,
                    CZTR_NM,
                    CZTR_FULL_NM,
                    CZTR_TANTO_NM,
                    CZTR_KBN,
                    CZTR_FLG,
                    CZTR_TYP
                FROM CZTR_MST 
                WHERE CZTR_KBN = :cztr_kbn
                    AND CZTR_FLG = :cztr_flg_active
                ORDER BY CZTR_ID
            """)
            
            results = session.execute(sql, {'cztr_kbn': cztr_kbn, 'cztr_flg_active': DatabaseConstants.FLG_ACTIVE}).fetchall()
            
            result = []
            for r in results:
                cztr_data = {
                    'CZTR_ID': r.CZTR_ID,
                    'CZTR_NM': r.CZTR_NM,
                    'CZTR_FULL_NM': r.CZTR_FULL_NM,
                    'CZTR_TANTO_NM': r.CZTR_TANTO_NM,
                    'CZTR_KBN': r.CZTR_KBN,
                    'CZTR_FLG': r.CZTR_FLG,
                    'CZTR_TYP': r.CZTR_TYP
                }
                result.append(cztr_data)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
    
    @staticmethod
    def get_customer_list():
        """得意先（CZTR_KBN=1）のリストを取得する"""
        from app.constants import DatabaseConstants
        return CztrMstModel.get_by_kbn(DatabaseConstants.CZTR_KBN_CUSTOMER)
    
    @staticmethod
    def get_process_company_list():
        """加工会社（CZTR_KBN=2）のリストを取得する"""
        return CztrMstModel.get_by_kbn(DatabaseConstants.CZTR_KBN_PROCESS_COMPANY)
    
    @staticmethod
    def get_all():
        """すべての委託先リストを取得する"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CZTR_ID,
                    CZTR_NM,
                    CZTR_FULL_NM,
                    CZTR_TANTO_NM,
                    CZTR_KBN,
                    CZTR_FLG,
                    CZTR_TYP
                FROM CZTR_MST 
                WHERE CZTR_FLG = :cztr_flg_active
                ORDER BY CZTR_ID
            """)
            
            results = session.execute(sql, {'cztr_flg_active': DatabaseConstants.FLG_ACTIVE}).fetchall()
            
            result = []
            for r in results:
                cztr_data = {
                    'CZTR_ID': r.CZTR_ID,
                    'CZTR_NM': r.CZTR_NM,
                    'CZTR_FULL_NM': r.CZTR_FULL_NM,
                    'CZTR_TANTO_NM': r.CZTR_TANTO_NM,
                    'CZTR_KBN': r.CZTR_KBN,
                    'CZTR_FLG': r.CZTR_FLG,
                    'CZTR_TYP': r.CZTR_TYP
                }
                result.append(cztr_data)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()

    @staticmethod
    def get_destination_choices():
        """出荷先の選択肢リストを取得する（CZTR_TYP=2のみ）"""
        cztr_list = [cztr for cztr in CztrMstModel.get_all() if cztr['CZTR_TYP'] == DatabaseConstants.CZTR_TYPE_BF]
        return [('', '全て')] + [(cztr['CZTR_ID'], cztr['CZTR_NM']) for cztr in cztr_list]

class PrdMstModel(Base):
    """製品マスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'PRD_MST'
    
    PRD_ID = Column(String(5), primary_key=True)  # 製品ID
    PRD_MONOMER = Column(String(30, collation='Japanese_CI_AS'))  # モノマー
    PRD_NAME = Column(String(40, collation='Japanese_CI_AS'))  # 呼び名
    PRD_LOWER_DIE = Column(String(20, collation='Japanese_CI_AS'))  # 下型
    PRD_UPPER_DIE = Column(String(20, collation='Japanese_CI_AS'))  # 上型
    PRD_FILM_COLOR = Column(String(20, collation='Japanese_CI_AS'))  # 膜カラー
    PRD_KBN = Column(Numeric(1, 0))  # 商品分類
    PRD_FLG = Column(Numeric(1, 0))  # フラグ
    PRD_DSP_NM = Column(String(70, collation='Japanese_CI_AS'))  # 表示名
    
    @staticmethod
    def get_by_prd_id(prd_id):
        """製品IDで製品マスタを取得（有効なもののみ）"""
        session = get_db_session()
        try:
            result = session.query(PrdMstModel).filter(
                PrdMstModel.PRD_ID == prd_id,
                PrdMstModel.PRD_FLG == DatabaseConstants.FLG_ACTIVE
            ).first()
            return result
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
    
    @staticmethod
    def get_all():
        """すべての製品マスタデータを取得する（有効なもののみ）"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    PRD_ID,
                    PRD_NAME,
                    PRD_DSP_NM
                FROM PRD_MST 
                WHERE PRD_FLG = :prd_flg_active
                ORDER BY PRD_ID
            """)
            
            results = session.execute(sql, {'prd_flg_active': DatabaseConstants.FLG_ACTIVE}).fetchall()
            
            result = []
            for r in results:
                prd = {
                    'PRD_ID': r.PRD_ID,
                    'PRD_NAME': r.PRD_NAME,
                    'PRD_DSP_NM': r.PRD_DSP_NM
                }
                result.append(prd)
            return result
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
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
                WHERE PRD_FLG = :prd_flg_active
                ORDER BY PRD_ID
            """)
            
            results = session.execute(sql, {'prd_flg_active': DatabaseConstants.FLG_ACTIVE}).fetchall()
            return [('', '選択してください')] + [(row.PRD_ID, f"{row.PRD_ID} - {row.PRD_DSP_NM or row.PRD_NAME or ''}") for row in results]
        except Exception as e:
            logging.error(f"製品選択肢取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

class CbcdMstModel(Base):
    """バーコードマスタテーブルのSQLAlchemyモデル"""
    __tablename__ = 'CBCD_MST'
    
    CBCD_ID = Column(Numeric(10, 0), primary_key=True)  # ID
    CBCD_PRD_ID = Column(String(5))  # 製品ID
    CBCD_TO = Column(Numeric(3, 0))  # 出荷先ID
    CBCD_NM = Column(String(100, collation='Japanese_CI_AS'))  # 製品名
    CBCD_NO1 = Column(String(60))  # バーコード１
    CBCD_NO2 = Column(String(60))  # バーコード２
    CBCD_FLG = Column(Numeric(1, 0))  # フラグ
    
    @staticmethod
    def get_by_prd_id_and_to(prd_id, to_id):
        """製品IDと出荷先IDでバーコードマスタを取得（有効なもののみ）"""
        session = get_db_session()
        try:
            sql = text("""
                SELECT 
                    CBCD_ID,
                    CBCD_PRD_ID,
                    CBCD_TO,
                    CBCD_NM,
                    CBCD_NO1,
                    CBCD_NO2
                FROM CBCD_MST 
                WHERE CBCD_PRD_ID = :prd_id
                    AND CBCD_TO = :to_id
                    AND CBCD_FLG = :cbcd_flg_active
            """)
            
            result = session.execute(sql, {
                'prd_id': prd_id, 
                'to_id': to_id,
                'cbcd_flg_active': DatabaseConstants.FLG_ACTIVE
            }).first()
            
            if result:
                return {
                    'CBCD_ID': result.CBCD_ID,
                    'CBCD_PRD_ID': result.CBCD_PRD_ID,
                    'CBCD_TO': result.CBCD_TO,
                    'CBCD_NM': result.CBCD_NM,
                    'CBCD_NO1': result.CBCD_NO1,
                    'CBCD_NO2': result.CBCD_NO2
                }
            return None
        except OperationalError as e:
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            logging.error(f"データベース接続エラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except SQLAlchemyError as e:
            error_msg = "データベースの操作中にエラーが発生しました。"
            logging.error(f"SQLエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        except Exception as e:
            error_msg = "予期せぬエラーが発生しました。"
            logging.error(f"予期せぬエラー: {str(e)}\n{traceback.format_exc()}")
            raise Exception(error_msg)
        finally:
            session.close()
