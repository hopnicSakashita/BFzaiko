from sqlalchemy import Column, String, Integer, ForeignKey, Float, DateTime, Boolean, Text, inspect, text, case
from sqlalchemy.types import Numeric
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from datetime import datetime
from sqlalchemy.orm import relationship
from app.database import Base, get_db_session
from app.models import log_error

class GprrDatModel(Base):
    """グラデ加工依頼データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'GPRR_DAT'
    
    GPRR_ID = Column(Integer, primary_key=True, autoincrement=True)
    GPRR_SPEC = Column(Numeric(2, 0))  # 規格
    GPRR_COLOR = Column(Numeric(2, 0))  # 色
    GPRR_REQ_TO = Column(Numeric(1, 0))  # 依頼先
    GPRR_REQ_DATE = Column(DateTime)  # 依頼日
    GPRR_QTY = Column(Numeric(5, 0))  # 数量

class GprcDatModel(Base):
    """グラデ加工データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'GPRC_DAT'
    
    GPRC_ID = Column(Integer, primary_key=True, autoincrement=True)
    GPRC_REQ_ID = Column(Numeric(10, 0))  # 依頼ID
    GPRC_REQ_TO = Column(Numeric(1, 0))  # 依頼先
    GPRC_DATE = Column(DateTime)  # 戻り日
    GPRC_QTY = Column(Numeric(5, 0))  # 戻り数
    GPRC_RET_NG_QTY = Column(Numeric(5, 0))  # 戻り不良数
    GPRC_INS_NG_QTY = Column(Numeric(5, 0))  # 検品不良数
    GPRC_SHK_ID = Column(Numeric(10, 0))  # 出荷ID
    GPRC_PASS_QTY = Column(Numeric(5, 0))  # 合格数
    GPRC_STS = Column(Numeric(1, 0))  # ステータス

class GshkDatModel(Base):
    """グラデ出荷データテーブルのSQLAlchemyモデル"""
    __tablename__ = 'GSHK_DAT'
    
    GSHK_ID = Column(Integer, primary_key=True, autoincrement=True)
    GSHK_STC_ID = Column(Numeric(10, 0))  # 在庫ID
    GSHK_TO = Column(Numeric(2, 0))  # 出荷先
    GSHK_DT = Column(DateTime)  # 出荷日
    GSHK_ORD_DT = Column(DateTime)  # 手配日
    GSHK_QTY = Column(Numeric(10, 0))  # 数量
    GSHK_FLG = Column(Numeric(10, 0))  # フラグ
    GSHK_REQ_ID = Column(Numeric(10, 0))  # 依頼ID

class Gradation:
    """グラデーション関連のモデルクラス"""
    
    @staticmethod
    def get_kbn_choices(kbn_id, pattern=0):
        """区分マスタから選択肢を取得する"""
        session = get_db_session()
        try:
            choices = session.execute(text("""
                SELECT KBN_NO, KBN_NM
                FROM KBN_MST 
                WHERE KBN_ID = :kbn_id
                AND KBN_FLG = 0
                ORDER BY KBN_NO
            """), {'kbn_id': kbn_id}).fetchall()
            if pattern == 1:
                return [('', '全て')] + [(str(r.KBN_NO), r.KBN_NM) for r in choices]
            if pattern == 2:
                return [('', '未選択')] + [(str(r.KBN_NO), r.KBN_NM) for r in choices]
            else:
                return [(str(r.KBN_NO), r.KBN_NM) for r in choices]
        except Exception as e:
            log_error(f"区分マスタ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def create_gprr(spec, color, req_to, req_date, qty):
        """グラデ加工依頼データを作成する"""
        session = get_db_session()
        try:
            # 新規データを作成
            new_gprr = GprrDatModel(
                GPRR_SPEC=spec,
                GPRR_COLOR=color,
                GPRR_REQ_TO=req_to,
                GPRR_REQ_DATE=req_date,
                GPRR_QTY=qty
            )
            session.add(new_gprr)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"グラデ加工依頼データの作成中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprr_list(spec=None, color=None, req_date_from=None, req_date_to=None, flg=None):
        """グラデ加工依頼データ一覧を取得する（検索機能付き）"""
        session = get_db_session()
        try:
            # 基本SQLクエリ
            sql = """
                SELECT 
                    GPRR_ID,
                    KBN1.KBN_NM AS GPRR_SPEC,
                    KBN2.KBN_NM AS GPRR_COLOR,
                    GPRR_REQ_DATE,
                    GPRR_QTY,
                    dbo.Get_GRD_PRC_ZAN_Qty(GPRR_ID) AS GPRR_PRC_ZAN_QTY
                FROM GPRR_DAT
                LEFT JOIN KBN_MST KBN1 ON GPRR_SPEC = KBN1.KBN_NO
                AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR_COLOR = KBN2.KBN_NO
                AND KBN2.KBN_ID = 'GCOLOR'
                WHERE GPRR_REQ_TO = 1
            """
            
            params = {}
            
            # 検索条件を追加
            if spec:
                sql += " AND GPRR_SPEC = :spec"
                params['spec'] = int(spec)
            if color:
                sql += " AND GPRR_COLOR = :color"
                params['color'] = int(color)
            if req_date_from:
                sql += " AND GPRR_REQ_DATE >= :req_date_from"
                params['req_date_from'] = req_date_from
            if req_date_to:
                sql += " AND GPRR_REQ_DATE <= :req_date_to"
                params['req_date_to'] = req_date_to
            if flg == '0':
                sql += " AND dbo.Get_GRD_PRC_ZAN_Qty(GPRR_ID) >= 0"
            elif flg == '1':
                sql += " AND dbo.Get_GRD_PRC_ZAN_Qty(GPRR_ID) = 0"

            
            sql += " ORDER BY GPRR_ID DESC"
            
            gprr_list = session.execute(text(sql), params).fetchall()
            return gprr_list
        except Exception as e:
            log_error(f"グラデ加工依頼データ一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def create_gprc(req_id, req_to, date, qty, ret_ng_qty=0, ins_ng_qty=0, shk_id=None, pass_qty=None, sts=0):
        """グラデ加工データを作成する"""
        session = get_db_session()
        try:
            # 依頼データの存在確認
            gprr = session.query(GprrDatModel).filter(GprrDatModel.GPRR_ID == req_id).first()
            if not gprr:
                raise Exception("指定された依頼データが見つかりません")
            
            # 加工残数の確認
            current_zan_qty = session.execute(text("""
                SELECT dbo.Get_GRD_PRC_ZAN_Qty(:req_id) AS zan_qty
            """), {'req_id': req_id}).fetchone()
            
            if current_zan_qty and current_zan_qty.zan_qty < qty:
                raise Exception(f"戻り数量({qty})が加工残数({current_zan_qty.zan_qty})を超えています")
            
            # データの整合性チェック
            if ret_ng_qty + ins_ng_qty > qty:
                raise Exception("戻り不良数と検品不良数の合計が戻り数を超えています")
            
            if pass_qty and pass_qty > (qty - ret_ng_qty - ins_ng_qty):
                raise Exception("合格数が有効数量を超えています")
            
            new_gprc = GprcDatModel(
                GPRC_REQ_ID=req_id,
                GPRC_REQ_TO=req_to,
                GPRC_DATE=date,
                GPRC_QTY=qty,
                GPRC_RET_NG_QTY=ret_ng_qty,
                GPRC_INS_NG_QTY=ins_ng_qty,
                GPRC_SHK_ID=shk_id if req_to == 2 else None,  # ニデック加工時のみ設定
                GPRC_PASS_QTY=pass_qty,
                GPRC_STS=sts
            )
            session.add(new_gprc)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"グラデ加工データの作成中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def create_gshk(stc_id, to, dt, ord_dt, qty, req_id=None):
        """グラデ出荷データを作成する"""
        session = get_db_session()
        try:
            new_gshk = GshkDatModel(
                GSHK_STC_ID=stc_id,
                GSHK_TO=to,
                GSHK_DT=dt,
                GSHK_ORD_DT=ord_dt,
                GSHK_QTY=qty,
                GSHK_FLG=0,
                GSHK_REQ_ID=req_id
            )
            session.add(new_gshk)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"グラデ出荷データの作成中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprr_by_id(gprr_id):
        """指定されたIDのグラデ加工依頼データを取得する"""
        session = get_db_session()
        try:
            gprr = session.query(GprrDatModel).filter(GprrDatModel.GPRR_ID == gprr_id).first()
            return gprr
        except Exception as e:
            log_error(f"グラデ加工依頼データの取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_gprr(gprr_id, spec, color, req_date, qty):
        """グラデ加工依頼データを更新する"""
        session = get_db_session()
        try:
            gprr = session.query(GprrDatModel).filter(GprrDatModel.GPRR_ID == gprr_id).first()
            if gprr:
                gprr.GPRR_SPEC = spec
                gprr.GPRR_COLOR = color
                gprr.GPRR_REQ_DATE = req_date
                gprr.GPRR_QTY = qty
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"グラデ加工依頼データの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete_gprr(gprr_id):
        """グラデ加工依頼データを削除する"""
        session = get_db_session()
        try:
            gprr = session.query(GprrDatModel).filter(GprrDatModel.GPRR_ID == gprr_id).first()
            if gprr:
                session.delete(gprr)
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"グラデ加工依頼データの削除中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprr_for_gprc(gprr_id):
        """GPRC作成用のGPRRデータを取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GPRR_ID,
                    KBN1.KBN_NM AS GPRR_SPEC_NM,
                    KBN2.KBN_NM AS GPRR_COLOR_NM,
                    GPRR_SPEC,
                    GPRR_COLOR,
                    GPRR_REQ_DATE,
                    GPRR_QTY,
                    dbo.Get_GRD_PRC_ZAN_Qty(GPRR_ID) AS GPRR_PRC_ZAN_QTY
                FROM GPRR_DAT
                LEFT JOIN KBN_MST KBN1 ON GPRR_SPEC = KBN1.KBN_NO
                AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR_COLOR = KBN2.KBN_NO
                AND KBN2.KBN_ID = 'GCOLOR'
                WHERE GPRR_ID = :gprr_id
            """
            result = session.execute(text(sql), {'gprr_id': gprr_id}).fetchone()
            return result
        except Exception as e:
            log_error(f"GPRC作成用GPRRデータの取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprc_by_req_id(req_id):
        """指定された依頼IDに紐づくGPRCデータ一覧を取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GPRC_ID,
                    GPRC_REQ_ID,
                    GPRC_REQ_TO,
                    CASE 
                        WHEN GPRC_REQ_TO = 1 THEN 'コンベックス'
                        WHEN GPRC_REQ_TO = 2 THEN 'ニデック'
                        ELSE '不明'
                    END AS GPRC_REQ_TO_NM,
                    GPRC_DATE,
                    GPRC_QTY,
                    GPRC_RET_NG_QTY,
                    GPRC_INS_NG_QTY,
                    GPRC_SHK_ID,
                    GPRC_PASS_QTY,
                    GPRC_STS
                FROM GPRC_DAT
                WHERE GPRC_REQ_ID = :req_id
                AND GPRC_REQ_TO = 1
                ORDER BY GPRC_DATE DESC, GPRC_ID DESC
            """
            result = session.execute(text(sql), {'req_id': req_id}).fetchall()
            return result
        except Exception as e:
            log_error(f"GPRCデータ一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprc_by_id(gprc_id):
        """指定されたIDのGPRCデータを取得する"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            return gprc
        except Exception as e:
            log_error(f"GPRCデータの取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_gprc(gprc_id, date, qty, ret_ng_qty=0, ins_ng_qty=0, shk_id=None, pass_qty=None, sts=0):
        """GPRCデータを更新する（依頼先は変更しない）"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc:
                gprc.GPRC_DATE = date
                gprc.GPRC_QTY = qty
                gprc.GPRC_RET_NG_QTY = ret_ng_qty
                gprc.GPRC_INS_NG_QTY = ins_ng_qty
                # ニデック加工時のみGPRC_SHK_IDを設定
                if gprc.GPRC_REQ_TO == 2:
                    gprc.GPRC_SHK_ID = shk_id
                gprc.GPRC_PASS_QTY = pass_qty
                gprc.GPRC_STS = sts
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"GPRCデータの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete_gprc(gprc_id):
        """GPRCデータを削除する"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc:
                session.delete(gprc)
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"GPRCデータの削除中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprr_zan_qty(gprr_id):
        """指定された依頼IDの加工残数を取得する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT dbo.Get_GRD_PRC_ZAN_Qty(:gprr_id) AS zan_qty
            """), {'gprr_id': gprr_id}).fetchone()
            
            # 数値変換を確実に行う
            zan_qty_raw = result.zan_qty if result else 0
            if zan_qty_raw is None:
                return 0
            try:
                return int(zan_qty_raw)
            except (ValueError, TypeError):
                log_error(f"加工残数の型変換エラー: {zan_qty_raw}(型:{type(zan_qty_raw)})")
                return 0
                
        except Exception as e:
            log_error(f"加工残数の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprr_req_to(gprr_id):
        """指定された依頼IDの依頼先を取得する"""
        session = get_db_session()
        try:
            gprr = session.query(GprrDatModel).filter(GprrDatModel.GPRR_ID == gprr_id).first()
            return gprr.GPRR_REQ_TO if gprr else None
        except Exception as e:
            log_error(f"依頼先の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprc_total_qty(req_id):
        """指定された依頼IDの戻り数合計を取得する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT COALESCE(SUM(GPRC_QTY), 0) AS total_qty
                FROM GPRC_DAT
                WHERE GPRC_REQ_ID = :req_id
            """), {'req_id': req_id}).fetchone()
            
            # 数値変換を確実に行う
            total_qty = result.total_qty if result else 0
            if total_qty is None:
                return 0
            try:
                return int(total_qty)
            except (ValueError, TypeError):
                log_error(f"戻り数合計の型変換エラー: {total_qty}(型:{type(total_qty)})")
                return 0
                
        except Exception as e:
            log_error(f"戻り数合計の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprc_total_qty_exclude(req_id, exclude_gprc_id):
        """指定された依頼IDの戻り数合計を取得する（指定されたGPRC_IDを除く）"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT COALESCE(SUM(GPRC_QTY), 0) AS total_qty
                FROM GPRC_DAT
                WHERE GPRC_REQ_ID = :req_id
                AND GPRC_ID != :exclude_gprc_id
            """), {'req_id': req_id, 'exclude_gprc_id': exclude_gprc_id}).fetchone()
            
            # 数値変換を確実に行う
            total_qty = result.total_qty if result else 0
            if total_qty is None:
                return 0
            try:
                return int(total_qty)
            except (ValueError, TypeError):
                log_error(f"戻り数合計（除外）の型変換エラー: {total_qty}(型:{type(total_qty)})")
                return 0
                
        except Exception as e:
            log_error(f"戻り数合計の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprc_list_for_shipping(spec=None, color=None, date_from=None, date_to=None, stock_status=None):
        """GPRCデータ一覧を取得する（GPRC_REQ_TO=1のみ、出荷用）"""
        session = get_db_session()
        try:
            # 基本SQLクエリ
            sql = """
                SELECT 
                    GPRC_ID,
                    GPRC_REQ_ID,
                    KBN1.KBN_NM AS GPRR_SPEC_NM,
                    KBN2.KBN_NM AS GPRR_COLOR_NM,
                    GPRC_DATE,
                    GPRC_QTY,
                    GPRC_RET_NG_QTY,
                    GPRC_INS_NG_QTY,
                    GPRC_PASS_QTY,
                    GPRC_STS,
                    dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) AS STOCK_QTY
                FROM GPRC_DAT
                LEFT JOIN GPRR_DAT ON GPRC_REQ_ID = GPRR_ID
                LEFT JOIN KBN_MST KBN1 ON GPRR_SPEC = KBN1.KBN_NO
                AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR_COLOR = KBN2.KBN_NO
                AND KBN2.KBN_ID = 'GCOLOR'
                WHERE GPRC_REQ_TO = 1
            """
            
            params = {}
            
            # 検索条件を追加
            if spec:
                sql += " AND GPRR_SPEC = :spec"
                params['spec'] = int(spec)
            if color:
                sql += " AND GPRR_COLOR = :color"
                params['color'] = int(color)
            if date_from:
                sql += " AND GPRC_DATE >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND GPRC_DATE <= :date_to"
                params['date_to'] = date_to
            if stock_status == '0':
                sql += " AND dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) = 0"
            elif stock_status == '1':
                sql += " AND dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) > 0"

            
            sql += " ORDER BY GPRC_ID DESC"
            
            gprc_list = session.execute(text(sql), params).fetchall()
            return gprc_list
        except Exception as e:
            log_error(f"GPRC一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_nidec_gprc_list(spec=None, color=None, date_from=None, date_to=None, stock_status=None):
        """ニデック加工データ一覧を取得する（GPRC_REQ_TO=2のみ）"""
        session = get_db_session()
        try:
            # 基本SQLクエリ（在庫数は除外）
            sql = """
                SELECT 
                    GPRC_ID,
                    GPRC_REQ_ID,
                    KBN1.KBN_NM AS GPRR_SPEC_NM,
                    KBN2.KBN_NM AS GPRR_COLOR_NM,
                    GPRC_DATE,
                    GPRC_QTY,
                    GPRC_RET_NG_QTY,
                    GPRC_INS_NG_QTY,
                    GPRC_PASS_QTY,
                    GPRC_STS
                FROM GPRC_DAT
                LEFT JOIN GPRR_DAT ON GPRC_REQ_ID = GPRR_ID
                LEFT JOIN KBN_MST KBN1 ON GPRR_SPEC = KBN1.KBN_NO
                AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR_COLOR = KBN2.KBN_NO
                AND KBN2.KBN_ID = 'GCOLOR'
                WHERE GPRC_REQ_TO = 2
            """
            
            params = {}
            
            # 検索条件を追加
            if spec:
                sql += " AND GPRR_SPEC = :spec"
                params['spec'] = int(spec)
            if color:
                sql += " AND GPRR_COLOR = :color"
                params['color'] = int(color)
            if date_from:
                sql += " AND GPRC_DATE >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND GPRC_DATE <= :date_to"
                params['date_to'] = date_to

            sql += " ORDER BY GPRC_ID DESC"
            
            gprc_list = session.execute(text(sql), params).fetchall()
            
            # 在庫数を計算して追加
            result_list = []
            for row in gprc_list:
                # ニデック加工専用の出荷可能数を計算
                stock_qty = Gradation.get_nidec_available_qty(row.GPRC_ID)
                
                # 在庫状況フィルター
                if stock_status == '0' and stock_qty > 0:
                    continue
                elif stock_status == '1' and stock_qty == 0:
                    continue
                
                # 結果に在庫数を追加
                row_dict = {
                    'GPRC_ID': row.GPRC_ID,
                    'GPRC_REQ_ID': row.GPRC_REQ_ID,
                    'GPRR_SPEC_NM': row.GPRR_SPEC_NM,
                    'GPRR_COLOR_NM': row.GPRR_COLOR_NM,
                    'GPRC_DATE': row.GPRC_DATE,
                    'GPRC_QTY': row.GPRC_QTY,
                    'GPRC_RET_NG_QTY': row.GPRC_RET_NG_QTY,
                    'GPRC_INS_NG_QTY': row.GPRC_INS_NG_QTY,
                    'GPRC_PASS_QTY': row.GPRC_PASS_QTY,
                    'GPRC_STS': row.GPRC_STS,
                    'STOCK_QTY': stock_qty
                }
                result_list.append(row_dict)
            
            return result_list
        except Exception as e:
            log_error(f"ニデック加工データ一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gprc_detail(gprc_id):
        """GPRCデータの詳細を取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GPRC_ID,
                    GPRC_REQ_ID,
                    GPRC_REQ_TO,
                    GPRC_DATE,
                    GPRC_QTY,
                    GPRC_RET_NG_QTY,
                    GPRC_INS_NG_QTY,
                    GPRC_PASS_QTY,
                    dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) AS STOCK_QTY,
                    GPRC_SHK_ID,
                    KBN1.KBN_NM AS GPRR_SPEC_NM,
                    KBN2.KBN_NM AS GPRR_COLOR_NM
                FROM GPRC_DAT
                INNER JOIN GPRR_DAT GPRR ON GPRC_REQ_ID = GPRR.GPRR_ID
                LEFT JOIN KBN_MST KBN1 ON GPRR.GPRR_SPEC = KBN1.KBN_NO
                AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR.GPRR_COLOR = KBN2.KBN_NO
                AND KBN2.KBN_ID = 'GCOLOR'
                WHERE GPRC_ID = :gprc_id
            """
            
            result = session.execute(text(sql), {'gprc_id': gprc_id}).fetchone()
            return result
        except Exception as e:
            log_error(f"GPRC詳細取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_shipping_list(gprc_id):
        """指定されたGPRC_IDの出庫履歴を取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GSHK_ID,
                    GSHK_DT,
                    GSHK_QTY,
                    GSHK_TO,
                    GSHK_FLG
                FROM GSHK_DAT
                WHERE GSHK_STC_ID = :gprc_id
                ORDER BY GSHK_DT DESC, GSHK_ID DESC
            """
            
            shipping_list = session.execute(text(sql), {'gprc_id': gprc_id}).fetchall()
            return shipping_list
        except Exception as e:
            log_error(f"出庫履歴取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_available_qty(gprc_id):
        """指定されたGPRC_IDの出庫可能数を取得する"""
        session = get_db_session()
        try:
            # GPRCデータの合格数を取得
            gprc_result = session.execute(text("""
                SELECT GPRC_PASS_QTY
                FROM GPRC_DAT
                WHERE GPRC_ID = :gprc_id
            """), {'gprc_id': gprc_id}).fetchone()
            
            if not gprc_result:
                return 0
            
            # 数値変換を確実に行う
            pass_qty_raw = gprc_result.GPRC_PASS_QTY
            pass_qty = int(pass_qty_raw) if pass_qty_raw is not None else 0
            
            # 既に出荷済みの数量を取得（最終出荷：GSHK_TO=3）
            shipped_qty_result = session.execute(text("""
                SELECT COALESCE(SUM(GSHK_QTY), 0) AS shipped_qty
                FROM GSHK_DAT
                WHERE GSHK_STC_ID = :gprc_id
                AND GSHK_TO = 3
            """), {'gprc_id': gprc_id}).fetchone()
            
            # 数値変換を確実に行う
            shipped_qty_raw = shipped_qty_result.shipped_qty if shipped_qty_result else 0
            shipped_qty = int(shipped_qty_raw) if shipped_qty_raw is not None else 0
            
            # 出庫可能数 = 合格数 - 既出庫数
            available_qty = max(0, pass_qty - shipped_qty)
            return available_qty
            
        except Exception as e:
            log_error(f"出庫可能数取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def create_shipping_batch(gprc_id, ord_date, shipping_date, shipping_qty, shipping_to=2):
        """GPRCデータから出庫を作成する（バッチ処理用）"""
        session = get_db_session()
        try:
            # GPRCデータを取得
            gprc_data = session.execute(text("""
                SELECT 
                    GPRC_ID,
                    GPRC_REQ_ID,
                    GPRC_PASS_QTY
                FROM GPRC_DAT
                WHERE GPRC_ID = :gprc_id
            """), {'gprc_id': gprc_id}).fetchone()
            
            if not gprc_data:
                return {'success': False, 'error': '指定された加工データが見つかりません'}
            
            # 出庫可能数をチェック
            available_qty = Gradation.get_available_qty(gprc_id)
            if int(shipping_qty) > available_qty:
                return {'success': False, 'error': f'出庫数量は出庫可能数({available_qty})以下で入力してください'}
            
            # 出庫データを作成（IDは自動生成）
            session.execute(text("""
                INSERT INTO GSHK_DAT (
                    GSHK_STC_ID,
                    GSHK_REQ_ID,
                    GSHK_DT,
                    GSHK_ORD_DT,
                    GSHK_QTY,
                    GSHK_TO,
                    GSHK_FLG
                ) VALUES (
                    :gprc_id,
                    :req_id,
                    :shipping_date,
                    :ord_date,
                    :qty,
                    :shipping_to,
                    0
                )
            """), {
                'gprc_id': gprc_id,
                'req_id': gprc_data.GPRC_REQ_ID,
                'shipping_date': shipping_date,
                'ord_date': ord_date,
                'qty': shipping_qty,
                'shipping_to': shipping_to
            })
            
            # 生成されたIDを取得
            shipping_id_result = session.execute(text("SELECT SCOPE_IDENTITY() AS shipping_id")).fetchone()
            shipping_id = int(shipping_id_result.shipping_id)
            
            session.commit()
            
            return {
                'success': True,
                'shipping_id': shipping_id,
                'message': f'出庫が正常に作成されました。出荷ID: {shipping_id}'
            }
            
        except Exception as e:
            session.rollback()
            log_error(f"出庫作成中にエラーが発生しました: {str(e)}")
            return {'success': False, 'error': f'出庫の作成中にエラーが発生しました: {str(e)}'}
        finally:
            session.close()

    @staticmethod
    def get_shipping_total_qty(gprc_id):
        """指定されたGPRC_IDの出庫数合計を取得する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT COALESCE(SUM(GSHK_QTY), 0) AS total_qty
                FROM GSHK_DAT
                WHERE GSHK_REQ_ID = (
                    SELECT GPRC_REQ_ID FROM GPRC_DAT WHERE GPRC_ID = :gprc_id
                )
            """), {'gprc_id': gprc_id}).fetchone()
            
            # 数値変換を確実に行う
            total_qty = result.total_qty if result else 0
            if total_qty is None:
                return 0
            try:
                return int(total_qty)
            except (ValueError, TypeError):
                log_error(f"出庫数合計の型変換エラー: {total_qty}(型:{type(total_qty)})")
                return 0
                
        except Exception as e:
            log_error(f"出庫数合計の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_grd_shk_zan_qty(gprc_id):
        """Get_GRD_SHK_ZAN_Qtyプロシージャを呼び出して在庫数を取得する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT dbo.Get_GRD_SHK_ZAN_Qty(:gprc_id) AS zan_qty
            """), {'gprc_id': gprc_id}).fetchone()
            
            # 数値変換を確実に行う
            zan_qty_raw = result.zan_qty if result else 0
            if zan_qty_raw is None:
                return 0
            try:
                return int(zan_qty_raw)
            except (ValueError, TypeError):
                log_error(f"在庫数の型変換エラー: {zan_qty_raw}(型:{type(zan_qty_raw)})")
                return 0
                
        except Exception as e:
            log_error(f"在庫数取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gshk_list(date_from=None, date_to=None, flg=None):
        """出庫データ一覧を取得する（GSHK_TO=2のみ）"""
        session = get_db_session()
        try:
            # 基本SQLクエリ
            sql = """
                SELECT 
                    GSHK.GSHK_ID,
                    GSHK.GSHK_TO,
                    GSHK.GSHK_STC_ID,
                    GSHK.GSHK_REQ_ID,
                    GSHK.GSHK_DT,
                    GSHK.GSHK_QTY,
                    GSHK.GSHK_FLG,
                    GSHK.GSHK_ORD_DT,
                    dbo.Get_GSHK_GPRC_Diff(GSHK.GSHK_ID) AS GSHK_DIFF_QTY,
                    KBN1.KBN_NM AS GPRR_SPEC_NM,
                    KBN2.KBN_NM AS GPRR_COLOR_NM
                FROM GSHK_DAT GSHK
                left join GPRR_DAT GPRR on GSHK.GSHK_REQ_ID = GPRR.GPRR_ID
                LEFT JOIN KBN_MST KBN1 ON GPRR.GPRR_SPEC = KBN1.KBN_NO AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR.GPRR_COLOR = KBN2.KBN_NO AND KBN2.KBN_ID = 'GCOLOR'
                WHERE GSHK.GSHK_TO = 2
            """
            
            params = {}
            
            # 検索条件を追加
            if date_from:
                sql += " AND GSHK.GSHK_DT >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND GSHK.GSHK_DT <= :date_to"
                params['date_to'] = date_to
            if flg == '0':
                sql += " AND dbo.Get_GSHK_GPRC_Diff(GSHK.GSHK_ID) > 0"
            elif flg == '1':
                sql += " AND dbo.Get_GSHK_GPRC_Diff(GSHK.GSHK_ID) = 0"

            sql += " ORDER BY GSHK.GSHK_DT DESC, GSHK.GSHK_ID DESC"
            
            gshk_list = session.execute(text(sql), params).fetchall()
            return gshk_list
        except Exception as e:
            log_error(f"出庫データ一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gshk_by_id(gshk_id):
        """指定されたGSHK_IDのデータを取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GSHK_ID,
                    GSHK_TO,
                    GSHK_STC_ID,
                    GSHK_REQ_ID,
                    GSHK_DT,
                    GSHK_QTY,
                    GSHK_FLG,
                    GSHK_ORD_DT,
                    dbo.Get_GSHK_GPRC_Diff(GSHK_ID) AS GSHK_DIFF_QTY
                FROM GSHK_DAT
                WHERE GSHK_ID = :gshk_id
            """
            
            result = session.execute(text(sql), {'gshk_id': gshk_id}).fetchone()
            return result
        except Exception as e:
            log_error(f"GSHKデータ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_gshk(gshk_id, stc_id, req_id, dt, ord_dt, qty, flg=0):
        """GSHKデータを更新する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                UPDATE GSHK_DAT
                SET GSHK_STC_ID = :stc_id,
                    GSHK_REQ_ID = :req_id,
                    GSHK_DT = :dt,
                    GSHK_ORD_DT = :ord_dt,
                    GSHK_QTY = :qty,
                    GSHK_FLG = :flg
                WHERE GSHK_ID = :gshk_id
            """), {
                'gshk_id': gshk_id,
                'stc_id': stc_id,
                'req_id': req_id,
                'dt': dt,
                'ord_dt': ord_dt,
                'qty': qty,
                'flg': flg
            })
            
            session.commit()
            return result.rowcount > 0
        except Exception as e:
            session.rollback()
            log_error(f"GSHKデータ更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete_gshk(gshk_id):
        """GSHKデータを削除する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                DELETE FROM GSHK_DAT
                WHERE GSHK_ID = :gshk_id
            """), {'gshk_id': gshk_id})
            
            session.commit()
            return result.rowcount > 0
        except Exception as e:
            session.rollback()
            log_error(f"GSHKデータ削除中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def create_nidec_proc(gshk_id, date, qty, ret_ng_qty=0, ins_ng_qty=0, pass_qty=None, sts=0):
        """ニデック加工データを作成する（GPRC_DATにGPRC_REQ_TO=2で保存）"""
        session = get_db_session()
        try:
            # GSHKデータを取得して依頼IDを取得
            gshk_data = session.execute(text("""
                SELECT GSHK_REQ_ID
                FROM GSHK_DAT
                WHERE GSHK_ID = :gshk_id
            """), {'gshk_id': gshk_id}).fetchone()
            
            if not gshk_data:
                raise Exception("指定された出庫データが見つかりません")
            
            # データの整合性チェック
            if ret_ng_qty + ins_ng_qty > qty:
                raise Exception("戻り不良数と検品不良数の合計が加工数量を超えています")
            
            if pass_qty and pass_qty > (qty - ret_ng_qty - ins_ng_qty):
                raise Exception("合格数が有効数量を超えています")
            
            # 日付変換
            if isinstance(date, str):
                date = datetime.strptime(date, '%Y-%m-%d')
            
            new_gprc = GprcDatModel(
                GPRC_REQ_ID=gshk_data.GSHK_REQ_ID,
                GPRC_REQ_TO=2,  # ニデック加工
                GPRC_DATE=date,
                GPRC_QTY=qty,
                GPRC_RET_NG_QTY=ret_ng_qty,
                GPRC_INS_NG_QTY=ins_ng_qty,
                GPRC_SHK_ID=gshk_id,  # 出庫IDを設定
                GPRC_PASS_QTY=pass_qty,
                GPRC_STS=sts
            )
            session.add(new_gprc)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            log_error(f"ニデック加工データの作成中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_nidec_proc_total_qty(gshk_id):
        """指定されたGSHK_IDのニデック加工数量合計を取得する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT COALESCE(SUM(GPRC_QTY), 0) AS total_qty
                FROM GPRC_DAT
                WHERE GPRC_SHK_ID = :gshk_id
                AND GPRC_REQ_TO = 2
            """), {'gshk_id': gshk_id}).fetchone()
            
            # 数値変換を確実に行う
            total_qty = result.total_qty if result else 0
            if total_qty is None:
                return 0
            try:
                return int(total_qty)
            except (ValueError, TypeError):
                log_error(f"ニデック加工数量合計の型変換エラー: {total_qty}(型:{type(total_qty)})")
                return 0
                
        except Exception as e:
            log_error(f"ニデック加工数量合計の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_nidec_proc_list(gshk_id):
        """指定されたGSHK_IDのニデック加工データ一覧を取得する"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GPRC_ID,
                    GPRC_DATE,
                    GPRC_QTY,
                    GPRC_RET_NG_QTY,
                    GPRC_INS_NG_QTY,
                    GPRC_PASS_QTY,
                    GPRC_STS
                FROM GPRC_DAT
                WHERE GPRC_SHK_ID = :gshk_id
                AND GPRC_REQ_TO = 2
                ORDER BY GPRC_DATE DESC, GPRC_ID DESC
            """
            result = session.execute(text(sql), {'gshk_id': gshk_id}).fetchall()
            return result
        except Exception as e:
            log_error(f"ニデック加工データ一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_nidec_proc(gprc_id, date, qty, ret_ng_qty=0, ins_ng_qty=0, pass_qty=None, sts=0):
        """ニデック加工データを更新する"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc and gprc.GPRC_REQ_TO == 2:  # ニデック加工データのみ更新
                # 日付変換
                if isinstance(date, str):
                    date = datetime.strptime(date, '%Y-%m-%d')
                
                gprc.GPRC_DATE = date
                gprc.GPRC_QTY = qty
                gprc.GPRC_RET_NG_QTY = ret_ng_qty
                gprc.GPRC_INS_NG_QTY = ins_ng_qty
                gprc.GPRC_PASS_QTY = pass_qty
                gprc.GPRC_STS = sts
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"ニデック加工データの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_nidec_inspect(gprc_id, ins_ng_qty=0, pass_qty=None, sts=0):
        """ニデック加工データの検査情報を更新する（検品不良数、合格数、ステータスのみ）"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc and gprc.GPRC_REQ_TO == 2:  # ニデック加工データのみ更新
                # データの整合性チェック
                if gprc.GPRC_RET_NG_QTY + ins_ng_qty > gprc.GPRC_QTY:
                    raise Exception("戻り不良数と検品不良数の合計が加工数量を超えています")
                
                if pass_qty and pass_qty > (gprc.GPRC_QTY - gprc.GPRC_RET_NG_QTY - ins_ng_qty):
                    raise Exception("合格数が有効数量を超えています")
                
                gprc.GPRC_INS_NG_QTY = ins_ng_qty
                gprc.GPRC_PASS_QTY = pass_qty
                gprc.GPRC_STS = sts
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"ニデック検査データの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_convex_inspect(gprc_id, ins_ng_qty=0, pass_qty=None, sts=0):
        """コンベックス加工データの検査情報を更新する（検品不良数、合格数、ステータスのみ）"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc and gprc.GPRC_REQ_TO == 1:  # コンベックス加工データのみ更新
                # データの整合性チェック
                if gprc.GPRC_RET_NG_QTY + ins_ng_qty > gprc.GPRC_QTY:
                    raise Exception("戻り不良数と検品不良数の合計が戻り数を超えています")
                
                if pass_qty and pass_qty > (gprc.GPRC_QTY - gprc.GPRC_RET_NG_QTY - ins_ng_qty):
                    raise Exception("合格数が有効数量を超えています")
                
                gprc.GPRC_INS_NG_QTY = ins_ng_qty
                gprc.GPRC_PASS_QTY = pass_qty
                gprc.GPRC_STS = sts
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"コンベックス検査データの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def update_inspect(gprc_id, ins_ng_qty=0, pass_qty=None, sts=0):
        """加工データの検査情報を更新する（汎用メソッド）"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc:
                # データの整合性チェック
                if gprc.GPRC_RET_NG_QTY + ins_ng_qty > gprc.GPRC_QTY:
                    raise Exception("戻り不良数と検品不良数の合計が戻り数を超えています")
                
                if pass_qty and pass_qty > (gprc.GPRC_QTY - gprc.GPRC_RET_NG_QTY - ins_ng_qty):
                    raise Exception("合格数が有効数量を超えています")
                
                gprc.GPRC_INS_NG_QTY = ins_ng_qty
                gprc.GPRC_PASS_QTY = pass_qty
                gprc.GPRC_STS = sts
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"検査データの更新中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def delete_nidec_proc(gprc_id):
        """ニデック加工データを削除する"""
        session = get_db_session()
        try:
            gprc = session.query(GprcDatModel).filter(GprcDatModel.GPRC_ID == gprc_id).first()
            if gprc and gprc.GPRC_REQ_TO == 2:  # ニデック加工データのみ削除
                session.delete(gprc)
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            log_error(f"ニデック加工データの削除中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_gshk_gprc_diff(gshk_id):
        """GSHKデータとGPRCデータの差分を取得する"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT dbo.Get_GSHK_GPRC_Diff(:gshk_id) AS diff_qty
            """), {'gshk_id': gshk_id}).fetchone()
            
            # 数値変換を確実に行う
            diff_qty_raw = result.diff_qty if result else 0
            if diff_qty_raw is None:
                return 0
            try:
                return int(diff_qty_raw)
            except (ValueError, TypeError):
                log_error(f"GSHK_GPRC差分の型変換エラー: {diff_qty_raw}(型:{type(diff_qty_raw)})")
                return 0
                
        except Exception as e:
            log_error(f"GSHK_GPRC差分取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_processing_matrix():
        """加工状況マトリックス表のデータを取得する（GPRC_STS別集計対応）"""
        session = get_db_session()
        try:
            # 規格と色の選択肢を取得
            spec_choices = Gradation.get_kbn_choices('GSPEC', 0)
            color_choices = Gradation.get_kbn_choices('GCOLOR', 0)
            
            # コンベックス加工状況を取得（在庫有りのみに最適化）
            convex_data = session.execute(text("""
                SELECT 
                    GPRR.GPRR_SPEC,
                    GPRR.GPRR_COLOR,
                    (SELECT SUM(dbo.Get_GRD_PRC_ZAN_Qty(GPRR_SUB.GPRR_ID)) FROM GPRR_DAT GPRR_SUB WHERE GPRR_SUB.GPRR_SPEC = GPRR.GPRR_SPEC AND GPRR_SUB.GPRR_COLOR = GPRR.GPRR_COLOR AND GPRR_SUB.GPRR_REQ_TO = 1) AS TOTAL_NOT_RETURNED,
                    SUM(CASE WHEN GPRC.GPRC_STS = 0 THEN GPRC.GPRC_PASS_QTY ELSE 0 END) AS TOTAL_PROC_BEFORE_INSPECT,
                    ISNULL(STOCK_SUM.TOTAL_STOCK, 0) AS TOTAL_PROC_AFTER_INSPECT
                FROM GPRR_DAT GPRR
                LEFT JOIN GPRC_DAT GPRC ON GPRR.GPRR_ID = GPRC.GPRC_REQ_ID AND GPRC.GPRC_REQ_TO = 1
                LEFT JOIN (
                    SELECT 
                        GPRR_SUB.GPRR_SPEC,
                        GPRR_SUB.GPRR_COLOR,
                        SUM(dbo.Get_GRD_SHK_ZAN_Qty(GPRC_SUB.GPRC_ID)) AS TOTAL_STOCK
                    FROM GPRC_DAT GPRC_SUB
                    INNER JOIN GPRR_DAT GPRR_SUB ON GPRC_SUB.GPRC_REQ_ID = GPRR_SUB.GPRR_ID
                    WHERE GPRC_SUB.GPRC_REQ_TO = 1 
                    AND GPRC_SUB.GPRC_STS = 1
                    AND dbo.Get_GRD_SHK_ZAN_Qty(GPRC_SUB.GPRC_ID) > 0
                    GROUP BY GPRR_SUB.GPRR_SPEC, GPRR_SUB.GPRR_COLOR
                ) STOCK_SUM ON GPRR.GPRR_SPEC = STOCK_SUM.GPRR_SPEC AND GPRR.GPRR_COLOR = STOCK_SUM.GPRR_COLOR
                WHERE GPRR.GPRR_REQ_TO = 1
                GROUP BY GPRR.GPRR_SPEC, GPRR.GPRR_COLOR, STOCK_SUM.TOTAL_STOCK
            """)).fetchall()
            
            # ニデック加工状況を取得（在庫有りのみに最適化）
            nidec_data = session.execute(text("""
                SELECT 
                    GPRR.GPRR_SPEC,
                    GPRR.GPRR_COLOR,
                    (SELECT SUM(dbo.Get_GSHK_GPRC_Diff(GSHK_SUB.GSHK_ID)) 
                     FROM GSHK_DAT GSHK_SUB 
                     INNER JOIN GPRR_DAT GPRR_SUB ON GSHK_SUB.GSHK_REQ_ID = GPRR_SUB.GPRR_ID
                     WHERE GPRR_SUB.GPRR_SPEC = GPRR.GPRR_SPEC 
                     AND GPRR_SUB.GPRR_COLOR = GPRR.GPRR_COLOR 
                     AND GSHK_SUB.GSHK_TO = 2) AS TOTAL_NOT_RETURNED,
                    SUM(CASE WHEN GPRC.GPRC_STS = 0 THEN GPRC.GPRC_PASS_QTY ELSE 0 END) AS TOTAL_PROC_BEFORE_INSPECT,
                    ISNULL(STOCK_SUM.TOTAL_STOCK, 0) AS TOTAL_PROC_AFTER_INSPECT
                FROM GPRR_DAT GPRR
                LEFT JOIN GSHK_DAT GSHK ON GPRR.GPRR_ID = GSHK.GSHK_REQ_ID AND GSHK.GSHK_TO = 2
                LEFT JOIN GPRC_DAT GPRC ON GSHK.GSHK_ID = GPRC.GPRC_SHK_ID AND GPRC.GPRC_REQ_TO = 2
                LEFT JOIN (
                    SELECT 
                        GPRR_SUB.GPRR_SPEC,
                        GPRR_SUB.GPRR_COLOR,
                        SUM(dbo.Get_GRD_SHK_ZAN_Qty(GPRC_SUB.GPRC_ID)) AS TOTAL_STOCK
                    FROM GPRC_DAT GPRC_SUB
                    INNER JOIN GSHK_DAT GSHK_SUB ON GPRC_SUB.GPRC_SHK_ID = GSHK_SUB.GSHK_ID
                    INNER JOIN GPRR_DAT GPRR_SUB ON GSHK_SUB.GSHK_REQ_ID = GPRR_SUB.GPRR_ID
                    WHERE GPRC_SUB.GPRC_REQ_TO = 2 
                    AND GPRC_SUB.GPRC_STS = 1
                    AND dbo.Get_GRD_SHK_ZAN_Qty(GPRC_SUB.GPRC_ID) > 0
                    GROUP BY GPRR_SUB.GPRR_SPEC, GPRR_SUB.GPRR_COLOR
                ) STOCK_SUM ON GPRR.GPRR_SPEC = STOCK_SUM.GPRR_SPEC AND GPRR.GPRR_COLOR = STOCK_SUM.GPRR_COLOR
                GROUP BY GPRR.GPRR_SPEC, GPRR.GPRR_COLOR, STOCK_SUM.TOTAL_STOCK
                HAVING SUM(CASE WHEN GSHK.GSHK_TO = 2 THEN GSHK.GSHK_QTY ELSE 0 END) > 0
            """)).fetchall()
            
            # データを辞書形式に変換
            convex_dict = {}
            for row in convex_data:
                key = (row.GPRR_SPEC, row.GPRR_COLOR)
                # 数値変換を確実に行う
                total_not_returned = int(row.TOTAL_NOT_RETURNED) if row.TOTAL_NOT_RETURNED is not None else 0
                total_proc_before = int(row.TOTAL_PROC_BEFORE_INSPECT) if row.TOTAL_PROC_BEFORE_INSPECT is not None else 0
                total_proc_after = int(row.TOTAL_PROC_AFTER_INSPECT) if row.TOTAL_PROC_AFTER_INSPECT is not None else 0
                
                convex_dict[key] = {
                    'total_proc_before_inspect': total_proc_before,
                    'total_proc_after_inspect': total_proc_after,
                    'not_returned': total_not_returned
                }
            
            nidec_dict = {}
            for row in nidec_data:
                key = (row.GPRR_SPEC, row.GPRR_COLOR)
                # 数値変換を確実に行う
                total_not_returned = int(row.TOTAL_NOT_RETURNED) if row.TOTAL_NOT_RETURNED is not None else 0
                total_proc_before = int(row.TOTAL_PROC_BEFORE_INSPECT) if row.TOTAL_PROC_BEFORE_INSPECT is not None else 0
                total_proc_after = int(row.TOTAL_PROC_AFTER_INSPECT) if row.TOTAL_PROC_AFTER_INSPECT is not None else 0
                
                nidec_dict[key] = {
                    'total_proc_before_inspect': total_proc_before,
                    'total_proc_after_inspect': total_proc_after,
                    'not_returned': total_not_returned
                }
            
            return {
                'spec_choices': spec_choices,
                'color_choices': color_choices,
                'convex_data': convex_dict,
                'nidec_data': nidec_dict
            }
            
        except Exception as e:
            log_error(f"加工状況マトリックスデータ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_nidec_proc_total_qty_exclude(gshk_id, exclude_gprc_id):
        """指定されたGSHK_IDのニデック加工数量合計を取得する（指定IDを除く）"""
        session = get_db_session()
        try:
            result = session.execute(text("""
                SELECT COALESCE(SUM(GPRC_QTY), 0) AS total_qty
                FROM GPRC_DAT
                WHERE GPRC_SHK_ID = :gshk_id
                AND GPRC_REQ_TO = 2
                AND GPRC_ID != :exclude_gprc_id
            """), {'gshk_id': gshk_id, 'exclude_gprc_id': exclude_gprc_id}).fetchone()
            
            # 数値変換を確実に行う
            total_qty = result.total_qty if result else 0
            if total_qty is None:
                return 0
            try:
                return int(total_qty)
            except (ValueError, TypeError):
                log_error(f"ニデック加工数量合計（除外）の型変換エラー: {total_qty}(型:{type(total_qty)})")
                return 0
                
        except Exception as e:
            log_error(f"ニデック加工数量合計（除外）の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_final_shipping_list(date_from=None, date_to=None, date_from2=None, date_to2=None, flg=None):
        """最終出荷データ一覧を取得する（GSHK_TO=3）"""
        session = get_db_session()
        try:
            sql = """
                SELECT 
                    GSHK.GSHK_ID,
                    GSHK.GSHK_STC_ID,
                    GSHK.GSHK_REQ_ID,
                    GSHK.GSHK_DT,
                    GSHK.GSHK_ORD_DT,
                    GSHK.GSHK_QTY,
                    GSHK.GSHK_FLG,
                    GPRR.GPRR_SPEC,
                    GPRR.GPRR_COLOR,
                    KBN1.KBN_NM AS SPEC_NM,
                    KBN2.KBN_NM AS COLOR_NM,
                    GPRC.GPRC_QTY AS PROC_QTY,
                    GPRC.GPRC_PASS_QTY AS PASS_QTY
                FROM GSHK_DAT GSHK
                LEFT JOIN GPRR_DAT GPRR ON GSHK.GSHK_REQ_ID = GPRR.GPRR_ID
                LEFT JOIN KBN_MST KBN1 ON GPRR.GPRR_SPEC = KBN1.KBN_NO AND KBN1.KBN_ID = 'GSPEC'
                LEFT JOIN KBN_MST KBN2 ON GPRR.GPRR_COLOR = KBN2.KBN_NO AND KBN2.KBN_ID = 'GCOLOR'
                LEFT JOIN GPRC_DAT GPRC ON GSHK.GSHK_STC_ID = GPRC.GPRC_ID
                WHERE GSHK.GSHK_TO = 3
            """
            
            params = {}
            
            if date_from:
                sql += " AND GSHK.GSHK_DT >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND GSHK.GSHK_DT <= :date_to"
                params['date_to'] = date_to
            if date_from2:
                sql += " AND GSHK.GSHK_ORD_DT >= :date_from2"
                params['date_from2'] = date_from2
            if date_to2:
                sql += " AND GSHK.GSHK_ORD_DT <= :date_to2"
                params['date_to2'] = date_to2
            if flg is not None and flg != '':
                sql += " AND GSHK.GSHK_FLG = :flg"
                params['flg'] = int(flg)
            
            sql += " ORDER BY GSHK.GSHK_DT DESC, GSHK.GSHK_ID DESC"
            
            result = session.execute(text(sql), params).fetchall()
            return result
        except Exception as e:
            log_error(f"最終出荷データ一覧の取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_final_shipping_matrix(date_from=None, date_to=None, date_from2=None, date_to2=None, flg=None):
        """最終出荷マトリックス表のデータを取得する（GSHK_TO=3）"""
        session = get_db_session()
        try:
            # 規格と色の選択肢を取得
            spec_choices = Gradation.get_kbn_choices('GSPEC', 0)
            color_choices = Gradation.get_kbn_choices('GCOLOR', 0)
            
            # 基本SQLクエリ
            sql = """
                SELECT 
                    GPRR.GPRR_SPEC,
                    GPRR.GPRR_COLOR,
                    SUM(GSHK.GSHK_QTY) AS TOTAL_SHIPPED_QTY,
                    COUNT(GSHK.GSHK_ID) AS SHIPPING_COUNT
                FROM GSHK_DAT GSHK
                LEFT JOIN GPRR_DAT GPRR ON GSHK.GSHK_REQ_ID = GPRR.GPRR_ID
                WHERE GSHK.GSHK_TO = 3
            """
            
            params = {}
            
            # 検索条件を追加
            if date_from:
                sql += " AND GSHK.GSHK_DT >= :date_from"
                params['date_from'] = date_from
            if date_to:
                sql += " AND GSHK.GSHK_DT <= :date_to"
                params['date_to'] = date_to
            if date_from2:
                sql += " AND GSHK.GSHK_ORD_DT >= :date_from2"
                params['date_from2'] = date_from2
            if date_to2:
                sql += " AND GSHK.GSHK_ORD_DT <= :date_to2"
                params['date_to2'] = date_to2
            if flg is not None and flg != '':
                sql += " AND GSHK.GSHK_FLG = :flg"
                params['flg'] = int(flg)
            
            sql += " GROUP BY GPRR.GPRR_SPEC, GPRR.GPRR_COLOR"
            
            # 最終出荷状況を取得
            final_shipping_data = session.execute(text(sql), params).fetchall()
            
            # データを辞書形式に変換
            final_shipping_dict = {}
            for row in final_shipping_data:
                key = (row.GPRR_SPEC, row.GPRR_COLOR)
                # 数値変換を確実に行う
                total_shipped = int(row.TOTAL_SHIPPED_QTY) if row.TOTAL_SHIPPED_QTY is not None else 0
                shipping_count = int(row.SHIPPING_COUNT) if row.SHIPPING_COUNT is not None else 0
                
                final_shipping_dict[key] = {
                    'total_shipped': total_shipped,
                    'shipping_count': shipping_count
                }
            
            return {
                'spec_choices': spec_choices,
                'color_choices': color_choices,
                'final_shipping_data': final_shipping_dict
            }
            
        except Exception as e:
            log_error(f"最終出荷マトリックスデータ取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_nidec_available_qty(gprc_id):
        """指定されたGPRC_IDのニデック加工出荷可能数を取得する（最終出荷専用）"""
        session = get_db_session()
        try:
            # GPRCデータの合格数を取得
            gprc_result = session.execute(text("""
                SELECT GPRC_PASS_QTY
                FROM GPRC_DAT
                WHERE GPRC_ID = :gprc_id
            """), {'gprc_id': gprc_id}).fetchone()
            
            if not gprc_result:
                return 0
            
            # 数値変換を確実に行う
            pass_qty_raw = gprc_result.GPRC_PASS_QTY
            pass_qty = int(pass_qty_raw) if pass_qty_raw is not None else 0
            
            # 既に最終出荷済みの数量を取得（GSHK_TO=3）
            shipped_qty_result = session.execute(text("""
                SELECT COALESCE(SUM(GSHK_QTY), 0) AS shipped_qty
                FROM GSHK_DAT
                WHERE GSHK_STC_ID = :gprc_id
                AND GSHK_TO = 3
            """), {'gprc_id': gprc_id}).fetchone()
            
            # 数値変換を確実に行う
            shipped_qty_raw = shipped_qty_result.shipped_qty if shipped_qty_result else 0
            shipped_qty = int(shipped_qty_raw) if shipped_qty_raw is not None else 0
            
            # 出荷可能数 = 合格数 - 既最終出荷数
            available_qty = max(0, pass_qty - shipped_qty)
            return available_qty
            
        except Exception as e:
            log_error(f"ニデック出荷可能数取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def execute_auto_shipping(spec, color, qty, order_date, shipping_date):
        """自動出荷を実行（GPRC_REQ_TO=2,GPRC_STS=1のデータをGPRC_DATEの古い順に出荷）"""
        session = get_db_session()
        try:
            # 出荷対象のGPRCデータを取得（GPRC_DATEの古い順）
            target_gprc_list = session.execute(text("""
                SELECT 
                    GPRC_ID,
                    GPRC_REQ_ID,
                    GPRC_DATE,
                    GPRC_PASS_QTY,
                    dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) AS STOCK_QTY
                FROM GPRC_DAT
                LEFT JOIN GPRR_DAT ON GPRC_REQ_ID = GPRR_ID
                WHERE GPRC_REQ_TO = 2
                AND GPRC_STS = 1
                AND GPRR_SPEC = :spec
                AND GPRR_COLOR = :color
                AND dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) > 0
                ORDER BY GPRC_DATE ASC, GPRC_ID ASC
            """), {'spec': spec, 'color': color}).fetchall()
            
            if not target_gprc_list:
                return {
                    'success': False, 
                    'error': f'指定された規格・色の出荷可能データが見つかりません（規格:{spec}, 色:{color}）'
                }
            
            # 出荷処理を実行
            remaining_qty = qty
            shipped_records = []
            total_shipped = 0
            
            for gprc in target_gprc_list:
                if remaining_qty <= 0:
                    break
                
                # このGPRCから出荷可能な数量を計算
                available_qty = min(remaining_qty, gprc.STOCK_QTY)
                
                if available_qty > 0:
                    # 出荷データを作成
                    session.execute(text("""
                        INSERT INTO GSHK_DAT (
                            GSHK_STC_ID,
                            GSHK_REQ_ID,
                            GSHK_DT,
                            GSHK_ORD_DT,
                            GSHK_QTY,
                            GSHK_TO,
                            GSHK_FLG
                        ) VALUES (
                            :gprc_id,
                            :req_id,
                            :shipping_date,
                            :order_date,
                            :qty,
                            3,
                            0
                        )
                    """), {
                        'gprc_id': gprc.GPRC_ID,
                        'req_id': gprc.GPRC_REQ_ID,
                        'shipping_date': shipping_date,
                        'order_date': order_date,
                        'qty': available_qty
                    })
                    
                    shipped_records.append({
                        'gprc_id': gprc.GPRC_ID,
                        'gprc_date': gprc.GPRC_DATE.strftime('%Y-%m-%d') if gprc.GPRC_DATE else '',
                        'shipped_qty': available_qty,
                        'stock_qty': gprc.STOCK_QTY
                    })
                    
                    remaining_qty -= available_qty
                    total_shipped += available_qty
            
            session.commit()
            
            # 結果を返す
            if total_shipped > 0:
                return {
                    'success': True,
                    'message': f'出荷が完了しました。出荷数量: {total_shipped}',
                    'total_shipped': total_shipped,
                    'remaining_qty': remaining_qty,
                    'shipped_records': shipped_records
                }
            else:
                return {
                    'success': False,
                    'error': '出荷可能なデータがありませんでした'
                }
                
        except Exception as e:
            session.rollback()
            log_error(f"自動出荷実行中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()

    @staticmethod
    def get_available_shipping_qty(spec, color):
        """指定された規格・色の出荷可能数量を取得する"""
        session = get_db_session()
        try:
            # 出荷可能なGPRCデータの合計を取得
            result = session.execute(text("""
                SELECT 
                    SUM(dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID)) AS TOTAL_AVAILABLE_QTY,
                    COUNT(*) AS RECORD_COUNT
                FROM GPRC_DAT
                LEFT JOIN GPRR_DAT ON GPRC_REQ_ID = GPRR_ID
                WHERE GPRC_REQ_TO = 2
                AND GPRC_STS = 1
                AND GPRR_SPEC = :spec
                AND GPRR_COLOR = :color
                AND dbo.Get_GRD_SHK_ZAN_Qty(GPRC_ID) > 0
            """), {'spec': spec, 'color': color}).fetchone()
            
            # 数値変換を確実に行う
            total_available = int(result.TOTAL_AVAILABLE_QTY) if result and result.TOTAL_AVAILABLE_QTY is not None else 0
            record_count = int(result.RECORD_COUNT) if result and result.RECORD_COUNT is not None else 0
            
            return {
                'total_available_qty': total_available,
                'record_count': record_count
            }
            
        except Exception as e:
            log_error(f"出荷可能数量取得中にエラーが発生しました: {str(e)}")
            raise
        finally:
            session.close()