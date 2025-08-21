#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
グラデーションデータ移行専用プログラム
GPRR_DATからCPRD_DATとCSHK_DATへの移行処理
"""

import os
import sys
import logging
from datetime import datetime
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# プロジェクトのルートディレクトリをパスに追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import get_db_session
from app.gradation import GprrDatModel, GprcDatModel, GshkDatModel
from app.models_common import CprdDatModel, CshkDatModel, CprcDatModel

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gradation_migration.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GradationMigration:
    """グラデーションデータ移行クラス"""
    
    # 製品IDマッピング（GPRR_SPEC, GPRR_COLOR -> CPDD_PRD_ID）
    PRODUCT_MAPPING = {
        (1, 1): 'K748', (1, 2): 'K753', (1, 3): 'K748', (1, 4): 'K748', (1, 5): 'K748',
        (2, 1): 'K749', (2, 2): 'K754', (2, 3): 'K749', (2, 4): 'K749', (2, 5): 'K749',
        (3, 1): 'K750', (3, 2): 'K755', (3, 3): 'K750', (3, 4): 'K750', (3, 5): 'K750',
        (4, 1): 'K751', (4, 2): 'K756', (4, 3): 'K751', (4, 4): 'K751', (4, 5): 'K751'
    }
    
    # 加工IDマッピング（GPRR_SPEC, GPRR_COLOR -> CSHK_PRC_ID）
    PROCESS_MAPPING = {
        (1, 1): 252, (1, 2): 247, (1, 3): 267, (1, 4): 262, (1, 5): 257,
        (2, 1): 253, (2, 2): 248, (2, 3): 268, (2, 4): 263, (2, 5): 258,
        (3, 1): 254, (3, 2): 249, (3, 3): 269, (3, 4): 264, (3, 5): 259,
        (4, 1): 255, (4, 2): 250, (4, 3): 270, (4, 4): 265, (4, 5): 260
    }
    
    # GPRC用製品IDマッピング（GPRR_SPEC, GPRR_COLOR -> CPDD_PRD_ID）
    GPRC_PRODUCT_MAPPING = {
        (1, 1): '2252', (1, 2): '2247', (1, 3): '2267', (1, 4): '2262', (1, 5): '2257',
        (2, 1): '2253', (2, 2): '2248', (2, 3): '2268', (2, 4): '2263', (2, 5): '2258',
        (3, 1): '2254', (3, 2): '2249', (3, 3): '2269', (3, 4): '2264', (3, 5): '2259',
        (4, 1): '2255', (4, 2): '2250', (4, 3): '2270', (4, 4): '2265', (4, 5): '2260'
    }
    
    # GPRC_REQ_TO=2用製品IDマッピング（GPRR_SPEC, GPRR_COLOR -> CPDD_PRD_ID）
    GPRC_REQ_TO_2_PRODUCT_MAPPING = {
        (1, 1): '2026', (1, 2): '2027', (1, 3): '2028', (1, 4): '2030', (1, 5): '2029',
        (2, 1): '2031', (2, 2): '2032', (2, 3): '2033', (2, 4): '2035', (2, 5): '2034',
        (3, 1): '2036', (3, 2): '2037', (3, 3): '2038', (3, 4): '2040', (3, 5): '2039',
        (4, 1): '2041', (4, 2): '2042', (4, 3): '2043', (4, 4): '2045', (4, 5): '2044'
    }
    
    # GSHK用製品ID・加工IDマッピング（GPRR_SPEC, GPRR_COLOR -> (CSHK_PRD_ID, CSHK_PRC_ID)）
    GSHK_PRODUCT_PROCESS_MAPPING = {
        (1, 1): ('2252', 26), (1, 2): ('2247', 27), (1, 3): ('2267', 28), (1, 4): ('2262', 30), (1, 5): ('2257', 29),
        (2, 1): ('2253', 31), (2, 2): ('2248', 32), (2, 3): ('2268', 33), (2, 4): ('2263', 35), (2, 5): ('2258', 34),
        (3, 1): ('2254', 36), (3, 2): ('2249', 37), (3, 3): ('2269', 38), (3, 4): ('2264', 40), (3, 5): ('2259', 39),
        (4, 1): ('2255', 41), (4, 2): ('2250', 42), (4, 3): ('2270', 43), (4, 4): ('2265', 45), (4, 5): ('2260', 44)
    }
    
    # GSHK_TO=3用製品IDマッピング（GPRR_SPEC, GPRR_COLOR -> CSHK_PRD_ID）
    GSHK_TO_3_PRODUCT_MAPPING = {
        (1, 1): '2026', (1, 2): '2027', (1, 3): '2028', (1, 4): '2030', (1, 5): '2029',
        (2, 1): '2031', (2, 2): '2032', (2, 3): '2033', (2, 4): '2035', (2, 5): '2034',
        (3, 1): '2036', (3, 2): '2037', (3, 3): '2038', (3, 4): '2040', (3, 5): '2039',
        (4, 1): '2041', (4, 2): '2042', (4, 3): '2043', (4, 4): '2045', (4, 5): '2044'
    }
    
    def __init__(self):
        self.session = None
        self.failed_records = []
        self.success_count = 0
        self.error_count = 0
        
    def get_product_id(self, spec, color):
        """製品IDを取得"""
        return self.PRODUCT_MAPPING.get((spec, color))
    
    def get_process_id(self, spec, color):
        """加工IDを取得"""
        return self.PROCESS_MAPPING.get((spec, color))
    
    def get_gprc_product_id(self, spec, color):
        """GPRC用製品IDを取得"""
        return self.GPRC_PRODUCT_MAPPING.get((spec, color))
    
    def get_gprc_req_to_2_product_id(self, spec, color):
        """GPRC_REQ_TO=2用製品IDを取得"""
        return self.GPRC_REQ_TO_2_PRODUCT_MAPPING.get((spec, color))
    
    def get_gshk_product_and_process_id(self, spec, color):
        """GSHK用製品IDと加工IDを取得"""
        return self.GSHK_PRODUCT_PROCESS_MAPPING.get((spec, color))
    
    def get_gshk_to_3_product_id(self, spec, color):
        """GSHK_TO=3用製品IDを取得"""
        return self.GSHK_TO_3_PRODUCT_MAPPING.get((spec, color))
    
    def get_max_split2(self, prd_id, lot):
        """CPDD_SPRIT2の最大値を取得"""
        try:
            sql = text("""
                SELECT ISNULL(MAX(CPDD_SPRIT2), 0) as max_split2
                FROM CPRD_DAT 
                WHERE CPDD_PRD_ID = :prd_id AND CPDD_LOT = :lot
            """)
            result = self.session.execute(sql, {
                'prd_id': prd_id,
                'lot': lot
            }).fetchone()
            return result.max_split2 if result else 0
        except Exception as e:
            logger.error(f"CPDD_SPRIT2最大値取得エラー: {str(e)}")
            raise
    
    def create_cprd_record(self, gprr_record):
        """CPRD_DATレコードを作成"""
        try:
            spec = int(gprr_record.GPRR_SPEC)
            color = int(gprr_record.GPRR_COLOR)
            req_date = gprr_record.GPRR_REQ_DATE
            qty = int(gprr_record.GPRR_QTY)
            
            # 製品IDを取得
            prd_id = self.get_product_id(spec, color)
            if not prd_id:
                raise ValueError(f"無効なSPEC/COLOR組み合わせ: SPEC={spec}, COLOR={color}")
            
            # LOTをYYMMDD形式で作成
            lot = int(req_date.strftime('%y%m%d'))
            
            # SPRIT2の最大値を取得して+1
            max_split2 = self.get_max_split2(prd_id, lot)
            split2 = max_split2 + 1
            
            # CPRD_DATレコードを作成
            cprd_record = CprdDatModel(
                CPDD_PRD_ID=prd_id,
                CPDD_LOT=lot,
                CPDD_SPRIT1=99,
                CPDD_SPRIT2=split2,
                CPDD_RANK=1,
                CPDD_QTY=qty,
                CPDD_FLG=0,
                CPDD_PCD_ID=0
            )
            
            self.session.add(cprd_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cprd_record.CPDD_ID
            
        except Exception as e:
            logger.error(f"CPRD_DAT作成エラー: {str(e)}")
            raise
    
    def create_cshk_record(self, gprr_record, cpdd_id):
        """CSHK_DATレコードを作成"""
        try:
            spec = int(gprr_record.GPRR_SPEC)
            color = int(gprr_record.GPRR_COLOR)
            req_date = gprr_record.GPRR_REQ_DATE
            qty = int(gprr_record.GPRR_QTY)
            
            # 製品IDと加工IDを取得
            prd_id = self.get_product_id(spec, color)
            prc_id = self.get_process_id(spec, color)
            
            if not prd_id or not prc_id:
                raise ValueError(f"無効なSPEC/COLOR組み合わせ: SPEC={spec}, COLOR={color}")
            
            # CSHK_DATレコードを作成
            cshk_record = CshkDatModel(
                CSHK_KBN=1,
                CSHK_TO=604,
                CSHK_PRC_ID=prc_id,
                CSHK_PRD_ID=prd_id,
                CSHK_DT=req_date,
                CSHK_ORD_DT=req_date,
                CSHK_PDD_ID=cpdd_id,
                CSHK_RCP_ID=None,
                CSHK_QTY=qty,
                CSHK_FLG=0
            )
            
            self.session.add(cshk_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cshk_record.CSHK_ID
            
        except Exception as e:
            logger.error(f"CSHK_DAT作成エラー: {str(e)}")
            raise
    
    def migrate_record(self, gprr_record):
        """1件のGPRR_DATレコードを移行"""
        try:
            # CPRD_DATを作成
            cpdd_id = self.create_cprd_record(gprr_record)
            
            # CSHK_DATを作成
            self.create_cshk_record(gprr_record, cpdd_id)
            
            self.success_count += 1
            logger.info(f"移行成功: GPRR_ID={gprr_record.GPRR_ID}")
            
        except Exception as e:
            self.error_count += 1
            error_info = {
                'gprr_id': gprr_record.GPRR_ID,
                'spec': gprr_record.GPRR_SPEC,
                'color': gprr_record.GPRR_COLOR,
                'req_date': gprr_record.GPRR_REQ_DATE,
                'qty': gprr_record.GPRR_QTY,
                'error': str(e)
            }
            self.failed_records.append(error_info)
            logger.error(f"移行失敗: GPRR_ID={gprr_record.GPRR_ID}, エラー={str(e)}")
            raise
    
    def get_all_gprr_records(self):
        """GPRR_DATの全レコードを取得"""
        try:
            return self.session.query(GprrDatModel).all()
        except Exception as e:
            logger.error(f"GPRR_DAT取得エラー: {str(e)}")
            raise
    
    def get_gprc_records(self):
        """GPRC_DATの条件に合うレコードを取得（GPRC_REQ_TO=1）"""
        try:
            return self.session.query(GprcDatModel).filter(
                GprcDatModel.GPRC_REQ_TO == 1
            ).all()
        except Exception as e:
            logger.error(f"GPRC_DAT取得エラー: {str(e)}")
            raise
    
    def get_gprc_req_to_2_records(self):
        """GPRC_DATの条件に合うレコードを取得（GPRC_REQ_TO=2）"""
        try:
            return self.session.query(GprcDatModel).filter(
                GprcDatModel.GPRC_REQ_TO == 2
            ).all()
        except Exception as e:
            logger.error(f"GPRC_DAT（REQ_TO=2）取得エラー: {str(e)}")
            raise
    
    def get_gshk_records(self):
        """GSHK_DATの条件に合うレコードを取得（GSHK_TO=2）"""
        try:
            # GSHK_TO=2 AND GSHK_STC_ID=GPRC_IDの条件で取得
            return self.session.query(GshkDatModel).filter(
                GshkDatModel.GSHK_TO == 2
            ).all()
        except Exception as e:
            logger.error(f"GSHK_DAT取得エラー: {str(e)}")
            raise
    
    def get_gshk_to_3_records(self):
        """GSHK_DATの条件に合うレコードを取得（GSHK_TO=3）"""
        try:
            # GSHK_TO=3 AND GSHK_STC_ID=GPRC_IDの条件で取得
            return self.session.query(GshkDatModel).filter(
                GshkDatModel.GSHK_TO == 3
            ).all()
        except Exception as e:
            logger.error(f"GSHK_DAT（TO=3）取得エラー: {str(e)}")
            raise
    
    def create_cprc_record(self, gprc_record, cshk_id):
        """CPRC_DATレコードを作成"""
        try:
            # CPRC_DATレコードを作成
            cprc_record = CprcDatModel(
                CPCD_SHK_ID=cshk_id,
                CPCD_DATE=gprc_record.GPRC_DATE,
                CPCD_QTY=gprc_record.GPRC_QTY,
                CPCD_RET_NG_QTY=gprc_record.GPRC_RET_NG_QTY,
                CPCD_INS_NG_QTY=gprc_record.GPRC_INS_NG_QTY,
                CPCD_PASS_QTY=gprc_record.GPRC_PASS_QTY
            )
            
            self.session.add(cprc_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cprc_record.CPCD_ID
            
        except Exception as e:
            logger.error(f"CPRC_DAT作成エラー: {str(e)}")
            raise
    
    def create_gprc_cprd_record(self, gprc_record, gprr_record, cpcd_id):
        """GPRC用CPRD_DATレコードを作成"""
        try:
            spec = int(gprr_record.GPRR_SPEC)
            color = int(gprr_record.GPRR_COLOR)
            prc_date = gprc_record.GPRC_DATE
            pass_qty = int(gprc_record.GPRC_PASS_QTY)
            
            # 製品IDを取得
            prd_id = self.get_gprc_product_id(spec, color)
            if not prd_id:
                raise ValueError(f"無効なSPEC/COLOR組み合わせ: SPEC={spec}, COLOR={color}")
            
            # LOTをYYMMDD形式で作成
            lot = int(prc_date.strftime('%y%m%d'))
            
            # SPRIT2の最大値を取得して+1
            max_split2 = self.get_max_split2(prd_id, lot)
            split2 = max_split2 + 1
            
            # CPRD_DATレコードを作成
            cprd_record = CprdDatModel(
                CPDD_PRD_ID=prd_id,
                CPDD_LOT=lot,
                CPDD_SPRIT1=99,
                CPDD_SPRIT2=split2,
                CPDD_RANK=1,
                CPDD_QTY=pass_qty,
                CPDD_FLG=0,
                CPDD_PCD_ID=cpcd_id
            )
            
            self.session.add(cprd_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cprd_record.CPDD_ID
            
        except Exception as e:
            logger.error(f"GPRC用CPRD_DAT作成エラー: {str(e)}")
            raise
    
    def migrate_gprc_record(self, gprc_record):
        """1件のGPRC_DATレコードを移行"""
        try:
            # GPRR_DATから対応するレコードを取得
            gprr_record = self.session.query(GprrDatModel).filter(
                GprrDatModel.GPRR_ID == gprc_record.GPRC_REQ_ID
            ).first()
            
            if not gprr_record:
                raise ValueError(f"GPRR_DATに対応するレコードが見つかりません: GPRR_ID={gprc_record.GPRC_REQ_ID}")
            
            # CSHK_DATから対応するレコードを取得
            cshk_record = self.session.query(CshkDatModel).filter(
                CshkDatModel.CSHK_PDD_ID == gprr_record.GPRR_ID
            ).first()
            
            if not cshk_record:
                raise ValueError(f"CSHK_DATに対応するレコードが見つかりません: GPRR_ID={gprr_record.GPRR_ID}")
            
            # CPRC_DATを作成
            cpcd_id = self.create_cprc_record(gprc_record, cshk_record.CSHK_ID)
            
            # CPRD_DATを作成
            self.create_gprc_cprd_record(gprc_record, gprr_record, cpcd_id)
            
            self.success_count += 1
            logger.info(f"GPRC移行成功: GPRC_ID={gprc_record.GPRC_ID}")
            
        except Exception as e:
            self.error_count += 1
            error_info = {
                'gprc_id': gprc_record.GPRC_ID,
                'gprr_id': gprc_record.GPRC_REQ_ID,
                'req_to': gprc_record.GPRC_REQ_TO,
                'prc_date': gprc_record.GPRC_DATE,
                'qty': gprc_record.GPRC_QTY,
                'error': str(e)
            }
            self.failed_records.append(error_info)
            logger.error(f"GPRC移行失敗: GPRC_ID={gprc_record.GPRC_ID}, エラー={str(e)}")
            raise
    
    def create_gprc_req_to_2_cprd_record(self, gprc_record, gprr_record, cpcd_id):
        """GPRC_REQ_TO=2用CPRD_DATレコードを作成"""
        try:
            spec = int(gprr_record.GPRR_SPEC)
            color = int(gprr_record.GPRR_COLOR)
            prc_date = gprc_record.GPRC_DATE
            pass_qty = int(gprc_record.GPRC_PASS_QTY)
            
            # 製品IDを取得
            prd_id = self.get_gprc_req_to_2_product_id(spec, color)
            if not prd_id:
                raise ValueError(f"無効なSPEC/COLOR組み合わせ: SPEC={spec}, COLOR={color}")
            
            # LOTをYYMMDD形式で作成
            lot = int(prc_date.strftime('%y%m%d'))
            
            # SPRIT2の最大値を取得して+1
            max_split2 = self.get_max_split2(prd_id, lot)
            split2 = max_split2 + 1
            
            # CPRD_DATレコードを作成
            cprd_record = CprdDatModel(
                CPDD_PRD_ID=prd_id,
                CPDD_LOT=lot,
                CPDD_SPRIT1=99,
                CPDD_SPRIT2=split2,
                CPDD_RANK=1,
                CPDD_QTY=pass_qty,
                CPDD_FLG=0,
                CPDD_PCD_ID=cpcd_id
            )
            
            self.session.add(cprd_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cprd_record.CPDD_ID
            
        except Exception as e:
            logger.error(f"GPRC_REQ_TO=2用CPRD_DAT作成エラー: {str(e)}")
            raise
    
    def migrate_gprc_req_to_2_record(self, gprc_record):
        """1件のGPRC_DATレコードを移行（GPRC_REQ_TO=2）"""
        try:
            # GPRR_DATから対応するレコードを取得
            gprr_record = self.session.query(GprrDatModel).filter(
                GprrDatModel.GPRR_ID == gprc_record.GPRC_REQ_ID
            ).first()
            
            if not gprr_record:
                raise ValueError(f"GPRR_DATに対応するレコードが見つかりません: GPRR_ID={gprc_record.GPRC_REQ_ID}")
            
            # GSHK_DATから対応するレコードを取得（GPRC_SHK_IDを使用）
            gshk_record = self.session.query(GshkDatModel).filter(
                GshkDatModel.GSHK_ID == gprc_record.GPRC_SHK_ID
            ).first()
            
            if not gshk_record:
                raise ValueError(f"GSHK_DATに対応するレコードが見つかりません: GSHK_ID={gprc_record.GPRC_SHK_ID}")
            
            # CSHK_DATから対応するレコードを取得（GSHK_IDを使用）
            cshk_record = self.session.query(CshkDatModel).filter(
                CshkDatModel.CSHK_PDD_ID == gshk_record.GSHK_ID
            ).first()
            
            if not cshk_record:
                raise ValueError(f"CSHK_DATに対応するレコードが見つかりません: GSHK_ID={gshk_record.GSHK_ID}")
            
            # CPRC_DATを作成
            cpcd_id = self.create_cprc_record(gprc_record, cshk_record.CSHK_ID)
            
            # CPRD_DATを作成
            self.create_gprc_req_to_2_cprd_record(gprc_record, gprr_record, cpcd_id)
            
            self.success_count += 1
            logger.info(f"GPRC_REQ_TO=2移行成功: GPRC_ID={gprc_record.GPRC_ID}")
            
        except Exception as e:
            self.error_count += 1
            error_info = {
                'gprc_id': gprc_record.GPRC_ID,
                'gprr_id': gprc_record.GPRC_REQ_ID,
                'req_to': gprc_record.GPRC_REQ_TO,
                'prc_date': gprc_record.GPRC_DATE,
                'qty': gprc_record.GPRC_QTY,
                'error': str(e)
            }
            self.failed_records.append(error_info)
            logger.error(f"GPRC_REQ_TO=2移行失敗: GPRC_ID={gprc_record.GPRC_ID}, エラー={str(e)}")
            raise
    
    def create_gshk_cshk_record(self, gshk_record, gprr_record, cpdd_id):
        """GSHK用CSHK_DATレコードを作成（GSHK_TO=2）"""
        try:
            spec = int(gprr_record.GPRR_SPEC)
            color = int(gprr_record.GPRR_COLOR)
            shk_qty = int(gshk_record.GSHK_QTY)
            
            # 製品IDと加工IDを取得
            mapping_result = self.get_gshk_product_and_process_id(spec, color)
            if not mapping_result:
                raise ValueError(f"無効なSPEC/COLOR組み合わせ: SPEC={spec}, COLOR={color}")
            
            prd_id, prc_id = mapping_result
            
            # CSHK_DATレコードを作成
            cshk_record = CshkDatModel(
                CSHK_KBN=1,
                CSHK_TO=602,
                CSHK_PRC_ID=prc_id,
                CSHK_PRD_ID=prd_id,
                CSHK_DT=gshk_record.GSHK_DT,
                CSHK_ORD_DT=gshk_record.GSHK_ORD_DT,
                CSHK_PDD_ID=cpdd_id,
                CSHK_RCP_ID=None,
                CSHK_QTY=shk_qty,
                CSHK_FLG=0
            )
            
            self.session.add(cshk_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cshk_record.CSHK_ID
            
        except Exception as e:
            logger.error(f"GSHK用CSHK_DAT作成エラー: {str(e)}")
            raise
    
    def create_gshk_to_3_cshk_record(self, gshk_record, gprr_record, cpdd_id):
        """GSHK_TO=3用CSHK_DATレコードを作成"""
        try:
            spec = int(gprr_record.GPRR_SPEC)
            color = int(gprr_record.GPRR_COLOR)
            shk_qty = int(gshk_record.GSHK_QTY)
            
            # 製品IDを取得
            prd_id = self.get_gshk_to_3_product_id(spec, color)
            if not prd_id:
                raise ValueError(f"無効なSPEC/COLOR組み合わせ: SPEC={spec}, COLOR={color}")
            
            # CSHK_DATレコードを作成
            cshk_record = CshkDatModel(
                CSHK_KBN=0,
                CSHK_TO=501,
                CSHK_PRC_ID=0,
                CSHK_PRD_ID=prd_id,
                CSHK_DT=gshk_record.GSHK_DT,
                CSHK_ORD_DT=gshk_record.GSHK_ORD_DT,
                CSHK_PDD_ID=cpdd_id,
                CSHK_RCP_ID=None,
                CSHK_QTY=shk_qty,
                CSHK_FLG=0
            )
            
            self.session.add(cshk_record)
            self.session.flush()  # IDを取得するためにフラッシュ
            
            return cshk_record.CSHK_ID
            
        except Exception as e:
            logger.error(f"GSHK_TO=3用CSHK_DAT作成エラー: {str(e)}")
            raise
    
    def migrate_gshk_record(self, gshk_record):
        """1件のGSHK_DATレコードを移行（GSHK_TO=2）"""
        try:
            # GPRC_DATから対応するレコードを取得（GSHK_STC_IDを使用）
            gprc_record = self.session.query(GprcDatModel).filter(
                GprcDatModel.GPRC_ID == gshk_record.GSHK_STC_ID
            ).first()
            
            if not gprc_record:
                raise ValueError(f"GPRC_DATに対応するレコードが見つかりません: GPRC_ID={gshk_record.GSHK_STC_ID}")
            
            # GPRR_DATから対応するレコードを取得
            gprr_record = self.session.query(GprrDatModel).filter(
                GprrDatModel.GPRR_ID == gprc_record.GPRC_REQ_ID
            ).first()
            
            if not gprr_record:
                raise ValueError(f"GPRR_DATに対応するレコードが見つかりません: GPRR_ID={gprc_record.GPRC_REQ_ID}")
            
            # GPRC用CPRD_DATから対応するレコードを取得
            cpdd_record = self.session.query(CprdDatModel).filter(
                CprdDatModel.CPDD_PCD_ID == gprc_record.GPRC_ID
            ).first()
            
            if not cpdd_record:
                raise ValueError(f"CPRD_DATに対応するレコードが見つかりません: GPRC_ID={gprc_record.GPRC_ID}")
            
            # CSHK_DATを作成
            self.create_gshk_cshk_record(gshk_record, gprr_record, cpdd_record.CPDD_ID)
            
            self.success_count += 1
            logger.info(f"GSHK移行成功: GSHK_ID={gshk_record.GSHK_ID}")
            
        except Exception as e:
            self.error_count += 1
            error_info = {
                'gshk_id': gshk_record.GSHK_ID,
                'gprc_id': gshk_record.GSHK_STC_ID,
                'shk_to': gshk_record.GSHK_TO,
                'shk_dt': gshk_record.GSHK_DT,
                'qty': gshk_record.GSHK_QTY,
                'error': str(e)
            }
            self.failed_records.append(error_info)
            logger.error(f"GSHK移行失敗: GSHK_ID={gshk_record.GSHK_ID}, エラー={str(e)}")
            raise
    
    def migrate_gshk_to_3_record(self, gshk_record):
        """1件のGSHK_DATレコードを移行（GSHK_TO=3）"""
        try:
            # GPRC_DATから対応するレコードを取得（GSHK_STC_IDを使用）
            gprc_record = self.session.query(GprcDatModel).filter(
                GprcDatModel.GPRC_ID == gshk_record.GSHK_STC_ID
            ).first()
            
            if not gprc_record:
                raise ValueError(f"GPRC_DATに対応するレコードが見つかりません: GPRC_ID={gshk_record.GSHK_STC_ID}")
            
            # GPRR_DATから対応するレコードを取得
            gprr_record = self.session.query(GprrDatModel).filter(
                GprrDatModel.GPRR_ID == gprc_record.GPRC_REQ_ID
            ).first()
            
            if not gprr_record:
                raise ValueError(f"GPRR_DATに対応するレコードが見つかりません: GPRR_ID={gprc_record.GPRC_REQ_ID}")
            
            # GPRC用CPRD_DATから対応するレコードを取得
            cpdd_record = self.session.query(CprdDatModel).filter(
                CprdDatModel.CPDD_PCD_ID == gprc_record.GPRC_ID
            ).first()
            
            if not cpdd_record:
                raise ValueError(f"CPRD_DATに対応するレコードが見つかりません: GPRC_ID={gprc_record.GPRC_ID}")
            
            # CSHK_DATを作成
            self.create_gshk_to_3_cshk_record(gshk_record, gprr_record, cpdd_record.CPDD_ID)
            
            self.success_count += 1
            logger.info(f"GSHK_TO=3移行成功: GSHK_ID={gshk_record.GSHK_ID}")
            
        except Exception as e:
            self.error_count += 1
            error_info = {
                'gshk_id': gshk_record.GSHK_ID,
                'gprc_id': gshk_record.GSHK_STC_ID,
                'shk_to': gshk_record.GSHK_TO,
                'shk_dt': gshk_record.GSHK_DT,
                'qty': gshk_record.GSHK_QTY,
                'error': str(e)
            }
            self.failed_records.append(error_info)
            logger.error(f"GSHK_TO=3移行失敗: GSHK_ID={gshk_record.GSHK_ID}, エラー={str(e)}")
            raise
    
    def get_failed_records_content(self):
        """失敗したレコードの内容を文字列として取得"""
        if not self.failed_records:
            return ""
        
        content = []
        content.append("移行失敗レコード一覧")
        content.append("=" * 50)
        content.append(f"作成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        content.append(f"失敗件数: {len(self.failed_records)}")
        content.append("")
        
        for record in self.failed_records:
            if 'gprr_id' in record and 'spec' in record:
                # GPRR移行の失敗レコード
                content.append(f"GPRR_ID: {record['gprr_id']}")
                content.append(f"SPEC: {record['spec']}")
                content.append(f"COLOR: {record['color']}")
                content.append(f"依頼日: {record['req_date']}")
                content.append(f"数量: {record['qty']}")
            elif 'gprc_id' in record and 'gprr_id' in record:
                # GPRC移行の失敗レコード
                content.append(f"GPRC_ID: {record['gprc_id']}")
                content.append(f"GPRR_ID: {record['gprr_id']}")
                content.append(f"依頼先: {record['req_to']}")
                content.append(f"戻り日: {record['prc_date']}")
                content.append(f"数量: {record['qty']}")
            else:
                # GSHK移行の失敗レコード
                content.append(f"GSHK_ID: {record['gshk_id']}")
                content.append(f"GPRC_ID: {record['gprc_id']}")
                content.append(f"出荷先: {record['shk_to']}")
                content.append(f"出荷日: {record['shk_dt']}")
                content.append(f"数量: {record['qty']}")
            
            content.append(f"エラー: {record['error']}")
            content.append("-" * 30)
        
        return "\n".join(content)
    
    def run_migration(self):
        """移行処理を実行"""
        logger.info("グラデーションデータ移行を開始します")
        
        try:
            self.session = get_db_session()
            
            # GPRR_DATの全レコードを取得
            gprr_records = self.get_all_gprr_records()
            gprr_total_count = len(gprr_records)
            
            logger.info(f"GPRR移行対象レコード数: {gprr_total_count}")
            
            if gprr_total_count > 0:
                # 1つのGPRRレコードに対して関連するすべての処理を順次実行
                for gprr_record in gprr_records:
                    try:
                        logger.info(f"=== GPRR_ID={gprr_record.GPRR_ID}の処理開始 ===")
                        
                        # 1. GPRR → CPRD_DAT + CSHK_DAT
                        cpdd_id = self.create_cprd_record(gprr_record)
                        cshk_id = self.create_cshk_record(gprr_record, cpdd_id)
                        self.success_count += 1
                        logger.info(f"GPRR移行成功: GPRR_ID={gprr_record.GPRR_ID}")
                        
                        # 2. 関連するGPRC_DAT（REQ_TO=1）を取得して処理
                        related_gprc_records = self.session.query(GprcDatModel).filter(
                            GprcDatModel.GPRC_REQ_ID == gprr_record.GPRR_ID,
                            GprcDatModel.GPRC_REQ_TO == 1
                        ).all()
                        
                        for gprc_record in related_gprc_records:
                            try:
                                # GPRC → CPRC_DAT + CPRD_DAT
                                # CPRC_DATを作成（GPRR用CSHK_DATのIDを使用）
                                cpcd_id = self.create_cprc_record(gprc_record, cshk_id)
                                
                                # GPRC用CPRD_DATを作成
                                gprc_cpdd_id = self.create_gprc_cprd_record(gprc_record, gprr_record, cpcd_id)
                                self.success_count += 1
                                logger.info(f"GPRC移行成功: GPRC_ID={gprc_record.GPRC_ID}")
                                
                                # 3. 関連するGSHK_DAT（TO=2）を取得して処理
                                related_gshk_records = self.session.query(GshkDatModel).filter(
                                    GshkDatModel.GSHK_STC_ID == gprc_record.GPRC_ID,
                                    GshkDatModel.GSHK_TO == 2
                                ).all()
                                
                                for gshk_record in related_gshk_records:
                                    try:
                                        # GSHK → CSHK_DAT（GPRC用CPRD_DATのIDを使用）
                                        gshk_cshk_id = self.create_gshk_cshk_record(gshk_record, gprr_record, gprc_cpdd_id)
                                        self.success_count += 1
                                        logger.info(f"GSHK_TO=2移行成功: GSHK_ID={gshk_record.GSHK_ID}")
                                        
                                        # 4. 関連するGPRC_DAT（REQ_TO=2）を取得して処理
                                        related_gprc_req_to_2 = self.session.query(GprcDatModel).filter(
                                            GprcDatModel.GPRC_SHK_ID == gshk_record.GSHK_ID,
                                            GprcDatModel.GPRC_REQ_TO == 2
                                        ).all()
                                        
                                        for gprc_req_to_2 in related_gprc_req_to_2:
                                            try:
                                                # GPRC_REQ_TO=2 → CPRC_DAT + CPRD_DAT（GSHK用CSHK_DATのIDを使用）
                                                cpcd_req_to_2_id = self.create_cprc_record(gprc_req_to_2, gshk_cshk_id)
                                                gprc_req_to_2_cpdd_id = self.create_gprc_req_to_2_cprd_record(gprc_req_to_2, gprr_record, cpcd_req_to_2_id)
                                                self.success_count += 1
                                                logger.info(f"GPRC_REQ_TO=2移行成功: GPRC_ID={gprc_req_to_2.GPRC_ID}")
                                                
                                                # 5. 関連するGSHK_DAT（TO=3）を取得して処理
                                                related_gshk_to_3 = self.session.query(GshkDatModel).filter(
                                                    GshkDatModel.GSHK_STC_ID == gprc_req_to_2.GPRC_ID,
                                                    GshkDatModel.GSHK_TO == 3
                                                ).all()
                                                
                                                for gshk_to_3_record in related_gshk_to_3:
                                                    try:
                                                        # GSHK_TO=3 → CSHK_DAT（GPRC_REQ_TO=2用CPRD_DATのIDを使用）
                                                        gshk_to_3_cshk_id = self.create_gshk_to_3_cshk_record(gshk_to_3_record, gprr_record, gprc_req_to_2_cpdd_id)
                                                        self.success_count += 1
                                                        logger.info(f"GSHK_TO=3移行成功: GSHK_ID={gshk_to_3_record.GSHK_ID}")
                                                        
                                                    except Exception as e:
                                                        self.error_count += 1
                                                        error_info = {
                                                            'gshk_id': gshk_to_3_record.GSHK_ID,
                                                            'gprc_id': gshk_to_3_record.GSHK_STC_ID,
                                                            'shk_to': gshk_to_3_record.GSHK_TO,
                                                            'shk_dt': gshk_to_3_record.GSHK_DT,
                                                            'qty': gshk_to_3_record.GSHK_QTY,
                                                            'error': str(e)
                                                        }
                                                        self.failed_records.append(error_info)
                                                        logger.error(f"GSHK_TO=3移行失敗: GSHK_ID={gshk_to_3_record.GSHK_ID}, エラー={str(e)}")
                                                        continue
                                                
                                            except Exception as e:
                                                self.error_count += 1
                                                error_info = {
                                                    'gprc_id': gprc_req_to_2.GPRC_ID,
                                                    'gprr_id': gprc_req_to_2.GPRC_REQ_ID,
                                                    'req_to': gprc_req_to_2.GPRC_REQ_TO,
                                                    'prc_date': gprc_req_to_2.GPRC_DATE,
                                                    'qty': gprc_req_to_2.GPRC_QTY,
                                                    'error': str(e)
                                                }
                                                self.failed_records.append(error_info)
                                                logger.error(f"GPRC_REQ_TO=2移行失敗: GPRC_ID={gprc_req_to_2.GPRC_ID}, エラー={str(e)}")
                                                continue
                                                
                                    except Exception as e:
                                        self.error_count += 1
                                        error_info = {
                                            'gshk_id': gshk_record.GSHK_ID,
                                            'gprc_id': gshk_record.GSHK_STC_ID,
                                            'shk_to': gshk_record.GSHK_TO,
                                            'shk_dt': gshk_record.GSHK_DT,
                                            'qty': gshk_record.GSHK_QTY,
                                            'error': str(e)
                                        }
                                        self.failed_records.append(error_info)
                                        logger.error(f"GSHK_TO=2移行失敗: GSHK_ID={gshk_record.GSHK_ID}, エラー={str(e)}")
                                        continue
                                
                                
                                
                            except Exception as e:
                                self.error_count += 1
                                error_info = {
                                    'gprc_id': gprc_record.GPRC_ID,
                                    'gprr_id': gprc_record.GPRC_REQ_ID,
                                    'req_to': gprc_record.GPRC_REQ_TO,
                                    'prc_date': gprc_record.GPRC_DATE,
                                    'qty': gprc_record.GPRC_QTY,
                                    'error': str(e)
                                }
                                self.failed_records.append(error_info)
                                logger.error(f"GPRC移行失敗: GPRC_ID={gprc_record.GPRC_ID}, エラー={str(e)}")
                                continue
                        
                        logger.info(f"=== GPRR_ID={gprr_record.GPRR_ID}の処理完了 ===")
                        
                    except Exception as e:
                        self.error_count += 1
                        error_info = {
                            'gprr_id': gprr_record.GPRR_ID,
                            'spec': gprr_record.GPRR_SPEC,
                            'color': gprr_record.GPRR_COLOR,
                            'req_date': gprr_record.GPRR_REQ_DATE,
                            'qty': gprr_record.GPRR_QTY,
                            'error': str(e)
                        }
                        self.failed_records.append(error_info)
                        logger.error(f"GPRR移行失敗: GPRR_ID={gprr_record.GPRR_ID}, エラー={str(e)}")
                        continue
            
            # 成功した場合はコミット
            if self.error_count == 0:
                self.session.commit()
                logger.info(f"移行完了: 成功={self.success_count}件")
            else:
                # エラーがある場合はロールバック
                self.session.rollback()
                logger.error(f"移行失敗: 成功={self.success_count}件, 失敗={self.error_count}件")
                
        except Exception as e:
            if self.session:
                self.session.rollback()
            logger.error(f"移行処理で致命的エラーが発生: {str(e)}")
            raise
        finally:
            if self.session:
                self.session.close()

def main():
    """メイン処理"""
    try:
        migration = GradationMigration()
        migration.run_migration()
        
        print(f"\n移行結果:")
        print(f"成功件数: {migration.success_count}")
        print(f"失敗件数: {migration.error_count}")
        
        if migration.failed_records:
            print(f"失敗レコードの詳細は failed_migration_*.txt ファイルを確認してください")
        
    except Exception as e:
        logger.error(f"移行処理でエラーが発生: {str(e)}")
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
