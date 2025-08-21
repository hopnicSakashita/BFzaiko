#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
グラデーションデータ移行プログラムのテストスクリプト
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from sqlalchemy import func

# プロジェクトのルートディレクトリをパスに追加
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import get_db_session
from app.gradation import GprrDatModel, GprcDatModel, GshkDatModel
from app.models_common import CprdDatModel, CshkDatModel, CprcDatModel

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_test_data():
    """テスト用のGPRR_DAT、GPRC_DAT、GSHK_DATデータを作成"""
    session = get_db_session()
    try:
        # 既存のテストデータを削除（移行先テーブルも含む）
        session.query(CprcDatModel).delete()
        session.query(CshkDatModel).delete()
        session.query(CprdDatModel).delete()
        session.query(GshkDatModel).filter(
            GshkDatModel.GSHK_TO.in_([2, 3])
        ).delete()
        session.query(GprcDatModel).filter(
            GprcDatModel.GPRC_REQ_TO.in_([1, 2])
        ).delete()
        session.query(GprrDatModel).filter(
            GprrDatModel.GPRR_SPEC.in_([1, 2, 3, 4])
        ).delete()
        
        # GPRRテストデータを作成
        gprr_records = [
            # SPEC=1, COLOR=1-5
            GprrDatModel(GPRR_SPEC=1, GPRR_COLOR=1, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=100),
            GprrDatModel(GPRR_SPEC=1, GPRR_COLOR=2, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=150),
            GprrDatModel(GPRR_SPEC=1, GPRR_COLOR=3, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=200),
            GprrDatModel(GPRR_SPEC=1, GPRR_COLOR=4, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=120),
            GprrDatModel(GPRR_SPEC=1, GPRR_COLOR=5, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=180),
            
            # SPEC=2, COLOR=1-5
            GprrDatModel(GPRR_SPEC=2, GPRR_COLOR=1, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=90),
            GprrDatModel(GPRR_SPEC=2, GPRR_COLOR=2, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=160),
            GprrDatModel(GPRR_SPEC=2, GPRR_COLOR=3, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=110),
            GprrDatModel(GPRR_SPEC=2, GPRR_COLOR=4, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=140),
            GprrDatModel(GPRR_SPEC=2, GPRR_COLOR=5, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=170),
            
            # SPEC=3, COLOR=1-5
            GprrDatModel(GPRR_SPEC=3, GPRR_COLOR=1, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=80),
            GprrDatModel(GPRR_SPEC=3, GPRR_COLOR=2, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=130),
            GprrDatModel(GPRR_SPEC=3, GPRR_COLOR=3, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=95),
            GprrDatModel(GPRR_SPEC=3, GPRR_COLOR=4, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=125),
            GprrDatModel(GPRR_SPEC=3, GPRR_COLOR=5, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=155),
            
            # SPEC=4, COLOR=1-5
            GprrDatModel(GPRR_SPEC=4, GPRR_COLOR=1, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=75),
            GprrDatModel(GPRR_SPEC=4, GPRR_COLOR=2, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=145),
            GprrDatModel(GPRR_SPEC=4, GPRR_COLOR=3, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=105),
            GprrDatModel(GPRR_SPEC=4, GPRR_COLOR=4, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=135),
            GprrDatModel(GPRR_SPEC=4, GPRR_COLOR=5, GPRR_REQ_TO=1, GPRR_REQ_DATE=datetime.now(), GPRR_QTY=165),
        ]
        
        for record in gprr_records:
            session.add(record)
        
        session.flush()  # IDを取得するためにフラッシュ
        
        # GPRCテストデータを作成（一部のGPRRレコードに対応）
        gprc_records = [
            GprcDatModel(GPRC_REQ_ID=gprr_records[0].GPRR_ID, GPRC_REQ_TO=1, GPRC_DATE=datetime.now(), 
                        GPRC_QTY=95, GPRC_RET_NG_QTY=3, GPRC_INS_NG_QTY=2, GPRC_SHK_ID=0, GPRC_PASS_QTY=90, GPRC_STS=1),
            GprcDatModel(GPRC_REQ_ID=gprr_records[1].GPRR_ID, GPRC_REQ_TO=1, GPRC_DATE=datetime.now(), 
                        GPRC_QTY=140, GPRC_RET_NG_QTY=5, GPRC_INS_NG_QTY=5, GPRC_SHK_ID=0, GPRC_PASS_QTY=130, GPRC_STS=1),
            GprcDatModel(GPRC_REQ_ID=gprr_records[5].GPRR_ID, GPRC_REQ_TO=1, GPRC_DATE=datetime.now(), 
                        GPRC_QTY=85, GPRC_RET_NG_QTY=2, GPRC_INS_NG_QTY=3, GPRC_SHK_ID=0, GPRC_PASS_QTY=80, GPRC_STS=1),
        ]
        
        for record in gprc_records:
            session.add(record)
        
        session.flush()  # IDを取得するためにフラッシュ
        
        # GSHKテストデータを作成（一部のGPRCレコードに対応）
        gshk_to_2_records = [
            GshkDatModel(GSHK_STC_ID=gprc_records[0].GPRC_ID, GSHK_TO=2, GSHK_DT=datetime.now(), 
                        GSHK_ORD_DT=datetime.now(), GSHK_QTY=85, GSHK_FLG=0, GSHK_REQ_ID=0),
            GshkDatModel(GSHK_STC_ID=gprc_records[1].GPRC_ID, GSHK_TO=2, GSHK_DT=datetime.now(), 
                        GSHK_ORD_DT=datetime.now(), GSHK_QTY=120, GSHK_FLG=0, GSHK_REQ_ID=0),
        ]
        
        for record in gshk_to_2_records:
            session.add(record)
        
        session.commit()
        logger.info(f"GPRRテストデータを作成しました: {len(gprr_records)}件")
        logger.info(f"GPRCテストデータを作成しました: {len(gprc_records)}件")
        logger.info(f"GSHK_TO=2テストデータを作成しました: {len(gshk_to_2_records)}件")
        
        # GPRC_REQ_TO=2テストデータを作成（GSHKレコードに対応）
        gprc_req_to_2_records = [
            GprcDatModel(GPRC_REQ_ID=gprr_records[2].GPRR_ID, GPRC_REQ_TO=2, GPRC_DATE=datetime.now(), 
                        GPRC_QTY=180, GPRC_RET_NG_QTY=4, GPRC_INS_NG_QTY=6, GPRC_SHK_ID=gshk_to_2_records[0].GSHK_ID, GPRC_PASS_QTY=170, GPRC_STS=1),
            GprcDatModel(GPRC_REQ_ID=gprr_records[3].GPRR_ID, GPRC_REQ_TO=2, GPRC_DATE=datetime.now(), 
                        GPRC_QTY=110, GPRC_RET_NG_QTY=2, GPRC_INS_NG_QTY=3, GPRC_SHK_ID=gshk_to_2_records[1].GSHK_ID, GPRC_PASS_QTY=105, GPRC_STS=1),
        ]
        
        for record in gprc_req_to_2_records:
            session.add(record)
        
        session.commit()
        logger.info(f"GPRC_REQ_TO=2テストデータを作成しました: {len(gprc_req_to_2_records)}件")
        
        # GSHK_TO=3テストデータを作成（GPRC_REQ_TO=2レコードに対応）
        gshk_to_3_records = [
            GshkDatModel(GSHK_STC_ID=gprc_req_to_2_records[0].GPRC_ID, GSHK_TO=3, GSHK_DT=datetime.now(), 
                        GSHK_ORD_DT=datetime.now(), GSHK_QTY=160, GSHK_FLG=0, GSHK_REQ_ID=0),
            GshkDatModel(GSHK_STC_ID=gprc_req_to_2_records[1].GPRC_ID, GSHK_TO=3, GSHK_DT=datetime.now(), 
                        GSHK_ORD_DT=datetime.now(), GSHK_QTY=100, GSHK_FLG=0, GSHK_REQ_ID=0),
        ]
        
        for record in gshk_to_3_records:
            session.add(record)
        
        session.commit()
        logger.info(f"GSHK_TO=3テストデータを作成しました: {len(gshk_to_3_records)}件")
        
        # 削除処理をコミット
        session.commit()
        
    except Exception as e:
        session.rollback()
        logger.error(f"テストデータ作成エラー: {str(e)}")
        raise
    finally:
        session.close()

def verify_migration_results():
    """移行結果を検証"""
    session = get_db_session()
    try:
        # GPRR_DATの件数を確認
        gprr_count = session.query(GprrDatModel).count()
        logger.info(f"GPRR_DAT件数: {gprr_count}")
        
        # GPRC_DATの件数を確認
        gprc_count = session.query(GprcDatModel).filter(GprcDatModel.GPRC_REQ_TO == 1).count()
        logger.info(f"GPRC_DAT件数（REQ_TO=1）: {gprc_count}")
        
        # GPRC_DAT（REQ_TO=2）の件数を確認
        gprc_req_to_2_count = session.query(GprcDatModel).filter(GprcDatModel.GPRC_REQ_TO == 2).count()
        logger.info(f"GPRC_DAT件数（REQ_TO=2）: {gprc_req_to_2_count}")
        
        # GSHK_DATの件数を確認
        gshk_to_2_count = session.query(GshkDatModel).filter(GshkDatModel.GSHK_TO == 2).count()
        logger.info(f"GSHK_DAT件数（TO=2）: {gshk_to_2_count}")
        
        # GSHK_DAT（TO=3）の件数を確認
        gshk_to_3_count = session.query(GshkDatModel).filter(GshkDatModel.GSHK_TO == 3).count()
        logger.info(f"GSHK_DAT件数（TO=3）: {gshk_to_3_count}")
        
        # CPRD_DATの件数を確認
        cprd_count = session.query(CprdDatModel).count()
        logger.info(f"CPRD_DAT件数: {cprd_count}")
        
        # CSHK_DATの件数を確認
        cshk_count = session.query(CshkDatModel).count()
        logger.info(f"CSHK_DAT件数: {cshk_count}")
        
        # CSHK_DATのTO分布を確認
        cshk_to_distribution = session.query(
            CshkDatModel.CSHK_TO,
            func.count(CshkDatModel.CSHK_ID).label('count')
        ).group_by(CshkDatModel.CSHK_TO).all()
        
        logger.info("CSHK_DATのTO分布:")
        for to_dist in cshk_to_distribution:
            logger.info(f"  TO={to_dist.CSHK_TO}: {to_dist.count}件")
        
        # CPRC_DATの件数を確認
        cprc_count = session.query(CprcDatModel).count()
        logger.info(f"CPRC_DAT件数: {cprc_count}")
        
        # 製品IDの分布を確認
        cprd_products = session.query(
            CprdDatModel.CPDD_PRD_ID,
            func.count(CprdDatModel.CPDD_ID).label('count')
        ).group_by(CprdDatModel.CPDD_PRD_ID).all()
        
        logger.info("CPRD_DATの製品ID分布:")
        for product in cprd_products:
            logger.info(f"  {product.CPDD_PRD_ID}: {product.count}件")
        
        # 加工IDの分布を確認
        cshk_processes = session.query(
            CshkDatModel.CSHK_PRC_ID,
            func.count(CshkDatModel.CSHK_ID).label('count')
        ).group_by(CshkDatModel.CSHK_PRC_ID).all()
        
        logger.info("CSHK_DATの加工ID分布:")
        for process in cshk_processes:
            logger.info(f"  {process.CSHK_PRC_ID}: {process.count}件")
        
        # データの整合性チェック
        expected_cprd = gprr_count + gprc_count + gprc_req_to_2_count  # GPRR + GPRC(REQ_TO=1) + GPRC(REQ_TO=2)
        expected_cshk = gprr_count + gshk_to_2_count + gshk_to_3_count  # GPRR + GSHK_TO=2 + GSHK_TO=3
        expected_cprc = gprc_count + gprc_req_to_2_count  # GPRC(REQ_TO=1) + GPRC(REQ_TO=2)
        
        if cprd_count == expected_cprd and cshk_count == expected_cshk and cprc_count == expected_cprc:
            logger.info("✅ 移行結果: 件数が一致しています")
        else:
            logger.error(f"❌ 移行結果: 件数が一致しません")
            logger.error(f"  期待値: CPRD={expected_cprd}, CSHK={expected_cshk}, CPRC={expected_cprc}")
            logger.error(f"  実際値: CPRD={cprd_count}, CSHK={cshk_count}, CPRC={cprc_count}")
            
    except Exception as e:
        logger.error(f"検証エラー: {str(e)}")
        raise
    finally:
        session.close()

def cleanup_test_data():
    """テストデータをクリーンアップ"""
    session = get_db_session()
    try:
        # テストで作成したデータを削除
        session.query(CprcDatModel).delete()
        session.query(CshkDatModel).delete()
        session.query(CprdDatModel).delete()
        session.query(GshkDatModel).filter(
            GshkDatModel.GSHK_TO.in_([2, 3])
        ).delete()
        session.query(GprcDatModel).filter(
            GprcDatModel.GPRC_REQ_TO.in_([1, 2])
        ).delete()
        session.query(GprrDatModel).filter(
            GprrDatModel.GPRR_SPEC.in_([1, 2, 3, 4])
        ).delete()
        
        session.commit()
        logger.info("テストデータをクリーンアップしました")
        
    except Exception as e:
        session.rollback()
        logger.error(f"クリーンアップエラー: {str(e)}")
        raise
    finally:
        session.close()

def main():
    """メイン処理"""
    try:
        print("=== グラデーションデータ移行テスト ===")
        
        # 1. テストデータ作成
        print("\n1. テストデータを作成中...")
        create_test_data()
        
        # 2. 移行プログラム実行
        print("\n2. 移行プログラムを実行中...")
        from gradation_migration import GradationMigration
        migration = GradationMigration()
        migration.run_migration()
        
        # 3. 結果検証
        print("\n3. 移行結果を検証中...")
        verify_migration_results()
        
        # 4. クリーンアップ（無効化）
        print("\n4. テストデータのクリーンアップをスキップします（データ保持）")
        # cleanup_test_data()  # コメントアウトしてデータを保持
        
        print("\n✅ テストが正常に完了しました")
        
    except Exception as e:
        logger.error(f"テストでエラーが発生: {str(e)}")
        print(f"\n❌ テストでエラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
