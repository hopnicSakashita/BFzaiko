import io
import csv
from datetime import datetime
from flask import jsonify
from sqlalchemy import text
import traceback
from app.constants import DatabaseConstants
from app.database import get_db_session
from app.models import log_error, BBcdDat
from app.barcode_generator import BarcodeGenerator
from app.models_common import CshkDatModel
from app.models_master import CbcdMstModel
from app.shipment_common import ShipmentCommon

class ShipmentBarcodeSaver:
    """出荷データのバーコード保存を担当するクラス"""
    
    @staticmethod
    def save_shipment_barcodes(base=None, adp=None, lr=None, color=None, proc_type=None, 
                              shipment_date=None, destination=None, order_no=None, shipment_status=None, order_date=None):
        """出荷データのバーコードをBBCD_DATテーブルに保存する
        
        Args:
            base: ベース
            adp: 加入度数
            lr: L/R
            color: 色
            proc_type: 加工タイプ
            shipment_date: 出荷日
            destination: 出荷先
            order_no: 受注番号
            shipment_status: 出荷ステータス
            order_date: 受注日
                
        Returns:
            tuple: (成功フラグ, 結果またはエラーメッセージ)
        """
        session = get_db_session()
        try:
            # デバッグ情報をログに出力
            log_error(f"バーコード保存処理開始: 検索条件 base={base}, adp={adp}, lr={lr}, color={color}, proc_type={proc_type}, shipment_date={shipment_date}, destination={destination}, order_no={order_no}, shipment_status={shipment_status}, order_date={order_date}")
            
            # 出荷データを取得
            query = text("""
                SELECT 
                    BPRD.BPDD_LOT as lot,
                    BFSP.BFSP_BASE as base,
                    BFSP.BFSP_ADP as adp,
                    BFSP.BFSP_LR as lr,
                    BFSP.BFSP_CLR as color,
                    BFSP.BFSP_PRD_ID as product_id,
                    BPRD.BPDD_CRT as coating_date,
                    format(BSHK.BSHK_DT, 'yyMMdd') as shipment_date,
                    BRCP.BRCP_PROC as proc_type,
                    BSHK.BSHK_TO as syukasaki
                FROM BSHK_DAT AS BSHK
                INNER JOIN BPRD_DAT AS BPRD
                    ON BSHK.BSHK_PDD_ID = BPRD.BPDD_ID
                INNER JOIN BFSP_MST AS BFSP
                    ON BPRD.BPDD_PRD_ID = BFSP.BFSP_PRD_ID
                LEFT OUTER JOIN BRCP_DAT AS BRCP
                    ON BSHK.BSHK_RCP_ID = BRCP.BRCP_ID
                WHERE BSHK.BSHK_FLG = :bshk_flg_not_shipped
            """)

            params = {
                'bshk_flg_not_shipped': DatabaseConstants.BSHK_FLG_NOT_SHIPPED
            }
            if base:
                query = text(str(query) + " AND BFSP.BFSP_BASE = :base")
                params['base'] = base
            if adp:
                query = text(str(query) + " AND BFSP.BFSP_ADP = :adp")
                params['adp'] = adp
            if lr:
                query = text(str(query) + " AND BFSP.BFSP_LR = :lr")
                params['lr'] = lr
            if color:
                query = text(str(query) + " AND BFSP.BFSP_CLR = :color")
                params['color'] = color
            if proc_type:
                if proc_type == 'NC':
                    query = text(str(query) + " AND BRCP.BRCP_PROC = :filter_proc_noncoat")
                    params['filter_proc_noncoat'] = DatabaseConstants.PROC_NON_COAT
                elif proc_type == 'HC':
                    query = text(str(query) + " AND BRCP.BRCP_PROC = :filter_proc_hardcoat")
                    params['filter_proc_hardcoat'] = DatabaseConstants.PROC_HARD_COAT
            if shipment_date:
                query = text(str(query) + " AND CAST(BSHK.BSHK_DT AS DATE) = :shipment_date")
                params['shipment_date'] = shipment_date
            if destination:
                query = text(str(query) + " AND BSHK.BSHK_TO = :destination")
                params['destination'] = destination
            if order_no:
                query = text(str(query) + " AND BSHK.BSHK_NO = :order_no")
                params['order_no'] = order_no
            if shipment_status:
                query = text(str(query) + " AND BSHK.BSHK_FLG = :shipment_status")
                params['shipment_status'] = shipment_status
            if order_date:
                query = text(str(query) + " AND CAST(BSHK.BSHK_ORD_DT AS DATE) = :order_date")
                params['order_date'] = order_date

            query = text(str(query) + " ORDER BY BPRD.BPDD_LOT")
            
            # デバッグ情報をログに出力
            log_error(f"SQLクエリ: {query}")
            log_error(f"SQLパラメータ: {params}")
            
            results = session.execute(query, params).fetchall()
            
            if not results:
                return False, "保存対象のデータが見つかりません。"
            
            # デバッグ情報をログに出力
            log_error(f"バーコード保存処理開始: 取得データ数={len(results)}")
            
            # 出荷先の分布を確認するデバッグクエリ
            debug_query = text("""
                SELECT BSHK_TO, COUNT(*) as count
                FROM BSHK_DAT 
                WHERE BSHK_FLG = :bshk_flg_not_shipped
                GROUP BY BSHK_TO
                ORDER BY BSHK_TO
            """)
            debug_results = session.execute(debug_query, {'bshk_flg_not_shipped': DatabaseConstants.BSHK_FLG_NOT_SHIPPED}).fetchall()
            log_error(f"出荷先分布: {[(r.BSHK_TO, r.count) for r in debug_results]}")
            
            # 既存のバーコードデータを削除（BBCD_KBN=BFのみ）
            delete_count = session.execute(text("DELETE FROM BBCD_DAT WHERE BBCD_KBN = :bbcd_kbn"), {"bbcd_kbn": DatabaseConstants.BBCD_KBN_BF}).rowcount
            log_error(f"区分{DatabaseConstants.BBCD_KBN_BF}の既存データ削除: {delete_count}件削除")
            
            # 削除をコミット
            session.commit()
            log_error(f"区分{DatabaseConstants.BBCD_KBN_BF}の既存データ削除完了")
            
            # バーコードデータを保存
            count = 1
            saved_count = 0
            columbus_count = 0
            dallas_count = 0
            younger_count = 0
            other_count = 0
            
            # タイムスタンプベースのID生成
            timestamp = datetime.now().strftime('%H%M%S')
            base_id = int(timestamp)
            
            for row in results:
                # countが999を超えた場合は処理を停止
                if count > 999:
                    break
                    
                lot = row.lot if row.lot else ''
                base = row.base or ''
                adp = row.adp or ''
                lr = row.lr or ''
                color = row.color or ''
                product_id = row.product_id or ''
                syukasaki = row.syukasaki or ''
                coating_date = row.coating_date or ''
                shipment_date = row.shipment_date or ''
                
                # デバッグ: 最初の数件のデータ詳細をログに出力
                if count <= 5:
                    log_error(f"処理データ{count}: LOT={lot}, 出荷先={syukasaki}, 製品ID={product_id}")
                
                # 出荷先別のカウント
                if int(syukasaki) == DatabaseConstants.ORDER_CMP_COLUMBUS:
                    columbus_count += 1
                elif int(syukasaki) == DatabaseConstants.ORDER_CMP_DALLAS:
                    dallas_count += 1
                elif int(syukasaki) == DatabaseConstants.ORDER_CMP_YOUNGER or int(syukasaki) == DatabaseConstants.ORDER_CMP_YOUNGER_EU:
                    younger_count += 1
                else:
                    other_count += 1
                    continue  # 対応していない出荷先はスキップ
                
                # 出荷数がある場合のみ保存
                if int(syukasaki) == DatabaseConstants.ORDER_CMP_COLUMBUS or int(syukasaki) == DatabaseConstants.ORDER_CMP_DALLAS:
                    # バーコード生成（MakeCode_S相当）
                    barcode = BarcodeGenerator.make_barcode_s(product_id, shipment_date, row.proc_type)
                    coat = 'HC' if row.proc_type == DatabaseConstants.PROC_HARD_COAT else 'NC'
                    barcode_name = f"{lot}/{base}/{adp}/{lr}/{color}/サンレー/{coat}"
                    
                    # データの挿入
                    stmt = text("""
                        INSERT INTO BBCD_DAT (BBCD_ID, BBCD_NO, BBCD_NM, BBCD_KBN)
                        VALUES (:bcd_id, :bcd_no, :bcd_nm, :bbcd_kbn)
                    """)
                    session.execute(stmt, {
                        'bcd_id': count,
                        'bcd_no': barcode,
                        'bcd_nm': barcode_name,
                        'bbcd_kbn': DatabaseConstants.BBCD_KBN_BF
                    })
                    session.commit()
                    saved_count += 1
                    count += 1
                    
                elif int(syukasaki) == DatabaseConstants.ORDER_CMP_YOUNGER or int(syukasaki) == DatabaseConstants.ORDER_CMP_YOUNGER_EU:
                    # バーコード生成（MakeCode_Y相当）
                    barcode = BarcodeGenerator.make_barcode_y(product_id, lot, coating_date)
                    barcode_name = f"{lot}/{coating_date}/{base}/{adp}/{lr}/{color}/ヤンガー"
                    
                    # BBCD_DATに保存（IDは3文字以内、タイムスタンプベース）
                    # データの挿入
                    stmt = text("""
                        INSERT INTO BBCD_DAT (BBCD_ID, BBCD_NO, BBCD_NM, BBCD_KBN)
                        VALUES (:bcd_id, :bcd_no, :bcd_nm, :bbcd_kbn)
                    """)
                    session.execute(stmt, {
                        'bcd_id': count,
                        'bcd_no': barcode,
                        'bcd_nm': barcode_name,
                        'bbcd_kbn': DatabaseConstants.BBCD_KBN_BF
                    })
                    session.commit()
                    saved_count += 1
                    count += 1
            
            # デバッグ情報をログに出力
            log_error(f"バーコード保存処理完了: 保存件数={saved_count}, コロンバス={columbus_count}, ダラス={dallas_count}, ヤンガー={younger_count}, その他={other_count}")
            
            
            return True, f"バーコードデータを{saved_count}件保存しました。"
            
        except Exception as e:
            session.rollback()
            tb = traceback.format_exc()
            log_error(f"バーコード保存中にエラーが発生しました: {str(e)}\n{tb}")
            
            # エラーメッセージを詳細化
            error_detail = str(e)
            if "truncated" in error_detail.lower():
                error_detail += " (データベースのカラム長制限を超えています)"
            elif "ambiguous column" in error_detail.lower():
                error_detail += " (SQLクエリの列名が曖昧です)"
            
            return False, f"バーコードデータの保存に失敗しました: {error_detail}"
        finally:
            session.close() 
    
    @staticmethod
    def save_shipment_barcodes_common(search_date_from=None, search_date_to=None, search_ord_date_from=None, search_ord_date_to=None, search_cshk_to=None):
        """出荷データのバーコードをBBCD_DATテーブルに保存する（共通処理）"""
        session = get_db_session()
        try:
            # 検索条件から出荷データを取得
            shipments = ShipmentCommon.get_shipment_list(
                date_from=search_date_from,
                date_to=search_date_to,
                ord_date_from=search_ord_date_from,
                ord_date_to=search_ord_date_to,
                cshk_to=search_cshk_to
            )

            if not shipments:
                return False, '出荷データが見つかりません。'

            # 既存のバーコードデータを削除（BBCD_KBN=一般のみ）
            delete_count = session.execute(text("DELETE FROM BBCD_DAT WHERE BBCD_KBN = :bbcd_kbn"), {"bbcd_kbn": DatabaseConstants.BBCD_KBN_COMMON}).rowcount
            log_error(f"区分{DatabaseConstants.BBCD_KBN_COMMON}の既存データ削除: {delete_count}件削除")
            session.commit()

            # バーコードデータを生成して保存
            cnt = 0
            error_count = 0
            for shipment in shipments:
                try:
                    # バーコードマスタから該当データを取得
                    barcode_data = CbcdMstModel.get_by_prd_id_and_to(
                        shipment.get('CSHK_PRD_ID'),
                        shipment.get('CSHK_TO')
                    )
                    if barcode_data and barcode_data.get('CBCD_NO1'):
                        # バーコード生成
                        barcode = BarcodeGenerator.make_barcode_s_shipment(
                            barcode_data['CBCD_NO1'],
                            str(shipment.get('CPDD_LOT'))
                        )
                        
                        if barcode:
                            cnt += 1
                            # BBCD_DATに保存
                            stmt = text("""
                                INSERT INTO BBCD_DAT (BBCD_ID, BBCD_NO, BBCD_NM, BBCD_KBN)
                                VALUES (:bcd_id, :bcd_no, :bcd_nm, :bbcd_kbn)
                            """)
                            session.execute(stmt, {
                                'bcd_id': cnt,
                                'bcd_no': barcode,
                                'bcd_nm': f"{shipment.get('PRD_DSP_NM')} - {str(shipment.get('CPDD_LOT'))}",
                                'bbcd_kbn': DatabaseConstants.BBCD_KBN_COMMON
                            })
                            
                            # 一定件数ごとにコミット
                            if cnt % 100 == 0:
                                session.commit()
                    else:
                        error_count += 1
                        log_error(f"バーコードマスタデータなし: PRD_ID={shipment.get('CSHK_PRD_ID')}, TO={shipment.get('CSHK_TO')}")
                except Exception as e:
                    error_count += 1
                    log_error(f"バーコード生成エラー: {str(e)}")
                    continue

            # 最終コミット
            session.commit()

            if cnt == 0:
                return False, 'バーコードデータが生成できませんでした。'

            return True, f"バーコードデータを{cnt}件保存しました。"

        except Exception as e:
            session.rollback()
            log_error(f"バーコード保存処理でエラーが発生: {str(e)}\n{traceback.format_exc()}")
            return False, f"バーコード保存処理でエラーが発生しました: {str(e)}"
        finally:
            session.close()
                    
