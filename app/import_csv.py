import csv
import traceback
from sqlalchemy import text

from app.constants import CsvConstants, DatabaseConstants
from app.database import get_db_session
from app.models import BfspMstModel, log_error, process_text_to_db, BprdMeiModel, PrdDatModel, CprdDatModel
from app.models_master import PrdMstModel, CtpdMstModel, CprcMstModel

def import_csv_nonecoat(file_path, has_header=False):
    """CSVファイルからデータを取り込みPRD_DATテーブルに登録する"""
    
    session = None
    result = {
        "total": 0,
        "success": 0,     # 総成功数
        "error": 0,
        "skipped": 0,
        "duplicate": 0,
        "errors": []
    }
    
    try:
        session = get_db_session()
        
        # 複数のエンコーディングを試行
        encodings = ['utf-8-sig', 'utf-8', 'shift-jis', 'cp932']
        encoding_used = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as test_file:
                    test_file.read()
                encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue
        
        if not encoding_used:
            error_msg = "CSVファイルのエンコーディングを判別できませんでした"
            result["errors"].append(error_msg)
            log_error(error_msg)
            return result
        
        with open(file_path, 'r', encoding=encoding_used) as csvfile:
            csv_reader = csv.reader(csvfile)
            
            # ヘッダー行をスキップ
            if has_header:
                try:
                    next(csv_reader)
                except StopIteration:
                    error_msg = "CSVファイルにデータがありません"
                    result["errors"].append(error_msg)
                    log_error(error_msg)
                    return result
            
            for i, row in enumerate(csv_reader):
                try:
                    result["total"] += 1
                    
                    # 必要な列数があるか確認
                    if len(row) < CsvConstants.CSV_NON_COAT_COLUMN_COUNT:  # 76列目をチェックするため最低76列必要
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 必要な列数がありません（{len(row)}列、最低76列必要）"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # 1列目の値を取得し必要な文字を抽出
                    col1_value = row[0].strip() if row else ""
                    log_error(f"行 {i+1}: 1列目の値: {col1_value}")
                    
                    col1_split = col1_value.split("-")
                    if len(col1_split) > 10:
                        continue
                    
                    sprit = col1_value[11:13] if len(col1_value) >= 13 and col1_value[11:13].isdigit() else "00"
                    sprit2 = col1_value[15:16] if len(col1_value) >= 16 and col1_value[15:16].isdigit() else "00"
                    no = int(sprit + sprit2)
                    
                    # 共通のデータを作成
                    common_data = {
                        'BPDD_PRD_ID': col1_value[:4] if len(col1_value) >= 4 else "",
                        'BPDD_LOT': int(col1_value[4:10]) if len(col1_value) >= 10 and col1_value[4:10].isdigit() else 0,
                        'BPDD_QTY': 0,
                        'BPDD_FLG': DatabaseConstants.BPDD_FLG_NOT_SHIPPED,
                        'BPDD_CRT': None,
                        'BPDD_PROC': DatabaseConstants.PROC_NON_COAT,
                    }
                    
                    # データの検証
                    if not common_data['BPDD_PRD_ID']:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 製品IDを取得できません。1列目: {col1_value}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    bfsp = session.query(BfspMstModel).filter(BfspMstModel.BFSP_PRD_ID == common_data['BPDD_PRD_ID'][:4]).first()
                    if not bfsp:
                        continue
                    
                    # 75列目（0始まりなので74）の値を取得して処理
                    qty_value_75 = row[74].strip() if len(row) > 74 else "0"
                    log_error(f"行 {i+1}: 75列目の値: {qty_value_75}")
                                            
                    # 数値変換とチェック
                    try:
                        qty_numeric_75 = int(qty_value_75)
                        
                        log_error(f"行 {i+1}: 75列目の数値: {qty_numeric_75}")
                        
                        if qty_numeric_75 == 0:
                            result["skipped"] += 1
                            log_error(f"行 {i+1}: 75列目が0のためスキップします")
                            continue
                    except ValueError:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 75列目の値が数値ではありません: 75列目={qty_value_75}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # --- 75列目の処理 ---
                    if qty_numeric_75 > 0:
                        try:
                            # 75列目用のデータを作成
                            prd_dat_data_75 = common_data.copy()
                            prd_dat_data_75['BPDD_QTY'] = qty_numeric_75  # 数値をそのまま使用
                            
                            log_error(f"行 {i+1}: 75列目のデータ処理開始: {prd_dat_data_75}")
                            
                            # 75列目用の一意性制約チェック
                            duplicate_check_75 = session.execute(
                                text("""
                                    SELECT COUNT(*) 
                                    FROM BPRD_MEI 
                                    WHERE BPDM_PRD_ID = :prd_id
                                    AND BPDM_LOT = :lot
                                    AND BPDM_NO = :no
                                """),
                                {
                                    "prd_id": prd_dat_data_75['BPDD_PRD_ID'],
                                    "lot": prd_dat_data_75['BPDD_LOT'],
                                    "no": no
                                }
                            ).scalar()
                            
                            log_error(f"行 {i+1}: 75列目の重複チェック結果: {duplicate_check_75}")
                            
                            if duplicate_check_75 > 0:
                                result["duplicate"] += 1
                                error_msg = (f"行 {i+1}: 75列目データの一意性制約違反 - 同じ組み合わせのデータが既に存在します。")
                                result["errors"].append(error_msg)
                                log_error(error_msg)
                            else:
                                # PrdDatModelの既存データをチェック
                                existing_prd = session.query(PrdDatModel).filter(
                                    PrdDatModel.BPDD_PRD_ID == prd_dat_data_75['BPDD_PRD_ID'],
                                    PrdDatModel.BPDD_LOT == prd_dat_data_75['BPDD_LOT']
                                ).first()

                                if existing_prd:
                                    # 既存データがある場合は数量を加算
                                    existing_prd.BPDD_QTY += qty_numeric_75
                                    log_error(f"行 {i+1}: 既存データの数量を更新: {existing_prd.BPDD_QTY}")
                                else:
                                    # 新規データ作成
                                    log_error(f"行 {i+1}: 新規データを作成します")
                                    processed_data_75 = {k: process_text_to_db(v) for k, v in prd_dat_data_75.items()}
                                    new_record_75 = PrdDatModel(**processed_data_75)
                                    session.add(new_record_75)

                                # BprdMeiModelにデータを追加
                                bprd_mei_data = {
                                    'BPDM_PRD_ID': prd_dat_data_75['BPDD_PRD_ID'],
                                    'BPDM_LOT': prd_dat_data_75['BPDD_LOT'],
                                    'BPDM_NO': no,
                                    'BPDM_QTY': qty_numeric_75
                                }
                                new_bprd_mei = BprdMeiModel(**bprd_mei_data)
                                session.add(new_bprd_mei)

                                # 更新日時情報を設定
                                session.flush()
                                result["success"] += 1
                                log_error(f"行 {i+1}: 75列目データ登録成功")
                                
                                # 75列目処理の個別コミット
                                session.commit()
                                log_error(f"行 {i+1}: 75列目データのコミット成功")
                        except Exception as e:
                            session.rollback()
                            result["error"] += 1
                            tb = traceback.format_exc()
                            error_msg = f"行 {i+1}: 75列目データ処理エラー: {str(e)}"
                            result["errors"].append(error_msg)
                            log_error(f"{error_msg}\n{tb}")
                            log_error(f"行 {i+1}: 75列目データ処理でエラー発生、ロールバックしました")
                    else:
                        log_error(f"行 {i+1}: 75列目の値が0または負数のためスキップします: {qty_numeric_75}")
                    
                
                except Exception as e:
                    result["error"] += 1
                    tb = traceback.format_exc()
                    error_msg = f"行 {i+1}: 処理エラー: {str(e)}"
                    result["errors"].append(error_msg)
                    log_error(f"{error_msg}\n{tb}")
                    log_error(f"行 {i+1}: 全体処理でエラー発生: {str(e)}")
                    continue
            
            try:
                # 全ての処理が終わったらコミット
                session.commit()
                log_error(f"CSV取り込み完了: 合計{result['total']}行、"
                            f"スキップ:{result['skipped']}行、"
                            f"重複:{result['duplicate']}行、"
                            f"エラー:{result['error']}行")
            except Exception as e:
                session.rollback()
                tb = traceback.format_exc()
                error_msg = f"コミット処理でエラーが発生しました: {str(e)}"
                result["errors"].append(error_msg)
                log_error(f"{error_msg}\n{tb}")
                raise
            
            return result
            
    except Exception as e:
        if session:
            session.rollback()
        tb = traceback.format_exc()
        error_msg = f"CSV取り込み処理に失敗しました: {str(e)}"
        log_error(f"{error_msg}\n{tb}")
        result["errors"].append(error_msg)
        raise Exception(error_msg)
    finally:
        if session:
            session.close()
            log_error("セッションをクローズしました")


def import_from_barcode(file_path, has_header=False, bbcd_kbn=1):
    """CSVファイルからバーコードデータをインポートする"""
    
    session = None
    result = {
        "total": 0,
        "success": 0,     # 総成功数
        "error": 0,
        "skipped": 0,
        "duplicate": 0,
        "errors": []
    }
    
    try:
        session = get_db_session()
        
        # 既存データの削除（区分指定）
        try:
            log_error(f"区分{bbcd_kbn}の既存バーコードデータを削除します")
            session.execute(text("DELETE FROM BBCD_DAT WHERE BBCD_KBN = :bbcd_kbn"), {"bbcd_kbn": bbcd_kbn})
            session.commit()
            log_error("既存データの削除が完了しました")
        except Exception as e:
            session.rollback()
            error_msg = f"既存データの削除中にエラーが発生しました: {str(e)}"
            result["errors"].append(error_msg)
            log_error(error_msg)
            raise Exception(error_msg)
        
        # 複数のエンコーディングを試行
        encodings = ['utf-8-sig', 'utf-8', 'shift-jis', 'cp932']
        encoding_used = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as test_file:
                    test_file.read()
                encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue
        
        if not encoding_used:
            error_msg = "CSVファイルのエンコーディングを判別できませんでした"
            result["errors"].append(error_msg)
            log_error(error_msg)
            return result
        
        with open(file_path, 'r', encoding=encoding_used) as csvfile:
            csv_reader = csv.reader(csvfile)
            
            # ヘッダー行をスキップ
            if has_header:
                try:
                    next(csv_reader)
                except StopIteration:
                    error_msg = "CSVファイルにデータがありません"
                    result["errors"].append(error_msg)
                    log_error(error_msg)
                    return result
            
            for i, row in enumerate(csv_reader):
                try:
                    result["total"] += 1
                    
                    # 必要な列数があるか確認
                    if len(row) < 3:  # BBCD_ID, BBCD_NO, BBCD_NMの3列必要
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 必要な列数がありません（{len(row)}列、最低3列必要）"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # データの取得とクリーニング
                    bcd_id = row[0].strip() if row[0] else ""
                    bcd_no = row[1].strip() if row[1] else ""
                    bcd_nm = row[2].strip() if row[2] else ""
                    
                    # データの検証
                    if not bcd_id:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: BBCD_IDが空です"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # BBCD_IDの長さチェック（20文字以内）
                    if len(bcd_id) > 20:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: BBCD_IDが20文字を超えています: {bcd_id}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # BBCD_NOの長さチェック（60文字以内）
                    if len(bcd_no) > 60:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: BBCD_NOが60文字を超えています: {bcd_no}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # BBCD_NMの長さチェック（30文字以内）
                    if len(bcd_nm) > 30:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: BBCD_NMが30文字を超えています: {bcd_nm}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # 重複チェック（区分も含めて）
                    duplicate_check = session.execute(
                        text("""
                            SELECT COUNT(*) 
                            FROM BBCD_DAT 
                            WHERE BBCD_ID = :bcd_id AND BBCD_KBN = :bbcd_kbn
                        """),
                        {
                            "bcd_id": bcd_id,
                            "bbcd_kbn": bbcd_kbn
                        }
                    ).scalar()
                    
                    if duplicate_check > 0:
                        result["duplicate"] += 1
                        error_msg = f"行 {i+1}: 重複データが存在します。BBCD_ID: {bcd_id}, 区分: {bbcd_kbn}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    try:
                        # データの挿入
                        stmt = text("""
                            INSERT INTO BBCD_DAT (BBCD_ID, BBCD_NO, BBCD_NM, BBCD_KBN)
                            VALUES (:bcd_id, :bcd_no, :bcd_nm, :bbcd_kbn)
                        """)
                        session.execute(stmt, {
                            'bcd_id': bcd_id,
                            'bcd_no': bcd_no,
                            'bcd_nm': bcd_nm,
                            'bbcd_kbn': bbcd_kbn
                        })
                        
                        # 個別コミット
                        session.commit()
                        result["success"] += 1
                        log_error(f"行 {i+1}: データ登録成功")
                        
                    except Exception as e:
                        session.rollback()
                        result["error"] += 1
                        tb = traceback.format_exc()
                        error_msg = f"行 {i+1}: データ登録エラー: {str(e)}"
                        result["errors"].append(error_msg)
                        log_error(f"{error_msg}\n{tb}")
                        continue
                    
                except Exception as e:
                    result["error"] += 1
                    tb = traceback.format_exc()
                    error_msg = f"行 {i+1}: 処理エラー: {str(e)}"
                    result["errors"].append(error_msg)
                    log_error(f"{error_msg}\n{tb}")
                    continue
            
            try:
                # 全ての処理が終わったらコミット
                session.commit()
                log_error(f"CSV取り込み完了: 合計{result['total']}行、"
                            f"成功:{result['success']}行、"
                            f"スキップ:{result['skipped']}行、"
                            f"重複:{result['duplicate']}行、"
                            f"エラー:{result['error']}行")
            except Exception as e:
                session.rollback()
                tb = traceback.format_exc()
                error_msg = f"コミット処理でエラーが発生しました: {str(e)}"
                result["errors"].append(error_msg)
                log_error(f"{error_msg}\n{tb}")
                raise
            
            return result
            
    except Exception as e:
        if session:
            session.rollback()
        tb = traceback.format_exc()
        error_msg = f"CSV取り込み処理に失敗しました: {str(e)}"
        log_error(f"{error_msg}\n{tb}")
        result["errors"].append(error_msg)
        raise Exception(error_msg)
    finally:
        if session:
            session.close()
            log_error("セッションをクローズしました")


def import_csv_common(file_path, has_header=False):
    """CSVファイルからデータを取り込みCPRD_DATテーブルに登録する"""
    
    session = None
    result = {
        "total": 0,
        "success": 0,     # 総成功数
        "error": 0,
        "skipped": 0,
        "duplicate": 0,
        "errors": []
    }
    
    try:
        session = get_db_session()
        
        # 複数のエンコーディングを試行
        encodings = ['utf-8-sig', 'utf-8', 'shift-jis', 'cp932']
        encoding_used = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as test_file:
                    test_file.read()
                encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue
        
        if not encoding_used:
            error_msg = "CSVファイルのエンコーディングを判別できませんでした"
            result["errors"].append(error_msg)
            log_error(error_msg)
            return result
        
        with open(file_path, 'r', encoding=encoding_used) as csvfile:
            csv_reader = csv.reader(csvfile)
            
            # ヘッダー行をスキップ
            if has_header:
                try:
                    next(csv_reader)
                except StopIteration:
                    error_msg = "CSVファイルにデータがありません"
                    result["errors"].append(error_msg)
                    log_error(error_msg)
                    return result
            
            for i, row in enumerate(csv_reader):
                try:
                    result["total"] += 1
                    
                    # 必要な列数があるか確認
                    if len(row) < CsvConstants.CSV_NON_COAT_COLUMN_COUNT:  # 76列目をチェックするため最低76列必要
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 必要な列数がありません（{len(row)}列、最低76列必要）"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # 1列目の値を取得し必要な文字を抽出
                    col1_value = row[0].strip() if row else ""
                    log_error(f"行 {i+1}: 1列目の値: {col1_value}")
                    
                    prd_id = ""
                    lot = ""
                    
                    col1_split = col1_value.split("-")
                    if len(col1_split[0]) == 10:
                        prd_id = col1_split[0][:4]
                        lot = col1_split[0][4:10]
                    elif len(col1_split[0]) == 11:
                        prd_id = col1_split[0][:5]
                        lot = col1_split[0][5:11]
                    
                    prd_mst = PrdMstModel.get_by_prd_id(prd_id)
                    if not prd_mst:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 製品IDが存在しません。1列目: {prd_id}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    if prd_mst.PRD_KBN != 1:
                        result["skipped"] += 1
                        # 製品区分が1ではない場合はスキップ エラーは表示しない
                        continue
                    
                    sprit1 = int(col1_split[1])
                    if len(col1_split) > 2:
                        sprit2 = int(col1_split[2])
                    else:
                        sprit2 = 0
                    
                    # 共通のデータを作成（CPRD_DAT用）
                    common_data = {
                        'CPDD_PRD_ID': prd_id,
                        'CPDD_LOT': lot,
                        'CPDD_SPRIT1': sprit1,
                        'CPDD_SPRIT2': sprit2,
                        'CPDD_RANK': 1,  # デフォルトランク
                        'CPDD_QTY': 0,
                        'CPDD_FLG': 0,
                        'CPDD_PCD_ID': 0,  # 加工ID（デフォルト0）
                    }
                    
                    # データの検証
                    if not common_data['CPDD_PRD_ID']:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 製品IDを取得できません。1列目: {col1_value}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # 75列目（0始まりなので74）の値を取得して処理
                    qty_value_75 = row[74].strip() if len(row) > 74 else "0"
                    log_error(f"行 {i+1}: 75列目の値: {qty_value_75}")
                    
                    # 76列目（0始まりなので75）の値を取得して処理
                    qty_value_76 = row[75].strip() if len(row) > 75 else "0"
                    log_error(f"行 {i+1}: 76列目の値: {qty_value_76}")
                                            
                    # 数値変換とチェック
                    try:
                        qty_numeric_75 = int(qty_value_75)
                        qty_numeric_76 = int(qty_value_76)
                        
                        log_error(f"行 {i+1}: 75列目の数値: {qty_numeric_75}")
                        log_error(f"行 {i+1}: 76列目の数値: {qty_numeric_76}")
                        
                        if qty_numeric_75 == 0 and qty_numeric_76 == 0:
                            result["skipped"] += 1
                            log_error(f"行 {i+1}: 75列目と76列目の両方が0のためスキップします")
                            continue
                    except ValueError:
                        result["skipped"] += 1
                        error_msg = f"行 {i+1}: 75列目または76列目の値が数値ではありません: 75列目={qty_value_75}, 76列目={qty_value_76}"
                        result["errors"].append(error_msg)
                        log_error(error_msg)
                        continue
                    
                    # --- 75列目の処理 ---
                    if qty_numeric_75 > 0:
                        try:
                            # 75列目用のデータを作成
                            cprd_dat_data_75 = common_data.copy()
                            cprd_dat_data_75['CPDD_QTY'] = qty_numeric_75  # 数値をそのまま使用
                            
                            log_error(f"行 {i+1}: 75列目のデータ処理開始: {cprd_dat_data_75}")
                            
                            # 75列目用の一意性制約チェック
                            duplicate_check_75 = session.execute(
                                text("""
                                    SELECT COUNT(*) 
                                    FROM CPRD_DAT 
                                    WHERE CPDD_PRD_ID = :prd_id
                                    AND CPDD_LOT = :lot
                                    AND CPDD_SPRIT1 = :sprit1
                                    AND CPDD_SPRIT2 = :sprit2
                                    AND CPDD_RANK = :rank
                                """),
                                {
                                    "prd_id": cprd_dat_data_75['CPDD_PRD_ID'],
                                    "lot": cprd_dat_data_75['CPDD_LOT'],
                                    "sprit1": cprd_dat_data_75['CPDD_SPRIT1'],
                                    "sprit2": cprd_dat_data_75['CPDD_SPRIT2'],
                                    "rank": cprd_dat_data_75['CPDD_RANK']
                                }
                            ).scalar()
                            
                            log_error(f"行 {i+1}: 75列目の重複チェック結果: {duplicate_check_75}")
                            
                            if duplicate_check_75 > 0:
                                result["duplicate"] += 1
                                error_msg = (f"行 {i+1}: 75列目データの一意性制約違反 - 同じ組み合わせのデータが既に存在します。")
                                result["errors"].append(error_msg)
                                log_error(error_msg)
                            else:
                                # CprdDatModelの既存データをチェック
                                existing_cprd = session.query(CprdDatModel).filter(
                                    CprdDatModel.CPDD_PRD_ID == cprd_dat_data_75['CPDD_PRD_ID'],
                                    CprdDatModel.CPDD_LOT == cprd_dat_data_75['CPDD_LOT'],
                                    CprdDatModel.CPDD_SPRIT1 == cprd_dat_data_75['CPDD_SPRIT1'],
                                    CprdDatModel.CPDD_SPRIT2 == cprd_dat_data_75['CPDD_SPRIT2'],
                                    CprdDatModel.CPDD_RANK == cprd_dat_data_75['CPDD_RANK']
                                ).first()

                                if existing_cprd:
                                    # 既存データがある場合はスキップ
                                    result["duplicate"] += 1
                                    result["skipped"] += 1
                                    error_msg = f"行 {i+1}: 75列目データは既に存在するためスキップします"
                                    result["errors"].append(error_msg)
                                    log_error(error_msg)
                                else:
                                    # 新規データ作成
                                    log_error(f"行 {i+1}: 新規データを作成します")
                                    processed_data_75 = {k: process_text_to_db(v) for k, v in cprd_dat_data_75.items()}
                                    new_record_75 = CprdDatModel(**processed_data_75)
                                    session.add(new_record_75)

                                # 更新日時情報を設定
                                session.flush()
                                result["success"] += 1
                                log_error(f"行 {i+1}: 75列目データ登録成功")
                                
                                # 75列目処理の個別コミット
                                session.commit()
                                log_error(f"行 {i+1}: 75列目データのコミット成功")
                        except Exception as e:
                            session.rollback()
                            result["error"] += 1
                            tb = traceback.format_exc()
                            error_msg = f"行 {i+1}: 75列目データ処理エラー: {str(e)}"
                            result["errors"].append(error_msg)
                            log_error(f"{error_msg}\n{tb}")
                            log_error(f"行 {i+1}: 75列目データ処理でエラー発生、ロールバックしました")
                    else:
                        log_error(f"行 {i+1}: 75列目の値が0または負数のためスキップします: {qty_numeric_75}")
                    
                    # --- 76列目の処理 ---
                    if qty_numeric_76 > 0:
                        try:
                            # 76列目用のデータを作成
                            cprd_dat_data_76 = common_data.copy()
                            cprd_dat_data_76['CPDD_RANK'] = 2  # ランク=2（76列目用）
                            cprd_dat_data_76['CPDD_QTY'] = qty_numeric_76  # 数値をそのまま使用
                            
                            log_error(f"行 {i+1}: 76列目のデータ処理開始: {cprd_dat_data_76}")
                            
                            # 76列目用の一意性制約チェック
                            duplicate_check_76 = session.execute(
                                text("""
                                    SELECT COUNT(*) 
                                    FROM CPRD_DAT 
                                    WHERE CPDD_PRD_ID = :prd_id
                                    AND CPDD_LOT = :lot
                                    AND CPDD_SPRIT1 = :sprit1
                                    AND CPDD_SPRIT2 = :sprit2
                                    AND CPDD_RANK = :rank
                                """),
                                {
                                    "prd_id": cprd_dat_data_76['CPDD_PRD_ID'],
                                    "lot": cprd_dat_data_76['CPDD_LOT'],
                                    "sprit1": cprd_dat_data_76['CPDD_SPRIT1'],
                                    "sprit2": cprd_dat_data_76['CPDD_SPRIT2'],
                                    "rank": cprd_dat_data_76['CPDD_RANK']
                                }
                            ).scalar()
                            
                            log_error(f"行 {i+1}: 76列目の重複チェック結果: {duplicate_check_76}")
                            
                            if duplicate_check_76 > 0:
                                result["duplicate"] += 1
                                error_msg = (f"行 {i+1}: 76列目データの一意性制約違反 - 同じ組み合わせのデータが既に存在します。")
                                result["errors"].append(error_msg)
                                log_error(error_msg)
                            else:
                                # CprdDatModelの既存データをチェック
                                existing_cprd_76 = session.query(CprdDatModel).filter(
                                    CprdDatModel.CPDD_PRD_ID == cprd_dat_data_76['CPDD_PRD_ID'],
                                    CprdDatModel.CPDD_LOT == cprd_dat_data_76['CPDD_LOT'],
                                    CprdDatModel.CPDD_SPRIT1 == cprd_dat_data_76['CPDD_SPRIT1'],
                                    CprdDatModel.CPDD_SPRIT2 == cprd_dat_data_76['CPDD_SPRIT2'],
                                    CprdDatModel.CPDD_RANK == cprd_dat_data_76['CPDD_RANK']
                                ).first()

                                if existing_cprd_76:
                                    # 既存データがある場合はスキップ
                                    result["duplicate"] += 1
                                    result["skipped"] += 1
                                    error_msg = f"行 {i+1}: 76列目データは既に存在するためスキップします"
                                    result["errors"].append(error_msg)
                                    log_error(error_msg)
                                else:
                                    # 新規データ作成
                                    log_error(f"行 {i+1}: 新規データを作成します")
                                    processed_data_76 = {k: process_text_to_db(v) for k, v in cprd_dat_data_76.items()}
                                    new_record_76 = CprdDatModel(**processed_data_76)
                                    session.add(new_record_76)

                                # 更新日時情報を設定
                                session.flush()
                                result["success"] += 1
                                log_error(f"行 {i+1}: 76列目データ登録成功")
                                
                                # 76列目処理の個別コミット
                                session.commit()
                                log_error(f"行 {i+1}: 76列目データのコミット成功")
                        except Exception as e:
                            session.rollback()
                            result["error"] += 1
                            tb = traceback.format_exc()
                            error_msg = f"行 {i+1}: 76列目データ処理エラー: {str(e)}"
                            result["errors"].append(error_msg)
                            log_error(f"{error_msg}\n{tb}")
                            log_error(f"行 {i+1}: 76列目データ処理でエラー発生、ロールバックしました")
                    else:
                        log_error(f"行 {i+1}: 76列目の値が0または負数のためスキップします: {qty_numeric_76}")
                    
                
                except Exception as e:
                    result["error"] += 1
                    tb = traceback.format_exc()
                    error_msg = f"行 {i+1}: 処理エラー: {str(e)}"
                    result["errors"].append(error_msg)
                    log_error(f"{error_msg}\n{tb}")
                    log_error(f"行 {i+1}: 全体処理でエラー発生: {str(e)}")
                    continue
            
            try:
                # 全ての処理が終わったらコミット
                session.commit()
                log_error(f"CSV取り込み完了: 合計{result['total']}行、"
                            f"成功:{result['success']}行、"
                            f"スキップ:{result['skipped']}行、"
                            f"重複:{result['duplicate']}行、"
                            f"エラー:{result['error']}行")
            except Exception as e:
                session.rollback()
                tb = traceback.format_exc()
                error_msg = f"コミット処理でエラーが発生しました: {str(e)}"
                result["errors"].append(error_msg)
                log_error(f"{error_msg}\n{tb}")
                raise
            
            return result
            
    except Exception as e:
        if session:
            session.rollback()
        tb = traceback.format_exc()
        error_msg = f"CSV取り込み処理に失敗しました: {str(e)}"
        log_error(f"{error_msg}\n{tb}")
        result["errors"].append(error_msg)
        raise Exception(error_msg)
    finally:
        if session:
            session.close()
            log_error("セッションをクローズしました")
