import os
import traceback
from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify, make_response, current_app
from werkzeug.utils import secure_filename
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from datetime import datetime

from app.auth import login_required
from app.barcode_saver import ShipmentBarcodeSaver
from app.import_csv import import_csv_common
from app.models_common import CprcDatModel, CprdDatModel
from app.models_master import PrdMstModel, CprcMstModel, KbnMstModel, CztrMstModel
from app.constants import DatabaseConstants, KbnConstants
from app.logger_utils import log_error
from app.export_pdf import process_request_export_pdf, shipment_list_export_pdf

from app.shipment_common import ShipmentCommon

# Blueprintの作成
common_bp = Blueprint('common', __name__, url_prefix='/common')

@common_bp.route('/csv_import', methods=['GET', 'POST'])
@login_required
def csv_import_common():
    """共通CSVインポート画面"""
    # エラーメッセージとデバッグ情報を初期化
    error_message = None
    debug_info = []
    imported_data = None
    
    if request.method == 'POST':
        try:
            # アップロードされたファイルを取得
            file = request.files.get('file')
            if not file or file.filename == '':
                error_message = 'ファイルが選択されていません。'
                return render_template('import_csv.html', error=error_message)
            
            # CSVファイルかチェック
            if not file.filename.lower().endswith('.csv'):
                error_message = 'CSVファイルを選択してください。'
                return render_template('import_csv.html', error=error_message)
            
            # ファイルサイズチェック（10MB制限）
            file.seek(0, 2)  # ファイル末尾に移動
            file_size = file.tell()
            file.seek(0)  # ファイル先頭に戻る
            
            if file_size > 10 * 1024 * 1024:  # 10MB
                error_message = 'ファイルサイズが大きすぎます（10MB以下のファイルを選択してください）。'
                return render_template('import_csv.html', error=error_message)
            
            # CSVインポート処理
            # ファイルを一時保存
            temp_dir = os.path.join(os.path.dirname(__file__), 'temp')
            os.makedirs(temp_dir, exist_ok=True)
            
            filename = secure_filename(file.filename)
            temp_path = os.path.join(temp_dir, filename)
            file.save(temp_path)
            
            try:
                # CSV取込処理を実行
                result = import_csv_common(temp_path, has_header=False)
                
                success_count = result['success']
                error_count = result['error']
                
                if error_count > 0:
                    error_message = f'インポート完了: 成功 {success_count}件, 失敗 {error_count}件'
                    if result['errors']:
                        error_message += '\n\nエラー詳細:\n' + '\n'.join(result['errors'][:10])
                else:
                    flash(f'{success_count}件のデータをインポートしました。', 'success')
                    return redirect(url_for('common.cprd_dat_list'))
                    
            finally:
                # 一時ファイルを削除
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception as e:
                    log_error("一時ファイル削除エラー", e)
                    
        except OperationalError as e:
            log_error("データベース接続エラー", e)
            error_message = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        except SQLAlchemyError as e:
            log_error("SQLエラー", e)
            error_message = "データベースの操作中にエラーが発生しました。"
        except Exception as e:
            log_error("予期せぬエラーが発生しました", e)
            error_message = f"予期せぬエラーが発生しました: {str(e)}"
    
    return render_template('import_csv.html', 
                         error=error_message, 
                         debug_info=[],
                         imported_data=None)

@common_bp.route('/cprd_dat', methods=['GET'])
@login_required
def cprd_dat_list():
    """入庫データ一覧画面"""
    try:
        # 検索パラメータの取得
        prd_id = request.args.get('prd_id', '').strip()
        prd_name = request.args.get('prd_name', '').strip()
        lot = request.args.get('lot', '').strip()
        rank = request.args.get('rank', '').strip()
        stock_status = request.args.get('stock_status', '').strip()
        search = request.args.get('search', '').strip()
        
        # 検索実行フラグ
        is_search_executed = search == '1'
        
        # ランクの変換
        rank_value = None
        if rank and rank.isdigit():
            rank_value = int(rank)

        # 在庫状況の変換
        stock_status_value = None
        if stock_status in ['0', '1']:
            stock_status_value = int(stock_status)

        # マスタデータの取得
        prd_list = PrdMstModel.get_all()
        rank_list = KbnMstModel.get_rank_list()

        # データの検索
        cprd_list = []
        if is_search_executed:
            if any([prd_id, prd_name, lot, rank_value is not None, stock_status_value is not None]):
                # 検索条件が指定された場合
                cprd_list = CprdDatModel.search_with_zaiko_zan(
                    prd_id=prd_id if prd_id else None,
                    prd_name=prd_name if prd_name else None,
                    lot=int(lot) if lot.isdigit() else None,
                    rank=rank_value,
                    stock_status=stock_status_value,
                    flg=0  # 有効なデータのみ検索
                )
            else:
                # 検索条件が指定されていない場合はすべて取得
                cprd_list = CprdDatModel.get_all_with_zaiko_zan()
        
        return render_template('common/cprd_dat_list.html',
                             cprd_list=cprd_list,
                             prd_list=prd_list,
                             rank_list=rank_list,
                             search_prd_id=prd_id,
                             search_prd_name=prd_name,
                             search_lot=lot,
                             search_rank=rank,
                             search_stock_status=stock_status,
                             is_search_executed=is_search_executed)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_rank_list()
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_list.html', 
                             cprd_list=[], 
                             prd_list=prd_list,
                             rank_list=rank_list,
                             is_search_executed=False)
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_rank_list()
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_list.html', 
                             cprd_list=[], 
                             prd_list=prd_list,
                             rank_list=rank_list,
                             is_search_executed=False)
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_rank_list()
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_list.html', 
                             cprd_list=[], 
                             prd_list=prd_list,
                             rank_list=rank_list,
                             is_search_executed=False)

@common_bp.route('/cprd_dat/create', methods=['GET', 'POST'])
@login_required
def cprd_dat_create():
    """入庫データ新規作成画面"""
    if request.method == 'GET':
        # 新規作成画面を表示
        try:
            # マスタデータを取得
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
            
            return render_template('common/cprd_dat_create.html', 
                                 prd_list=prd_list, 
                                 rank_list=rank_list)
        except Exception as e:
            log_error("マスタデータ取得エラー", e)
            flash('マスタデータの取得に失敗しました。', 'error')
            return render_template('common/cprd_dat_create.html', 
                                 prd_list=[], 
                                 rank_list=[])
    
    # POST処理
    try:
        # フォームデータを取得
        prd_id = request.form.get('prd_id', '').strip()
        lot = request.form.get('lot', '').strip()
        sprit1 = request.form.get('sprit1', '').strip()
        sprit2 = request.form.get('sprit2', '').strip()
        rank = request.form.get('rank', '').strip()
        qty = request.form.get('qty', '').strip()
        
        # 登録処理
        cpdd_id = CprdDatModel.create(prd_id, lot, sprit1, sprit2, rank, qty)
        
        flash('入庫データを登録しました。', 'success')
        return redirect(url_for('common.cprd_dat_list'))
        
    except ValueError as e:
        flash(str(e), 'error')
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_create.html', 
                             prd_list=prd_list, 
                             rank_list=rank_list)
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_create.html', 
                             prd_list=prd_list, 
                             rank_list=rank_list)
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_create.html', 
                             prd_list=prd_list, 
                             rank_list=rank_list)
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
        except:
            prd_list = []
            rank_list = []
        return render_template('common/cprd_dat_create.html', 
                             prd_list=prd_list, 
                             rank_list=rank_list)

@common_bp.route('/cprd_dat/<int:cpdd_id>/edit', methods=['GET', 'POST'])
@login_required
def cprd_dat_edit(cpdd_id):
    """入庫データ編集画面"""
    if request.method == 'GET':
        # 編集画面を表示
        try:
            cprd_data = CprdDatModel.get_by_id(cpdd_id)
            if not cprd_data:
                flash('指定された入庫データが見つかりません。', 'error')
                return redirect(url_for('common.cprd_dat_list'))
            
            # マスタデータを取得
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
            
            return render_template('common/cprd_dat_edit.html', 
                                 cprd_data=cprd_data,
                                 prd_list=prd_list,
                                 rank_list=rank_list)
            
        except OperationalError as e:
            log_error("データベース接続エラー", e)
            error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
            flash(error_msg, 'error')
            return redirect(url_for('common.cprd_dat_list'))
        except SQLAlchemyError as e:
            log_error("SQLエラー", e)
            error_msg = "データベースの操作中にエラーが発生しました。"
            flash(error_msg, 'error')
            return redirect(url_for('common.cprd_dat_list'))
        except Exception as e:
            log_error("予期せぬエラーが発生しました", e)
            error_msg = "予期せぬエラーが発生しました。"
            flash(error_msg, 'error')
            return redirect(url_for('common.cprd_dat_list'))
    
    try:
        # フォームデータを取得
        prd_id = request.form.get('prd_id', '').strip()
        lot = request.form.get('lot', '').strip()
        sprit1 = request.form.get('sprit1', '').strip()
        sprit2 = request.form.get('sprit2', '').strip()
        rank = request.form.get('rank', '').strip()
        qty = request.form.get('qty', '').strip()
        
        # 更新処理
        CprdDatModel.update(cpdd_id, prd_id, lot, sprit1, sprit2, rank, qty)
        
        flash('入庫データを更新しました。', 'success')
        return redirect(url_for('common.cprd_dat_list'))
        
    except ValueError as e:
        flash(str(e), 'error')
        # エラー時もマスタデータを取得して再表示
        try:
            cprd_data = CprdDatModel.get_by_id(cpdd_id)
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_kbn_list(KbnConstants.KBN_ID_RANK)
            return render_template('common/cprd_dat_edit.html', 
                                 cprd_data=cprd_data,
                                 prd_list=prd_list,
                                 rank_list=rank_list)
        except Exception:
            return redirect(url_for('common.cprd_dat_edit', cpdd_id=cpdd_id))
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        return redirect(url_for('common.cprd_dat_edit', cpdd_id=cpdd_id))
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        return redirect(url_for('common.cprd_dat_edit', cpdd_id=cpdd_id))
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        return redirect(url_for('common.cprd_dat_edit', cpdd_id=cpdd_id))

@common_bp.route('/cprd_dat/<int:cpdd_id>/delete', methods=['POST'])
@login_required
def cprd_dat_delete(cpdd_id):
    """入庫データ削除処理"""
    try:
        # 削除処理
        CprdDatModel.delete(cpdd_id)
        
        return jsonify({'success': True})
        
    except ValueError as e:
        error_msg = str(e)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/api/get_customer_list', methods=['GET'])
@login_required
def get_customer_list():
    """得意先リストを取得するAPI"""
    try:
        customer_list = CztrMstModel.get_customer_list()
        return jsonify({'success': True, 'data': customer_list})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/api/get_cprc_list/<string:prd_id>', methods=['GET'])
@login_required
def get_cprc_list(prd_id):
    """加工マスタリストを取得するAPI"""
    try:
        cprc_list = CprcMstModel.get_cprc_list_by_prd_id(prd_id)
        return jsonify({'success': True, 'data': cprc_list})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/cprd_dat/shipment', methods=['POST'])
@login_required
def cprd_dat_shipment():
    """入庫データからの出庫処理（新仕様対応）"""
    try:
        data = request.get_json()
        cpdd_id = data.get('cpdd_id')
        cshk_kbn = data.get('cshk_kbn')  # 出荷区分: 0=出荷, 1=加工, 2=欠損
        qty = data.get('qty')
        
        # 共通項目
        cshk_dt = data.get('cshk_dt')
        cshk_ord_dt = data.get('cshk_ord_dt')
        
        # 出荷区分別の項目
        cshk_to = data.get('cshk_to')  # 出荷先（出荷時）
        cshk_prc_id = data.get('cshk_prc_id')  # 加工ID（加工時）
        
        # 基本バリデーション
        if not cpdd_id or cshk_kbn is None or not qty:
            return jsonify({'success': False, 'error': '必須項目が入力されていません。'})
        
        # 出荷区分のバリデーション
        if cshk_kbn not in [DatabaseConstants.CSHK_KBN_SHIPMENT, 
                           DatabaseConstants.CSHK_KBN_PROCESS, 
                           DatabaseConstants.CSHK_KBN_LOSS]:
            return jsonify({'success': False, 'error': '不正な出荷区分です。'})
        
        # 区分別バリデーション
        if cshk_kbn == DatabaseConstants.CSHK_KBN_SHIPMENT:  # 出荷
            if not cshk_to:
                return jsonify({'success': False, 'error': '出荷先を選択してください。'})
        elif cshk_kbn == DatabaseConstants.CSHK_KBN_PROCESS:  # 加工
            if not cshk_prc_id:
                return jsonify({'success': False, 'error': '加工IDを選択してください。'})
        
        # 出荷データを作成
        cshk_id = ShipmentCommon.create_shipment_new(
            cpdd_id=cpdd_id,
            cshk_kbn=cshk_kbn,
            qty=qty,
            cshk_dt=cshk_dt,
            cshk_ord_dt=cshk_ord_dt,
            cshk_to=cshk_to,
            cshk_prc_id=cshk_prc_id
        )
        
        return jsonify({'success': True, 'cshk_id': cshk_id, 'message': '出庫処理が完了しました。'})
        
    except ValueError as e:
        error_msg = str(e)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/cprd_dat/shipment/update', methods=['POST'])
@login_required
def cprd_dat_shipment_update():
    """出荷データの更新処理（gprcパターンに準拠）"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        cshk_id = data.get('cshk_id')
        cshk_kbn = data.get('cshk_kbn')
        qty = data.get('qty')
        cshk_dt = data.get('cshk_dt')
        cshk_ord_dt = data.get('cshk_ord_dt')
        cshk_to = data.get('cshk_to')
        cshk_prc_id = data.get('cshk_prc_id')
        
        # バリデーション
        if not cshk_id or cshk_kbn is None or qty is None or not cshk_dt:
            return jsonify({'success': False, 'error': '必須項目を入力してください。'})
        
        try:
            # 数値変換
            cshk_id = int(cshk_id)
            cshk_kbn = int(cshk_kbn)
            qty = int(qty)
            
            # 日付変換
            cshk_dt = datetime.strptime(cshk_dt, '%Y-%m-%d')
            if cshk_ord_dt:
                cshk_ord_dt = datetime.strptime(cshk_ord_dt, '%Y-%m-%d')
            
            # 数値の妥当性チェック
            if qty <= 0:
                return jsonify({'success': False, 'error': '出荷数量は0より大きい値を入力してください。'})
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'})
        
        # 区分に応じて出荷先・加工IDを設定
        if cshk_kbn == DatabaseConstants.CSHK_KBN_SHIPMENT:  # 出荷
            cshk_to = int(cshk_to) if cshk_to else None
            cshk_prc_id = None
        elif cshk_kbn == DatabaseConstants.CSHK_KBN_PROCESS:  # 加工
            cshk_to = None
            cshk_prc_id = int(cshk_prc_id) if cshk_prc_id else None
        else:  # 欠損
            cshk_to = None
            cshk_prc_id = None
        
        # データベースを更新（gprcと同じパターン）
        success = ShipmentCommon.update_shipment(cshk_id, cshk_kbn, qty, cshk_dt, cshk_ord_dt, cshk_to, cshk_prc_id)
        
        if success:
            return jsonify({'success': True, 'message': '出荷データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'})
            
    except Exception as e:
        log_error("データベース更新エラー", e)
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'})

@common_bp.route('/cprd_dat/shipment/delete', methods=['POST'])
@login_required
def cprd_dat_shipment_delete():
    """出荷データの削除処理"""
    try:
        data = request.get_json()
        cshk_id = data.get('cshk_id')
        
        # 基本バリデーション
        if not cshk_id:
            return jsonify({'success': False, 'error': '出荷IDが指定されていません。'})
        
        # 出荷データを削除
        success = ShipmentCommon.delete_shipment(cshk_id)
        
        if success:
            return jsonify({'success': True, 'message': '出荷データを削除しました。'})
        else:
            return jsonify({'success': False, 'error': '削除に失敗しました。'})
        
    except ValueError as e:
        error_msg = str(e)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/cprd_dat/<int:cpdd_id>/shipment_create', methods=['GET'])
@login_required
def cprd_dat_shipment_create(cpdd_id):
    """入庫データからの出荷データ作成画面（gprc_create_from_gprr.html参考）"""
    try:
        # 入庫データを在庫残数付きで取得
        cprd_data = CprdDatModel.get_by_id_with_details(cpdd_id)
        if not cprd_data:
            flash('指定された入庫データが見つかりません。', 'error')
            return redirect(url_for('common.cprd_dat_list'))
        
        return render_template('common/cprd_dat_shipment_create.html', cprd_data=cprd_data)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        return redirect(url_for('common.cprd_dat_list'))
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        return redirect(url_for('common.cprd_dat_list'))
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        return redirect(url_for('common.cprd_dat_list'))

@common_bp.route('/api/get_shipment_list/<int:cpdd_id>', methods=['GET'])
@login_required
def get_shipment_list(cpdd_id):
    """入庫IDに関連する出荷データ一覧を取得するAPI"""
    try:
        log_error(f"出荷データ一覧取得リクエスト: 入庫ID={cpdd_id}")
        shipment_list = ShipmentCommon.get_by_cpdd_id_with_details(cpdd_id)
        log_error(f"出荷データ一覧取得完了: 件数={len(shipment_list) if shipment_list else 0}")
        if shipment_list:
            log_error(f"取得データサンプル: {shipment_list[0] if len(shipment_list) > 0 else 'なし'}")
        return jsonify({'success': True, 'data': shipment_list})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/api/get_zaiko_zan/<int:cpdd_id>', methods=['GET'])
@login_required
def get_zaiko_zan(cpdd_id):
    """在庫残数を取得するAPI"""
    try:
        zaiko_zan = CprdDatModel.get_zaiko_zan(cpdd_id)
        return jsonify({'success': True, 'zaiko_zan': zaiko_zan})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/process_request', methods=['GET'])
@login_required
def process_request_list():
    """加工依頼一覧画面（CSHK_KBN=1のデータ）"""
    try:
        # 検索パラメータの取得
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        prd_id = request.args.get('prd_id', '').strip()
        prc_id = request.args.get('prc_id', '').strip()
        cztr_id = request.args.get('cztr_id', '').strip()
        return_status = request.args.get('return_status', '').strip()
        search = request.args.get('search', '').strip()
        
        # 検索実行フラグ
        is_search_executed = search == '1'
        
        # 日付の変換処理
        date_from_obj = None
        date_to_obj = None
        try:
            if date_from:
                date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
            if date_to:
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
        except ValueError:
            flash('日付の形式が正しくありません。', 'error')
            date_from = ''
            date_to = ''
        
        # 加工IDの変換処理
        prc_id_int = None
        try:
            if prc_id:
                prc_id_int = int(prc_id)
        except ValueError:
            flash('加工IDは数値で入力してください。', 'error')
            prc_id = ''
        
        # 加工先IDの変換処理
        cztr_id_int = None
        try:
            if cztr_id:
                cztr_id_int = int(cztr_id)
        except ValueError:
            flash('加工先IDは数値で入力してください。', 'error')
            cztr_id = ''
        
        # 戻り残数の変換処理
        return_status_int = None
        if return_status in ['0', '1']:
            return_status_int = int(return_status)

        # マスタデータの取得
        prd_list = PrdMstModel.get_all()
        
        # 加工マスタの取得
        cprc_list = CprcMstModel.get_all()
        
        # 委託先マスタの取得（加工会社のみ）
        cztr_list = CztrMstModel.get_process_company_list()

        # データの検索
        process_request_list = []
        if is_search_executed:
            process_request_list = ShipmentCommon.get_process_request_list(
                date_from=date_from_obj,
                date_to=date_to_obj,
                prd_id=prd_id if prd_id else None,
                prc_id=prc_id_int,
                cztr_id=cztr_id_int,
                return_status=return_status_int
            )
            

        
        return render_template('common/process_request_list.html', 
                             process_request_list=process_request_list,
                             prd_list=prd_list,
                             cprc_list=cprc_list,
                             cztr_list=cztr_list,
                             search_date_from=date_from,
                             search_date_to=date_to,
                             search_prd_id=prd_id,
                             search_prc_id=prc_id,
                             search_cztr_id=cztr_id,
                             search_return_status=return_status,
                             is_search_executed=is_search_executed)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            cprc_list = CprcMstModel.get_all()
            cztr_list = CztrMstModel.get_process_company_list()
        except:
            prd_list = []
            cprc_list = []
            cztr_list = []
        return render_template('common/process_request_list.html', 
                             process_request_list=[], 
                             prd_list=prd_list,
                             cprc_list=cprc_list,
                             cztr_list=cztr_list,
                             search_date_from='',
                             search_date_to='',
                             search_prd_id='',
                             search_prc_id='',
                             search_cztr_id='',
                             is_search_executed=False)
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            cprc_list = CprcMstModel.get_all()
            cztr_list = CztrMstModel.get_process_company_list()
        except:
            prd_list = []
            cprc_list = []
            cztr_list = []
        return render_template('common/process_request_list.html', 
                             process_request_list=[], 
                             prd_list=prd_list,
                             cprc_list=cprc_list,
                             cztr_list=cztr_list,
                             search_date_from='',
                             search_date_to='',
                             search_prd_id='',
                             search_prc_id='',
                             search_cztr_id='',
                             is_search_executed=False)
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            cprc_list = CprcMstModel.get_all()
            cztr_list = CztrMstModel.get_process_company_list()
        except:
            prd_list = []
            cprc_list = []
            cztr_list = []
        return render_template('common/process_request_list.html', 
                             process_request_list=[], 
                             prd_list=prd_list,
                             cprc_list=cprc_list,
                             cztr_list=cztr_list,
                             search_date_from='',
                             search_date_to='',
                             search_prd_id='',
                             search_prc_id='',
                             search_cztr_id='',
                             is_search_executed=False)

@common_bp.route('/cprc_dat/<int:cshk_id>/create', methods=['GET'])
@login_required
def cprc_dat_create(cshk_id):
    """出荷データからの加工戻りデータ作成画面"""
    try:
        # 出荷データを取得
        target_cshk = ShipmentCommon.get_by_cshk_id_with_details(cshk_id)
        if not target_cshk:
            flash('指定された出荷データが見つかりません。', 'error')
            return redirect(url_for('common.process_request_list'))
        
        # 出荷区分が加工（1）でない場合はエラー
        if target_cshk['CSHK_KBN'] != DatabaseConstants.CSHK_KBN_PROCESS:
            flash('加工区分でない出荷データは処理できません。', 'error')
            return redirect(url_for('common.process_request_list'))
        
        # 戻っていない残数を取得
        prc_zan_qty = ShipmentCommon.get_prc_zan_qty(cshk_id)
        
        return render_template('common/cprc_dat_create.html', 
                             cshk_data=target_cshk, 
                             prc_zan_qty=prc_zan_qty)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        return redirect(url_for('common.process_request_list'))
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        return redirect(url_for('common.process_request_list'))
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        return redirect(url_for('common.process_request_list'))

@common_bp.route('/cprc_dat', methods=['POST'])
@login_required
def cprc_dat_submit():
    """加工戻りデータの作成処理"""
    try:
        data = request.get_json()
        # フォームデータを取得
        shk_id = data.get('shk_id')
        date = data.get('date')
        qty = data.get('qty')
        ret_ng_qty = data.get('ret_ng_qty', 0)
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        # バリデーション
        if not shk_id or not date or qty is None:
            error_msg = '必須項目を入力してください。'
            log_error(error_msg)
            return jsonify({'success': False, 'error': error_msg})
        # 加工データを作成
        cprc_id = CprcDatModel.create(shk_id, date, qty, ret_ng_qty, ins_ng_qty, pass_qty)
        return jsonify({'success': True, 'cprc_id': cprc_id, 'message': '加工戻りデータを登録しました。'})
    except ValueError as e:
        error_msg = str(e)
        log_error(error_msg)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        log_error(f"データベース接続エラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        log_error(f"SQLエラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        log_error(f"予期せぬエラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/cprc_dat/update', methods=['POST'])
@login_required
def cprc_dat_update():
    """加工戻りデータの更新処理"""
    try:
        data = request.get_json()
        # フォームデータを取得
        cprc_id = data.get('cprc_id')
        date = data.get('date')
        qty = data.get('qty')
        ret_ng_qty = data.get('ret_ng_qty', 0)
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        # バリデーション
        if not cprc_id or not date or qty is None:
            error_msg = '必須項目を入力してください。'
            log_error(error_msg)
            return jsonify({'success': False, 'error': error_msg})
        # 加工データを更新
        CprcDatModel.update(cprc_id, date, qty, ret_ng_qty, ins_ng_qty, pass_qty)
        return jsonify({'success': True, 'message': '加工戻りデータを更新しました。'})
    except ValueError as e:
        error_msg = str(e)
        log_error(error_msg)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        log_error(f"データベース接続エラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        log_error(f"SQLエラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        log_error(f"予期せぬエラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/cprc_dat/delete', methods=['POST'])
@login_required
def cprc_dat_delete():
    """加工戻りデータの削除処理"""
    try:
        data = request.get_json()
        cprc_id = data.get('cprc_id')
        if not cprc_id:
            error_msg = '削除対象のIDが指定されていません。'
            log_error(error_msg)
            return jsonify({'success': False, 'error': error_msg})
        # 加工データを削除
        CprcDatModel.delete(cprc_id)
        return jsonify({'success': True, 'message': '加工戻りデータを削除しました。'})
    except ValueError as e:
        error_msg = str(e)
        log_error(error_msg)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        log_error(f"データベース接続エラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        log_error(f"SQLエラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        log_error(f"予期せぬエラー: {str(e)}")
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/api/get_cprc_dat_list/<int:shk_id>', methods=['GET'])
@login_required
def get_cprc_dat_list(shk_id):
    """出荷IDに関連する加工戻りデータ一覧を取得するAPI"""
    try:
        cprc_list = CprcDatModel.get_by_shk_id(shk_id)
        return jsonify({'success': True, 'data': cprc_list})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/delete_shipment', methods=['POST'])
@login_required
def delete_shipment():
    """出荷データの削除処理"""
    try:
        data = request.get_json()
        cshk_id = data.get('cshk_id')
        
        # 基本バリデーション
        if not cshk_id:
            return jsonify({'success': False, 'error': '出荷IDが指定されていません。'})
        
        # 出荷データを削除
        success = ShipmentCommon.delete_shipment(cshk_id)
        
        if success:
            return jsonify({'success': True, 'message': '出荷データを削除しました。'})
        else:
            return jsonify({'success': False, 'error': '削除に失敗しました。'})
        
    except ValueError as e:
        error_msg = str(e)
        return jsonify({'success': False, 'error': error_msg})
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        return jsonify({'success': False, 'error': error_msg})
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        return jsonify({'success': False, 'error': error_msg})

@common_bp.route('/stock_detail/<prd_id>/<rank>', methods=['GET'])
@login_required
def stock_detail(prd_id, rank):
    """在庫詳細画面（CPRD_DATの在庫があるデータの一覧）"""
    try:
        # ランクの変換
        rank_value = None
        if rank and rank.isdigit():
            rank_value = int(rank)

        # データの取得
        stock_detail_list = CprdDatModel.get_stock_detail(prd_id, rank_value)
        
        # 製品名とランク名を取得
        product_name = ''
        rank_name = ''
        if stock_detail_list:
            product_name = stock_detail_list[0].get('PRD_DSP_NM', '')
            rank_name = stock_detail_list[0].get('RANK_NAME', '')
        
        return render_template('common/common_stock_detail.html', 
                             stock_detail_list=stock_detail_list,
                             prd_id=prd_id,
                             rank=rank,
                             product_name=product_name,
                             rank_name=rank_name)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        return render_template('common/common_stock_detail.html', 
                             stock_detail_list=[], 
                             prd_id=prd_id,
                             rank=rank,
                             product_name='',
                             rank_name='')
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        return render_template('common/common_stock_detail.html', 
                             stock_detail_list=[], 
                             prd_id=prd_id,
                             rank=rank,
                             product_name='',
                             rank_name='')
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        return render_template('common/common_stock_detail.html', 
                             stock_detail_list=[], 
                             prd_id=prd_id,
                             rank=rank,
                             product_name='',
                             rank_name='')

@common_bp.route('/stock_summary', methods=['GET'])
@login_required
def stock_summary():
    """在庫集計一覧画面（CPDD_PRD_ID,CPDD_RANKで集計）"""
    try:
        # 検索パラメータの取得
        prd_id = request.args.get('prd_id', '').strip()
        rank = request.args.get('rank', '').strip()
        search = request.args.get('search', '').strip()
        
        # 検索実行フラグ
        is_search_executed = search == '1'
        
        # ランクの変換
        rank_value = None
        if rank and rank.isdigit():
            rank_value = int(rank)

        # マスタデータの取得
        prd_list = PrdMstModel.get_all()
        rank_list = KbnMstModel.get_rank_list()

        # データの検索
        stock_summary_list = []
        if is_search_executed:
            stock_summary_list = CprdDatModel.get_stock_summary(
                prd_id=prd_id if prd_id else None,
                rank=rank_value
            )
        
        return render_template('common/common_stock_summary.html', 
                             stock_summary_list=stock_summary_list,
                             prd_list=prd_list,
                             rank_list=rank_list,
                             search_prd_id=prd_id,
                             search_rank=rank,
                             is_search_executed=is_search_executed)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_rank_list()
        except:
            prd_list = []
            rank_list = []
        return render_template('common/common_stock_summary.html', 
                             stock_summary_list=[], 
                             prd_list=prd_list,
                             rank_list=rank_list,
                             search_prd_id='',
                             search_rank='',
                             is_search_executed=False)
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_rank_list()
        except:
            prd_list = []
            rank_list = []
        return render_template('common/common_stock_summary.html', 
                             stock_summary_list=[], 
                             prd_list=prd_list,
                             rank_list=rank_list,
                             search_prd_id='',
                             search_rank='',
                             is_search_executed=False)
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            prd_list = PrdMstModel.get_all()
            rank_list = KbnMstModel.get_rank_list()
        except:
            prd_list = []
            rank_list = []
        return render_template('common/common_stock_summary.html', 
                             stock_summary_list=[], 
                             prd_list=prd_list,
                             rank_list=rank_list,
                             search_prd_id='',
                             search_rank='',
                             is_search_executed=False)

@common_bp.route('/shipment_list', methods=['GET'])
@login_required
def shipment_list():
    """出荷一覧画面（CSHK_KBN=0のデータ）"""
    try:
        # 検索パラメータの取得
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        ord_date_from = request.args.get('ord_date_from', '').strip()
        ord_date_to = request.args.get('ord_date_to', '').strip()
        cshk_to = request.args.get('cshk_to', '').strip()
        search = request.args.get('search', '').strip()
        
        # 検索実行フラグ
        is_search_executed = search == '1'
        
        # 日付の変換処理
        date_from_obj = None
        date_to_obj = None
        ord_date_from_obj = None
        ord_date_to_obj = None
        try:
            if date_from:
                date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
            if date_to:
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
            if ord_date_from:
                ord_date_from_obj = datetime.strptime(ord_date_from, '%Y-%m-%d')
            if ord_date_to:
                ord_date_to_obj = datetime.strptime(ord_date_to, '%Y-%m-%d')
        except ValueError:
            flash('日付の形式が正しくありません。', 'error')
            date_from = ''
            date_to = ''
            ord_date_from = ''
            ord_date_to = ''
        
        # 出荷先IDの変換処理
        cshk_to_int = None
        try:
            if cshk_to:
                cshk_to_int = int(cshk_to)
        except ValueError:
            flash('出荷先IDは数値で入力してください。', 'error')
            cshk_to = ''

        # マスタデータの取得
        # 得意先マスタの取得（出荷先として使用）
        cztr_list = CztrMstModel.get_customer_list()

        # データの検索
        shipment_list = []
        if is_search_executed:
            shipment_list = ShipmentCommon.get_shipment_list(
                date_from=date_from_obj,
                date_to=date_to_obj,
                ord_date_from=ord_date_from_obj,
                ord_date_to=ord_date_to_obj,
                cshk_to=cshk_to_int
            )
        
        return render_template('common/common_shipment_list.html', 
                             shipment_list=shipment_list,
                             cztr_list=cztr_list,
                             search_date_from=date_from,
                             search_date_to=date_to,
                             search_ord_date_from=ord_date_from,
                             search_ord_date_to=ord_date_to,
                             search_cshk_to=cshk_to,
                             is_search_executed=is_search_executed)
        
    except OperationalError as e:
        log_error("データベース接続エラー", e)
        error_msg = "データベースへの接続に失敗しました。システム管理者に連絡してください。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            cztr_list = CztrMstModel.get_customer_list()
        except:
            cztr_list = []
        return render_template('common/common_shipment_list.html', 
                             shipment_list=[], 
                             cztr_list=cztr_list,
                             search_date_from='',
                             search_date_to='',
                             search_ord_date_from='',
                             search_ord_date_to='',
                             search_cshk_to='',
                             is_search_executed=False)
    except SQLAlchemyError as e:
        log_error("SQLエラー", e)
        error_msg = "データベースの操作中にエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            cztr_list = CztrMstModel.get_customer_list()
        except:
            cztr_list = []
        return render_template('common/common_shipment_list.html', 
                             shipment_list=[], 
                             cztr_list=cztr_list,
                             search_date_from='',
                             search_date_to='',
                             search_ord_date_from='',
                             search_ord_date_to='',
                             search_cshk_to='',
                             is_search_executed=False)
    except Exception as e:
        log_error("予期せぬエラーが発生しました", e)
        error_msg = "予期せぬエラーが発生しました。"
        flash(error_msg, 'error')
        # エラー時でもマスタデータは表示する
        try:
            cztr_list = CztrMstModel.get_customer_list()
        except:
            cztr_list = []
        return render_template('common/common_shipment_list.html', 
                             shipment_list=[], 
                             cztr_list=cztr_list,
                             search_date_from='',
                             search_date_to='',
                             search_ord_date_from='',
                             search_ord_date_to='',
                             search_cshk_to='',
                             is_search_executed=False)

@common_bp.route('/process_request_pdf', methods=['GET'])
@login_required
def process_request_pdf():
    try:
        # 検索パラメータの取得（HTML画面と同じ条件）
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        prd_id = request.args.get('prd_id', '').strip()
        prc_id = request.args.get('prc_id', '').strip()
        cztr_id = request.args.get('cztr_id', '').strip()
        return_status = request.args.get('return_status', '').strip()
        
        # 日付の変換処理
        date_from_obj = None
        date_to_obj = None
        try:
            if date_from:
                date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
            if date_to:
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
        except ValueError:
            flash('日付の形式が正しくありません。', 'error')
            return redirect(url_for('common.process_request_list'))
        
        # 加工IDの変換処理
        prc_id_int = None
        try:
            if prc_id:
                prc_id_int = int(prc_id)
        except ValueError:
            flash('加工IDは数値で入力してください。', 'error')
            return redirect(url_for('common.process_request_list'))
        
        # 加工先IDの変換処理
        cztr_id_int = None
        try:
            if cztr_id:
                cztr_id_int = int(cztr_id)
        except ValueError:
            flash('加工先IDは数値で入力してください。', 'error')
            return redirect(url_for('common.process_request_list'))
        
        # 戻り残数の変換処理
        return_status_int = None
        if return_status in ['0', '1']:
            return_status_int = int(return_status)
        
        # PDF出力処理をexport_pdf.pyに委譲
        pdf = process_request_export_pdf(
            date_from=date_from_obj,
            date_to=date_to_obj,
            prd_id=prd_id if prd_id else None,
            prc_id=prc_id_int,
            cztr_id=cztr_id_int,
            return_status=return_status_int
        )
        
        response = make_response(pdf)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=process_request.pdf'
        return response
        
    except Exception as e:
        print(f"加工依頼書PDF出力エラー: {str(e)}\n{traceback.format_exc()}")
        flash(f'PDF出力中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('common.process_request_list'))

@common_bp.route('/export_shipment_pdf', methods=['GET'])
@login_required
def export_shipment_pdf():
    """出荷一覧をPDFで出力する"""
    try:
        # 検索パラメータの取得
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        ord_date_from = request.args.get('ord_date_from', '').strip()
        ord_date_to = request.args.get('ord_date_to', '').strip()
        cshk_to = request.args.get('cshk_to', '').strip()

        # 日付の変換処理
        date_from_obj = None
        date_to_obj = None
        ord_date_from_obj = None
        ord_date_to_obj = None
        try:
            if date_from:
                date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
            if date_to:
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
            if ord_date_from:
                ord_date_from_obj = datetime.strptime(ord_date_from, '%Y-%m-%d')
            if ord_date_to:
                ord_date_to_obj = datetime.strptime(ord_date_to, '%Y-%m-%d')
        except ValueError:
            flash('日付の形式が正しくありません。', 'error')
            return redirect(url_for('common.shipment_list'))

        # 出荷先IDの変換処理
        cshk_to_int = None
        try:
            if cshk_to:
                cshk_to_int = int(cshk_to)
        except ValueError:
            flash('出荷先IDは数値で入力してください。', 'error')
            return redirect(url_for('common.shipment_list'))

        # 出荷データの検索
        shipment_list = ShipmentCommon.get_shipment_list(
            date_from=date_from_obj,
            date_to=date_to_obj,
            ord_date_from=ord_date_from_obj,
            ord_date_to=ord_date_to_obj,
            cshk_to=cshk_to_int
        )

        if not shipment_list:
            flash('出力対象のデータが見つかりません。', 'warning')
            return redirect(url_for('common.shipment_list'))

        # PDF生成（新しい関数を使用）
        pdf_data = shipment_list_export_pdf(shipment_list)

        # PDFファイルとしてダウンロード
        response = make_response(pdf_data)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=shipment_list.pdf'
        
        return response

    except Exception as e:
        log_error(f"PDF出力中にエラーが発生しました: {str(e)}\n{traceback.format_exc()}")
        flash('PDFの出力に失敗しました。', 'error')
        return redirect(url_for('common.shipment_list'))

@common_bp.route('/shipment_barcode')
@login_required
def shipment_barcode():
    """検索条件に一致する出荷データのバーコードを生成する"""
    try:
        # 検索パラメータの取得と変換
        search_date_from = request.args.get('search_date_from', '').strip()
        search_date_to = request.args.get('search_date_to', '').strip()
        search_ord_date_from = request.args.get('search_ord_date_from', '').strip()
        search_ord_date_to = request.args.get('search_ord_date_to', '').strip()
        search_cshk_to = request.args.get('search_cshk_to', '').strip()

        # 日付の変換処理
        date_format = '%Y-%m-%d'
        
        try:
            search_date_from = datetime.strptime(search_date_from, date_format) if search_date_from else None
            search_date_to = datetime.strptime(search_date_to, date_format) if search_date_to else None
            search_ord_date_from = datetime.strptime(search_ord_date_from, date_format) if search_ord_date_from else None
            search_ord_date_to = datetime.strptime(search_ord_date_to, date_format) if search_ord_date_to else None
        except ValueError as e:
            log_error(f"日付フォーマットエラー: {str(e)}")
            return jsonify({'error': '日付の形式が正しくありません。YYYY-MM-DD形式で入力してください。'}), 400

        # 出荷先IDの変換
        try:
            search_cshk_to = int(search_cshk_to) if search_cshk_to else None
        except ValueError:
            log_error(f"出荷先ID変換エラー: {search_cshk_to}")
            return jsonify({'error': '出荷先IDは数値で入力してください。'}), 400

        # バーコード生成処理の実行
        success, result = ShipmentBarcodeSaver.save_shipment_barcodes_common(
            search_date_from=search_date_from,
            search_date_to=search_date_to,
            search_ord_date_from=search_ord_date_from,
            search_ord_date_to=search_ord_date_to,
            search_cshk_to=search_cshk_to
        )

        # 結果の返却（save_shipment_barcodes_commonの戻り値をそのまま返す）
        flash(result, success)
        return redirect(url_for('common.shipment_list'))

    except Exception as e:
        error_msg = f"バーコード生成中にエラーが発生しました: {str(e)}"
        log_error(error_msg + f"\n{traceback.format_exc()}")
        flash('バーコード生成中にエラーが発生しました。', 'error')
        return redirect(url_for('common.shipment_list'))
