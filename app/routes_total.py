from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app import app
from app.database import get_db_session
from app.models import log_error
from app.gradation import Gradation
from app.auth import login_required
from sqlalchemy import text
from datetime import datetime
from app.models_total import CprgMstModel
from sqlalchemy.exc import SQLAlchemyError, OperationalError

total_bp = Blueprint('total', __name__, url_prefix='/total')

@total_bp.route('/processing_matrix_cprg')
@login_required
def processing_matrix_cprg():
    """加工集計マトリックス表を表示"""
    try:
        # 検索条件を取得
        cprg_id = request.args.get('cprg_id', '').strip()
        searched = request.args.get('searched', '').strip()
        
        # 加工集計グループ一覧を取得
        cprg_groups = CprgMstModel.get_cprg_groups()
        
        matrix_data = {}
        cprg_details = []
        
        # 検索ボタンが押された場合のみデータを取得
        if searched == '1' and cprg_id:
            matrix_data = CprgMstModel.get_processing_matrix_data(cprg_id)
            cprg_details = CprgMstModel.get_cprg_details(cprg_id)
        
        return render_template('total/processing_matrix_cprg.html', 
                             cprg_groups=cprg_groups,
                             matrix_data=matrix_data,
                             cprg_details=cprg_details,
                             search_cprg_id=cprg_id,
                             searched=searched)
        
    except Exception as e:
        log_error(f"加工集計マトリックス表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

# 以下、CprgMstModelのマスター管理ルートを追加

@total_bp.route('/cprg/list')
@login_required
def cprg_list():
    """加工集計グループマスタ一覧画面を表示"""
    try:
        # 検索条件を取得
        cprg_id = request.args.get('cprg_id', '').strip()
        
        # 加工集計グループIDの選択肢を取得
        cprg_id_choices = CprgMstModel.get_group_choices()
        
        # データを取得
        if cprg_id:
            # 特定のグループIDで絞り込み
            cprg_list = [item for item in CprgMstModel.get_all() if item['CPRG_ID'] == cprg_id]
        else:
            cprg_list = CprgMstModel.get_all()
        
        return render_template('master/cprg_list.html', 
                             cprg_list=cprg_list, 
                             cprg_id_choices=cprg_id_choices,
                             search_cprg_id=cprg_id)
    except Exception as e:
        flash(f'加工集計グループマスタ一覧の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@total_bp.route('/cprg/create', methods=['GET', 'POST'])
@login_required
def cprg_create():
    """加工集計グループマスタ新規作成画面"""
    if request.method == 'POST':
        try:
            # フォームデータを取得
            cprg_id = request.form.get('cprg_id', '').strip()
            cprg_prd_id = request.form.get('cprg_prd_id', '').strip()
            cprg_prc_id = request.form.get('cprg_prc_id', '').strip()
            cprg_g_nm = request.form.get('cprg_g_nm', '').strip()
            cprg_col_nm = request.form.get('cprg_col_nm', '').strip()
            cprg_row_nm = request.form.get('cprg_row_nm', '').strip()
            cprg_af_prd_id = request.form.get('cprg_af_prd_id', '').strip()
            cprg_col_key = request.form.get('cprg_col_key', '').strip()
            cprg_row_key = request.form.get('cprg_row_key', '').strip()
            
            # バリデーション
            if not cprg_id:
                flash('グループIDは必須です。', 'error')
                return render_template('master/cprg_create.html')
            
            if not cprg_prd_id:
                flash('製品IDは必須です。', 'error')
                return render_template('master/cprg_create.html')
            
            if not cprg_prc_id:
                flash('加工IDは必須です。', 'error')
                return render_template('master/cprg_create.html')
            
            if not cprg_g_nm:
                flash('グループ名は必須です。', 'error')
                return render_template('master/cprg_create.html')
            
            # 数値変換
            try:
                cprg_prc_id = int(cprg_prc_id) if cprg_prc_id else None
                cprg_col_key = int(cprg_col_key) if cprg_col_key else None
                cprg_row_key = int(cprg_row_key) if cprg_row_key else None
            except ValueError:
                flash('加工ID、列キー、行キーは数値で入力してください。', 'error')
                return render_template('master/cprg_create.html')
            
            # 重複チェック
            existing = CprgMstModel.get_by_id(cprg_id, cprg_prd_id, cprg_prc_id)
            if existing:
                flash('同じグループID、製品ID、加工IDの組み合わせは既に存在します。', 'error')
                return render_template('master/cprg_create.html')
            
            # データを挿入
            cprg_data = {
                'cprg_id': cprg_id,
                'cprg_prd_id': cprg_prd_id,
                'cprg_prc_id': cprg_prc_id,
                'cprg_g_nm': cprg_g_nm,
                'cprg_col_nm': cprg_col_nm,
                'cprg_row_nm': cprg_row_nm,
                'cprg_af_prd_id': cprg_af_prd_id,
                'cprg_col_key': cprg_col_key,
                'cprg_row_key': cprg_row_key
            }
            
            CprgMstModel.create(cprg_data)
            flash('加工集計グループマスタを登録しました。', 'success')
            return redirect(url_for('total.cprg_list'))
                
        except SQLAlchemyError as e:
            flash(f'データベースエラーが発生しました: {str(e)}', 'error')
            return render_template('master/cprg_create.html')
        except Exception as e:
            flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
            return render_template('master/cprg_create.html')
    
    # GETリクエストの場合
    try:
        # 選択肢を取得
        cprg_id_choices = CprgMstModel.get_group_choices()
        product_choices = CprgMstModel.get_product_choices()
        process_choices = CprgMstModel.get_process_choices()
        
        return render_template('master/cprg_form.html',
                             title="加工集計グループマスタ新規作成",
                             icon="bi-plus-circle",
                             form_type="入力",
                             form_action=url_for('total.cprg_create'),
                             submit_text="登録",
                             is_edit=False,
                             cprg_id_choices=cprg_id_choices,
                             product_choices=product_choices,
                             process_choices=process_choices)
    except Exception as e:
        flash(f'画面表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('total.cprg_list'))

@total_bp.route('/cprg/<cprg_id>/<cprg_prd_id>/<int:cprg_prc_id>/edit', methods=['GET', 'POST'])
@login_required
def cprg_edit(cprg_id, cprg_prd_id, cprg_prc_id):
    """加工集計グループマスタ編集画面"""
    try:
        if request.method == 'POST':
            # フォームデータを取得
            cprg_g_nm = request.form.get('cprg_g_nm', '').strip()
            cprg_col_nm = request.form.get('cprg_col_nm', '').strip()
            cprg_row_nm = request.form.get('cprg_row_nm', '').strip()
            cprg_af_prd_id = request.form.get('cprg_af_prd_id', '').strip()
            cprg_col_key = request.form.get('cprg_col_key', '').strip()
            cprg_row_key = request.form.get('cprg_row_key', '').strip()
            
            # バリデーション
            if not cprg_g_nm:
                flash('グループ名は必須です。', 'error')
                return redirect(url_for('total.cprg_edit', cprg_id=cprg_id, cprg_prd_id=cprg_prd_id, cprg_prc_id=cprg_prc_id))
            
            # 数値変換
            try:
                cprg_col_key = int(cprg_col_key) if cprg_col_key else None
                cprg_row_key = int(cprg_row_key) if cprg_row_key else None
            except ValueError:
                flash('列キー、行キーは数値で入力してください。', 'error')
                return redirect(url_for('total.cprg_edit', cprg_id=cprg_id, cprg_prd_id=cprg_prd_id, cprg_prc_id=cprg_prc_id))
            
            # データを更新
            cprg_data = {
                'cprg_g_nm': cprg_g_nm,
                'cprg_col_nm': cprg_col_nm,
                'cprg_row_nm': cprg_row_nm,
                'cprg_af_prd_id': cprg_af_prd_id,
                'cprg_col_key': cprg_col_key,
                'cprg_row_key': cprg_row_key
            }
            
            CprgMstModel.update(cprg_id, cprg_prd_id, cprg_prc_id, cprg_data)
            flash('加工集計グループマスタを更新しました。', 'success')
            return redirect(url_for('total.cprg_list'))
        
        # GETリクエストの場合、データを取得
        cprg_data = CprgMstModel.get_by_id(cprg_id, cprg_prd_id, cprg_prc_id)
        
        if not cprg_data:
            flash('指定された加工集計グループマスタが見つかりません。', 'error')
            return redirect(url_for('total.cprg_list'))
        
        # 選択肢を取得
        product_choices = CprgMstModel.get_product_choices()
        process_choices = CprgMstModel.get_process_choices()
        
        return render_template('master/cprg_form.html',
                             title="加工集計グループマスタ編集",
                             icon="bi-pencil-square",
                             form_type="編集",
                             form_action=url_for('total.cprg_edit', cprg_id=cprg_id, cprg_prd_id=cprg_prd_id, cprg_prc_id=cprg_prc_id),
                             submit_text="更新",
                             is_edit=True,
                             cprg=cprg_data,
                             product_choices=product_choices,
                             process_choices=process_choices)
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('total.cprg_list'))
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('total.cprg_list'))

@total_bp.route('/cprg/<cprg_id>/<cprg_prd_id>/<int:cprg_prc_id>/delete', methods=['POST'])
@login_required
def cprg_delete(cprg_id, cprg_prd_id, cprg_prc_id):
    """加工集計グループマスタ削除処理"""
    try:
        # データの存在確認
        existing = CprgMstModel.get_by_id(cprg_id, cprg_prd_id, cprg_prc_id)
        
        if not existing:
            flash('指定された加工集計グループマスタが見つかりません。', 'error')
            return redirect(url_for('total.cprg_list'))
        
        # データを削除
        CprgMstModel.delete(cprg_id, cprg_prd_id, cprg_prc_id)
        flash('加工集計グループマスタを削除しました。', 'success')
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
    
    return redirect(url_for('total.cprg_list'))


@total_bp.route('/api/cprg/process_choices/<prd_id>', methods=['GET'])
@login_required
def api_cprg_process_choices(prd_id):
    """製品IDに紐づく加工IDの選択肢を取得するAPI"""
    try:
        if not prd_id or prd_id == '':
            return jsonify({'success': True, 'choices': [('', '選択してください')]})
        
        process_choices = CprgMstModel.get_process_choices_by_product(prd_id)
        return jsonify({'success': True, 'choices': process_choices})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

