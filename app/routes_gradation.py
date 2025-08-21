from flask import render_template, request, redirect, url_for, flash, jsonify
from app import app
from app.database import get_db_session
from app.models import log_error
from app.gradation import Gradation
from app.auth import login_required
from sqlalchemy import text
from datetime import datetime
from app.constants import DatabaseConstants, KbnConstants

@app.route('/gradation/gprr_create', methods=['GET'])
@login_required
def create_gprr():
    """グラデ加工依頼データ作成画面を表示"""
    try:
        # 区分マスタから選択肢を取得
        spec_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GSPEC, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        color_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GCOLOR, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        
        return render_template('gradation/gprr_create.html',
                             spec_choices=spec_choices,
                             color_choices=color_choices)
        
    except Exception as e:
        log_error(f"画面表示エラー: {str(e)}")
        flash('画面の表示中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/save_gprr', methods=['POST'])
@login_required
def save_gprr():
    """グラデ加工依頼データを保存"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        spec = data.get('spec')
        color = data.get('color')
        req_to = DatabaseConstants.GPRR_REQ_TO_CONVEX
        req_date = data.get('req_date')
        qty = data.get('qty')
        
        # バリデーション
        if not all([spec, color, req_date, qty]):
            return jsonify({'success': False, 'error': 'すべての項目を入力してください。'}), 400
        
        try:
            # 数値変換
            spec = int(spec)
            color = int(color)
            qty = int(qty)
            
            # 日付変換
            req_date = datetime.strptime(req_date, '%Y-%m-%d')
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # データベースに保存
        Gradation.create_gprr(spec, color, req_to, req_date, qty)
        return jsonify({'success': True, 'message': 'グラデ加工依頼データを登録しました。'})
            
    except Exception as e:
        log_error(f"データベース保存エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの保存中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/gprr_list')
@login_required
def gprr_list():
    """GPRRデータ一覧を表示"""
    try:
        # 検索条件を取得
        spec = request.args.get('spec', '').strip()
        color = request.args.get('color', '').strip()
        req_date_from = request.args.get('req_date_from', '').strip()
        req_date_to = request.args.get('req_date_to', '').strip()
        flg = request.args.get('flg', '').strip()
        searched = request.args.get('searched', '').strip()  # 検索実行フラグ
        
        gprr_list = []
        total_count = 0
        
        # 検索ボタンが押された場合のみデータを取得
        if searched == '1':
            gprr_list = Gradation.get_gprr_list(spec, color, req_date_from, req_date_to, flg)
            total_count = len(gprr_list) if gprr_list else 0
        
        # 検索用の選択肢を取得
        spec_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GSPEC, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        color_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GCOLOR, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        
        return render_template('gradation/gprr_list.html', 
                             gprr_list=gprr_list,
                             spec_choices=spec_choices,
                             color_choices=color_choices,
                             search_spec=spec,
                             search_color=color,
                             search_req_date_from=req_date_from,
                             search_req_date_to=req_date_to,
                             search_flg=flg,
                             searched=searched,
                             total_count=total_count)
        
    except Exception as e:
        log_error(f"GPRR一覧表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/edit_gprr/<int:gprr_id>', methods=['GET'])
@login_required
def edit_gprr(gprr_id):
    """グラデ加工依頼データ編集画面を表示"""
    session = get_db_session()
    try:
        # 編集フォームを表示
        gprr = Gradation.get_gprr_by_id(gprr_id)
        if not gprr:
            flash('指定されたデータが見つかりません。', 'error')
            return redirect(url_for('gprr_list'))
        
        # 区分マスタから選択肢を取得
        spec_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GSPEC, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        color_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GCOLOR, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        
        return render_template('gradation/gprr_edit.html',
                             gprr=gprr,
                             spec_choices=spec_choices,
                             color_choices=color_choices)
        
    except Exception as e:
        log_error(f"編集画面表示エラー: {str(e)}")
        flash('画面の表示中にエラーが発生しました。', 'error')
        return redirect(url_for('gprr_list'))
        
    finally:
        session.close()

@app.route('/gradation/update_gprr/<int:gprr_id>', methods=['PUT'])
@login_required
def update_gprr(gprr_id):
    """グラデ加工依頼データを更新"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        spec = data.get('spec')
        color = data.get('color')
        req_date = data.get('req_date')
        qty = data.get('qty')
        
        # バリデーション
        if not all([spec, color, req_date, qty]):
            return jsonify({'success': False, 'error': 'すべての項目を入力してください。'}), 400
        
        try:
            # 数値変換
            spec = int(spec)
            color = int(color)
            qty = int(qty)
            
            # 日付変換
            req_date = datetime.strptime(req_date, '%Y-%m-%d')
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # データベースを更新
        if Gradation.update_gprr(gprr_id, spec, color, req_date, qty):
            return jsonify({'success': True, 'message': 'グラデ加工依頼データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"データベース更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/delete_gprr/<int:gprr_id>', methods=['DELETE'])
@login_required
def delete_gprr(gprr_id):
    """グラデ加工依頼データを削除"""
    try:
        if Gradation.delete_gprr(gprr_id):
            return jsonify({'success': True, 'message': 'グラデ加工依頼データを削除しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
    except Exception as e:
        log_error(f"削除エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの削除中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/gprc_create_from_gprr/<int:gprr_id>', methods=['GET'])
@login_required
def gprc_create(gprr_id):
    """指定されたGPRR_IDからグラデ加工データ作成画面を表示"""
    try:
        # GPRRデータを取得
        gprr_data = Gradation.get_gprr_for_gprc(gprr_id)
        if not gprr_data:
            flash('指定された依頼データが見つかりません。', 'error')
            return redirect(url_for('gprr_list'))
        
        return render_template('gradation/gprc_create_from_gprr.html',
                             gprr_data=gprr_data)
        
    except Exception as e:
        log_error(f"入庫画面表示エラー: {str(e)}")
        flash('画面の表示中にエラーが発生しました。', 'error')
        return redirect(url_for('gprr_list'))

@app.route('/gradation/save_gprc_from_gprr/<int:req_id>', methods=['POST'])
@login_required
def save_gprc_from_gprr(req_id):
    """GPRR指定でGPRCデータを保存"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        date = data.get('date')
        qty = data.get('qty')
        ret_ng_qty = data.get('ret_ng_qty', 0)
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        sts = data.get('sts', 0)
        
        # バリデーション
        if not date:
            return jsonify({'success': False, 'error': '戻り日を入力してください。'}), 400
        if not qty:
            return jsonify({'success': False, 'error': '戻り数を入力してください。'}), 400
        
        try:
            # 数値変換
            qty = int(qty)
            ret_ng_qty = int(ret_ng_qty) if ret_ng_qty else 0
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            sts = int(sts) if sts else 0
            
            # 日付変換
            date = datetime.strptime(date, '%Y-%m-%d')
            
            # 数値の妥当性チェック
            if qty <= 0:
                return jsonify({'success': False, 'error': '戻り数は0より大きい値を入力してください。'}), 400
            if ret_ng_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数は0以上の値を入力してください。'}), 400
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
            # 合格数の妥当性チェック
            calculated_pass_qty = qty - ret_ng_qty - ins_ng_qty
            if calculated_pass_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数と検品不良数の合計が戻り数を超えています。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # 依頼先を自動的に設定（GPRRデータから取得）
        req_to = Gradation.get_gprr_req_to(req_id)
        if req_to is None:
            return jsonify({'success': False, 'error': '依頼データが見つかりません。'}), 400
        
        # 依頼数量との比較チェック
        gprr_data = Gradation.get_gprr_by_id(req_id)
        if gprr_data:
            # 既存の戻り数合計を取得
            existing_total_raw = Gradation.get_gprc_total_qty(req_id)
            # 型変換を確実に行う
            existing_total = int(existing_total_raw) if existing_total_raw is not None else 0
            
            # デバッグ用ログ（詳細版）
            log_error(f"数量チェック詳細: 既存生データ={existing_total_raw}(型:{type(existing_total_raw)}), 既存変換後={existing_total}, 新規={qty}(型:{type(qty)}), 合計={existing_total + qty}, 依頼数量={gprr_data.GPRR_QTY}")
            
            new_total = existing_total + qty
            
            if new_total > gprr_data.GPRR_QTY:
                return jsonify({'success': False, 'error': f'戻り数の合計({new_total})が依頼数量({gprr_data.GPRR_QTY})を超えています（既存: {existing_total}, 新規: {qty}）'}), 400
        
        # データベースに保存（出荷IDは0に固定、合格数は自動計算値を使用）
        Gradation.create_gprc(req_id, req_to, date, qty, ret_ng_qty, ins_ng_qty, 0, calculated_pass_qty, sts)
        return jsonify({'success': True, 'message': 'グラデ加工データを登録しました。'})
        
    except Exception as e:
        log_error(f"データベース保存エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの保存中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_gprc_list/<int:req_id>', methods=['GET'])
@login_required
def get_gprc_list(req_id):
    """指定された依頼IDに紐づくGPRCデータ一覧を取得"""
    try:
        gprc_list = Gradation.get_gprc_by_req_id(req_id)
        # 日付を文字列に変換
        result = []
        for gprc in gprc_list:
            gprc_dict = {
                'GPRC_ID': gprc.GPRC_ID,
                'GPRC_REQ_ID': gprc.GPRC_REQ_ID,
                'GPRC_REQ_TO': gprc.GPRC_REQ_TO,
                'GPRC_REQ_TO_NM': gprc.GPRC_REQ_TO_NM,
                'GPRC_DATE': gprc.GPRC_DATE.strftime('%Y-%m-%d') if gprc.GPRC_DATE else '',
                'GPRC_QTY': gprc.GPRC_QTY,
                'GPRC_RET_NG_QTY': gprc.GPRC_RET_NG_QTY,
                'GPRC_INS_NG_QTY': gprc.GPRC_INS_NG_QTY,
                'GPRC_SHK_ID': gprc.GPRC_SHK_ID,
                'GPRC_PASS_QTY': gprc.GPRC_PASS_QTY,
                'GPRC_STS': gprc.GPRC_STS
            }
            result.append(gprc_dict)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        log_error(f"GPRC一覧取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/update_gprc/<int:gprc_id>', methods=['PUT'])
@login_required
def update_gprc(gprc_id):
    """GPRCデータを更新"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        date = data.get('date')
        qty = data.get('qty')
        ret_ng_qty = data.get('ret_ng_qty', 0)
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        sts = data.get('sts', 0)
        
        # バリデーション
        if not date:
            return jsonify({'success': False, 'error': '戻り日を入力してください。'}), 400
        if not qty:
            return jsonify({'success': False, 'error': '戻り数を入力してください。'}), 400
        
        try:
            # 数値変換
            qty = int(qty)
            ret_ng_qty = int(ret_ng_qty) if ret_ng_qty else 0
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            sts = int(sts) if sts else 0
            
            # 日付変換
            date = datetime.strptime(date, '%Y-%m-%d')
            
            # 数値の妥当性チェック
            if qty <= 0:
                return jsonify({'success': False, 'error': '戻り数は0より大きい値を入力してください。'}), 400
            if ret_ng_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数は0以上の値を入力してください。'}), 400
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
            # 合格数の妥当性チェック
            calculated_pass_qty = qty - ret_ng_qty - ins_ng_qty
            if calculated_pass_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数と検品不良数の合計が戻り数を超えています。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # データベースを更新（依頼先は変更しない、出荷IDは0に固定、合格数は自動計算値を使用）
        # まず、更新対象のデータを取得して依頼IDを確認
        gprc_data = Gradation.get_gprc_by_id(gprc_id)
        if not gprc_data:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
        
        # 依頼数量との比較チェック
        gprr_data = Gradation.get_gprr_by_id(gprc_data.GPRC_REQ_ID)
        if gprr_data:
            # 既存の戻り数合計を取得（更新対象を除く）
            existing_total_raw = Gradation.get_gprc_total_qty_exclude(gprc_data.GPRC_REQ_ID, gprc_id)
            # 型変換を確実に行う
            existing_total = int(existing_total_raw) if existing_total_raw is not None else 0
            
            # デバッグ用ログ（詳細版）
            log_error(f"数量チェック詳細（更新）: 既存生データ={existing_total_raw}(型:{type(existing_total_raw)}), 既存変換後={existing_total}, 新規={qty}(型:{type(qty)}), 合計={existing_total + qty}, 依頼数量={gprr_data.GPRR_QTY}")
            
            new_total = existing_total + qty
            
            if new_total > gprr_data.GPRR_QTY:
                return jsonify({'success': False, 'error': f'戻り数の合計({new_total})が依頼数量({gprr_data.GPRR_QTY})を超えています（既存: {existing_total}, 新規: {qty}）'}), 400
        
        if Gradation.update_gprc(gprc_id, date, qty, ret_ng_qty, ins_ng_qty, 0, calculated_pass_qty, sts):
            return jsonify({'success': True, 'message': 'グラデ加工データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"データベース更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/delete_gprc/<int:gprc_id>', methods=['DELETE'])
@login_required
def delete_gprc(gprc_id):
    """GPRCデータを削除"""
    try:
        if Gradation.delete_gprc(gprc_id):
            return jsonify({'success': True, 'message': 'グラデ加工データを削除しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
    except Exception as e:
        log_error(f"削除エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの削除中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_gprr_zan_qty/<int:gprr_id>', methods=['GET'])
@login_required
def get_gprr_zan_qty(gprr_id):
    """指定された依頼IDの加工残数を取得"""
    try:
        zan_qty = Gradation.get_gprr_zan_qty(gprr_id)
        return jsonify({'success': True, 'zan_qty': zan_qty})
    except Exception as e:
        log_error(f"加工残数取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'加工残数の取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_gprc_total_qty/<int:gprr_id>', methods=['GET'])
@login_required
def get_gprc_total_qty(gprr_id):
    """指定された依頼IDの戻り数合計を取得"""
    try:
        total_qty = Gradation.get_gprc_total_qty(gprr_id)
        return jsonify({'success': True, 'total_qty': total_qty})
    except Exception as e:
        log_error(f"戻り数合計取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'戻り数合計の取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/gprc_list')
@login_required
def gprc_list():
    """GPRCデータ一覧を表示（GPRC_REQ_TO=1のみ）"""
    try:
        # 検索条件を取得
        spec = request.args.get('spec', '').strip()
        color = request.args.get('color', '').strip()
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        stock_status = request.args.get('stock_status', '').strip()  # 在庫有り無し
        searched = request.args.get('searched', '').strip()  # 検索実行フラグ
        
        gprc_list = []
        total_count = 0
        
        # 検索ボタンが押された場合のみデータを取得
        if searched == '1':
            gprc_list = Gradation.get_gprc_list_for_shipping(spec, color, date_from, date_to, stock_status)
            total_count = len(gprc_list) if gprc_list else 0
        
        # 検索用の選択肢を取得
        spec_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GSPEC, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        color_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GCOLOR, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        
        return render_template('gradation/gprc_list.html', 
                             gprc_list=gprc_list,
                             spec_choices=spec_choices,
                             color_choices=color_choices,
                             search_spec=spec,
                             search_color=color,
                             search_date_from=date_from,
                             search_date_to=date_to,
                             search_stock_status=stock_status,
                             searched=searched,
                             total_count=total_count)
        
    except Exception as e:
        log_error(f"GPRC一覧表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/nidec_gprc_list')
@login_required
def nidec_gprc_list():
    """ニデック加工データ一覧を表示（GPRC_REQ_TO=2のみ）"""
    try:
        # 検索条件を取得
        spec = request.args.get('spec', '').strip()
        color = request.args.get('color', '').strip()
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        stock_status = request.args.get('stock_status', '').strip()  # 在庫有り無し
        searched = request.args.get('searched', '').strip()  # 検索実行フラグ
        
        gprc_list = []
        total_count = 0
        
        # 検索ボタンが押された場合のみデータを取得
        if searched == '1':
            gprc_list = Gradation.get_nidec_gprc_list(spec, color, date_from, date_to, stock_status)
            total_count = len(gprc_list) if gprc_list else 0
        
        # 検索用の選択肢を取得
        spec_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GSPEC, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        color_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GCOLOR, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        
        return render_template('gradation/nidec_gprc_list.html', 
                             gprc_list=gprc_list,
                             spec_choices=spec_choices,
                             color_choices=color_choices,
                             search_spec=spec,
                             search_color=color,
                             search_date_from=date_from,
                             search_date_to=date_to,
                             search_stock_status=stock_status,
                             searched=searched,
                             total_count=total_count)
        
    except Exception as e:
        log_error(f"ニデック加工データ一覧表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/nidec_shipping_create/<int:gprc_id>')
@login_required
def nidec_shipping_create(gprc_id):
    """ニデック加工データから出荷作成画面を表示"""
    try:
        # GPRCデータを取得
        gprc_data = Gradation.get_gprc_detail(gprc_id)
        
        if not gprc_data:
            flash('指定された加工データが見つかりません。', 'error')
            return redirect(url_for('nidec_gprc_list'))
        
        # 出荷可能数を正しく計算
        available_qty = Gradation.get_available_qty(gprc_id)
        
        return render_template('gradation/nidec_shipping_create.html', 
                             gprc_data=gprc_data, 
                             available_qty=available_qty)
        
    except Exception as e:
        log_error(f"ニデック出荷作成画面表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('nidec_gprc_list'))

@app.route('/gradation/create_nidec_shipping_batch/<int:gprc_id>', methods=['POST'])
@login_required
def create_nidec_shipping_batch(gprc_id):
    """ニデック加工データから出荷を作成する（バッチ処理用）"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        ord_date = data.get('ord_date')
        shipping_date = data.get('shipping_date')
        shipping_qty = data.get('shipping_qty')
        
        # バリデーション
        if not all([ord_date, shipping_date, shipping_qty]):
            return jsonify({'success': False, 'error': 'すべての項目を入力してください。'}), 400
        
        try:
            # 数値変換
            shipping_qty = int(shipping_qty)
            
            # 日付変換
            ord_date = datetime.strptime(ord_date, '%Y-%m-%d')
            shipping_date = datetime.strptime(shipping_date, '%Y-%m-%d')
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # 出荷可能数をチェック
        available_qty = Gradation.get_nidec_available_qty(gprc_id)
        if shipping_qty > available_qty:
            return jsonify({'success': False, 'error': f'出荷数量は出荷可能数({available_qty})以下で入力してください。'}), 400
        
        # データベースに保存
        result = Gradation.create_shipping_batch(gprc_id, ord_date, shipping_date, shipping_qty, shipping_to=3)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        log_error(f"出荷作成エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'出荷の作成中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/final_shipping_search')
@login_required
def final_shipping_search():
    """最終出荷データ検索画面を表示"""
    try:
        # 検索条件を取得
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        date_from2 = request.args.get('date_from2', '').strip()
        date_to2 = request.args.get('date_to2', '').strip()
        flg = request.args.get('flg', '').strip()
        searched = request.args.get('searched', '').strip()  # 検索実行フラグ
        
        shipping_list = []
        total_count = 0
        
        # 検索ボタンが押された場合のみデータを取得
        if searched == '1':
            shipping_list = Gradation.get_final_shipping_list(date_from, date_to, date_from2, date_to2, flg)
            total_count = len(shipping_list) if shipping_list else 0
        
        # マトリックスデータを取得
        matrix_data = Gradation.get_final_shipping_matrix(date_from, date_to, date_from2, date_to2, flg)
        
        return render_template('gradation/final_shipping_search.html', 
                             shipping_list=shipping_list,
                             search_date_from=date_from,
                             search_date_to=date_to,
                             search_date_from2=date_from2,
                             search_date_to2=date_to2,
                             search_flg=flg,
                             searched=searched,
                             total_count=total_count,
                             **matrix_data)
        
    except Exception as e:
        log_error(f"最終出荷検索画面表示エラー: {str(e)}")
        flash('画面の表示中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/shipping_create/<int:gprc_id>')
@login_required
def shipping_create(gprc_id):
    """コンベックス加工データから出庫作成画面を表示"""
    try:
        # GPRCデータを取得
        gprc_data = Gradation.get_gprc_detail(gprc_id)
        
        if not gprc_data:
            flash('指定された加工データが見つかりません。', 'error')
            return redirect(url_for('gprc_list'))
        
        # コンベックス加工データ（GPRC_REQ_TO=1）かチェック
        if gprc_data.GPRC_REQ_TO != DatabaseConstants.GPRR_REQ_TO_CONVEX:
            flash('このデータはコンベックス加工データではありません。', 'error')
            return redirect(url_for('gprc_list'))
        
        return render_template('gradation/shipping_create.html', gprc_data=gprc_data)
        
    except Exception as e:
        log_error(f"コンベックス出庫作成画面表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('gprc_list'))

@app.route('/gradation/create_shipping_batch/<int:gprc_id>', methods=['POST'])
@login_required
def create_shipping_batch(gprc_id):
    """GPRCデータから出庫を作成（バッチ処理用）"""
    try:
        data = request.get_json()
        
        # デバッグ用ログ
        log_error(f"受信データ: {data}")
        
        ord_date = data.get('ord_date')
        shipping_date = data.get('shipping_date')
        shipping_qty = data.get('shipping_qty')
        shipping_to = data.get('shipping_to', 2)  # デフォルト値2
        
        # デバッグ用ログ
        log_error(f"パラメータ: ord_date={ord_date}, shipping_date={shipping_date}, shipping_qty={shipping_qty}, shipping_to={shipping_to}")
        
        if not ord_date or not shipping_date or not shipping_qty:
            return jsonify({'success': False, 'error': 'パラメータが不足しています'}), 400
        
        # 出庫処理を実行
        result = Gradation.create_shipping_batch(gprc_id, ord_date, shipping_date, shipping_qty, shipping_to)
        
        if result['success']:
            return jsonify({
                'success': True, 
                'message': f'出庫が正常に作成されました。出荷ID: {result["shipping_id"]}'
            })
        else:
            return jsonify({'success': False, 'error': result['error']}), 400
            
    except Exception as e:
        log_error(f"出庫作成エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'出庫の作成中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_shipping_list/<int:gprc_id>', methods=['GET'])
@login_required
def get_shipping_list(gprc_id):
    """指定されたGPRC_IDの出庫履歴を取得"""
    try:
        shipping_list = Gradation.get_shipping_list(gprc_id)
        return jsonify({'success': True, 'data': shipping_list})
    except Exception as e:
        log_error(f"出庫履歴取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'出庫履歴の取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_available_qty/<int:gprc_id>', methods=['GET'])
@login_required
def get_available_qty(gprc_id):
    """指定されたGPRC_IDの出庫可能数を取得"""
    try:
        available_qty = Gradation.get_available_qty(gprc_id)
        return jsonify({'success': True, 'available_qty': available_qty})
    except Exception as e:
        log_error(f"出庫可能数取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'出庫可能数の取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/gshk_list')
@login_required
def gshk_list():
    """出庫データ一覧を表示（GSHK_TO=2のみ）"""
    try:
        # 検索条件を取得
        date_from = request.args.get('date_from', '').strip()
        date_to = request.args.get('date_to', '').strip()
        flg = request.args.get('flg', '').strip()  # 処理状況
        searched = request.args.get('searched', '').strip()  # 検索実行フラグ
        
        gshk_list = []
        total_count = 0
        
        # 検索ボタンが押された場合のみデータを取得
        if searched == '1':
            gshk_list = Gradation.get_gshk_list(date_from, date_to, flg)
            total_count = len(gshk_list) if gshk_list else 0
        
        return render_template('gradation/gshk_list.html', 
                             gshk_list=gshk_list,
                             search_date_from=date_from,
                             search_date_to=date_to,
                             search_flg=flg,
                             searched=searched,
                             total_count=total_count)
        
    except Exception as e:
        log_error(f"出庫データ一覧表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/edit_gshk/<int:gshk_id>', methods=['GET'])
@login_required
def edit_gshk(gshk_id):
    """ニデック加工依頼データ編集画面を表示"""
    try:
        # GSHKデータを取得
        gshk_data = Gradation.get_gshk_by_id(gshk_id)
        
        if not gshk_data:
            flash('指定されたデータが見つかりません。', 'error')
            return redirect(url_for('gshk_list'))
        
        return render_template('gradation/gshk_edit.html', gshk_data=gshk_data)
        
    except Exception as e:
        log_error(f"ニデック加工依頼編集画面表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('gshk_list'))

@app.route('/gradation/update_gshk/<int:gshk_id>', methods=['PUT'])
@login_required
def update_gshk(gshk_id):
    """GSHKデータを更新"""
    try:
        data = request.get_json()
        
        # 必須パラメータの取得
        stc_id = data.get('stc_id')
        req_id = data.get('req_id')
        dt = data.get('dt')
        ord_dt = data.get('ord_dt')
        qty = data.get('qty')
        flg = data.get('flg', 0)
        
        if not stc_id or not req_id or not dt or not ord_dt or not qty:
            return jsonify({'success': False, 'error': '必須項目が不足しています'}), 400
        
        # データベースを更新
        if Gradation.update_gshk(gshk_id, stc_id, req_id, dt, ord_dt, qty, flg):
            return jsonify({'success': True, 'message': 'GSHKデータを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"データベース更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/delete_gshk/<int:gshk_id>', methods=['DELETE'])
@login_required
def delete_gshk(gshk_id):
    """GSHKデータを削除"""
    try:
        if Gradation.delete_gshk(gshk_id):
            return jsonify({'success': True, 'message': 'GSHKデータを削除しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
    except Exception as e:
        log_error(f"削除エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの削除中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/nidec_proc_create/<int:gshk_id>', methods=['GET'])
@login_required
def nidec_proc_create(gshk_id):
    """ニデック加工データ作成画面を表示"""
    try:
        # GSHKデータを取得（差分も含む）
        gshk_data = Gradation.get_gshk_by_id(gshk_id)
        
        if not gshk_data:
            flash('指定された出庫データが見つかりません。', 'error')
            return redirect(url_for('gshk_list'))
        
        return render_template('gradation/nidec_proc_create.html', gshk_data=gshk_data)
        
    except Exception as e:
        log_error(f"ニデック加工データ作成画面表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('gshk_list'))

@app.route('/gradation/save_nidec_proc/<int:gshk_id>', methods=['POST'])
@login_required
def save_nidec_proc(gshk_id):
    """ニデック加工データを保存"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        proc_date = data.get('proc_date')
        proc_qty = data.get('proc_qty')
        ret_ng_qty = data.get('ret_ng_qty', 0)
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        proc_sts = data.get('proc_sts', 0)
        
        # バリデーション
        if not proc_date:
            return jsonify({'success': False, 'error': '加工日を入力してください。'}), 400
        if not proc_qty:
            return jsonify({'success': False, 'error': '加工数量を入力してください。'}), 400
        
        try:
            # 数値変換
            proc_qty = int(proc_qty)
            ret_ng_qty = int(ret_ng_qty) if ret_ng_qty else 0
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            proc_sts = int(proc_sts) if proc_sts else 0
            
            # 日付変換
            proc_date = datetime.strptime(proc_date, '%Y-%m-%d')
            
            # 数値の妥当性チェック
            if proc_qty <= 0:
                return jsonify({'success': False, 'error': '加工数量は0より大きい値を入力してください。'}), 400
            if ret_ng_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数は0以上の値を入力してください。'}), 400
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
            # 合格数の妥当性チェック
            calculated_pass_qty = proc_qty - ret_ng_qty - ins_ng_qty
            if calculated_pass_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数と検品不良数の合計が加工数量を超えています。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # 出庫数量との比較チェック
        gshk_data = Gradation.get_gshk_by_id(gshk_id)
        if gshk_data:
            # 既存の加工数量合計を取得
            existing_total_raw = Gradation.get_nidec_proc_total_qty(gshk_id)
            # 型変換を確実に行う
            existing_total = int(existing_total_raw) if existing_total_raw is not None else 0
            
            # デバッグ用ログ（詳細版）
            log_error(f"ニデック数量チェック詳細: 既存生データ={existing_total_raw}(型:{type(existing_total_raw)}), 既存変換後={existing_total}, 新規={proc_qty}(型:{type(proc_qty)}), 合計={existing_total + proc_qty}")
            
            new_total = existing_total + proc_qty
            
            # 出庫データを取得して出庫数量を確認
            gshk_data = Gradation.get_gshk_by_id(gshk_id)
            if gshk_data:
                # 出庫数量を取得（数値変換を確実に行う）
                shipping_qty_raw = gshk_data.GSHK_QTY
                shipping_qty = int(shipping_qty_raw) if shipping_qty_raw is not None else 0
                
                if new_total > shipping_qty:
                    return jsonify({'success': False, 'error': f'加工数量の合計({new_total})が出庫数量({shipping_qty})を超えています（既存: {existing_total}, 新規: {proc_qty}）'}), 400
        
        # データベースを更新（合格数は自動計算値を使用）
        if Gradation.create_nidec_proc(gshk_id, proc_date, proc_qty, ret_ng_qty, ins_ng_qty, calculated_pass_qty, proc_sts):
            return jsonify({'success': True, 'message': 'ニデック加工データを保存しました。'})
        else:
            return jsonify({'success': False, 'error': 'データの保存に失敗しました。'}), 500
            
    except Exception as e:
        log_error(f"ニデック加工データ保存エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの保存中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_nidec_proc_total_qty/<int:gshk_id>', methods=['GET'])
@login_required
def get_nidec_proc_total_qty(gshk_id):
    """指定されたGSHK_IDのニデック加工数量合計を取得"""
    try:
        total_qty = Gradation.get_nidec_proc_total_qty(gshk_id)
        return jsonify({'success': True, 'total_qty': total_qty})
    except Exception as e:
        log_error(f"ニデック加工数量合計取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'数量合計の取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/get_nidec_proc_list/<int:gshk_id>', methods=['GET'])
@login_required
def get_nidec_proc_list(gshk_id):
    """指定されたGSHK_IDのニデック加工データ一覧を取得"""
    try:
        proc_list = Gradation.get_nidec_proc_list(gshk_id)
        # 日付を文字列に変換
        result = []
        for proc in proc_list:
            proc_dict = {
                'GPRC_ID': proc.GPRC_ID,
                'GPRC_DATE': proc.GPRC_DATE.strftime('%Y-%m-%d') if proc.GPRC_DATE else '',
                'GPRC_QTY': proc.GPRC_QTY,
                'GPRC_RET_NG_QTY': proc.GPRC_RET_NG_QTY,
                'GPRC_INS_NG_QTY': proc.GPRC_INS_NG_QTY,
                'GPRC_PASS_QTY': proc.GPRC_PASS_QTY,
                'GPRC_STS': proc.GPRC_STS
            }
            result.append(proc_dict)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        log_error(f"ニデック加工データ一覧取得エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの取得中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/update_nidec_proc/<int:gprc_id>', methods=['PUT'])
@login_required
def update_nidec_proc(gprc_id):
    """ニデック加工データを更新"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        proc_date = data.get('proc_date')
        proc_qty = data.get('proc_qty')
        ret_ng_qty = data.get('ret_ng_qty', 0)
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        sts = data.get('sts', 0)
        
        # バリデーション
        if not proc_date:
            return jsonify({'success': False, 'error': '加工日を入力してください。'}), 400
        if not proc_qty:
            return jsonify({'success': False, 'error': '加工数量を入力してください。'}), 400
        
        try:
            # 数値変換
            proc_qty = int(proc_qty)
            ret_ng_qty = int(ret_ng_qty) if ret_ng_qty else 0
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            sts = int(sts) if sts else 0
            
            # 日付変換
            proc_date = datetime.strptime(proc_date, '%Y-%m-%d')
            
            # 数値の妥当性チェック
            if proc_qty <= 0:
                return jsonify({'success': False, 'error': '加工数量は0より大きい値を入力してください。'}), 400
            if ret_ng_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数は0以上の値を入力してください。'}), 400
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
            # 合格数の妥当性チェック
            calculated_pass_qty = proc_qty - ret_ng_qty - ins_ng_qty
            if calculated_pass_qty < 0:
                return jsonify({'success': False, 'error': '戻り不良数と検品不良数の合計が加工数量を超えています。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値または日付の形式が正しくありません。'}), 400
        
        # 出庫数量との比較チェック
        gprc_data = Gradation.get_gprc_by_id(gprc_id)
        if gprc_data:
            # 既存の加工数量合計を取得（更新対象を除く）
            existing_total_raw = Gradation.get_nidec_proc_total_qty_exclude(gprc_data.GPRC_SHK_ID, gprc_id)
            # 型変換を確実に行う
            existing_total = int(existing_total_raw) if existing_total_raw is not None else 0
            
            # デバッグ用ログ（詳細版）
            log_error(f"ニデック数量チェック詳細（更新）: 既存生データ={existing_total_raw}(型:{type(existing_total_raw)}), 既存変換後={existing_total}, 新規={proc_qty}(型:{type(proc_qty)}), 合計={existing_total + proc_qty}")
            
            new_total = existing_total + proc_qty
            
            # 出庫データを取得して出庫数量を確認
            gshk_data = Gradation.get_gshk_by_id(gprc_data.GPRC_SHK_ID)
            if gshk_data:
                # 出庫数量を取得（数値変換を確実に行う）
                shipping_qty_raw = gshk_data.GSHK_QTY
                shipping_qty = int(shipping_qty_raw) if shipping_qty_raw is not None else 0
                
                if new_total > shipping_qty:
                    return jsonify({'success': False, 'error': f'加工数量の合計({new_total})が出庫数量({shipping_qty})を超えています（既存: {existing_total}, 新規: {proc_qty}）'}), 400
        
        # データベースを更新（合格数は自動計算値を使用）
        if Gradation.update_nidec_proc(gprc_id, proc_date, proc_qty, ret_ng_qty, ins_ng_qty, calculated_pass_qty, sts):
            return jsonify({'success': True, 'message': 'ニデック加工データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"ニデック加工データ更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/update_nidec_inspect/<int:gprc_id>', methods=['PUT'])
@login_required
def update_nidec_inspect(gprc_id):
    """ニデック加工データの検査情報を更新（検品不良数、合格数、ステータスのみ）"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        sts = data.get('sts', 0)
        
        try:
            # 数値変換
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            sts = int(sts) if sts else 0
            
            # 数値の妥当性チェック
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値の形式が正しくありません。'}), 400
        
        # 汎用メソッドを使用してデータベースを更新
        if Gradation.update_inspect(gprc_id, ins_ng_qty, pass_qty, sts):
            return jsonify({'success': True, 'message': '検査データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"ニデック検査データ更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/update_convex_inspect/<int:gprc_id>', methods=['PUT'])
@login_required
def update_convex_inspect(gprc_id):
    """コンベックス加工データの検査情報を更新（検品不良数、合格数、ステータスのみ）"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        sts = data.get('sts', 0)
        
        try:
            # 数値変換
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            sts = int(sts) if sts else 0
            
            # 数値の妥当性チェック
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値の形式が正しくありません。'}), 400
        
        # 汎用メソッドを使用してデータベースを更新
        if Gradation.update_inspect(gprc_id, ins_ng_qty, pass_qty, sts):
            return jsonify({'success': True, 'message': '検査データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"コンベックス検査データ更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/update_inspect/<int:gprc_id>', methods=['PUT'])
@login_required
def update_inspect(gprc_id):
    """加工データの検査情報を更新（汎用API）"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        ins_ng_qty = data.get('ins_ng_qty', 0)
        pass_qty = data.get('pass_qty', 0)
        sts = data.get('sts', 0)
        
        try:
            # 数値変換
            ins_ng_qty = int(ins_ng_qty) if ins_ng_qty else 0
            pass_qty = int(pass_qty) if pass_qty else 0
            sts = int(sts) if sts else 0
            
            # 数値の妥当性チェック
            if ins_ng_qty < 0:
                return jsonify({'success': False, 'error': '検品不良数は0以上の値を入力してください。'}), 400
            
        except ValueError:
            return jsonify({'success': False, 'error': '数値の形式が正しくありません。'}), 400
        
        # データベースを更新（合格数は自動計算値を使用）
        if Gradation.update_inspect(gprc_id, ins_ng_qty, pass_qty, sts):
            return jsonify({'success': True, 'message': '検査データを更新しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
            
    except Exception as e:
        log_error(f"検査データ更新エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの更新中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/delete_nidec_proc/<int:gprc_id>', methods=['DELETE'])
@login_required
def delete_nidec_proc(gprc_id):
    """ニデック加工データを削除"""
    try:
        if Gradation.delete_nidec_proc(gprc_id):
            return jsonify({'success': True, 'message': 'ニデック加工データを削除しました。'})
        else:
            return jsonify({'success': False, 'error': '指定されたデータが見つかりません。'}), 404
    except Exception as e:
        log_error(f"ニデック加工データ削除エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'データの削除中にエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/processing_matrix')
@login_required
def processing_matrix():
    """加工状況マトリックス表を表示"""
    try:
        matrix_data = Gradation.get_processing_matrix()
        
        return render_template('gradation/processing_matrix.html', 
                             spec_choices=matrix_data['spec_choices'],
                             color_choices=matrix_data['color_choices'],
                             convex_data=matrix_data['convex_data'],
                             nidec_data=matrix_data['nidec_data'])
        
    except Exception as e:
        log_error(f"加工状況マトリックス表示エラー: {str(e)}")
        flash('データの取得中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/auto_shipping')
@login_required
def auto_shipping():
    """自動出荷画面を表示"""
    try:
        # 検索用の選択肢を取得
        spec_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GSPEC, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        color_choices = Gradation.get_kbn_choices(KbnConstants.KBN_ID_GCOLOR, KbnConstants.CHOICE_PATTERN_UNSELECTED)
        
        return render_template('gradation/auto_shipping.html', 
                             spec_choices=spec_choices,
                             color_choices=color_choices)
        
    except Exception as e:
        log_error(f"自動出荷画面表示エラー: {str(e)}")
        flash('画面の表示中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/execute_auto_shipping', methods=['POST'])
@login_required
def execute_auto_shipping():
    """自動出荷を実行"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        spec = data.get('spec')
        color = data.get('color')
        qty = data.get('qty')
        order_date = data.get('order_date')
        shipping_date = data.get('shipping_date')
        
        # バリデーション
        if not spec:
            return jsonify({'success': False, 'error': '規格を選択してください。'}), 400
        if not color:
            return jsonify({'success': False, 'error': '色を選択してください。'}), 400
        if not qty:
            return jsonify({'success': False, 'error': '数量を入力してください。'}), 400
        if not order_date:
            return jsonify({'success': False, 'error': '手配日を入力してください。'}), 400
        if not shipping_date:
            return jsonify({'success': False, 'error': '出荷日を入力してください。'}), 400
        
        try:
            # 数値変換
            spec = int(spec)
            color = int(color)
            qty = int(qty)
            
            # 日付変換
            order_date = datetime.strptime(order_date, '%Y-%m-%d')
            shipping_date = datetime.strptime(shipping_date, '%Y-%m-%d')
            
            # 数値の妥当性チェック
            if qty <= 0:
                return jsonify({'success': False, 'error': '数量は0より大きい値を入力してください。'}), 400
            
            # 自動出荷を実行
            result = Gradation.execute_auto_shipping(spec, color, qty, order_date, shipping_date)
            
            return jsonify(result)
            
        except ValueError as e:
            return jsonify({'success': False, 'error': f'数値変換エラー: {str(e)}'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': f'処理中にエラーが発生しました: {str(e)}'}), 400
            
    except Exception as e:
        log_error(f"自動出荷実行エラー: {str(e)}")
        return jsonify({'success': False, 'error': f'システムエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/check_available_shipping_qty', methods=['POST'])
@login_required
def check_available_shipping_qty():
    """出荷可能数量をチェック"""
    try:
        data = request.get_json()
        
        # フォームデータを取得
        spec = data.get('spec')
        color = data.get('color')
        
        # バリデーション
        if not spec:
            return jsonify({'success': False, 'error': '規格を選択してください。'}), 400
        if not color:
            return jsonify({'success': False, 'error': '色を選択してください。'}), 400
        
        try:
            # 数値変換
            spec = int(spec)
            color = int(color)
            
            # 出荷可能数量を取得
            result = Gradation.get_available_shipping_qty(spec, color)
            
            return jsonify({
                'success': True,
                'total_available_qty': result['total_available_qty'],
                'record_count': result['record_count']
            })
            
        except ValueError as e:
            return jsonify({'success': False, 'error': f'数値変換エラー: {str(e)}'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': f'処理中にエラーが発生しました: {str(e)}'}), 400
            
    except Exception as e:
        log_error(f"出荷可能数量チェックエラー: {str(e)}")
        return jsonify({'success': False, 'error': f'システムエラーが発生しました: {str(e)}'}), 500

@app.route('/gradation/migration_execute')
@login_required
def migration_execute():
    """グラデーションデータ移行実行画面を表示"""
    try:
        return render_template('gradation/migration_execute.html')
        
    except Exception as e:
        log_error(f"移行実行画面表示エラー: {str(e)}")
        flash('画面の表示中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

@app.route('/gradation/execute_migration', methods=['POST'])
@login_required
def execute_gradation_migration():
    """グラデーションデータ移行処理を実行"""
    try:
        from app.gradation_migration import GradationMigration
        
        # 移行処理を実行
        migration = GradationMigration()
        migration.run_migration()
        
        # 結果を取得
        result = {
            'success': True,
            'success_count': migration.success_count,
            'error_count': migration.error_count,
            'failed_records_count': len(migration.failed_records)
        }
        
        # 失敗レコードがある場合は内容を追加
        if migration.failed_records:
            result['failed_records_content'] = migration.get_failed_records_content()
        
        return jsonify(result)
        
    except Exception as e:
        log_error(f"移行処理実行エラー: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'移行処理の実行中にエラーが発生しました: {str(e)}'
        }), 500 