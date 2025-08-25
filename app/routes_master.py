from flask import render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import traceback
import logging

from app import app
from app.models_master import KbnMstModel, CprcMstModel, CztrMstModel, PrdMstModel, CbcdMstModel
from app.database import get_db_session
from app.constants import DatabaseConstants, KbnConstants
from app.auth import login_required

@app.route('/master/kbn/list')
@login_required
def master_kbn_list():
    """区分マスタ一覧画面を表示"""
    try:
        # 検索条件を取得
        kbn_id = request.args.get('kbn_id', '').strip()
        only_active = request.args.get('only_active') == '1'
        
        # 区分IDの選択肢を取得（既存の区分IDを動的に取得）
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT KBN_ID 
                FROM KBN_MST 
                ORDER BY KBN_ID
            """)
            kbn_id_choices = session.execute(sql).fetchall()
            kbn_id_choices = [('', '全て')] + [(row.KBN_ID, row.KBN_ID) for row in kbn_id_choices]
        finally:
            session.close()
        
        # データを取得
        if kbn_id:
            kbn_list = KbnMstModel.get_kbn_list(kbn_id, only_active)
        else:
            # 全区分を取得
            session = get_db_session()
            try:
                sql = text("""
                    SELECT 
                        KBN_ID,
                        KBN_NO,
                        KBN_NM,
                        KBN_FLG
                    FROM KBN_MST 
                    WHERE 1=1
                """)
                params = {}
                
                if only_active:
                    sql = text(str(sql) + " AND KBN_FLG = :kbn_flg")
                    params['kbn_flg'] = KbnConstants.KBN_FLG_ACTIVE
                
                sql = text(str(sql) + " ORDER BY KBN_ID, KBN_NO")
                
                results = session.execute(sql, params).fetchall()
                kbn_list = []
                for r in results:
                    kbn_data = {
                        'KBN_ID': r.KBN_ID,
                        'KBN_NO': r.KBN_NO,
                        'KBN_NM': r.KBN_NM,
                        'KBN_FLG': r.KBN_FLG
                    }
                    kbn_list.append(kbn_data)
            finally:
                session.close()
        
        return render_template('master/kbn_list.html', 
                             kbn_list=kbn_list, 
                             kbn_id_choices=kbn_id_choices,
                             search_kbn_id=kbn_id,
                             only_active=only_active)
    except Exception as e:
        flash(f'区分マスタ一覧の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/master/kbn/create', methods=['GET', 'POST'])
@login_required
def master_kbn_create():
    """区分マスタ新規作成画面"""
    if request.method == 'POST':
        try:
            # フォームデータを取得
            kbn_id = request.form.get('kbn_id', '').strip()
            kbn_no = request.form.get('kbn_no', '').strip()
            kbn_nm = request.form.get('kbn_nm', '').strip()
            
            # バリデーション
            if not kbn_id:
                flash('区分IDは必須です。', 'error')
                return render_template('master/kbn_form.html')
            
            if not kbn_no:
                flash('区分番号は必須です。', 'error')
                return render_template('master/kbn_form.html')
            
            if not kbn_nm:
                flash('区分名は必須です。', 'error')
                return render_template('master/kbn_form.html')
            
            # 数値変換
            try:
                kbn_no = int(kbn_no)
            except ValueError:
                flash('区分番号は数値で入力してください。', 'error')
                return render_template('master/kbn_form.html')
            
            # 重複チェック
            session = get_db_session()
            try:
                existing = session.execute(
                    text("SELECT COUNT(*) as cnt FROM KBN_MST WHERE KBN_ID = :kbn_id AND KBN_NO = :kbn_no"),
                    {'kbn_id': kbn_id, 'kbn_no': kbn_no}
                ).first()
                
                if existing.cnt > 0:
                    flash('同じ区分IDと区分番号の組み合わせは既に存在します。', 'error')
                    return render_template('master/kbn_form.html')
                
                # データを挿入（フラグはデフォルト0）
                session.execute(
                    text("""
                        INSERT INTO KBN_MST (KBN_ID, KBN_NO, KBN_NM, KBN_FLG)
                        VALUES (:kbn_id, :kbn_no, :kbn_nm, :kbn_flg)
                    """),
                    {
                        'kbn_id': kbn_id,
                        'kbn_no': kbn_no,
                        'kbn_nm': kbn_nm,
                        'kbn_flg': DatabaseConstants.FLG_ACTIVE
                    }
                )
                session.commit()
                flash('区分マスタを登録しました。', 'success')
                return redirect(url_for('master_kbn_list'))
                
            finally:
                session.close()
                
        except SQLAlchemyError as e:
            flash(f'データベースエラーが発生しました: {str(e)}', 'error')
            return render_template('master/kbn_form.html')
        except Exception as e:
            flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
            return render_template('master/kbn_form.html')
    
    # GETリクエストの場合
    return render_template('master/kbn_form.html')

@app.route('/master/kbn/<kbn_id>/<int:kbn_no>/edit', methods=['GET', 'POST'])
@login_required
def master_kbn_edit(kbn_id, kbn_no):
    """区分マスタ編集画面"""
    session = get_db_session()
    try:
        if request.method == 'POST':
            # フォームデータを取得
            kbn_nm = request.form.get('kbn_nm', '').strip()
            
            # バリデーション
            if not kbn_nm:
                flash('区分名は必須です。', 'error')
                return redirect(url_for('master_kbn_edit', kbn_id=kbn_id, kbn_no=kbn_no))
            
            # データを更新（フラグは変更しない）
            session.execute(
                text("""
                    UPDATE KBN_MST 
                    SET KBN_NM = :kbn_nm
                    WHERE KBN_ID = :kbn_id AND KBN_NO = :kbn_no
                """),
                {
                    'kbn_id': kbn_id,
                    'kbn_no': kbn_no,
                    'kbn_nm': kbn_nm
                }
            )
            session.commit()
            flash('区分マスタを更新しました。', 'success')
            return redirect(url_for('master_kbn_list'))
        
        # GETリクエストの場合、データを取得
        result = session.execute(
            text("""
                SELECT KBN_ID, KBN_NO, KBN_NM, KBN_FLG
                FROM KBN_MST 
                WHERE KBN_ID = :kbn_id AND KBN_NO = :kbn_no
            """),
            {'kbn_id': kbn_id, 'kbn_no': kbn_no}
        ).first()
        
        if not result:
            flash('指定された区分マスタが見つかりません。', 'error')
            return redirect(url_for('master_kbn_list'))
        
        kbn_data = {
            'KBN_ID': result.KBN_ID,
            'KBN_NO': result.KBN_NO,
            'KBN_NM': result.KBN_NM,
            'KBN_FLG': result.KBN_FLG
        }
        
        return render_template('master/kbn_form.html', kbn=kbn_data)
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_kbn_list'))
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_kbn_list'))
    finally:
        session.close()

@app.route('/master/kbn/<kbn_id>/<int:kbn_no>/delete', methods=['POST'])
@login_required
def master_kbn_delete(kbn_id, kbn_no):
    """区分マスタ論理削除処理"""
    session = get_db_session()
    try:
        # データの存在確認
        result = session.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM KBN_MST 
                WHERE KBN_ID = :kbn_id AND KBN_NO = :kbn_no
            """),
            {'kbn_id': kbn_id, 'kbn_no': kbn_no}
        ).first()
        
        if result.cnt == 0:
            flash('指定された区分マスタが見つかりません。', 'error')
            return redirect(url_for('master_kbn_list'))
        
        # 論理削除（フラグを9に更新）
        session.execute(
            text("""
                UPDATE KBN_MST 
                SET KBN_FLG = :kbn_flg
                WHERE KBN_ID = :kbn_id AND KBN_NO = :kbn_no
            """),
            {
                'kbn_id': kbn_id, 
                'kbn_no': kbn_no,
                'kbn_flg': DatabaseConstants.FLG_DELETED
            }
        )
        session.commit()
        flash('区分マスタを削除しました。', 'success')
        
    except SQLAlchemyError as e:
        session.rollback()
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
    except Exception as e:
        session.rollback()
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
    finally:
        session.close()
    
    return redirect(url_for('master_kbn_list'))

@app.route('/api/master/kbn/<kbn_id>', methods=['GET'])
@login_required
def api_master_kbn_list(kbn_id):
    """区分マスタのAPI（JSON形式で返す）"""
    try:
        only_active = request.args.get('only_active', 'true').lower() == 'true'
        kbn_list = KbnMstModel.get_kbn_list(kbn_id, only_active)
        return jsonify({'success': True, 'data': kbn_list})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# =============================================================================
# 加工マスタ関連ルート
# =============================================================================

@app.route('/master/cprc/list')
@login_required
def master_cprc_list():
    """加工マスタ一覧画面を表示"""
    try:
        # 検索条件を取得
        prd_id = request.args.get('prd_id', '').strip()
        only_active = request.args.get('only_active') == '1'
        cprc_nm = request.args.get('cprc_nm', '').strip()
        cprc_prd_nm = request.args.get('cprc_prd_nm', '').strip()
        
        # 製品IDの選択肢を取得
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT CPRC_PRD_ID 
                FROM CPRC_MST 
                WHERE CPRC_PRD_ID IS NOT NULL
                ORDER BY CPRC_PRD_ID
            """)
            prd_id_choices = session.execute(sql).fetchall()
            prd_id_choices = [('', '全て')] + [(row.CPRC_PRD_ID, row.CPRC_PRD_ID) for row in prd_id_choices]
        finally:
            session.close()
        
        # データを取得
        if prd_id:
            cprc_list = CprcMstModel.get_cprc_list_by_prd_id(prd_id)
        else:
            # 全データを取得（フラグ条件はモデル内で処理）
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
                    WHERE 1=1
                """)
                params = {}
                
                # 部分一致検索条件を追加
                if cprc_nm:
                    sql = text(str(sql) + " AND CPRC_NM LIKE :cprc_nm")
                    params['cprc_nm'] = f'%{cprc_nm}%'
                
                if cprc_prd_nm:
                    sql = text(str(sql) + " AND CPRC_PRD_NM LIKE :cprc_prd_nm")
                    params['cprc_prd_nm'] = f'%{cprc_prd_nm}%'
                
                if only_active:
                    sql = text(str(sql) + " AND CPRC_FLG = :cprc_flg")
                    params['cprc_flg'] = DatabaseConstants.FLG_ACTIVE
                
                sql = text(str(sql) + " ORDER BY CPRC_ID")
                
                results = session.execute(sql, params).fetchall()
                cprc_list = []
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
                    cprc_list.append(cprc_data)
            finally:
                session.close()
        
        return render_template('master/cprc_list.html', 
                             cprc_list=cprc_list, 
                             prd_id_choices=prd_id_choices,
                             search_prd_id=prd_id,
                             only_active=only_active,
                             search_cprc_nm=cprc_nm,
                             search_cprc_prd_nm=cprc_prd_nm)
    except Exception as e:
        flash(f'加工マスタ一覧の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/master/cprc/create', methods=['GET', 'POST'])
@login_required
def master_cprc_create():
    """加工マスタ新規作成画面"""
    if request.method == 'POST':
        try:
            # フォームデータを取得
            cprc_nm = request.form.get('cprc_nm', '').strip()
            cprc_prd_nm = request.form.get('cprc_prd_nm', '').strip()
            cprc_to = request.form.get('cprc_to', '').strip()
            cprc_time = request.form.get('cprc_time', '').strip()
            cprc_prd_id = request.form.get('cprc_prd_id', '').strip()
            cprc_af_prd_id = request.form.get('cprc_af_prd_id', '').strip()
            
            # バリデーション
            if not cprc_nm:
                flash('加工名は必須です。', 'error')
                # マスタデータを取得して再表示
                cztr_list = CztrMstModel.get_process_company_list()
                prd_list = PrdMstModel.get_all()
                return render_template('master/cprc_form.html',
                                     cztr_list=cztr_list,
                                     prd_list=prd_list)
            
            if not cprc_prd_nm:
                flash('加工前製品名は必須です。', 'error')
                # マスタデータを取得して再表示
                cztr_list = CztrMstModel.get_process_company_list()
                prd_list = PrdMstModel.get_all()
                return render_template('master/cprc_form.html',
                                     cztr_list=cztr_list,
                                     prd_list=prd_list)
            
            # 数値変換
            try:
                cprc_to = int(cprc_to) if cprc_to else None
                cprc_time = int(cprc_time) if cprc_time else None
            except ValueError:
                flash('加工依頼先、加工日数は数値で入力してください。', 'error')
                # マスタデータを取得して再表示
                cztr_list = CztrMstModel.get_process_company_list()
                prd_list = PrdMstModel.get_all()
                return render_template('master/cprc_form.html',
                                     cztr_list=cztr_list,
                                     prd_list=prd_list)
            
            # データを挿入（フラグはデフォルト0）
            session = get_db_session()
            try:
                session.execute(
                    text("""
                        INSERT INTO CPRC_MST (CPRC_NM, CPRC_PRD_NM, CPRC_TO, CPRC_TIME, CPRC_FLG, CPRC_PRD_ID, CPRC_AF_PRD_ID)
                        VALUES (:cprc_nm, :cprc_prd_nm, :cprc_to, :cprc_time, :cprc_flg, :cprc_prd_id, :cprc_af_prd_id)
                    """),
                    {
                        'cprc_nm': cprc_nm,
                        'cprc_prd_nm': cprc_prd_nm,
                        'cprc_to': cprc_to,
                        'cprc_time': cprc_time,
                        'cprc_flg': DatabaseConstants.FLG_ACTIVE,
                        'cprc_prd_id': cprc_prd_id,
                        'cprc_af_prd_id': cprc_af_prd_id
                    }
                )
                session.commit()
                flash('加工マスタを登録しました。', 'success')
                return redirect(url_for('master_cprc_list'))
                
            finally:
                session.close()
                
        except SQLAlchemyError as e:
            flash(f'データベースエラーが発生しました: {str(e)}', 'error')
            # マスタデータを取得して再表示
            cztr_list = CztrMstModel.get_process_company_list()
            prd_list = PrdMstModel.get_all()
            return render_template('master/cprc_form.html',
                                 cztr_list=cztr_list,
                                 prd_list=prd_list)
        except Exception as e:
            flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
            # マスタデータを取得して再表示
            cztr_list = CztrMstModel.get_process_company_list()
            prd_list = PrdMstModel.get_all()
            return render_template('master/cprc_form.html',
                                 cztr_list=cztr_list,
                                 prd_list=prd_list)
    
    # GETリクエストの場合
    # マスタデータを取得
    cztr_list = CztrMstModel.get_process_company_list()
    prd_list = PrdMstModel.get_all()
    
    return render_template('master/cprc_form.html',
                         cztr_list=cztr_list,
                         prd_list=prd_list)

@app.route('/master/cprc/<int:cprc_id>/edit', methods=['GET', 'POST'])
@login_required
def master_cprc_edit(cprc_id):
    """加工マスタ編集画面"""
    session = get_db_session()
    try:
        if request.method == 'POST':
            # フォームデータを取得
            cprc_nm = request.form.get('cprc_nm', '').strip()
            cprc_prd_nm = request.form.get('cprc_prd_nm', '').strip()
            cprc_to = request.form.get('cprc_to', '').strip()
            cprc_time = request.form.get('cprc_time', '').strip()
            cprc_prd_id = request.form.get('cprc_prd_id', '').strip()
            cprc_af_prd_id = request.form.get('cprc_af_prd_id', '').strip()
            
            # 現在のデータを取得（エラー時の再表示用）
            current_result = session.execute(
                text("""
                    SELECT CPRC_ID, CPRC_NM, CPRC_PRD_NM, CPRC_TO, CPRC_TIME, 
                           CPRC_FLG, CPRC_PRD_ID, CPRC_AF_PRD_ID
                    FROM CPRC_MST 
                    WHERE CPRC_ID = :cprc_id
                """),
                {'cprc_id': cprc_id}
            ).first()
            
            if current_result:
                cprc_data = {
                    'CPRC_ID': current_result.CPRC_ID,
                    'CPRC_NM': current_result.CPRC_NM,
                    'CPRC_PRD_NM': current_result.CPRC_PRD_NM,
                    'CPRC_TO': current_result.CPRC_TO,
                    'CPRC_TIME': current_result.CPRC_TIME,
                    'CPRC_FLG': current_result.CPRC_FLG,
                    'CPRC_PRD_ID': current_result.CPRC_PRD_ID,
                    'CPRC_AF_PRD_ID': current_result.CPRC_AF_PRD_ID
                }
            else:
                flash('指定された加工マスタが見つかりません。', 'error')
                return redirect(url_for('master_cprc_list'))
            
            # バリデーション
            if not cprc_nm:
                flash('加工名は必須です。', 'error')
                # マスタデータを取得して再表示
                cztr_list = CztrMstModel.get_process_company_list()
                prd_list = PrdMstModel.get_all()
                return render_template('master/cprc_form.html',
                                     cprc=cprc_data,
                                     cztr_list=cztr_list,
                                     prd_list=prd_list)
            
            if not cprc_prd_nm:
                flash('加工前製品名は必須です。', 'error')
                # マスタデータを取得して再表示
                cztr_list = CztrMstModel.get_process_company_list()
                prd_list = PrdMstModel.get_all()
                return render_template('master/cprc_form.html',
                                     cprc=cprc_data,
                                     cztr_list=cztr_list,
                                     prd_list=prd_list)
            
            # 数値変換
            try:
                cprc_to = int(cprc_to) if cprc_to else None
                cprc_time = int(cprc_time) if cprc_time else None
            except ValueError:
                flash('加工依頼先、加工日数は数値で入力してください。', 'error')
                # マスタデータを取得して再表示
                cztr_list = CztrMstModel.get_process_company_list()
                prd_list = PrdMstModel.get_all()
                return render_template('master/cprc_form.html',
                                     cprc=cprc_data,
                                     cztr_list=cztr_list,
                                     prd_list=prd_list)
            
            # データを更新（フラグは変更しない）
            session.execute(
                text("""
                    UPDATE CPRC_MST 
                    SET CPRC_NM = :cprc_nm, CPRC_PRD_NM = :cprc_prd_nm, 
                        CPRC_TO = :cprc_to, CPRC_TIME = :cprc_time, 
                        CPRC_PRD_ID = :cprc_prd_id, 
                        CPRC_AF_PRD_ID = :cprc_af_prd_id
                    WHERE CPRC_ID = :cprc_id
                """),
                {
                    'cprc_id': cprc_id,
                    'cprc_nm': cprc_nm,
                    'cprc_prd_nm': cprc_prd_nm,
                    'cprc_to': cprc_to,
                    'cprc_time': cprc_time,
                    'cprc_prd_id': cprc_prd_id,
                    'cprc_af_prd_id': cprc_af_prd_id
                }
            )
            session.commit()
            flash('加工マスタを更新しました。', 'success')
            return redirect(url_for('master_cprc_list'))
        
        # GETリクエストの場合、データを取得
        result = session.execute(
            text("""
                SELECT CPRC_ID, CPRC_NM, CPRC_PRD_NM, CPRC_TO, CPRC_TIME, 
                       CPRC_FLG, CPRC_PRD_ID, CPRC_AF_PRD_ID
                FROM CPRC_MST 
                WHERE CPRC_ID = :cprc_id
            """),
            {'cprc_id': cprc_id}
        ).first()
        
        if not result:
            flash('指定された加工マスタが見つかりません。', 'error')
            return redirect(url_for('master_cprc_list'))
        
        cprc_data = {
            'CPRC_ID': result.CPRC_ID,
            'CPRC_NM': result.CPRC_NM,
            'CPRC_PRD_NM': result.CPRC_PRD_NM,
            'CPRC_TO': result.CPRC_TO,
            'CPRC_TIME': result.CPRC_TIME,
            'CPRC_FLG': result.CPRC_FLG,
            'CPRC_PRD_ID': result.CPRC_PRD_ID,
            'CPRC_AF_PRD_ID': result.CPRC_AF_PRD_ID
        }
        
        # マスタデータを取得
        cztr_list = CztrMstModel.get_process_company_list()
        prd_list = PrdMstModel.get_all()
        
        return render_template('master/cprc_form.html', cprc=cprc_data, cztr_list=cztr_list, prd_list=prd_list)
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_cprc_list'))
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_cprc_list'))
    finally:
        session.close()

@app.route('/master/cprc/<int:cprc_id>/delete', methods=['POST'])
@login_required
def master_cprc_delete(cprc_id):
    """加工マスタ削除処理"""
    session = get_db_session()
    try:
        # データの存在確認
        result = session.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM CPRC_MST 
                WHERE CPRC_ID = :cprc_id
            """),
            {'cprc_id': cprc_id}
        ).first()
        
        if result.cnt == 0:
            flash('指定された加工マスタが見つかりません。', 'error')
            return redirect(url_for('master_cprc_list'))
        
        # 論理削除（フラグを9に更新）
        session.execute(
            text("""
                UPDATE CPRC_MST 
                SET CPRC_FLG = :cprc_flg
                WHERE CPRC_ID = :cprc_id
            """),
            {
                'cprc_id': cprc_id,
                'cprc_flg': DatabaseConstants.FLG_DELETED
            }
        )
        session.commit()
        flash('加工マスタを削除しました。', 'success')
        
    except SQLAlchemyError as e:
        session.rollback()
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
    except Exception as e:
        session.rollback()
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
    finally:
        session.close()
    
    return redirect(url_for('master_cprc_list'))

@app.route('/api/master/cprc/<prd_id>', methods=['GET'])
@login_required
def api_master_cprc_list(prd_id):
    """加工マスタのAPI（JSON形式で返す）"""
    try:
        cprc_list = CprcMstModel.get_cprc_list_by_prd_id(prd_id)
        return jsonify({'success': True, 'data': cprc_list})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500 

@app.route('/master/cztr/list')
@login_required
def master_cztr_list():
    """取引先マスタ一覧画面を表示"""
    try:
        # 検索条件を取得
        cztr_kbn = request.args.get('cztr_kbn', '').strip()
        cztr_flg = request.args.get('cztr_flg')
        cztr_nm = request.args.get('cztr_nm', '').strip()
        cztr_full_nm = request.args.get('cztr_full_nm', '').strip()
        cztr_tanto_nm = request.args.get('cztr_tanto_nm', '').strip()
        
        # 取引先区分の選択肢を取得
        cztr_kbn_choices = [
            ('', '全て'),
            ('1', '得意先'),
            ('2', '加工会社'),
            ('3', 'その他')
        ]
        
        # データを取得
        session = get_db_session()
        try:
            sql = text("""
                SELECT CZTR_ID, CZTR_NM, CZTR_FULL_NM, CZTR_TANTO_NM, CZTR_KBN, CZTR_TYP, CZTR_FLG
                FROM CZTR_MST 
                WHERE 1=1
            """)
            params = {}
            
            if cztr_kbn:
                sql = text(str(sql) + " AND CZTR_KBN = :cztr_kbn")
                params['cztr_kbn'] = int(cztr_kbn)
            
            # 部分一致検索条件を追加
            if cztr_nm:
                sql = text(str(sql) + " AND CZTR_NM LIKE :cztr_nm")
                params['cztr_nm'] = f'%{cztr_nm}%'
            
            if cztr_full_nm:
                sql = text(str(sql) + " AND CZTR_FULL_NM LIKE :cztr_full_nm")
                params['cztr_full_nm'] = f'%{cztr_full_nm}%'
            
            if cztr_tanto_nm:
                sql = text(str(sql) + " AND CZTR_TANTO_NM LIKE :cztr_tanto_nm")
                params['cztr_tanto_nm'] = f'%{cztr_tanto_nm}%'
            
            # フラグで絞り込み（cztr_flgが'0'の場合のみ有効なもののみ表示）
            if cztr_flg == '0':
                sql = text(str(sql) + " AND CZTR_FLG = :cztr_flg")
                params['cztr_flg'] = DatabaseConstants.FLG_ACTIVE
            
            sql = text(str(sql) + " ORDER BY CZTR_ID")
            
            results = session.execute(sql, params).fetchall()
            cztr_list = []
            for r in results:
                cztr_data = {
                    'CZTR_ID': r.CZTR_ID,
                    'CZTR_NM': r.CZTR_NM,
                    'CZTR_FULL_NM': r.CZTR_FULL_NM,
                    'CZTR_TANTO_NM': r.CZTR_TANTO_NM,
                    'CZTR_KBN': r.CZTR_KBN,
                    'CZTR_TYP': r.CZTR_TYP,
                    'CZTR_FLG': r.CZTR_FLG
                }
                cztr_list.append(cztr_data)
        finally:
            session.close()
        
        return render_template('master/cztr_list.html', 
                             cztr_list=cztr_list, 
                             cztr_kbn_choices=cztr_kbn_choices,
                             search_cztr_kbn=cztr_kbn,
                             cztr_flg=cztr_flg,
                             search_cztr_nm=cztr_nm,
                             search_cztr_full_nm=cztr_full_nm,
                             search_cztr_tanto_nm=cztr_tanto_nm)
    except Exception as e:
        flash(f'取引先マスタ一覧の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/master/prd/list')
@login_required
def master_prd_list():
    """製品マスタ一覧画面を表示"""
    try:
        # 検索条件を取得
        prd_kbn = request.args.get('prd_kbn', '').strip()
        prd_flg = request.args.get('prd_flg')
        prd_name = request.args.get('prd_name', '').strip()
        prd_film_color = request.args.get('prd_film_color', '').strip()
        prd_monomer = request.args.get('prd_monomer', '').strip()
        
        # 商品分類の選択肢を取得
        prd_kbn_choices = [
            ('', '全て'),
            ('1', '製造レンズ'),
            ('2', 'BF'),
            ('3', '基材'),
            ('4', '加工レンズ'),
            ('5', 'その他')
        ]
        
        # データを取得
        session = get_db_session()
        try:
            sql = text("""
                SELECT PRD_ID, PRD_MONOMER, PRD_NAME, PRD_LOWER_DIE, PRD_UPPER_DIE,
                       PRD_FILM_COLOR, PRD_KBN, PRD_FLG, PRD_DSP_NM
                FROM PRD_MST 
                WHERE 1=1
            """)
            params = {}
            
            if prd_kbn:
                sql = text(str(sql) + " AND PRD_KBN = :prd_kbn")
                params['prd_kbn'] = int(prd_kbn)
            
            # 部分一致検索条件を追加
            if prd_name:
                sql = text(str(sql) + " AND PRD_NAME LIKE :prd_name")
                params['prd_name'] = f'%{prd_name}%'
            
            if prd_film_color:
                sql = text(str(sql) + " AND PRD_FILM_COLOR LIKE :prd_film_color")
                params['prd_film_color'] = f'%{prd_film_color}%'
            
            if prd_monomer:
                sql = text(str(sql) + " AND PRD_MONOMER LIKE :prd_monomer")
                params['prd_monomer'] = f'%{prd_monomer}%'
            
            # フラグで絞り込み（prd_flgが'0'の場合のみ有効なもののみ表示）
            if prd_flg == '0':
                sql = text(str(sql) + " AND PRD_FLG = :prd_flg")
                params['prd_flg'] = DatabaseConstants.FLG_ACTIVE
            
            sql = text(str(sql) + " ORDER BY PRD_ID")
            
            results = session.execute(sql, params).fetchall()
            prd_list = []
            for r in results:
                prd_data = {
                    'PRD_ID': r.PRD_ID,
                    'PRD_MONOMER': r.PRD_MONOMER,
                    'PRD_NAME': r.PRD_NAME,
                    'PRD_LOWER_DIE': r.PRD_LOWER_DIE,
                    'PRD_UPPER_DIE': r.PRD_UPPER_DIE,
                    'PRD_FILM_COLOR': r.PRD_FILM_COLOR,
                    'PRD_KBN': r.PRD_KBN,
                    'PRD_FLG': r.PRD_FLG,
                    'PRD_DSP_NM': r.PRD_DSP_NM
                }
                prd_list.append(prd_data)
        finally:
            session.close()
        
        return render_template('master/prd_list.html', 
                             prd_list=prd_list, 
                             prd_kbn_choices=prd_kbn_choices,
                             search_prd_kbn=prd_kbn,
                             prd_flg=prd_flg,
                             search_prd_name=prd_name,
                             search_prd_film_color=prd_film_color,
                             search_prd_monomer=prd_monomer)
    except Exception as e:
        flash(f'製品マスタ一覧の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/master/cbcd/list')
@login_required
def master_cbcd_list():
    """バーコードマスタ一覧画面を表示"""
    try:
        # 検索条件を取得
        prd_id = request.args.get('prd_id', '').strip()
        to_id = request.args.get('to_id', '').strip()
        
        # 製品IDの選択肢を取得
        session = get_db_session()
        try:
            sql = text("""
                SELECT DISTINCT CBCD_PRD_ID 
                FROM CBCD_MST 
                WHERE CBCD_PRD_ID IS NOT NULL
                ORDER BY CBCD_PRD_ID
            """)
            prd_id_choices = session.execute(sql).fetchall()
            prd_id_choices = [('', '全て')] + [(row.CBCD_PRD_ID, row.CBCD_PRD_ID) for row in prd_id_choices]
            
            # 出荷先IDの選択肢を取得
            sql = text("""
                SELECT DISTINCT CBCD_TO 
                FROM CBCD_MST 
                WHERE CBCD_TO IS NOT NULL
                ORDER BY CBCD_TO
            """)
            to_id_choices = session.execute(sql).fetchall()
            to_id_choices = [('', '全て')] + [(row.CBCD_TO, row.CBCD_TO) for row in to_id_choices]
        finally:
            session.close()
        
        # データを取得
        session = get_db_session()
        try:
            sql = text("""
                SELECT CBCD_ID, CBCD_PRD_ID, CBCD_TO, CBCD_NM, CBCD_NO1, CBCD_NO2
                FROM CBCD_MST
                WHERE 1=1
            """)
            params = {}
            
            if prd_id:
                sql = text(str(sql) + " AND CBCD_PRD_ID = :prd_id")
                params['prd_id'] = prd_id
            
            if to_id:
                sql = text(str(sql) + " AND CBCD_TO = :to_id")
                params['to_id'] = int(to_id)
            
            sql = text(str(sql) + " ORDER BY CBCD_ID")
            
            cbcd_list = session.execute(sql, params).fetchall()
        finally:
            session.close()
        
        return render_template('master/cbcd_list.html', 
                             cbcd_list=cbcd_list,
                             prd_id_choices=prd_id_choices,
                             to_id_choices=to_id_choices,
                             search_prd_id=prd_id,
                             search_to_id=to_id)
    except Exception as e:
        flash(f'バーコードマスタ一覧の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/master/cztr/create', methods=['GET', 'POST'])
@login_required
def master_cztr_create():
    """取引先マスタ新規作成画面"""
    if request.method == 'POST':
        try:
            # フォームデータを取得
            cztr_id = request.form.get('cztr_id', '').strip()
            cztr_nm = request.form.get('cztr_nm', '').strip()
            cztr_full_nm = request.form.get('cztr_full_nm', '').strip()
            cztr_tanto_nm = request.form.get('cztr_tanto_nm', '').strip()
            cztr_kbn = request.form.get('cztr_kbn', '').strip()
            cztr_typ = request.form.get('cztr_typ', '').strip()
            
            # バリデーション
            if not cztr_id:
                flash('取引先IDは必須です。', 'error')
                return render_template('master/cztr_form.html')
            
            if not cztr_nm:
                flash('取引先名は必須です。', 'error')
                return render_template('master/cztr_form.html')
            
            # 数値変換
            try:
                cztr_id = int(cztr_id)
                cztr_kbn = int(cztr_kbn) if cztr_kbn else None
                cztr_typ = int(cztr_typ) if cztr_typ else None
            except ValueError:
                flash('ID、区分、タイプは数値で入力してください。', 'error')
                return render_template('master/cztr_form.html')
            
            # データを挿入
            session = get_db_session()
            try:
                session.execute(
                    text("""
                        INSERT INTO CZTR_MST (CZTR_ID, CZTR_NM, CZTR_FULL_NM, CZTR_TANTO_NM, CZTR_KBN, CZTR_FLG, CZTR_TYP)
                        VALUES (:cztr_id, :cztr_nm, :cztr_full_nm, :cztr_tanto_nm, :cztr_kbn, :cztr_flg, :cztr_typ)
                    """),
                    {
                        'cztr_id': cztr_id,
                        'cztr_nm': cztr_nm,
                        'cztr_full_nm': cztr_full_nm,
                        'cztr_tanto_nm': cztr_tanto_nm,
                        'cztr_kbn': cztr_kbn,
                        'cztr_flg': DatabaseConstants.FLG_ACTIVE,
                        'cztr_typ': cztr_typ
                    }
                )
                session.commit()
                flash('取引先マスタを登録しました。', 'success')
                return redirect(url_for('master_cztr_list'))
                
            finally:
                session.close()
                
        except SQLAlchemyError as e:
            flash(f'データベースエラーが発生しました: {str(e)}', 'error')
            return render_template('master/cztr_form.html')
        except Exception as e:
            flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
            return render_template('master/cztr_form.html')
    
    # GETリクエストの場合
    return render_template('master/cztr_form.html')

@app.route('/master/cztr/<int:cztr_id>/edit', methods=['GET', 'POST'])
@login_required
def master_cztr_edit(cztr_id):
    """取引先マスタ編集画面"""
    session = get_db_session()
    try:
        if request.method == 'POST':
            # フォームデータを取得
            cztr_nm = request.form.get('cztr_nm', '').strip()
            cztr_full_nm = request.form.get('cztr_full_nm', '').strip()
            cztr_tanto_nm = request.form.get('cztr_tanto_nm', '').strip()
            cztr_kbn = request.form.get('cztr_kbn', '').strip()
            cztr_typ = request.form.get('cztr_typ', '').strip()
            
            # バリデーション
            if not cztr_nm:
                flash('取引先名は必須です。', 'error')
                return redirect(url_for('master_cztr_edit', cztr_id=cztr_id))
            
            # 数値変換
            try:
                cztr_kbn = int(cztr_kbn) if cztr_kbn else None
                cztr_typ = int(cztr_typ) if cztr_typ else None
            except ValueError:
                flash('区分、タイプは数値で入力してください。', 'error')
                return redirect(url_for('master_cztr_edit', cztr_id=cztr_id))
            
            # データを更新（フラグは変更しない）
            session.execute(
                text("""
                    UPDATE CZTR_MST 
                    SET CZTR_NM = :cztr_nm, CZTR_FULL_NM = :cztr_full_nm, 
                        CZTR_TANTO_NM = :cztr_tanto_nm, CZTR_KBN = :cztr_kbn, 
                        CZTR_TYP = :cztr_typ
                    WHERE CZTR_ID = :cztr_id
                """),
                {
                    'cztr_id': cztr_id,
                    'cztr_nm': cztr_nm,
                    'cztr_full_nm': cztr_full_nm,
                    'cztr_tanto_nm': cztr_tanto_nm,
                    'cztr_kbn': cztr_kbn,
                    'cztr_typ': cztr_typ
                }
            )
            session.commit()
            flash('取引先マスタを更新しました。', 'success')
            return redirect(url_for('master_cztr_list'))
        
        # GETリクエストの場合、データを取得
        result = session.execute(
            text("""
                SELECT CZTR_ID, CZTR_NM, CZTR_FULL_NM, CZTR_TANTO_NM, 
                       CZTR_KBN, CZTR_FLG, CZTR_TYP
                FROM CZTR_MST 
                WHERE CZTR_ID = :cztr_id
            """),
            {'cztr_id': cztr_id}
        ).first()
        
        if not result:
            flash('指定された取引先マスタが見つかりません。', 'error')
            return redirect(url_for('master_cztr_list'))
        
        cztr_data = {
            'CZTR_ID': result.CZTR_ID,
            'CZTR_NM': result.CZTR_NM,
            'CZTR_FULL_NM': result.CZTR_FULL_NM,
            'CZTR_TANTO_NM': result.CZTR_TANTO_NM,
            'CZTR_KBN': result.CZTR_KBN,
            'CZTR_FLG': result.CZTR_FLG,
            'CZTR_TYP': result.CZTR_TYP
        }
        
        return render_template('master/cztr_form.html', cztr=cztr_data)
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_cztr_list'))
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_cztr_list'))
    finally:
        session.close()

@app.route('/master/prd/create', methods=['GET', 'POST'])
@login_required
def master_prd_create():
    """製品マスタ新規作成画面"""
    if request.method == 'POST':
        try:
            # フォームデータを取得
            prd_id = request.form.get('prd_id', '').strip()
            prd_monomer = request.form.get('prd_monomer', '').strip()
            prd_name = request.form.get('prd_name', '').strip()
            prd_lower_die = request.form.get('prd_lower_die', '').strip()
            prd_upper_die = request.form.get('prd_upper_die', '').strip()
            prd_film_color = request.form.get('prd_film_color', '').strip()
            prd_kbn = request.form.get('prd_kbn', '').strip()
            prd_dsp_nm = request.form.get('prd_dsp_nm', '').strip()
            
            # バリデーション
            if not prd_id:
                flash('製品IDは必須です。', 'error')
                return render_template('master/prd_form.html')
            
            if not prd_name:
                flash('製品名は必須です。', 'error')
                return render_template('master/prd_form.html')
            
            # 数値変換
            try:
                prd_kbn = int(prd_kbn) if prd_kbn else None
            except ValueError:
                flash('商品分類は数値で入力してください。', 'error')
                return render_template('master/prd_form.html')
            
            # データを挿入
            session = get_db_session()
            try:
                session.execute(
                    text("""
                        INSERT INTO PRD_MST (PRD_ID, PRD_MONOMER, PRD_NAME, PRD_LOWER_DIE, 
                                            PRD_UPPER_DIE, PRD_FILM_COLOR, PRD_KBN, PRD_FLG, PRD_DSP_NM)
                        VALUES (:prd_id, :prd_monomer, :prd_name, :prd_lower_die, 
                                :prd_upper_die, :prd_film_color, :prd_kbn, :prd_flg, :prd_dsp_nm)
                    """),
                    {
                        'prd_id': prd_id,
                        'prd_monomer': prd_monomer,
                        'prd_name': prd_name,
                        'prd_lower_die': prd_lower_die,
                        'prd_upper_die': prd_upper_die,
                        'prd_film_color': prd_film_color,
                        'prd_kbn': prd_kbn,
                        'prd_flg': DatabaseConstants.FLG_ACTIVE,
                        'prd_dsp_nm': prd_dsp_nm
                    }
                )
                session.commit()
                flash('製品マスタを登録しました。', 'success')
                return redirect(url_for('master_prd_list'))
                
            finally:
                session.close()
                
        except SQLAlchemyError as e:
            flash(f'データベースエラーが発生しました: {str(e)}', 'error')
            return render_template('master/prd_form.html')
        except Exception as e:
            flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
            return render_template('master/prd_form.html')
    
    # GETリクエストの場合
    return render_template('master/prd_form.html')

@app.route('/master/prd/<prd_id>/edit', methods=['GET', 'POST'])
@login_required
def master_prd_edit(prd_id):
    """製品マスタ編集画面"""
    session = get_db_session()
    try:
        if request.method == 'POST':
            # フォームデータを取得
            prd_monomer = request.form.get('prd_monomer', '').strip()
            prd_name = request.form.get('prd_name', '').strip()
            prd_lower_die = request.form.get('prd_lower_die', '').strip()
            prd_upper_die = request.form.get('prd_upper_die', '').strip()
            prd_film_color = request.form.get('prd_film_color', '').strip()
            prd_kbn = request.form.get('prd_kbn', '').strip()
            prd_dsp_nm = request.form.get('prd_dsp_nm', '').strip()
            
            # バリデーション
            if not prd_name:
                flash('製品名は必須です。', 'error')
                return redirect(url_for('master_prd_edit', prd_id=prd_id))
            
            # 数値変換
            try:
                prd_kbn = int(prd_kbn) if prd_kbn else None
            except ValueError:
                flash('商品分類は数値で入力してください。', 'error')
                return redirect(url_for('master_prd_edit', prd_id=prd_id))
            
            # データを更新（フラグは変更しない）
            session.execute(
                text("""
                    UPDATE [PRD_MST] 
                    SET PRD_MONOMER = :prd_monomer, PRD_NAME = :prd_name, 
                        PRD_LOWER_DIE = :prd_lower_die, PRD_UPPER_DIE = :prd_upper_die,
                        PRD_FILM_COLOR = :prd_film_color, PRD_KBN = :prd_kbn, 
                        PRD_DSP_NM = :prd_dsp_nm
                    WHERE PRD_ID = :prd_id
                """),
                {
                    'prd_id': prd_id,
                    'prd_monomer': prd_monomer,
                    'prd_name': prd_name,
                    'prd_lower_die': prd_lower_die,
                    'prd_upper_die': prd_upper_die,
                    'prd_film_color': prd_film_color,
                    'prd_kbn': prd_kbn,
                    'prd_dsp_nm': prd_dsp_nm
                }
            )
            session.commit()
            flash('製品マスタを更新しました。', 'success')
            return redirect(url_for('master_prd_list'))
        
        # GETリクエストの場合、データを取得
        result = session.execute(
            text("""
                SELECT PRD_ID, PRD_MONOMER, PRD_NAME, PRD_LOWER_DIE, PRD_UPPER_DIE,
                       PRD_FILM_COLOR, PRD_KBN, PRD_FLG, PRD_DSP_NM
                FROM [PRD_MST] 
                WHERE PRD_ID = :prd_id
            """),
            {'prd_id': prd_id}
        ).first()
        
        if not result:
            flash('指定された製品マスタが見つかりません。', 'error')
            return redirect(url_for('master_prd_list'))
        
        prd_data = {
            'PRD_ID': result.PRD_ID,
            'PRD_MONOMER': result.PRD_MONOMER,
            'PRD_NAME': result.PRD_NAME,
            'PRD_LOWER_DIE': result.PRD_LOWER_DIE,
            'PRD_UPPER_DIE': result.PRD_UPPER_DIE,
            'PRD_FILM_COLOR': result.PRD_FILM_COLOR,
            'PRD_KBN': result.PRD_KBN,
            'PRD_FLG': result.PRD_FLG,
            'PRD_DSP_NM': result.PRD_DSP_NM
        }
        
        return render_template('master/prd_form.html', prd=prd_data)
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_prd_list'))
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_prd_list'))
    finally:
        session.close()

@app.route('/master/cbcd/create', methods=['GET', 'POST'])
@login_required
def master_cbcd_create():
    """バーコードマスタ新規作成画面"""
    if request.method == 'POST':
        try:
            # フォームデータを取得
            cbcd_prd_id = request.form.get('cbcd_prd_id', '').strip()
            cbcd_to = request.form.get('cbcd_to', '').strip()
            cbcd_nm = request.form.get('cbcd_nm', '').strip()
            cbcd_no1 = request.form.get('cbcd_no1', '').strip()
            cbcd_no2 = request.form.get('cbcd_no2', '').strip()
            
            # バリデーション
            if not cbcd_prd_id:
                flash('製品IDは必須です。', 'error')
                # マスタデータを取得して再表示
                prd_list = PrdMstModel.get_all()
                cztr_list = CztrMstModel.get_customer_list()
                return render_template('master/cbcd_form.html', prd_list=prd_list, cztr_list=cztr_list)
            
            if not cbcd_nm:
                flash('製品名は必須です。', 'error')
                # マスタデータを取得して再表示
                prd_list = PrdMstModel.get_all()
                cztr_list = CztrMstModel.get_customer_list()
                return render_template('master/cbcd_form.html', prd_list=prd_list, cztr_list=cztr_list)
            
            # 数値変換
            try:
                cbcd_to = int(cbcd_to) if cbcd_to else None
            except ValueError:
                flash('出荷先IDは数値で入力してください。', 'error')
                # マスタデータを取得して再表示
                prd_list = PrdMstModel.get_all()
                cztr_list = CztrMstModel.get_customer_list()
                return render_template('master/cbcd_form.html', prd_list=prd_list, cztr_list=cztr_list)
            
            # データを挿入
            session = get_db_session()
            try:
                session.execute(
                    text("""
                        INSERT INTO CBCD_MST (CBCD_PRD_ID, CBCD_TO, CBCD_NM, CBCD_NO1, CBCD_NO2)
                        VALUES (:cbcd_prd_id, :cbcd_to, :cbcd_nm, :cbcd_no1, :cbcd_no2)
                    """),
                    {
                        'cbcd_prd_id': cbcd_prd_id,
                        'cbcd_to': cbcd_to,
                        'cbcd_nm': cbcd_nm,
                        'cbcd_no1': cbcd_no1,
                        'cbcd_no2': cbcd_no2
                    }
                )
                session.commit()
                flash('バーコードマスタを登録しました。', 'success')
                return redirect(url_for('master_cbcd_list'))
                
            finally:
                session.close()
                
        except SQLAlchemyError as e:
            flash(f'データベースエラーが発生しました: {str(e)}', 'error')
            # マスタデータを取得して再表示
            prd_list = PrdMstModel.get_all()
            cztr_list = CztrMstModel.get_customer_list()
            return render_template('master/cbcd_form.html', prd_list=prd_list, cztr_list=cztr_list)
        except Exception as e:
            flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
            # マスタデータを取得して再表示
            prd_list = PrdMstModel.get_all()
            cztr_list = CztrMstModel.get_customer_list()
            return render_template('master/cbcd_form.html', prd_list=prd_list, cztr_list=cztr_list)
    
    # GETリクエストの場合
    # マスタデータを取得
    prd_list = PrdMstModel.get_all()
    cztr_list = CztrMstModel.get_customer_list()
    
    return render_template('master/cbcd_form.html', prd_list=prd_list, cztr_list=cztr_list)

@app.route('/master/cbcd/<int:cbcd_id>/edit', methods=['GET', 'POST'])
@login_required
def master_cbcd_edit(cbcd_id):
    """バーコードマスタ編集画面"""
    session = get_db_session()
    try:
        if request.method == 'POST':
            # フォームデータを取得
            cbcd_prd_id = request.form.get('cbcd_prd_id', '').strip()
            cbcd_to = request.form.get('cbcd_to', '').strip()
            cbcd_nm = request.form.get('cbcd_nm', '').strip()
            cbcd_no1 = request.form.get('cbcd_no1', '').strip()
            cbcd_no2 = request.form.get('cbcd_no2', '').strip()
            
            # 現在のデータを取得（エラー時の再表示用）
            current_result = session.execute(
                text("""
                    SELECT CBCD_ID, CBCD_PRD_ID, CBCD_TO, CBCD_NM, CBCD_NO1, CBCD_NO2
                    FROM CBCD_MST 
                    WHERE CBCD_ID = :cbcd_id
                """),
                {'cbcd_id': cbcd_id}
            ).first()
            
            if current_result:
                cbcd_data = {
                    'CBCD_ID': current_result.CBCD_ID,
                    'CBCD_PRD_ID': current_result.CBCD_PRD_ID,
                    'CBCD_TO': current_result.CBCD_TO,
                    'CBCD_NM': current_result.CBCD_NM,
                    'CBCD_NO1': current_result.CBCD_NO1,
                    'CBCD_NO2': current_result.CBCD_NO2
                }
            else:
                flash('指定されたバーコードマスタが見つかりません。', 'error')
                return redirect(url_for('master_cbcd_list'))
            
            # バリデーション
            if not cbcd_prd_id:
                flash('製品IDは必須です。', 'error')
                # マスタデータを取得して再表示
                prd_list = PrdMstModel.get_all()
                cztr_list = CztrMstModel.get_customer_list()
                return render_template('master/cbcd_form.html', cbcd=cbcd_data, prd_list=prd_list, cztr_list=cztr_list)
            
            if not cbcd_nm:
                flash('製品名は必須です。', 'error')
                # マスタデータを取得して再表示
                prd_list = PrdMstModel.get_all()
                cztr_list = CztrMstModel.get_customer_list()
                return render_template('master/cbcd_form.html', cbcd=cbcd_data, prd_list=prd_list, cztr_list=cztr_list)
            
            # 数値変換
            try:
                cbcd_to = int(cbcd_to) if cbcd_to else None
            except ValueError:
                flash('出荷先IDは数値で入力してください。', 'error')
                # マスタデータを取得して再表示
                prd_list = PrdMstModel.get_all()
                cztr_list = CztrMstModel.get_customer_list()
                return render_template('master/cbcd_form.html', cbcd=cbcd_data, prd_list=prd_list, cztr_list=cztr_list)
            
            # データを更新
            session.execute(
                text("""
                    UPDATE CBCD_MST 
                    SET CBCD_PRD_ID = :cbcd_prd_id, CBCD_TO = :cbcd_to, 
                        CBCD_NM = :cbcd_nm, CBCD_NO1 = :cbcd_no1, CBCD_NO2 = :cbcd_no2
                    WHERE CBCD_ID = :cbcd_id
                """),
                {
                    'cbcd_id': cbcd_id,
                    'cbcd_prd_id': cbcd_prd_id,
                    'cbcd_to': cbcd_to,
                    'cbcd_nm': cbcd_nm,
                    'cbcd_no1': cbcd_no1,
                    'cbcd_no2': cbcd_no2
                }
            )
            session.commit()
            flash('バーコードマスタを更新しました。', 'success')
            return redirect(url_for('master_cbcd_list'))
        
        # GETリクエストの場合、データを取得
        result = session.execute(
            text("""
                SELECT CBCD_ID, CBCD_PRD_ID, CBCD_TO, CBCD_NM, CBCD_NO1, CBCD_NO2
                FROM CBCD_MST 
                WHERE CBCD_ID = :cbcd_id
            """),
            {'cbcd_id': cbcd_id}
        ).first()
        
        if not result:
            flash('指定されたバーコードマスタが見つかりません。', 'error')
            return redirect(url_for('master_cbcd_list'))
        
        cbcd_data = {
            'CBCD_ID': result.CBCD_ID,
            'CBCD_PRD_ID': result.CBCD_PRD_ID,
            'CBCD_TO': result.CBCD_TO,
            'CBCD_NM': result.CBCD_NM,
            'CBCD_NO1': result.CBCD_NO1,
            'CBCD_NO2': result.CBCD_NO2
        }
        
        # マスタデータを取得
        prd_list = PrdMstModel.get_all()
        cztr_list = CztrMstModel.get_customer_list()
        
        return render_template('master/cbcd_form.html', cbcd=cbcd_data, prd_list=prd_list, cztr_list=cztr_list)
        
    except SQLAlchemyError as e:
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_cbcd_list'))
    except Exception as e:
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('master_cbcd_list'))
    finally:
        session.close() 

@app.route('/master/cztr/<int:cztr_id>/delete', methods=['POST'])
@login_required
def master_cztr_delete(cztr_id):
    """取引先マスタ削除処理"""
    session = get_db_session()
    try:
        # データの存在確認
        result = session.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM CZTR_MST 
                WHERE CZTR_ID = :cztr_id
            """),
            {'cztr_id': cztr_id}
        ).first()
        
        if result.cnt == 0:
            flash('指定された取引先マスタが見つかりません。', 'error')
            return redirect(url_for('master_cztr_list'))
        
        # 論理削除（フラグを9に更新）
        session.execute(
            text("""
                UPDATE CZTR_MST 
                SET CZTR_FLG = :cztr_flg
                WHERE CZTR_ID = :cztr_id
            """),
            {
                'cztr_id': cztr_id,
                'cztr_flg': DatabaseConstants.FLG_DELETED
            }
        )
        session.commit()
        flash('取引先マスタを削除しました。', 'success')
        
    except SQLAlchemyError as e:
        session.rollback()
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
    except Exception as e:
        session.rollback()
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
    finally:
        session.close()
    
    return redirect(url_for('master_cztr_list')) 

@app.route('/master/prd/<prd_id>/delete', methods=['POST'])
@login_required
def master_prd_delete(prd_id):
    """製品マスタ削除処理"""
    session = get_db_session()
    try:
        # データの存在確認
        result = session.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM PRD_MST 
                WHERE PRD_ID = :prd_id
            """),
            {'prd_id': prd_id}
        ).first()
        
        if result.cnt == 0:
            flash('指定された製品マスタが見つかりません。', 'error')
            return redirect(url_for('master_prd_list'))
        
        # 論理削除（フラグを9に更新）
        session.execute(
            text("""
                UPDATE PRD_MST 
                SET PRD_FLG = :prd_flg
                WHERE PRD_ID = :prd_id
            """),
            {
                'prd_id': prd_id,
                'prd_flg': DatabaseConstants.FLG_DELETED
            }
        )
        session.commit()
        flash('製品マスタを削除しました。', 'success')
        
    except SQLAlchemyError as e:
        session.rollback()
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
    except Exception as e:
        session.rollback()
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
    finally:
        session.close()
    
    return redirect(url_for('master_prd_list')) 

@app.route('/master/cbcd/<int:cbcd_id>/delete', methods=['POST'])
@login_required
def master_cbcd_delete(cbcd_id):
    """バーコードマスタ削除処理"""
    session = get_db_session()
    try:
        # データの存在確認
        result = session.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM CBCD_MST 
                WHERE CBCD_ID = :cbcd_id
            """),
            {'cbcd_id': cbcd_id}
        ).first()
        
        if result.cnt == 0:
            flash('指定されたバーコードマスタが見つかりません。', 'error')
            return redirect(url_for('master_cbcd_list'))
        
        # 論理削除（フラグを9に更新）
        session.execute(
            text("""
                UPDATE CBCD_MST 
                SET CBCD_FLG = :cbcd_flg
                WHERE CBCD_ID = :cbcd_id
            """),
            {
                'cbcd_id': cbcd_id,
                'cbcd_flg': DatabaseConstants.FLG_DELETED
            }
        )
        session.commit()
        flash('バーコードマスタを削除しました。', 'success')
        
    except SQLAlchemyError as e:
        session.rollback()
        flash(f'データベースエラーが発生しました: {str(e)}', 'error')
    except Exception as e:
        session.rollback()
        flash(f'予期せぬエラーが発生しました: {str(e)}', 'error')
    finally:
        session.close()
    
    return redirect(url_for('master_cbcd_list')) 