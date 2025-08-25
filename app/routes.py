from flask import render_template, request, redirect, url_for, flash, jsonify, current_app, make_response, send_file
from app import app
from app.constants import DatabaseConstants
from app.import_csv import import_csv_nonecoat, import_from_barcode
from app.import_excel import import_excel_hardcoat
from app.models import BfspMst, PrdDat, log_error, BfspMstModel, BrcpDat, BprdMei, BprdMeiModel, PrdDatModel, BBcdDat
from app.database import get_db_session
from sqlalchemy import text, cast, Integer
from datetime import datetime
from app.forms import NoncoatStockSearchForm, ShipmentSearchForm, ProcOrderSearchForm, HardcoatStockForm, BrcpSearchForm
from app.shipment import Shipment
from app.export_excel import write_to_proc_excel
from app.export_pdf import shipment_export_pdf, noncoat_stock_export_pdf, hardcoat_stock_export_pdf
from urllib.parse import quote
import traceback
import tempfile
import os
import shutil
from app.auth import login_required
from app.barcode_saver import ShipmentBarcodeSaver
from app.barcode_generator import BarcodeGenerator
from werkzeug.utils import secure_filename
        

@app.route('/')
@login_required
def index():
    """トップページを表示する"""
    try:
        # 受注残データを取得
        order_summary = BrcpDat.get_order_summary()
        
        
        return render_template('index.html', order_summary=order_summary)
    except Exception as e:
        return str(e), 500

@app.route('/prd_dat/import_csv', methods=['GET', 'POST'])
@login_required
def import_csv():
    """CSV取り込み処理画面"""
    if request.method == 'POST':
        try:
            # ファイルがアップロードされているか確認
            if 'csv_file' not in request.files:
                flash('ファイルが選択されていません', 'error')
                return redirect(request.url)
                
            file = request.files['csv_file']
            
            # ファイル名が空でないか確認
            if file.filename == '':
                flash('ファイルが選択されていません', 'error')
                return redirect(request.url)
                
            # ファイルがCSVかどうか確認
            if not file.filename.lower().endswith('.csv'):
                flash('CSVファイルを選択してください（.csv拡張子のファイル）', 'error')
                return redirect(request.url)
            
            # 一時ファイルに保存
            
            temp_dir = tempfile.gettempdir()
            temp_path = os.path.join(temp_dir, file.filename)
            try:
                file.save(temp_path)
            except Exception as e:
                flash(f'ファイル保存エラー: {str(e)}', 'error')
                return redirect(request.url)
            
            # ファイルサイズを確認
            file_size = os.path.getsize(temp_path)
            if file_size == 0:
                os.remove(temp_path)
                flash('ファイルが空です', 'error')
                return redirect(request.url)
            
            # ヘッダーの有無を取得
            has_header = 'has_header' in request.form
            
            # CSV取り込み処理
            try:
                result = import_csv_nonecoat(temp_path, has_header)
            except Exception as e:
                tb = traceback.format_exc()
                log_error(f"CSV取り込み処理でエラーが発生しました: {str(e)}\n{tb}")
                flash(f'CSV取り込み処理エラー: {str(e)}', 'error')
                # 一時ファイルを削除
                os.remove(temp_path)
                return redirect(request.url)
            
            # 一時ファイルを削除
            os.remove(temp_path)
            
            # 結果を表示
            if result.get("success", 0) > 0:
                flash(f"CSV取り込み完了: 合計{result.get('total', 0)}行、"
                    f"成功{result.get('success', 0)}行、"
                    f"スキップ{result.get('skipped', 0)}行、重複{result.get('duplicate', 0)}行、エラー{result.get('error', 0)}行", 'success')
            else:
                flash(f"CSV取り込み結果: 合計{result.get('total', 0)}行、"
                    f"成功{result.get('success', 0)}行、"
                    f"スキップ{result.get('skipped', 0)}行、重複{result.get('duplicate', 0)}行、エラー{result.get('error', 0)}行", 'warning')
            
            # エラーがあれば表示 (最大5件まで)
            if result['errors']:
                for i, error in enumerate(result['errors'][:5]):
                    flash(f"{error}", 'error')
                if len(result['errors']) > 5:
                    flash(f"その他 {len(result['errors']) - 5} 件のエラーがあります", 'warning')
            
            return redirect(request.url)
                
        except Exception as e:
            tb = traceback.format_exc()
            log_error(f"CSV取り込み画面でエラーが発生しました: {str(e)}\n{tb}")
            flash(f'エラーが発生しました: {str(e)}', 'error')
            return redirect(request.url)
            
    # GETリクエストの場合、アップロードフォームを表示
    return render_template('import_csv.html')

@app.route('/bprd_mei/<int:mei_id>/edit', methods=['GET'])
@login_required
def edit_bprd_mei(mei_id):
    """製造明細の編集画面を表示"""
    session = get_db_session()
    try:
        mei = session.query(BprdMeiModel).get(mei_id)
        if not mei:
            flash('指定された製造明細が見つかりません。', 'error')
            return redirect(url_for('list_bprd_mei'))
        return render_template('bprd_mei_edit.html', mei=mei)
    except Exception as e:
        flash(f'製造明細の編集画面表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('list_bprd_mei'))
    finally:
        session.close()

@app.route('/bprd_mei/<int:mei_id>/update', methods=['POST'])
@login_required
def update_bprd_mei(mei_id):
    """製造明細の更新処理"""
    session = get_db_session()
    try:
        # トランザクション開始
        mei = session.query(BprdMeiModel).get(mei_id)
        if not mei:
            flash('指定された製造明細が見つかりません。', 'error')
            return redirect(url_for('list_bprd_mei'))

        # 新しい数量を取得
        new_qty = int(request.form.get('qty', 0))
        if new_qty < 0:
            flash('数量は0以上で入力してください。', 'error')
            return redirect(url_for('edit_bprd_mei', mei_id=mei_id))

        # 数量の差分を計算
        qty_diff = new_qty - mei.BPDM_QTY

        # PrdDatModelの更新
        prd_dat = session.query(PrdDatModel).filter(
            PrdDatModel.BPDD_PRD_ID == mei.BPDM_PRD_ID,
            PrdDatModel.BPDD_LOT == mei.BPDM_LOT
        ).first()

        if prd_dat:
            prd_dat.BPDD_QTY += qty_diff
            if prd_dat.BPDD_QTY < 0:
                flash('製造明細の変更により入庫データの合計数量が0未満になるため更新できません。', 'error')
                return redirect(url_for('edit_bprd_mei', mei_id=mei_id))

        # BprdMeiModelの更新
        mei.BPDM_QTY = new_qty

        # 変更をコミット
        session.commit()
        flash('製造明細を更新しました。', 'success')
        return redirect(url_for('list_bprd_mei'))

    except ValueError:
        flash('数量は正しい数値で入力してください。', 'error')
        return redirect(url_for('edit_bprd_mei', mei_id=mei_id))
    except Exception as e:
        session.rollback()
        flash(f'更新中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('edit_bprd_mei', mei_id=mei_id))
    finally:
        session.close()

@app.route('/bprd_mei/list')
@login_required
def list_bprd_mei():
    """製造明細の一覧を表示"""
    session = get_db_session()
    try:
        # 検索条件を取得
        prd_id = request.args.get('prd_id', '').strip()
        lot = request.args.get('lot', '').strip()

        # クエリを構築
        query = session.query(BprdMeiModel)

        # 検索条件を適用
        if prd_id:
            query = query.filter(BprdMeiModel.BPDM_PRD_ID.like(f'%{prd_id}%'))
        if lot:
            try:
                lot_num = int(lot)
                query = query.filter(BprdMeiModel.BPDM_LOT == lot_num)
            except ValueError:
                flash('ロットは数値で入力してください。', 'error')

        # 並び順を設定
        query = query.order_by(
            BprdMeiModel.BPDM_PRD_ID,
            BprdMeiModel.BPDM_LOT,
            BprdMeiModel.BPDM_NO
        )

        # データを取得
        mei_list = query.all()

        return render_template('bprd_mei_list.html', mei_list=mei_list)
    except Exception as e:
        flash(f'一覧の取得中にエラーが発生しました: {str(e)}', 'error')
        return render_template('bprd_mei_list.html', mei_list=[])
    finally:
        session.close()

@app.route('/bprd_mei/create', methods=['GET', 'POST'])
@login_required
def create_bprd_mei():
    """製造明細の新規作成"""
    session = get_db_session()
    try:
        if request.method == 'POST':
            # フォームデータを取得
            prd_id = request.form.get('prd_id')
            lot = request.form.get('lot')
            no = request.form.get('no')
            qty = request.form.get('qty')

            try:
                # モデルの新規作成メソッドを呼び出し
                BprdMei.create(prd_id, lot, no, qty)
                flash('製造明細を登録しました。', 'success')
                return redirect(url_for('list_bprd_mei'))
            except ValueError as e:
                flash(str(e), 'error')
                return redirect(url_for('create_bprd_mei'))
            except Exception as e:
                flash(f'製造明細の作成中にエラーが発生しました: {str(e)}', 'error')
                return redirect(url_for('create_bprd_mei'))

        else:
            # GET: 製品一覧を取得して新規作成フォームを表示
            prd_list = session.query(BfspMstModel).order_by(BfspMstModel.BFSP_SORT).all()
            return render_template('bprd_mei_create.html', prd_list=prd_list)

    except Exception as e:
        flash(f'製造明細の作成画面表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('list_bprd_mei'))
    finally:
        session.close()

@app.route('/noncoat-stock', methods=['GET', 'POST'])
@login_required
def noncoat_stock_search():
    form = NoncoatStockSearchForm()
    
    try:
        # 各フィールドの選択肢を設定
        form.base.choices = BfspMst.get_choices('BFSP_BASE')
        form.adp.choices = BfspMst.get_choices('BFSP_ADP')
        form.lr.choices = BfspMst.get_choices('BFSP_LR')
        form.clr.choices = BfspMst.get_choices('BFSP_CLR')

        stocks = []
        if request.method == 'POST':
            stocks = PrdDat.search_noncoat_stock(
                product_id=form.product_id.data,
                lot=form.lot.data,
                base=form.base.data,
                adp=form.adp.data,
                lr=form.lr.data,
                clr=form.clr.data
            )
            if not stocks:
                flash('検索条件に一致する在庫データが見つかりませんでした。', 'info')
        
        return render_template('noncoat_stock_search.html', form=form, stocks=stocks)
    except Exception as e:
        flash(f'在庫検索中にエラーが発生しました: {str(e)}', 'error')
        return render_template('noncoat_stock_search.html', form=form, stocks=[])
    
@app.route('/noncoat_stock_pdf')
@login_required
def noncoat_stock_pdf():
    """ノンコート在庫一覧をPDFで出力"""
    try:
        print("ノンコート在庫PDF出力処理を開始: /noncoat_stock_pdf")
        # フォームから検索条件を取得
        form = NoncoatStockSearchForm(request.args)
        
        print(f"検索条件: product_id={form.product_id.data}, lot={form.lot.data}, "
              f"base={form.base.data}, adp={form.adp.data}, lr={form.lr.data}, clr={form.clr.data}")
        
        # 在庫データを検索
        stocks = PrdDat.search_noncoat_stock(
            product_id=form.product_id.data,
            lot=form.lot.data,
            base=form.base.data,
            adp=form.adp.data,
            lr=form.lr.data,
            clr=form.clr.data
        )
        
        print(f"検索結果: {len(stocks)}件のデータを取得")

        if not stocks:
            flash('在庫データが見つかりません。', 'warning')
            return redirect(url_for('noncoat_stock_search'))

        # PDFを生成
        print("PDF生成開始")
        pdf = noncoat_stock_export_pdf(stocks)
        print("PDF生成完了")

        # PDFをダウンロード
        response = make_response(pdf)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=noncoat_stocks.pdf'
        print("レスポンス作成完了")
        return response

    except Exception as e:
        print(f"PDF出力エラー: {str(e)}")
        print(traceback.format_exc())
        flash(f'PDF出力中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('noncoat_stock_search'))

@app.route('/hardcoat_stock', methods=['GET', 'POST'])
@login_required
def hardcoat_stock_search():
    form = NoncoatStockSearchForm()
    
    try:
        # 各フィールドの選択肢を設定
        form.base.choices = BfspMst.get_choices('BFSP_BASE')
        form.adp.choices = BfspMst.get_choices('BFSP_ADP')
        form.lr.choices = BfspMst.get_choices('BFSP_LR')
        form.clr.choices = BfspMst.get_choices('BFSP_CLR')
        
        stocks = []
        if request.method == 'POST':
            stocks = PrdDat.search_hardcoat_stock(
                product_id=form.product_id.data,
                lot=form.lot.data,
                base=form.base.data,
                adp=form.adp.data,
                lr=form.lr.data,
                clr=form.clr.data
            )
            if not stocks:
                flash('検索条件に一致する在庫データが見つかりませんでした。', 'info')
        
        return render_template('hardcoat_stock_search.html', form=form, stocks=stocks)
    except Exception as e:
        flash(f'ハードコート在庫検索中にエラーが発生しました: {str(e)}', 'error')
        return render_template('hardcoat_stock_search.html', form=form, stocks=[])

@app.route('/hardcoat_stock_pdf')
@login_required
def hardcoat_stock_pdf():
    """ハードコート在庫一覧をPDFで出力"""
    try:
        print("ハードコート在庫PDF出力処理を開始: /hardcoat_stock_pdf")
        # フォームから検索条件を取得
        form = NoncoatStockSearchForm(request.args)
        
        print(f"検索条件: product_id={form.product_id.data}, lot={form.lot.data}, "
              f"base={form.base.data}, adp={form.adp.data}, lr={form.lr.data}, clr={form.clr.data}")
        
        # 在庫データを検索
        stocks = PrdDat.search_hardcoat_stock(
            product_id=form.product_id.data,
            lot=form.lot.data,
            base=form.base.data,
            adp=form.adp.data,
            lr=form.lr.data,
            clr=form.clr.data
        )
        
        print(f"検索結果: {len(stocks)}件のデータを取得")

        if not stocks:
            flash('在庫データが見つかりません。', 'warning')
            return redirect(url_for('hardcoat_stock_search'))

        # PDFを生成
        print("PDF生成開始")
        pdf = hardcoat_stock_export_pdf(stocks)
        print("PDF生成完了")

        # PDFをダウンロード
        response = make_response(pdf)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=hardcoat_stocks.pdf'
        print("レスポンス作成完了")
        return response

    except Exception as e:
        print(f"PDF出力エラー: {str(e)}")
        print(traceback.format_exc())
        flash(f'PDF出力中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('hardcoat_stock_search'))

@app.route('/shipping/create/<int:stock_id>', methods=['GET'])
@login_required
def create_shipping(stock_id):
    """出荷作成画面を表示"""
    try:
        # Shipmentモデルを使用して在庫情報を取得
        stock = Shipment.get_stock_info(stock_id)

        if not stock:
            flash('指定された在庫が見つかりません。', 'error')
            return redirect(url_for('noncoat_stock_search'))

        # Shipmentモデルを使用してノンコート受注残を取得
        nc_orders = Shipment.get_noncoat_orders(stock_id)
        
        # Shipmentモデルを使用してハードコート受注残を取得
        hc_orders = Shipment.get_hardcoat_orders(stock_id)
        
        proc_order = Shipment.get_proc_order(stock.BPDD_PRD_ID)
        
        # Shipmentモデルを使用して出荷先リストを取得
        shipping_destinations = Shipment.get_shipping_destinations()
        
        today = datetime.now().strftime('%Y-%m-%d')

        return render_template('shipping/create.html',
                             stock=stock,
                             nc_orders=nc_orders,
                             hc_orders=hc_orders,
                             shipping_destinations=shipping_destinations,
                             today=today,
                             proc_order=proc_order
                             )
    except Exception as e:
        flash(f'出荷作成画面の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('noncoat_stock_search'))

@app.route('/shipping/save/<int:stock_id>', methods=['POST'])
@login_required
def save_shipping(stock_id):
    """出荷データを保存"""
    try:
        data = request.get_json()
        
        if not data or 'shipments' not in data:
            return jsonify({'success': False, 'error': '無効なデータ形式です。'})
        
        # Shipmentモデルを使用して出荷データを保存
        result = Shipment.save(stock_id, data['shipments'], DatabaseConstants.PROC_NON_COAT)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/order_input')
@login_required
def order_input():
    """受注データ入力画面を表示"""
    session = get_db_session()
    try:
        # 取引先リストを取得
        cztr_list = session.execute(text("SELECT CZTR_ID, CZTR_NM FROM CZTR_MST WHERE CZTR_TYP = :cztr_type_bf ORDER BY CZTR_ID"), {'cztr_type_bf': DatabaseConstants.CZTR_TYPE_BF}).fetchall()
        
        # 製品リストを取得
        bfsp_list = session.execute(text("SELECT BFSP_PRD_ID, BFSP_BASE, BFSP_ADP, BFSP_LR, BFSP_CLR, BFSP_SORT FROM BFSP_MST ORDER BY BFSP_SORT")).fetchall()
        
        return render_template('order_input.html', 
                             cztr_list=cztr_list, 
                             bfsp_list=bfsp_list,
                             SHIPMENT_TO_PROCESS=DatabaseConstants.SHIPMENT_TO_PROCESS)
    except Exception as e:
        flash(f'受注データ入力画面の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))
    finally:
        session.close()

@app.route('/api/orders/search', methods=['POST'])
@login_required
def search_order():
    """受注データを検索"""
    try:
        data = request.get_json()
        result = BrcpDat.search(data)
        
        if 'error' in result:
            return jsonify({'error': result['error']}), 500
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/orders/save', methods=['POST'])
@login_required
def save_order():
    """受注データを保存"""
    try:
        data = request.get_json()
        result = BrcpDat.save(data)
        
        if 'error' in result:
            return jsonify({'error': result['error']}), 500
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/shipments', methods=['GET', 'POST'])
@login_required
def shipment_list():
    """出荷一覧画面を表示"""
    form = ShipmentSearchForm()
    
    # セレクトボックスの選択肢を設定
    form.base.choices = BfspMst.get_choices('BFSP_BASE')
    form.adp.choices = BfspMst.get_choices('BFSP_ADP')
    
    try:
        shipments = []
        if request.method == 'POST':
            # 検索条件を取得
            base = form.base.data
            adp = form.adp.data
            lr = form.lr.data
            color = form.color.data
            proc_type = form.proc_type.data
            shipment_date = form.shipment_date.data
            destination = form.destination.data
            order_no = form.order_no.data
            shipment_status = form.shipment_status.data
            order_date = form.order_date.data
            
            # 出荷データを検索
            shipments = Shipment.search(
                base=base,
                adp=adp,
                lr=lr,
                color=color,
                proc_type=proc_type,
                shipment_date=shipment_date.strftime('%Y-%m-%d') if shipment_date else None,
                destination=destination,
                order_no=order_no,
                shipment_status=shipment_status,
                order_date=order_date.strftime('%Y-%m-%d') if order_date else None
            )
            
            if not shipments:
                flash('検索条件に一致する出荷データが見つかりませんでした。', 'info')

        return render_template('shipment_list.html', form=form, shipments=shipments)
    except Exception as e:
        flash(f'出荷一覧の検索中にエラーが発生しました: {str(e)}', 'error')
        return render_template('shipment_list.html', form=form, shipments=[])
    
@app.route('/proc_order', methods=['GET', 'POST'])
@login_required
def proc_order():
    """加工手配画面を表示"""
    form = ProcOrderSearchForm()
    if request.method == 'POST':
        try:
            # フォームデータを取得
            shipment_date = request.form.get('shipment_date')
            print(f"受け取った出荷日: {shipment_date}")

            if not shipment_date:
                flash('出荷日を入力してください。', 'error')
                return render_template('proc_order.html', form=form, orders=[])

            # 日付文字列をdatetimeオブジェクトに変換
            try:
                shipment_date = datetime.strptime(shipment_date, '%Y-%m-%d')
                print(f"変換後の出荷日: {shipment_date}")
            except ValueError as e:
                flash('日付の形式が正しくありません。', 'error')
                return render_template('proc_order.html', form=form, orders=[])

            # 加工手配を取得
            orders = Shipment.get_by_dt_CT(shipment_date)
            print(f"取得した加工手配データ数: {len(orders) if orders else 0}")

            if not orders:
                flash('該当する出荷データが見つかりませんでした。', 'info')
                return render_template('proc_order.html', form=form, orders=[])

            return render_template('proc_order.html', form=form, orders=orders)

        except Exception as e:
            print(f"エラーが発生しました: {str(e)}")
            flash(f'加工手配の検索中にエラーが発生しました: {str(e)}', 'error')
            return render_template('proc_order.html', form=form, orders=[])

    return render_template('proc_order.html', form=form, orders=[])

@app.route('/shipment_pdf')
@login_required
def shipment_pdf():
    """出荷一覧をPDFで出力"""
    try:
        print("PDF出力処理を開始: /shipment_pdf")
        # フォームから検索条件を取得
        form = ShipmentSearchForm(request.args)
        
        print(f"検索条件: base={form.base.data}, adp={form.adp.data}, lr={form.lr.data}, "
              f"color={form.color.data}, proc_type={form.proc_type.data}, "
              f"shipment_date={form.shipment_date.data}, destination={form.destination.data}")
        
        # 出荷データを検索
        shipments = Shipment.search(
            base=form.base.data,
            adp=form.adp.data,
            lr=form.lr.data,
            color=form.color.data,
            proc_type=form.proc_type.data,
            shipment_date=form.shipment_date.data.strftime('%Y-%m-%d') if form.shipment_date.data else None,
            destination=form.destination.data,
            order_no=form.order_no.data,
            shipment_status=form.shipment_status.data,
            order_date=form.order_date.data.strftime('%Y-%m-%d') if form.order_date.data else None
        )
        
        print(f"検索結果: {len(shipments)}件のデータを取得")

        if not shipments:
            flash('出荷データが見つかりません。', 'warning')
            return redirect(url_for('shipment_list'))

        # PDFを生成
        print("PDF生成開始")
        pdf = shipment_export_pdf(
            shipments
        )
        print("PDF生成完了")

        # PDFをダウンロード
        response = make_response(pdf)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=shipments.pdf'
        print("レスポンス作成完了")
        return response

    except Exception as e:
        print(f"PDF出力エラー: {str(e)}")
        print(traceback.format_exc())
        flash(f'PDF出力中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('shipment_list'))

@app.route('/proc_order_export_excel')
@login_required
def proc_order_export_excel():
    """加工手配をExcelで出力"""
    temp_dir = None
    try:
        # フォームから検索条件を取得
        form = ProcOrderSearchForm(request.args)
        
        if not form.shipment_date.data:
            # JSONレスポンスでエラーを返す
            return jsonify({'success': False, 'error': '出荷日を入力してください。'}), 400

        # 日付データを取得
        shipment_date = form.shipment_date.data
        
        # 検索条件を適用
        orders = Shipment.get_by_dt_CT(shipment_date)
        
        if not orders:
            # JSONレスポンスでエラーを返す
            return jsonify({'success': False, 'error': '該当する出荷データが見つかりませんでした。'}), 404

        # 一時ディレクトリを作成
        temp_dir = tempfile.mkdtemp()
        output_path = write_to_proc_excel(shipment_date, orders, temp_dir)
        
        if not os.path.exists(output_path):
            # JSONレスポンスでエラーを返す
            return jsonify({'success': False, 'error': 'ファイルの作成に失敗しました。'}), 500

        # ファイル名をURLエンコード
        filename = f'coating_process_{shipment_date.strftime("%Y%m%d")}.xlsx'
        encoded_filename = quote(f'ハードコート指図_{shipment_date.strftime("%Y%m%d")}.xlsx')

        try:
            # ファイルを読み込んでメモリに保持
            with open(output_path, 'rb') as f:
                file_data = f.read()

            # レスポンスを作成
            response = make_response(file_data)
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f"attachment; filename={filename}; filename*=UTF-8''{encoded_filename}"
            return response

        except Exception as e:
            print(f"ファイル送信エラー: {str(e)}")
            # JSONレスポンスでエラーを返す
            return jsonify({'success': False, 'error': 'ファイルのダウンロードに失敗しました。'}), 500
    
    except Exception as e:
        print(f"Excelファイル作成エラー: {str(e)}")
        # JSONレスポンスでエラーを返す
        return jsonify({'success': False, 'error': f'Excelファイル作成中にエラーが発生しました: {str(e)}'}), 500
    
    finally:
        # 一時ディレクトリとファイルの削除
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"一時ディレクトリの削除に失敗: {str(e)}")

@app.route('/hardcoat_read_excel', methods=['GET'])
@login_required
def hardcoat_read_excel():
    """ハードコート在庫データ作成画面を表示"""
    form = HardcoatStockForm()
    return render_template('hardcoat_read_excel.html', form=form)

@app.route('/hardcoat_read_excel/import', methods=['POST'])
@login_required
def hardcoat_read_excel_import():
    """ハードコート在庫データのインポート処理"""
    try:
        if 'file' not in request.files:
            flash('ファイルが選択されていません', 'error')
            return redirect(url_for('hardcoat_read_excel'))
            
        file = request.files['file']
        if file.filename == '':
            flash('ファイルが選択されていません', 'error')
            return redirect(url_for('hardcoat_read_excel'))
            
        # ファイルの拡張子をチェック
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            flash('Excelファイルを選択してください', 'error')
            return redirect(url_for('hardcoat_read_excel'))
        
        return import_excel_hardcoat(file)
            
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"ハードコート在庫データのインポートでエラーが発生しました: {str(e)}\n{tb}")
        flash(f'Excelファイルのインポート中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('hardcoat_read_excel'))

@app.route('/shipping/create_hard/<int:stock_id>', methods=['GET'])
@login_required
def create_shipping_hard(stock_id):
    """ハードコート出荷作成画面を表示"""
    try:
        # Shipmentモデルを使用して在庫情報を取得
        stock = Shipment.get_stock_info(stock_id)

        if not stock:
            flash('指定された在庫が見つかりません。', 'error')
            return redirect(url_for('hardcoat_stock_search'))

        # Shipmentモデルを使用してハードコート受注残を取得
        hc_orders = Shipment.get_hardcoat_orders(stock_id)
        
        # Shipmentモデルを使用して出荷先リストを取得
        shipping_destinations = Shipment.get_shipping_destinations()
        
        today = datetime.now().strftime('%Y-%m-%d')

        return render_template('shipping/create_hard.html',
                             stock=stock,
                             hc_orders=hc_orders,
                             shipping_destinations=shipping_destinations,
                             today=today,
                             SHIPMENT_TO_PROCESS=DatabaseConstants.SHIPMENT_TO_PROCESS)
    except Exception as e:
        flash(f'出荷作成画面の表示中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('hardcoat_stock_search'))

@app.route('/shipping/save_hard/<int:stock_id>', methods=['POST'])
@login_required
def save_shipping_hard(stock_id):
    """出荷データを保存"""
    try:
        data = request.get_json()
        
        if not data or 'shipments' not in data:
            return jsonify({'success': False, 'error': '無効なデータ形式です。'})
        
        # Shipmentモデルを使用して出荷データを保存
        result = Shipment.save(stock_id, data['shipments'], DatabaseConstants.PROC_HARD_COAT)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/shipment/<int:shipment_id>', methods=['DELETE'])
@login_required
def delete_shipment(shipment_id):
    """出荷データを削除"""
    try:
        print(f"Deleting shipment ID: {shipment_id}")  # デバッグログ
        result = Shipment.delete_shipment(shipment_id)
        print(f"Delete result: {result}")  # デバッグログ
        return jsonify(result)
    except Exception as e:
        print(f"Error deleting shipment: {str(e)}")  # デバッグログ
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/shipment_barcode')
@login_required
def shipment_barcode():
    """出荷バーコードを保存"""
    try:
        # デバッグ情報をログに出力
        log_error(f"バーコード処理開始: request.args={dict(request.args)}")
        
        form = ShipmentSearchForm(request.args)
        
        # セレクトボックスの選択肢を設定
        form.base.choices = BfspMst.get_choices('BFSP_BASE')
        form.adp.choices = BfspMst.get_choices('BFSP_ADP')
        
        # デバッグ情報をログに出力
        log_error(f"バーコード処理: 検索条件 base={form.base.data}, adp={form.adp.data}, lr={form.lr.data}, color={form.color.data}, proc_type={form.proc_type.data}, shipment_date={form.shipment_date.data}, destination={form.destination.data}")
        
        # バーコード保存実行
        success, result = ShipmentBarcodeSaver.save_shipment_barcodes(
            base=form.base.data,
            adp=form.adp.data,
            lr=form.lr.data,
            color=form.color.data,
            proc_type=form.proc_type.data,
            shipment_date=form.shipment_date.data,
            destination=form.destination.data,
            order_no=form.order_no.data,
            shipment_status=form.shipment_status.data,
            order_date=form.order_date.data.strftime('%Y-%m-%d') if form.order_date.data else None
        )
        
        if success:
            flash(result, 'success')
        else:
            flash(result, 'error')
            # エラーの場合はログにも出力
            log_error(f"バーコード保存エラー: {result}")
        
        return redirect(url_for('shipment_list'))
            
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"バーコード保存中にエラーが発生しました: {str(e)}\n{tb}")
        flash(f'バーコードデータの保存に失敗しました: {str(e)}', 'error')
        return redirect(url_for('shipment_list'))

@app.route('/order_search', methods=['GET', 'POST'])
@login_required
def order_search():
    """受注データ検索画面を表示"""
    form = BrcpSearchForm()
    session = None
    orders = []
    
    try:
        session = get_db_session()
        
        # 各フィールドの選択肢を設定
        try:
            form.base.choices = BfspMst.get_choices('BFSP_BASE')
            form.adp.choices = BfspMst.get_choices('BFSP_ADP')
            form.lr.choices = BfspMst.get_choices('BFSP_LR')
            form.clr.choices = BfspMst.get_choices('BFSP_CLR')
        except Exception as e:
            flash('選択肢の取得中にエラーが発生しました。', 'error')
            return render_template('order_search.html', form=form, orders=[])
        
        # 出荷先の選択肢を取得
        try:
            cztr_list = session.execute(text("SELECT CZTR_ID, CZTR_NM FROM CZTR_MST WHERE CZTR_TYP = :cztr_type_bf ORDER BY CZTR_ID"), {'cztr_type_bf': DatabaseConstants.CZTR_TYPE_BF}).fetchall()
            form.order_company.choices = [('', '全て')] + [(str(cztr[0]), cztr[1]) for cztr in cztr_list]
        except Exception as e:
            flash('出荷先リストの取得中にエラーが発生しました。', 'error')
            return render_template('order_search.html', form=form, orders=[])
        
        if request.method == 'POST':
            try:
                # 数値項目のバリデーション
                if form.order_no.data and not str(form.order_no.data).isdigit():
                    flash('客先受注番号は数値で入力してください。', 'error')
                    return render_template('order_search.html', form=form, orders=[])
                
                # 検索条件を取得
                orders = BrcpDat.search_orders(
                    order_date=form.order_date.data,
                    product_id=form.product_id.data,
                    base=form.base.data,
                    adp=form.adp.data,
                    lr=form.lr.data,
                    clr=form.clr.data,
                    proc=form.proc.data,
                    order_company=form.order_company.data,
                    order_no=int(form.order_no.data) if form.order_no.data else None,
                    zan_select=form.zan_select.data
                )
                
                if not orders:
                    flash('検索条件に一致する受注データが見つかりませんでした。', 'info')
                
            except ValueError as e:
                flash(str(e), 'error')
                return render_template('order_search.html', form=form, orders=[])
                
            except Exception as e:
                flash('受注データの検索中にエラーが発生しました。', 'error')
                return render_template('order_search.html', form=form, orders=[])
        
        return render_template('order_search.html', form=form, orders=orders)
        
    except Exception as e:
        flash(f'受注データ検索中にエラーが発生しました: {str(e)}', 'error')
        return render_template('order_search.html', form=form, orders=[])
        
    finally:
        if session:
            session.close()

@app.route('/api/products_barcode', methods=['GET'])
@login_required
def get_products():
    """製品一覧を取得"""
    session = get_db_session()
    try:
        products = session.execute(text("""
            SELECT 
                BFSP_PRD_ID,
                BFSP_BASE,
                BFSP_ADP,
                BFSP_LR,
                BFSP_CLR
            FROM BFSP_MST 
            ORDER BY BFSP_SORT
        """)).fetchall()
        
        result = []
        for product in products:
            result.append({
                'id': product.BFSP_PRD_ID,
                'display': f"{product.BFSP_BASE}/{product.BFSP_ADP}/{product.BFSP_LR}/{product.BFSP_CLR}"
            })
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/generate_barcode', methods=['POST'])
@login_required
def generate_barcode():
    """バーコードを生成"""
    try:
        data = request.get_json()
        product_id = data.get('product_id')
        coating_date = data.get('coating_date')
        lot = data.get('lot')
        barcode_type = data.get('barcode_type')  # 'younger' or 'sunray'
        coating_type = data.get('coating_type')
        
        try:
            lot_obj = datetime.strptime(lot, '%Y-%m-%d')
            lot_str = lot_obj.strftime('%y%m%d')
        except ValueError:
            return jsonify({'error': '日付の形式が正しくありません'}), 400
            
        
        if barcode_type == 'younger':
            try:
                date_obj = datetime.strptime(coating_date, '%Y-%m-%d')
                date_str = date_obj.strftime('%y%m%d')
            except ValueError:
                return jsonify({'error': '日付の形式が正しくありません'}), 400
            
            # ヤンガーバーコード生成
            barcode = BarcodeGenerator.make_barcode_y(product_id, lot_str, date_str)
        elif barcode_type == 'sunray':
            # サンレーバーコード生成（日付をYYMMDD形式に変換）
            # コーティングタイプを決定（HC固定とする）
            barcode = BarcodeGenerator.make_barcode_s(product_id, lot_str, coating_type)
        else:
            return jsonify({'error': '無効なバーコードタイプです'}), 400
        
        if barcode is None:
            return jsonify({'error': 'バーコード生成中にエラーが発生しました'}), 500
        
        return jsonify({'barcode': barcode})
        
    except Exception as e:
        log_error(f"バーコード生成中にエラーが発生: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/barcode_scan')
@login_required
def barcode_scan():
    """バーコード読込画面を表示"""
    return render_template('barcode_scan.html')

@app.route('/barcode_scan_csv')
@login_required
def barcode_scan_csv():
    """バーコードスキャンCSV画面を表示"""
    try:
        # リクエストパラメータからBBCD_KBNを取得
        bbcd_kbn = request.args.get('bbcd_kbn', '')
        
        # BBCD_DATテーブルからデータを取得
        session = get_db_session()
        query = session.query(BBcdDat)
        
        # BBCD_KBNが指定されている場合は絞り込み
        if bbcd_kbn:
            query = query.filter(BBcdDat.BBCD_KBN == bbcd_kbn)
        
        # BBCD_IDを数値としてソート
        bcd_data = query.order_by(cast(BBcdDat.BBCD_ID, Integer)).all()
        session.close()
        
        # バーコード区分の選択肢を取得
        from app.constants import FormChoiceConstants
        bbcd_kbn_choices = FormChoiceConstants.BBCD_KBN_CHOICES
        
        return render_template('barcode_scan_csv.html', 
                             bcd_data=bcd_data, 
                             bbcd_kbn_choices=bbcd_kbn_choices,
                             selected_bbcd_kbn=bbcd_kbn)
    except Exception as e:
        flash(f'データの取得中にエラーが発生しました: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/validate_barcode', methods=['POST'])
@login_required
def validate_barcode():
    """バーコードの検証を行う"""
    try:
        data = request.get_json()
        barcode = data.get('barcode', '').strip().upper()
        validation_str = data.get('validationStr', '').strip().upper()
        
        if not barcode:
            return jsonify({
                'invalid': True,
                'message': 'バーコードが入力されていません'
            })
        
        if not validation_str:
            return jsonify({
                'invalid': True,
                'message': '検証データが入力されていません'
            })
        
        # バーコードと検証データの比較
        if barcode == validation_str:
            return jsonify({
                'invalid': True,
                'message': 'バーコードが有効です'
            })
        else:
            return jsonify({
                'invalid': True,
                'message': f'バーコードが一致しません。入力値: {barcode}, 期待値: {validation_str}'
            })
            
    except Exception as e:
        log_error(f'バーコード検証中にエラーが発生しました: {str(e)}')
        return jsonify({
            'invalid': True,
            'message': f'検証中にエラーが発生しました: {str(e)}'
        })

@app.route('/barcode_import', methods=['GET', 'POST'])
@login_required
def barcode_import():
    """バーコードCSV取込画面を表示"""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('ファイルが選択されていません', 'error')
            return redirect(request.url)
            
        file = request.files['file']
        if file.filename == '':
            flash('ファイルが選択されていません', 'error')
            return redirect(request.url)
            
        # ファイルの拡張子をチェック
        if not file.filename.lower().endswith('.csv'):
            flash('CSVファイルを選択してください', 'error')
            return redirect(request.url)
            
        # 一時ファイルとして保存
        temp_dir = tempfile.gettempdir()
        filename = secure_filename(file.filename)
        temp_path = os.path.join(temp_dir, filename)
        file.save(temp_path)
        
        # CSV取り込み処理
        try:
            has_header = 'hasHeader' in request.form
            bbcd_kbn = request.form.get('bbcdKbn')
            
            # 区分の検証
            if not bbcd_kbn:
                flash('バーコード区分を選択してください', 'error')
                os.remove(temp_path)
                return redirect(request.url)
            
            result = import_from_barcode(temp_path, has_header, bbcd_kbn)
        except Exception as e:
            tb = traceback.format_exc()
            log_error(f"CSV取り込み処理でエラーが発生しました: {str(e)}\n{tb}")
            flash(f'CSV取り込み処理エラー: {str(e)}', 'error')
            # 一時ファイルを削除
            os.remove(temp_path)
            return redirect(request.url)
        
        # 一時ファイルを削除
        os.remove(temp_path)
        
        # 結果を表示
        bbcd_kbn_name = {'1': 'BF', '2': '一般', '3': '箱詰', '4': '検品'}.get(bbcd_kbn, '不明')
        if result.get("success", 0) > 0:
            flash(f"区分「{bbcd_kbn_name}」のCSV取り込み完了: 合計{result.get('total', 0)}行、"
                f"成功{result.get('success', 0)}行、"
                f"スキップ{result.get('skipped', 0)}行、重複{result.get('duplicate', 0)}行、エラー{result.get('error', 0)}行", 'success')
        else:
            flash(f"区分「{bbcd_kbn_name}」のCSV取り込み結果: 合計{result.get('total', 0)}行、"
                f"成功{result.get('success', 0)}行、"
                f"スキップ{result.get('skipped', 0)}行、重複{result.get('duplicate', 0)}行、エラー{result.get('error', 0)}行", 'warning')
        
        # エラーがあれば表示 (最大5件まで)
        if result['errors']:
            for i, error in enumerate(result['errors'][:5]):
                flash(f"{error}", 'error')
            if len(result['errors']) > 5:
                flash(f"その他 {len(result['errors']) - 5} 件のエラーがあります", 'warning')
        
        return redirect(request.url)
        
    return render_template('barcode_import.html')

@app.route('/hardcoat_auto_shipping', methods=['GET'])
@login_required
def hardcoat_auto_shipping():
    """ハードコート自動出荷画面を表示"""
    form = NoncoatStockSearchForm()
    
    try:
        # 各フィールドの選択肢を設定
        form.base.choices = BfspMst.get_choices('BFSP_BASE')
        form.adp.choices = BfspMst.get_choices('BFSP_ADP')
        form.lr.choices = BfspMst.get_choices('BFSP_LR')
        form.clr.choices = BfspMst.get_choices('BFSP_CLR')
        
        shipping_destinations = []
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 出荷先リストを取得
        shipping_destinations = Shipment.get_shipping_destinations()
        
        return render_template('hardcoat_auto_shipping.html', 
                             form=form, 
                             shipping_destinations=shipping_destinations,
                             today=today,
                             SHIPMENT_TO_PROCESS=DatabaseConstants.SHIPMENT_TO_PROCESS)
    except Exception as e:
        flash(f'自動出荷画面の表示中にエラーが発生しました: {str(e)}', 'error')
        return render_template('hardcoat_auto_shipping.html', 
                             form=form, 
                             shipping_destinations=[],
                             today=today,
                             SHIPMENT_TO_PROCESS=DatabaseConstants.SHIPMENT_TO_PROCESS)

@app.route('/hardcoat_auto_shipping_save', methods=['POST'])
@login_required
def hardcoat_auto_shipping_save():
    """ハードコート自動出荷処理を実行"""
    try:
        # フォームデータを取得
        base = request.form.get('base')
        adp = request.form.get('adp')
        lr = request.form.get('lr')
        clr = request.form.get('clr')
        ship_to = request.form.get('ship_to')
        order_date = request.form.get('order_date')
        ship_date = request.form.get('ship_date')
        quantity = int(request.form.get('quantity', 0))
        
        # バリデーション
        if not all([base, adp, lr, clr, ship_to, order_date, ship_date, quantity]):
            return jsonify({'success': False, 'error': '必須項目が入力されていません。'})
        
        if quantity <= 0:
            return jsonify({'success': False, 'error': '出荷数は1以上で入力してください。'})
        
        # 在庫データを検索（BPDD_LOTの小さい順）
        stocks = PrdDat.search_hardcoat_stock(
            base=base,
            adp=adp,
            lr=lr,
            clr=clr
        )
        
        if not stocks:
            return jsonify({'success': False, 'error': '指定された条件の在庫が見つかりません。'})
        
        # 総在庫数をチェック
        total_stock = sum(stock.stock_qty for stock in stocks)
        if quantity > total_stock:
            return jsonify({'success': False, 'error': f'出荷数({quantity})が利用可能在庫数({total_stock})を超えています。'})
        
        # 自動出荷処理を実行
        result = Shipment.auto_shipping_hardcoat(
            base=base,
            adp=adp,
            lr=lr,
            clr=clr,
            ship_to=ship_to,
            order_date=order_date,
            ship_date=ship_date,
            quantity=quantity
        )
        
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({'success': False, 'error': f'入力値が不正です: {str(e)}'})
    except Exception as e:
        log_error(f"自動出荷処理中にエラーが発生しました: {str(e)}")
        return jsonify({'success': False, 'error': f'自動出荷処理中にエラーが発生しました: {str(e)}'})

@app.route('/hardcoat_check_stock', methods=['POST'])
@login_required
def hardcoat_check_stock():
    """ハードコート在庫数量をチェック"""
    try:
        base = request.form.get('base')
        adp = request.form.get('adp')
        lr = request.form.get('lr')
        clr = request.form.get('clr')
        
        if not all([base, adp, lr, clr]):
            return jsonify({'success': False, 'error': '必須項目が入力されていません。'})
        
        # 在庫データを検索
        stocks = PrdDat.search_hardcoat_stock(
            base=base,
            adp=adp,
            lr=lr,
            clr=clr
        )
        
        total_stock = sum(stock.stock_qty for stock in stocks)
        
        return jsonify({
            'success': True,
            'total_stock': total_stock,
            'record_count': len(stocks)
        })
        
    except Exception as e:
        log_error(f"在庫チェック中にエラーが発生しました: {str(e)}")
        return jsonify({'success': False, 'error': f'在庫チェック中にエラーが発生しました: {str(e)}'})

