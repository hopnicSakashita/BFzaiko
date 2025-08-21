from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer, PageBreak, PageTemplate, Frame, NextPageTemplate
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from io import BytesIO
import os
import platform
import traceback
from datetime import datetime

from app.models import log_error
from app.constants import PdfConstants, ErrorMessages
from app.models_common import CshkDatModel
from app.models_master import CztrMstModel
from app.document_number_manager import DocumentNumberManager
from app.shipment_common import ShipmentCommon


def create_header_footer_template(header_info, doc_number, font_name, total_pages=None):
    """ヘッダーとフッターを含むページテンプレートを作成"""
    
    def header_footer(canvas, doc):
        # ヘッダー部分
        canvas.saveState()
        
        # ヘッダー背景（必要に応じて）
        # canvas.setFillColor(colors.lightgrey)
        # canvas.rect(15*mm, A4[1]-25*mm, A4[0]-30*mm, 10*mm, fill=1)
        
        # 会社名（左上）
        cztr_full_nm = header_info.get('CZTR_FULL_NM', '')
        cztr_tanto_nm = header_info.get('CZTR_TANTO_NM', '')
        if cztr_tanto_nm:
            cztr_full_nm = cztr_full_nm + ' 御中' + ' ' + cztr_tanto_nm + '様'
        else:
            cztr_full_nm = cztr_full_nm + ' 御中'
        
        canvas.setFont(font_name, 12)
        canvas.drawString(15*mm, A4[1]-25*mm, cztr_full_nm)
        
        # No.欄（右上）
        canvas.setFont(font_name, 10)
        canvas.drawString(A4[0]-60*mm, A4[1]-25*mm, f"No. {doc_number}")
        
        # タイトル（中央）
        canvas.setFont(font_name, 16)
        title = '加　工　依　頼　書'
        title_width = canvas.stringWidth(title, font_name, 16)
        canvas.drawString((A4[0] - title_width) / 2, A4[1]-40*mm, title)
        
        # タイトル下線
        canvas.line((A4[0] - title_width) / 2, A4[1]-42*mm, (A4[0] + title_width) / 2, A4[1]-42*mm)
        
        # フッター部分
        # 備考欄（左下）
        canvas.setFont(font_name, 10)
        canvas.drawString(15*mm, 20*mm, '備考：')
        
        # 会社名（中央）
        canvas.setFont(font_name, 11)
        footer_text = '株式会社　ホプニック研究所'
        footer_width = canvas.stringWidth(footer_text, font_name, 11)
        canvas.drawString((A4[0] - footer_width) / 2, 15*mm, footer_text)
        
        # ページ番号（右下）
        page_num = canvas.getPageNumber()
        canvas.setFont(font_name, 8)
        if total_pages:
            page_text = f"ページ {page_num} / {total_pages}"
        else:
            page_text = f"ページ {page_num}"
        canvas.drawString(A4[0]-35*mm, 15*mm, page_text)
        
        canvas.restoreState()
    
    return header_footer

def get_japanese_font():
    """プラットフォームに応じて日本語フォントを取得する"""
    system = platform.system()
    
    # フォント候補リスト（日本語対応のもののみ）
    font_candidates = []
    
    if system == "Windows":
        font_candidates = [
            ('C:\\Windows\\Fonts\\msgothic.ttc', 'MSGothic'),
            ('C:\\Windows\\Fonts\\msgothic.ttf', 'MSGothic'),
            ('C:\\Windows\\Fonts\\yugothic.ttf', 'YuGothic'),
            ('C:\\Windows\\Fonts\\yugothib.ttf', 'YuGothic'),
            ('C:\\Windows\\Fonts\\meiryo.ttc', 'Meiryo'),
        ]
    elif system == "Darwin":  # macOS
        font_candidates = [
            ('/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc', 'HiraginoSans'),
            ('/Library/Fonts/ヒラギノ角ゴ ProN W3.otf', 'HiraginoSans'),
            ('/System/Library/Fonts/Arial Unicode MS.ttf', 'ArialUnicodeMS'),
        ]
    else:  # Linux and others
        font_candidates = [
            # Noto CJK フォント（日本語対応）
            ('/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc', 'NotoSansCJK'),
            ('/usr/share/fonts/truetype/noto-cjk/NotoSansCJK-Regular.ttc', 'NotoSansCJK'),
            ('/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc', 'NotoSerifCJK'),
            # IPAフォント（日本語対応）- インストール済み
            ('/usr/share/fonts/truetype/fonts-japanese-gothic.ttf', 'IPAGothic'),
            ('/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf', 'IPAGothic'),
            ('/usr/share/fonts/opentype/ipafont-mincho/ipam.ttf', 'IPAMincho'),
            # その他の日本語フォント候補
            ('/usr/share/fonts/truetype/vlgothic/VL-Gothic-Regular.ttf', 'VLGothic'),
        ]
    
    # フォントファイルの存在確認と登録
    for font_path, font_name in font_candidates:
        if os.path.exists(font_path):
            try:
                pdfmetrics.registerFont(TTFont(font_name, font_path))
                print(f"日本語フォントを登録しました: {font_name} ({font_path})")
                return font_name
            except Exception as e:
                print(f"フォント登録に失敗: {font_name} - {str(e)}")
                continue
    
    # 日本語フォントが見つからない場合はエラーを発生
    error_msg = f"日本語対応フォントが見つかりません。プラットフォーム: {system}"
    print(error_msg)
    
    if system == "Linux":
        print("以下のコマンドで日本語フォントをインストールしてください:")
        print("Ubuntu/Debian: sudo apt-get install fonts-noto-cjk fonts-ipafont-gothic")
        print("CentOS/RHEL: sudo yum install google-noto-cjk-fonts ipa-gothic-fonts")
        print("または: sudo dnf install google-noto-cjk-fonts ipa-gothic-fonts")
    
    raise Exception(f"日本語対応フォントが利用できません。適切なフォントをインストールしてください。({system})")

def shipment_export_pdf(shipments):
    """出荷データをPDFで出力する"""
    try:
        print("PDF出力処理を開始します")

        print("必要なモジュールをインポートしました")

        # フォントの登録
        font_name = get_japanese_font()
        print(f"使用するフォント: {font_name}")

        # PDFバッファを作成
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=PdfConstants.MARGIN_RIGHT*mm,
            leftMargin=PdfConstants.MARGIN_LEFT*mm,
            topMargin=PdfConstants.MARGIN_TOP*mm,
            bottomMargin=PdfConstants.MARGIN_BOTTOM*mm
        )

        # スタイルの設定
        styles = getSampleStyleSheet()
        japanese_style = ParagraphStyle(
            name='Japanese',
            fontName=font_name,
            fontSize=10,
            leading=14
        )
        
        # 要素リストの作成
        elements = []

        # タイトルの追加
        title = Paragraph('出荷一覧', ParagraphStyle(
            name='Title',
            fontName=font_name,
            fontSize=15,
            spaceAfter=5*mm
        ))
        elements.append(title)

        # テーブルヘッダー
        headers = ['出荷日', '出荷先', '注文番号', 'LOT', 'Base', '度数', 
                    'LR', '色', '数量', 'コート日', 'CT']

        # テーブルデータの作成
        data = [[Paragraph(header, japanese_style) for header in headers]]
        print(f"出荷データ数: {len(shipments)}")
        
        # データの前処理（Paragraphオブジェクトに変換）
        for s in shipments:
            row = [
                Paragraph(str(s['shipment_date']), japanese_style),
                Paragraph(str(s['destination']), japanese_style),
                Paragraph(str(s['order_number']), japanese_style),
                Paragraph(str(s['lot']), japanese_style),
                Paragraph(str(s['base']), japanese_style),
                Paragraph(str(s['adp']), japanese_style),
                Paragraph(str(s['lr']), japanese_style),
                Paragraph(str(s['color']), japanese_style),
                Paragraph(str(s['quantity']), japanese_style),
                Paragraph(str(s['coating_date']), japanese_style),
                Paragraph(str(s['proc_type']), japanese_style)
            ]
            data.append(row)

        # 列幅の設定
        col_widths = [width*mm for width in [22, 25, 25, 18, 15, 15, 10, 10, 15, 20, 15]]

        # テーブルの作成
        table = Table(data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
            ('LEFTPADDING', (0, 0), (-1, -1), 3*mm),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3*mm),
            ('TOPPADDING', (0, 0), (-1, -1), 3*mm),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3*mm),
        ]))
        elements.append(table)
        print("テーブルを作成しました")

        # PDFの生成
        doc.build(elements)
        print("PDFの生成が完了しました")

        pdf = buffer.getvalue()
        buffer.close()
        return pdf

    except Exception as e:
        print(f"PDF出力中にエラーが発生しました: {str(e)}")
        print(traceback.format_exc())
        log_error(f"PDF出力中にエラーが発生しました: {str(e)}\n{traceback.format_exc()}")
        
            
        raise Exception(f'PDFファイルの作成に失敗しました: {str(e)}')

def noncoat_stock_export_pdf(stocks):
    """ノンコート在庫データをPDFで出力する"""
    try:
        print("ノンコート在庫PDF出力処理を開始します")

        # フォントの登録
        font_name = get_japanese_font()
        print(f"使用するフォント: {font_name}")

        # PDFバッファを作成
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=PdfConstants.MARGIN_RIGHT*mm,
            leftMargin=PdfConstants.MARGIN_LEFT*mm,
            topMargin=PdfConstants.MARGIN_TOP*mm,
            bottomMargin=PdfConstants.MARGIN_BOTTOM*mm
        )

        # スタイルの設定
        styles = getSampleStyleSheet()
        japanese_style = ParagraphStyle(
            name='Japanese',
            fontName=font_name,
            fontSize=10,
            leading=14
        )
        
        # 要素リストの作成
        elements = []

        # タイトルの追加
        title = Paragraph('ノンコート在庫一覧', ParagraphStyle(
            name='Title',
            fontName=font_name,
            fontSize=15,
            spaceAfter=5*mm
        ))
        elements.append(title)

        # テーブルヘッダー
        headers = ['LOT', 'ベース', '加入度数', 'LR', 'カラー', '在庫数']

        # テーブルデータの作成
        data = [[Paragraph(header, japanese_style) for header in headers]]
        print(f"在庫データ数: {len(stocks)}")
        
        # データの前処理（Paragraphオブジェクトに変換）
        for stock in stocks:
            row = [
                Paragraph(str(stock.BPDD_LOT), japanese_style),
                Paragraph(str(stock.BFSP_BASE), japanese_style),
                Paragraph(str(stock.BFSP_ADP), japanese_style),
                Paragraph(str(stock.BFSP_LR), japanese_style),
                Paragraph(str(stock.BFSP_CLR), japanese_style),
                Paragraph(str(stock.stock_qty), japanese_style)
            ]
            data.append(row)

        # 列幅の設定
        col_widths = [width*mm for width in [25, 20, 25, 15, 15, 20]]

        # テーブルの作成
        table = Table(data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
            ('LEFTPADDING', (0, 0), (-1, -1), 3*mm),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3*mm),
            ('TOPPADDING', (0, 0), (-1, -1), 3*mm),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3*mm),
        ]))
        elements.append(table)
        print("テーブルを作成しました")

        # PDFの生成
        doc.build(elements)
        print("PDFの生成が完了しました")

        pdf = buffer.getvalue()
        buffer.close()
        return pdf

    except Exception as e:
        print(f"PDF出力中にエラーが発生しました: {str(e)}")
        print(traceback.format_exc())
        log_error(f"ノンコート在庫PDF出力中にエラーが発生しました: {str(e)}\n{traceback.format_exc()}")
        
        raise Exception(f'PDFファイルの作成に失敗しました: {str(e)}')

def hardcoat_stock_export_pdf(stocks):
    """ハードコート在庫データをPDFで出力する"""
    try:
        print("ハードコート在庫PDF出力処理を開始します")

        # フォントの登録
        font_name = get_japanese_font()
        print(f"使用するフォント: {font_name}")

        # PDFバッファを作成
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=PdfConstants.MARGIN_RIGHT*mm,
            leftMargin=PdfConstants.MARGIN_LEFT*mm,
            topMargin=PdfConstants.MARGIN_TOP*mm,
            bottomMargin=PdfConstants.MARGIN_BOTTOM*mm
        )

        # スタイルの設定
        styles = getSampleStyleSheet()
        japanese_style = ParagraphStyle(
            name='Japanese',
            fontName=font_name,
            fontSize=10,
            leading=14
        )
        
        # 要素リストの作成
        elements = []

        # タイトルの追加
        title = Paragraph('ハードコート在庫一覧', ParagraphStyle(
            name='Title',
            fontName=font_name,
            fontSize=15,
            spaceAfter=5*mm
        ))
        elements.append(title)

        # テーブルヘッダー
        headers = ['LOT', 'ベース', '加入度数', 'LR', 'カラー', '在庫数', 'HC残', 'コート日']

        # テーブルデータの作成
        data = [[Paragraph(header, japanese_style) for header in headers]]
        print(f"在庫データ数: {len(stocks)}")
        
        # データの前処理（Paragraphオブジェクトに変換）
        for stock in stocks:
            row = [
                Paragraph(str(stock.BPDD_LOT), japanese_style),
                Paragraph(str(stock.BFSP_BASE), japanese_style),
                Paragraph(str(stock.BFSP_ADP), japanese_style),
                Paragraph(str(stock.BFSP_LR), japanese_style),
                Paragraph(str(stock.BFSP_CLR), japanese_style),
                Paragraph(str(stock.stock_qty), japanese_style),
                Paragraph(str(stock.order_remaining), japanese_style),
                Paragraph(str(stock.BPDD_CRT), japanese_style)
            ]
            data.append(row)

        # 列幅の設定
        col_widths = [width*mm for width in [20, 15, 20, 10, 10, 15, 15, 20]]

        # テーブルの作成
        table = Table(data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
            ('LEFTPADDING', (0, 0), (-1, -1), 3*mm),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3*mm),
            ('TOPPADDING', (0, 0), (-1, -1), 3*mm),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3*mm),
        ]))
        elements.append(table)
        print("テーブルを作成しました")

        # PDFの生成
        doc.build(elements)
        print("PDFの生成が完了しました")

        pdf = buffer.getvalue()
        buffer.close()
        return pdf

    except Exception as e:
        print(f"PDF出力中にエラーが発生しました: {str(e)}")
        print(traceback.format_exc())
        log_error(f"ハードコート在庫PDF出力中にエラーが発生しました: {str(e)}\n{traceback.format_exc()}")
        
        raise Exception(f'PDFファイルの作成に失敗しました: {str(e)}')

def process_request_export_pdf(date_from=None, date_to=None, prd_id=None, prc_id=None, cztr_id=None, return_status=None):
    """加工依頼書PDFを出力する"""
    try:
        # 検索条件に基づいて明細データ取得（PDF出力用）
        process_request_list = ShipmentCommon.get_process_request_list_for_pdf(
            date_from=date_from,
            date_to=date_to,
            prd_id=prd_id,
            prc_id=prc_id,
            cztr_id=cztr_id,
            return_status=return_status
        )
        
        # 複数の取引先にまたがる場合のバリデーション
        if process_request_list:
            # 最初の取引先IDを取得
            first_cztr_id = process_request_list[0].get('CZTR_ID')
            
            # 全てのデータが同じ取引先IDかチェック
            for req in process_request_list:
                if req.get('CZTR_ID') != first_cztr_id:
                    raise Exception('複数の取引先にまたがる加工依頼書は出力できません。検索条件を絞り込んでください。')
        
        # 取引先情報の取得（ヘッダー用）
        header_info = {}
        if process_request_list:
            header_info = {
                'CZTR_FULL_NM': process_request_list[0].get('PRC_TO_FULL_NM', ''),
                'CZTR_TANTO_NM': process_request_list[0].get('PRC_TO_TANTO_NM', '')
            }
        
        # ドキュメント番号を生成
        doc_number = DocumentNumberManager().get_next_number('process_request')
        
        # PDF用データ整形
        details = []
        for req in process_request_list:
            details.append({
                'CPRC_PRD_NM': req.get('CPRC_PRD_NM', ''),
                'CPDD_LOT': req.get('CPDD_LOT', ''),
                'CSHK_QTY': req.get('CSHK_QTY', 0),
                'CPRC_NM': req.get('CPRC_NM', '')
            })
        
        font_name = get_japanese_font()
        # --- 見た目重視の寸法調整 ---
        PAGE_WIDTH = 210 * mm
        LEFT_MARGIN = 15 * mm
        RIGHT_MARGIN = 15 * mm
        TOP_MARGIN = 15 * mm
        BOTTOM_MARGIN = 15 * mm
        TABLE_WIDTH = PAGE_WIDTH - LEFT_MARGIN - RIGHT_MARGIN  # 約180mm
        # 列幅（品名, LOT, 数量, 加工内容）
        col_widths = [70*mm, 30*mm, 25*mm, 55*mm]
        row_height = 10*mm
        # --- PDFドキュメント設定（ヘッダー・フッター付き） ---
        buffer = BytesIO()
        
        # 通常のPDFドキュメント設定（ページテンプレートなし）
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=RIGHT_MARGIN/mm,
            leftMargin=LEFT_MARGIN/mm,
            topMargin=TOP_MARGIN/mm,
            bottomMargin=BOTTOM_MARGIN/mm
        )
        
        styles = getSampleStyleSheet()
        elements = []
        # ページング機能付き明細テーブル
        headers = ['品名', '', '数量', '加工内容']
        col_widths = [80*mm, 20*mm, 30*mm, 50*mm]
        row_height = 12*mm
        max_rows_per_page = 16  # 1ページあたりの最大行数（ヘッダー行含む）
        
        # データを行ごとに分割
        all_data_rows = []
        for d in details:
            row = [
                Paragraph(str(d.get('CPRC_PRD_NM', '')), ParagraphStyle(name='TableCell', fontName=font_name, fontSize=11)),
                Paragraph(str(d.get('CPDD_LOT', '')), ParagraphStyle(name='TableCell', fontName=font_name, fontSize=11)),
                Paragraph(str(d.get('CSHK_QTY', '')), ParagraphStyle(name='TableCell', fontName=font_name, fontSize=11, alignment=2)),
                Paragraph(str(d.get('CPRC_NM', '')), ParagraphStyle(name='TableCell', fontName=font_name, fontSize=11)),
            ]
            all_data_rows.append(row)
        
        # 合計計算
        total_qty = sum(int(d.get('CSHK_QTY', 0) or 0) for d in details)
        
        # ページ分割処理（各ページにヘッダー・フッターを手動追加）
        current_page = 1
        total_pages = (len(all_data_rows) + max_rows_per_page - 1) // max_rows_per_page  # 切り上げ
        
        for page_start in range(0, len(all_data_rows), max_rows_per_page - 1):  # ヘッダー行を除く
            page_end = min(page_start + max_rows_per_page - 1, len(all_data_rows))
            page_data = all_data_rows[page_start:page_end]
            
            # 全ページにヘッダーを追加
            # ヘッダーテーブル（会社名とNo.）
            cztr_full_nm = header_info.get('CZTR_FULL_NM', '')
            cztr_tanto_nm = header_info.get('CZTR_TANTO_NM', '')
            if cztr_tanto_nm:
                cztr_full_nm = cztr_full_nm + ' 御中' + ' ' + cztr_tanto_nm + '様'
            else:
                cztr_full_nm = cztr_full_nm + ' 御中'
            
            company_table = Table([
                [Paragraph(f"{cztr_full_nm}", ParagraphStyle(name='Header1', fontName=font_name, fontSize=14, alignment=1))]
            ], colWidths=[80*mm], hAlign='LEFT')
            company_table.setStyle(TableStyle([
                ('LINEBELOW', (0,0), (0,0), 0.7, colors.black),
                ('BOTTOMPADDING', (0,0), (0,0), 2*mm),
                ('LEFTPADDING', (0,0), (0,0), 0),
                ('RIGHTPADDING', (0,0), (0,0), 0),
            ]))
            
            no_table = Table([
                [Paragraph("No.", ParagraphStyle(name='Header1', fontName=font_name, fontSize=12, alignment=0)),
                 Paragraph(doc_number, ParagraphStyle(name='Header1', fontName=font_name, fontSize=12, alignment=0))]
            ], colWidths=[10*mm, 40*mm], hAlign='RIGHT')
            no_table.setStyle(TableStyle([
                ('LINEBELOW', (0,0), (1,0), 0.7, colors.black),
                ('BOTTOMPADDING', (0,0), (1,0), 0.5*mm),
                ('LEFTPADDING', (0,0), (1,0), 0),
                ('RIGHTPADDING', (0,0), (1,0), 0),
            ]))
            
            header_table = Table([
                [company_table, no_table]
            ], colWidths=[120*mm, 60*mm])
            header_table.setStyle(TableStyle([
                ('ALIGN', (0,0), (0,0), 'LEFT'),
                ('ALIGN', (1,0), (1,0), 'RIGHT'),
                ('LEFTPADDING', (0,0), (1,0), 0),
                ('RIGHTPADDING', (0,0), (1,0), 0),
                ('TOPPADDING', (0,0), (1,0), 0),
                ('BOTTOMPADDING', (0,0), (1,0), 0),
            ]))
            
            elements.append(header_table)
            elements.append(Spacer(1, 5*mm))
            
            # タイトル
            elements.append(Table([
                [Paragraph('加　工　依　頼　書', ParagraphStyle(name='Title', fontName=font_name, fontSize=18, alignment=1, leading=22))]
            ], colWidths=[70*mm], style=TableStyle([
                ('ALIGN', (0,0), (0,0), 'CENTER'),
                ('LINEBELOW', (0,0), (0,0), 1, colors.black),
                ('BOTTOMPADDING', (0,0), (0,0), 2*mm),
            ])))
            elements.append(Spacer(1, 15*mm))
            
            # ヘッダー行を追加
            table_data = [[Paragraph(h, ParagraphStyle(name='TableHeader', fontName=font_name, fontSize=12, alignment=1)) for h in headers]]
            table_data.extend(page_data)
            
            # 最後のページまたは16行未満の場合は空行を追加
            if len(table_data) < max_rows_per_page:
                empty_rows_needed = max_rows_per_page - len(table_data) - 1  # 合計行分を引く
                for _ in range(empty_rows_needed):
                    table_data.append(['', '', '', ''])
                
                # 合計行を追加
                table_data.append([
                    Paragraph('合計', ParagraphStyle(name='TotalLabel', fontName=font_name, fontSize=12, alignment=2)),
                    '',
                    Paragraph(str(total_qty), ParagraphStyle(name='TotalValue', fontName=font_name, fontSize=12, alignment=2)),
                    ''
                ])
            
            # テーブルを作成
            table = Table(table_data, colWidths=col_widths, rowHeights=[row_height]*len(table_data), repeatRows=1)
            table.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 0.7, colors.black),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 2*mm),
                ('RIGHTPADDING', (0, 0), (-1, -1), 2*mm),
                ('TOPPADDING', (0, 0), (-1, -1), 1*mm),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 1*mm),
            ]))
            
            elements.append(table)
            
            # フッター部分（各ページに追加）
            elements.append(Spacer(1, 2*mm))
            elements.append(Paragraph('備考：', ParagraphStyle(name='Biko', fontName=font_name, fontSize=12, alignment=0, spaceAfter=2*mm)))
            elements.append(Spacer(1, 8*mm))
            elements.append(Paragraph('株式会社　ホプニック研究所', ParagraphStyle(name='Footer', fontName=font_name, fontSize=13, alignment=1)))
            
            # ページ番号（右下）
            page_text = f"ページ {current_page} / {total_pages}"
            elements.append(Paragraph(page_text, ParagraphStyle(name='PageNumber', fontName=font_name, fontSize=8, alignment=2)))
            
            # ページ分割（最後のページ以外）
            if current_page < total_pages:
                elements.append(PageBreak())
            
            current_page += 1
        doc.build(elements)
        # バッファの内容を取得
        pdf = buffer.getvalue()
        buffer.close()
        return pdf
    except Exception as e:
        print(f"加工依頼書PDF出力中にエラー: {str(e)}")
        print(traceback.format_exc())
        log_error(f"加工依頼書PDF出力中にエラー: {str(e)}\n{traceback.format_exc()}")
        raise Exception(f'PDFファイルの作成に失敗しました: {str(e)}')

def shipment_list_export_pdf(shipments):
    """出荷一覧画面用のPDFを出力する（横向き）"""
    try:
        print("出荷一覧PDF出力処理を開始します")

        # フォントの登録
        font_name = get_japanese_font()
        print(f"使用するフォント: {font_name}")

        # PDFバッファを作成（横向き）
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),  # 横向きに設定
            rightMargin=PdfConstants.MARGIN_RIGHT*mm,
            leftMargin=PdfConstants.MARGIN_LEFT*mm,
            topMargin=10*mm,  # 上部マージンを小さく設定
            bottomMargin=5*mm  # 下部マージンを小さく設定
        )

        # 1ページあたりの表示可能行数を計算
        page_height = landscape(A4)[1] - 15*mm  # 全体の高さから上下マージンを引く
        row_height = 6*mm + 6*mm  # 行の高さ（セル内のパディング含む）
        title_height = 20*mm  # タイトル部分の高さ
        header_height = row_height  # ヘッダー行の高さ
        available_height = page_height - title_height - header_height
        rows_per_page = int(available_height / row_height)
        print(f"1ページあたりの表示可能行数: {rows_per_page}")

        # スタイルの設定
        styles = getSampleStyleSheet()
        japanese_style = ParagraphStyle(
            name='Japanese',
            fontName=font_name,
            fontSize=10,
            leading=14
        )
        japanese_style_right = ParagraphStyle(
            name='JapaneseRight',
            fontName=font_name,
            fontSize=10,
            leading=14,
            alignment=2  # 右揃え
        )
        footer_style = ParagraphStyle(
            name='Footer',
            fontName=font_name,
            fontSize=8,
            leading=10,
            alignment=1  # 中央揃え
        )

        # 要素リストの作成
        elements = []

        # タイトルの追加
        title = Paragraph('出荷一覧', ParagraphStyle(
            name='Title',
            fontName=font_name,
            fontSize=15,
            spaceAfter=5*mm
        ))
        elements.append(title)

        # テーブルヘッダー
        headers = ['出荷日', '手配日', '出荷先', '製品名', '製品ID', 'LOT', 'LANK', '出荷数量']

        # テーブルデータの作成
        data = [[Paragraph(header, japanese_style) for header in headers]]
        print(f"出荷データ数: {len(shipments)}")
        
        # データの前処理（Paragraphオブジェクトに変換）
        for shipment in shipments:
            sprit1 = ''
            sprit2 = ''
            if shipment.get('CPDD_SPRIT1', '') != '' and shipment.get('CPDD_SPRIT1', '') != 0 and shipment.get('CPDD_SPRIT1', '') != None:
                sprit1 = '-' + str(shipment.get('CPDD_SPRIT1', '')).zfill(2)
            if shipment.get('CPDD_SPRIT2', '') != '' and shipment.get('CPDD_SPRIT2', '') != 0 and shipment.get('CPDD_SPRIT2', '') != None:
                sprit2 = '-' + str(shipment.get('CPDD_SPRIT2', '')).zfill(2)
            lot = str(shipment.get('CPDD_LOT', '')).zfill(6) + sprit1 + sprit2
            row = [
                Paragraph(str(shipment.get('CSHK_DT', '－')), japanese_style),
                Paragraph(str(shipment.get('CSHK_ORD_DT', '－')), japanese_style),
                Paragraph(str(shipment.get('SHIPMENT_TO_NAME', '－')), japanese_style),
                Paragraph(str(shipment.get('PRD_DSP_NM', '－')), japanese_style),
                Paragraph(str(shipment.get('CSHK_PRD_ID', '－')), japanese_style),
                Paragraph(lot, japanese_style),
                Paragraph(str(shipment.get('RANK_NAME', '－')), japanese_style),
                Paragraph(str(shipment.get('CSHK_QTY', 0)), japanese_style_right)  # 右揃えスタイルを適用
            ]
            data.append(row)

        # 列幅の設定（横向きA4用に調整）
        col_widths = [width*mm for width in [25, 25, 55, 80, 20, 30, 15, 25]]

        # テーブルの作成
        table = Table(data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (-1, 1), (-1, -1), 'RIGHT'),  # 最後の列（出荷数量）のみ右揃え
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
            ('LEFTPADDING', (0, 0), (-1, -1), 3*mm),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3*mm),
            ('TOPPADDING', (0, 0), (-1, -1), 3*mm),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3*mm),
        ]))
        elements.append(table)
        print("テーブルを作成しました")

        # ページ番号を追加するためのキャンバス設定
        def add_page_number(canvas, doc):
            canvas.saveState()
            canvas.setFont(font_name, 8)
            page_num = canvas.getPageNumber()
            # 総ページ数を計算（実際の表示可能行数から計算）
            total_pages = len(shipments) // rows_per_page + (1 if len(shipments) % rows_per_page > 0 else 1)
            text = f"{page_num} / {total_pages}"
            canvas.drawCentredString(doc.pagesize[0]/2, 5*mm, text)
            canvas.restoreState()

        # PDFの生成（ページ番号付き）
        doc.build(elements, onFirstPage=add_page_number, onLaterPages=add_page_number)
        print("PDFの生成が完了しました")

        pdf = buffer.getvalue()
        buffer.close()
        return pdf

    except Exception as e:
        print(f"PDF出力中にエラーが発生しました: {str(e)}")
        print(traceback.format_exc())
        log_error(f"出荷一覧PDF出力中にエラーが発生しました: {str(e)}\n{traceback.format_exc()}")
        raise Exception(f'PDFファイルの作成に失敗しました: {str(e)}')

