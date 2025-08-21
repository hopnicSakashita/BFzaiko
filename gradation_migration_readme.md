# グラデーションデータ移行専用プログラム

## 概要
このプログラムは、GPRR_DAT（グラデ加工依頼データ）、GPRC_DAT（グラデ加工データ）、GSHK_DAT（グラデ出荷データ）からCPRD_DAT（製造データ）、CSHK_DAT（出荷データ）、CPRC_DAT（加工データ）への移行を実行する専用ツールです。

## 機能
- GPRR_DATの全レコードを抽出し、CPRD_DATとCSHK_DATを作成
- GPRC_DATの条件に合うレコードを抽出し、CPRC_DATとCPRD_DATを作成（REQ_TO=1）
- GSHK_DATの条件に合うレコードを抽出し、CSHK_DATを作成（GSHK_TO=2）
- GSHK_DATの条件に合うレコードを抽出し、CSHK_DATを作成（GSHK_TO=3）
- GPRC_DATの条件に合うレコードを抽出し、CPRC_DATとCPRD_DATを作成（REQ_TO=2）
- 仕様に基づいた適切なマッピング処理
- トランザクション管理による安全な移行
- 失敗したデータの詳細出力
- 全件ロールバック機能

## 移行処理の順序と依存関係

移行処理は以下の順序で実行され、依存関係を考慮して段階的に処理されます：

1. **GPRR_DAT** → CPRD_DAT・CSHK_DAT
2. **GPRC_DAT（REQ_TO=1）** → CPRC_DAT・CPRD_DAT
3. **GSHK_DAT（GSHK_TO=2）** → CSHK_DAT（GPRC_DAT（REQ_TO=1）に依存）
4. **GSHK_DAT（GSHK_TO=3）** → CSHK_DAT（GPRC_DAT（REQ_TO=1）に依存）
5. **GPRC_DAT（REQ_TO=2）** → CPRC_DAT・CPRD_DAT（GSHK_DATに依存）

### 依存関係の詳細
- **GPRC_DAT（REQ_TO=1）**: GPRR_DATのCSHK_DATレコードを参照
- **GSHK_DAT（GSHK_TO=2）**: GPRC_DAT（REQ_TO=1）のCPRD_DATレコードを参照
- **GSHK_DAT（GSHK_TO=3）**: GPRC_DAT（REQ_TO=1）のCPRD_DATレコードを参照
- **GPRC_DAT（REQ_TO=2）**: GSHK_DATのCSHK_DATレコードを参照

## 移行ルール（処理順序）

### 1. GPRR_DAT → CPRD_DAT・CSHK_DAT

#### 抽出条件
- GPRR_DATの全レコード

#### CPRD_DAT作成ルール（GPRR用）
- **CPDD_PRD_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定
- **CPDD_LOT**: GPRR_REQ_DATEをYYMMDD形式で数値変換
- **CPDD_SPRIT1**: 99（固定値）
- **CPDD_SPRIT2**: 同一CPDD_PRD_ID、CPDD_LOTの最大値+1
- **CPDD_RANK**: 1（固定値）
- **CPDD_QTY**: GPRR_QTY
- **CPDD_FLG**: 0（固定値）
- **CPDD_PCD_ID**: 0（固定値）

#### CSHK_DAT作成ルール
- **CSHK_KBN**: 1（固定値）
- **CSHK_TO**: 604（固定値）
- **CSHK_PRC_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定
- **CSHK_PRD_ID**: CPDD_PRD_IDと同じ値
- **CSHK_DT**: GPRR_REQ_DATE
- **CSHK_ORD_DT**: GPRR_REQ_DATE
- **CSHK_PDD_ID**: 作成されたCPDD_ID
- **CSHK_RCP_ID**: NULL
- **CSHK_QTY**: GPRR_QTY
- **CSHK_FLG**: 0（固定値）

### 2. GPRC_DAT → CPRC_DAT・CPRD_DAT（REQ_TO=1）

#### 抽出条件
- GPRC_REQ_TO = 1

#### 依存関係
- GPRR_DATのCSHK_DATレコードが事前に作成されている必要があります

#### CPRC_DAT作成ルール
- **CPCD_SHK_ID**: CSHK_ID（GPRR_IDとCSHK_PDD_IDで関連付け）
- **CPCD_DATE**: GPRC_DATE
- **CPCD_QTY**: GPRC_QTY
- **CPCD_RET_NG_QTY**: GPRC_RET_NG_QTY
- **CPCD_INS_NG_QTY**: GPRC_INS_NG_QTY
- **CPCD_PASS_QTY**: GPRC_PASS_QTY

#### CPRD_DAT作成ルール（GPRC用）
- **CPDD_PRD_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定（GPRC用マッピング）
- **CPDD_LOT**: GPRC_DATEをYYMMDD形式で数値変換
- **CPDD_SPRIT1**: 99（固定値）
- **CPDD_SPRIT2**: 同一CPDD_PRD_ID、CPDD_LOTの最大値+1
- **CPDD_RANK**: 1（固定値）
- **CPDD_QTY**: GPRC_PASS_QTY
- **CPDD_FLG**: 0（固定値）
- **CPDD_PCD_ID**: 作成されたCPCD_ID

### 3. GSHK_DAT → CSHK_DAT（GSHK_TO=2）

#### 抽出条件
- GSHK_TO = 2
- GSHK_STC_ID = GPRC_ID

#### 依存関係
- GPRC_DAT（REQ_TO=1）のCPRD_DATレコードが事前に作成されている必要があります

#### CSHK_DAT作成ルール（GSHK_TO=2用）
- **CSHK_KBN**: 1（固定値）
- **CSHK_TO**: 602（固定値）
- **CSHK_PRC_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定（GSHK用マッピング）
- **CSHK_PRD_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定（GSHK用マッピング）
- **CSHK_DT**: GSHK_DT
- **CSHK_ORD_DT**: GSHK_ORD_DT
- **CSHK_PDD_ID**: GPRC用CPRD_DATのCPDD_ID
- **CSHK_RCP_ID**: NULL
- **CSHK_QTY**: GSHK_QTY
- **CSHK_FLG**: 0（固定値）

### 4. GSHK_DAT → CSHK_DAT（GSHK_TO=3）

#### 抽出条件
- GSHK_TO = 3
- GSHK_STC_ID = GPRC_ID

#### 依存関係
- GPRC_DAT（REQ_TO=1）のCPRD_DATレコードが事前に作成されている必要があります

#### CSHK_DAT作成ルール（GSHK_TO=3用）
- **CSHK_KBN**: 0（固定値）
- **CSHK_TO**: 501（固定値）
- **CSHK_PRC_ID**: 0（固定値）
- **CSHK_PRD_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定（GSHK_TO=3用マッピング）
- **CSHK_DT**: GSHK_DT
- **CSHK_ORD_DT**: GSHK_ORD_DT
- **CSHK_PDD_ID**: GPRC用CPRD_DATのCPDD_ID
- **CSHK_RCP_ID**: NULL
- **CSHK_QTY**: GSHK_QTY
- **CSHK_FLG**: 0（固定値）

### 5. GPRC_DAT → CPRC_DAT・CPRD_DAT（REQ_TO=2）

#### 抽出条件
- GPRC_REQ_TO = 2
- GPRC_SHK_ID = GSHK_ID

#### 依存関係
- GSHK_DATのCSHK_DATレコードが事前に作成されている必要があります

#### CPRC_DAT作成ルール
- **CPCD_SHK_ID**: CSHK_ID（GSHK_IDとCSHK_PDD_IDで関連付け）
- **CPCD_DATE**: GPRC_DATE
- **CPCD_QTY**: GPRC_QTY
- **CPCD_RET_NG_QTY**: GPRC_RET_NG_QTY
- **CPCD_INS_NG_QTY**: GPRC_INS_NG_QTY
- **CPCD_PASS_QTY**: GPRC_PASS_QTY

#### CPRD_DAT作成ルール（GPRC_REQ_TO=2用）
- **CPDD_PRD_ID**: GPRR_SPECとGPRR_COLORの組み合わせで決定（GPRC_REQ_TO=2用マッピング）
- **CPDD_LOT**: GPRC_DATEをYYMMDD形式で数値変換
- **CPDD_SPRIT1**: 99（固定値）
- **CPDD_SPRIT2**: 同一CPDD_PRD_ID、CPDD_LOTの最大値+1
- **CPDD_RANK**: 1（固定値）
- **CPDD_QTY**: GPRC_PASS_QTY
- **CPDD_FLG**: 0（固定値）
- **CPDD_PCD_ID**: 作成されたCPCD_ID

## 製品ID・加工IDマッピング

### 1. GPRR用製品IDマッピング（CPDD_PRD_ID）
| GPRR_SPEC | GPRR_COLOR | CPDD_PRD_ID |
|-----------|------------|-------------|
| 1 | 1 | K748 |
| 1 | 2 | K753 |
| 1 | 3 | K748 |
| 1 | 4 | K748 |
| 1 | 5 | K748 |
| 2 | 1 | K749 |
| 2 | 2 | K754 |
| 2 | 3 | K749 |
| 2 | 4 | K749 |
| 2 | 5 | K749 |
| 3 | 1 | K750 |
| 3 | 2 | K755 |
| 3 | 3 | K750 |
| 3 | 4 | K750 |
| 3 | 5 | K750 |
| 4 | 1 | K751 |
| 4 | 2 | K756 |
| 4 | 3 | K751 |
| 4 | 4 | K751 |
| 4 | 5 | K751 |

### 2. GPRC用製品IDマッピング（CPDD_PRD_ID）
| GPRR_SPEC | GPRR_COLOR | CPDD_PRD_ID |
|-----------|------------|-------------|
| 1 | 1 | 2252 |
| 1 | 2 | 2247 |
| 1 | 3 | 2267 |
| 1 | 4 | 2262 |
| 1 | 5 | 2257 |
| 2 | 1 | 2253 |
| 2 | 2 | 2248 |
| 2 | 3 | 2268 |
| 2 | 4 | 2263 |
| 2 | 5 | 2258 |
| 3 | 1 | 2254 |
| 3 | 2 | 2249 |
| 3 | 3 | 2269 |
| 3 | 4 | 2264 |
| 3 | 5 | 2259 |
| 4 | 1 | 2255 |
| 4 | 2 | 2250 |
| 4 | 3 | 2270 |
| 4 | 4 | 2265 |
| 4 | 5 | 2260 |

### 3. GPRC_REQ_TO=2用製品IDマッピング（CPDD_PRD_ID）
| GPRR_SPEC | GPRR_COLOR | CPDD_PRD_ID |
|-----------|------------|-------------|
| 1 | 1 | 2026 |
| 1 | 2 | 2027 |
| 1 | 3 | 2028 |
| 1 | 4 | 2030 |
| 1 | 5 | 2029 |
| 2 | 1 | 2031 |
| 2 | 2 | 2032 |
| 2 | 3 | 2033 |
| 2 | 4 | 2035 |
| 2 | 5 | 2034 |
| 3 | 1 | 2036 |
| 3 | 2 | 2037 |
| 3 | 3 | 2038 |
| 3 | 4 | 2040 |
| 3 | 5 | 2039 |
| 4 | 1 | 2041 |
| 4 | 2 | 2042 |
| 4 | 3 | 2043 |
| 4 | 4 | 2045 |
| 4 | 5 | 2044 |

### 4. 加工IDマッピング（CSHK_PRC_ID）
| GPRR_SPEC | GPRR_COLOR | CSHK_PRC_ID |
|-----------|------------|-------------|
| 1 | 1 | 252 |
| 1 | 2 | 247 |
| 1 | 3 | 267 |
| 1 | 4 | 262 |
| 1 | 5 | 257 |
| 2 | 1 | 253 |
| 2 | 2 | 248 |
| 2 | 3 | 268 |
| 2 | 4 | 263 |
| 2 | 5 | 258 |
| 3 | 1 | 254 |
| 3 | 2 | 249 |
| 3 | 3 | 269 |
| 3 | 4 | 264 |
| 3 | 5 | 259 |
| 4 | 1 | 255 |
| 4 | 2 | 250 |
| 4 | 3 | 270 |
| 4 | 4 | 265 |
| 4 | 5 | 260 |

### 5. GSHK用製品ID・加工IDマッピング（CSHK_PRD_ID・CSHK_PRC_ID）
| GPRR_SPEC | GPRR_COLOR | CSHK_PRD_ID | CSHK_PRC_ID |
|-----------|------------|-------------|-------------|
| 1 | 1 | 2252 | 26 |
| 1 | 2 | 2247 | 27 |
| 1 | 3 | 2267 | 28 |
| 1 | 4 | 2262 | 30 |
| 1 | 5 | 2257 | 29 |
| 2 | 1 | 2253 | 31 |
| 2 | 2 | 2248 | 32 |
| 2 | 3 | 2268 | 33 |
| 2 | 4 | 2263 | 35 |
| 2 | 5 | 2258 | 34 |
| 3 | 1 | 2254 | 36 |
| 3 | 2 | 2249 | 37 |
| 3 | 3 | 2269 | 38 |
| 3 | 4 | 2264 | 40 |
| 3 | 5 | 2259 | 39 |
| 4 | 1 | 2255 | 41 |
| 4 | 2 | 2250 | 42 |
| 4 | 3 | 2270 | 43 |
| 4 | 4 | 2265 | 45 |
| 4 | 5 | 2260 | 44 |

### 6. GSHK_TO=3用製品IDマッピング（CSHK_PRD_ID）
| GPRR_SPEC | GPRR_COLOR | CSHK_PRD_ID |
|-----------|------------|-------------|
| 1 | 1 | 2026 |
| 1 | 2 | 2027 |
| 1 | 3 | 2028 |
| 1 | 4 | 2030 |
| 1 | 5 | 2029 |
| 2 | 1 | 2031 |
| 2 | 2 | 2032 |
| 2 | 3 | 2033 |
| 2 | 4 | 2035 |
| 2 | 5 | 2034 |
| 3 | 1 | 2036 |
| 3 | 2 | 2037 |
| 3 | 3 | 2038 |
| 3 | 4 | 2040 |
| 3 | 5 | 2039 |
| 4 | 1 | 2041 |
| 4 | 2 | 2042 |
| 4 | 3 | 2043 |
| 4 | 4 | 2045 |
| 4 | 5 | 2044 |

## 使用方法

### 1. 環境準備
- Python 3.7以上
- 必要なパッケージのインストール（requirements.txtに記載）
- データベース接続設定（.envファイル）

### 2. 実行前の確認事項
- データベースのバックアップを取得
- GPRR_DATテーブルにデータが存在することを確認
- CPRD_DAT、CSHK_DATテーブルが存在することを確認

### 3. プログラム実行
```bash
python gradation_migration.py
```

### 4. 実行結果の確認
- コンソール出力で成功件数・失敗件数を確認
- ログファイル（gradation_migration.log）で詳細を確認
- 失敗がある場合はfailed_migration_*.txtファイルで詳細を確認

## 安全機能

### トランザクション管理
- 移行処理は単一トランザクションで実行
- エラーが発生した場合は全件ロールバック
- データの整合性を保証

### エラーハンドリング
- 個別レコードのエラーは記録して処理継続
- 致命的エラーの場合は即座にロールバック
- 詳細なエラーログを出力

### 失敗レコードの出力
- 失敗したレコードの詳細情報をファイルに保存
- エラー内容と原因を明確に記録
- 再実行時の参考資料として活用可能

## 注意事項

### 実行前
- **必ずデータベースのバックアップを取得してください**
- 本番環境での実行は慎重に行ってください
- テスト環境での事前検証を推奨します

### 実行中
- プログラム実行中は他の処理を停止してください
- 大量データの場合は処理時間がかかる可能性があります
- ログファイルで進捗状況を確認してください

### 実行後
- 移行結果を必ず確認してください
- 失敗レコードがある場合は原因を調査してください
- 必要に応じて手動でのデータ修正を行ってください

## トラブルシューティング

### よくあるエラーと対処法

1. **データベース接続エラー**
   - .envファイルの設定を確認
   - データベースサーバーの状態を確認

2. **テーブル不存在エラー**
   - 必要なテーブルが作成されているか確認
   - テーブル名の大文字小文字を確認

3. **マッピングエラー**
   - GPRR_SPEC、GPRR_COLORの値が想定範囲内か確認
   - マッピングテーブルの設定を確認

4. **制約違反エラー**
   - 既存データとの重複がないか確認
   - 外部キー制約を確認

## ログファイル

### gradation_migration.log
- 実行ログの詳細
- エラー情報
- 進捗状況

### failed_migration_*.txt
- 失敗したレコードの詳細
- エラー内容
- 再実行時の参考資料

## サポート
プログラムの実行で問題が発生した場合は、以下を確認してください：
1. ログファイルの内容
2. データベースの状態
3. 環境設定ファイル
4. エラーメッセージの詳細
