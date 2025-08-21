/* グラデ出荷データ */
CREATE TABLE GSHK_DAT (
    GSHK_ID decimal(10)  identity(1,1)  /* ID */,
    GSHK_STC_ID decimal(10)    /* 在庫ID */,
    GSHK_TO decimal(2)    /* 出荷先 */,
    GSHK_DT datetime2    /* 出荷日 */,
    GSHK_ORD_DT datetime2    /* 手配日 */,
    GSHK_QTY decimal(10)    /* 数量 */,
    GSHK_FLG decimal(10)    /* フラグ */,
    GSHK_REQ_ID decimal(10)    /* 依頼ID */,

    PRIMARY KEY (GSHK_ID)

);