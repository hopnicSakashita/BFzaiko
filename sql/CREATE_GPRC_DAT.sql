/* グラデ加工データ */
CREATE TABLE GPRC_DAT (
    GPRC_ID decimal(10)  identity(1,1)  /* ID */,
    GPRC_REQ_ID decimal(10)    /* 依頼ID */,
    GPRC_REQ_TO decimal(1)    /* 依頼先 */,
    GPRC_DATE datetime2    /* 戻り日 */,
    GPRC_QTY decimal(5)    /* 戻り数 */,
    GPRC_RET_NG_QTY decimal(5)    /* 戻り不良数 */,
    GPRC_INS_NG_QTY decimal(5)    /* 検品不良数 */,
    GPRC_SHK_ID decimal(10)    /* 出荷ID */,
    GPRC_PASS_QTY decimal(5)    /* 合格数 */,
    GPRC_STS decimal(1)    /* ステータス */,
    
    PRIMARY KEY (GPRC_ID)

);