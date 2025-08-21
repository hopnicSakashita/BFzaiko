/* 加工データ */
CREATE TABLE CPRC_DAT (
    CPCD_ID decimal(10) NOT NULL identity(1,1)   /* ID */,
    CPCD_SHK_ID decimal(10)    /* 出荷ID */,
    CPCD_DATE datetime2    /* 戻り日 */,
    CPCD_QTY decimal(5)    /* 戻り数 */,
    CPCD_RET_NG_QTY decimal(5)    /* 戻り不良数 */,
    CPCD_INS_NG_QTY decimal(5)    /* 検品不良数 */,
    CPCD_PASS_QTY decimal(5)    /* 合格数 */,

    PRIMARY KEY (CPCD_ID)
);