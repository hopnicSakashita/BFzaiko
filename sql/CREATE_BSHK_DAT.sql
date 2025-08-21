/* 出荷データ */
CREATE TABLE BSHK_DAT (
    BSHK_ID decimal(10) identity(1,1)    /* 出荷ID */,
    BSHK_TO decimal(3)    /* 出荷先ID */,
    BSHK_PDD_ID decimal(8)    /* 製造ID */,
    BSHK_RCP_ID decimal(10)    /* 受注ID */,
    BSHK_DT datetime2    /* 出荷日 */,
    BSHK_QTY decimal(5)    /* 数量 */,
    BSHK_FLG decimal(1)    /* フラグ */,
    BSHK_ORD_DT datetime2    /* 手配日 */,

    PRIMARY KEY (BSHK_ID)
);
