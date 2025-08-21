/* 出荷データ */
CREATE TABLE CSHK_DAT (
    CSHK_ID decimal(10) NOT NULL identity(1,1)    /* 出荷ID */,
    CSHK_KBN decimal(1)    /* 出荷区分 */,
    CSHK_TO decimal(3)    /* 出荷先ID */,
    CSHK_PRC_ID decimal(10)    /* 加工ID */,
    CSHK_PRD_ID varchar(5)    /* 製品ID */,
    CSHK_DT datetime2    /* 出荷日 */,
    CSHK_ORD_DT datetime2    /* 手配日 */,
    CSHK_PDD_ID decimal(10)    /* 製造ID */,
    CSHK_RCP_ID decimal(10)    /* 受注ID */,
    CSHK_QTY decimal(5)    /* 数量 */,
    CSHK_FLG decimal(1)    /* フラグ */,

    PRIMARY KEY (CSHK_ID)
);
