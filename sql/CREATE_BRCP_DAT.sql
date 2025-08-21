/* 受注データ */
CREATE TABLE BRCP_DAT (
    BRCP_ID decimal(10) NOT NULL identity(1,1)    /* 受注ID */,
    BRCP_DT datetime2    /* 受注日 */,
    BRCP_PRD_ID varchar(4)    /* 製品ID */,
    BRCP_PROC decimal(1)    /* 加工ID */,
    BRCP_ORDER_CMP decimal(3)    /* 出荷先 */,
    BRCP_ORDER_NO decimal(10)    /* 客先受注番号 */,
    BRCP_QTY decimal(5)    /* 数量 */,
    BRCP_FLG decimal(1)    /* フラグ */,

    PRIMARY KEY (BRCP_ID)
);