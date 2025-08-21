/* 製造明細 */
CREATE TABLE BPRD_MEI (
    BPDM_ID decimal(8) identity(1,1)    /* 明細ID */,
    BPDM_PRD_ID varchar(4)    /* 製品ID */,
    BPDM_LOT decimal(6)    /* LOT */,
    BPDM_NO decimal(4)    /* 分割番号 */,
    BPDM_QTY decimal(5)    /* 数量 */,
    PRIMARY KEY (BPDM_ID)
);