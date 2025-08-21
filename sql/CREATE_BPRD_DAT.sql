/* 製造データ */
CREATE TABLE BPRD_DAT (
    BPDD_ID decimal(8) identity(1,1)    /* 製造ID */,
    BPDD_PROC decimal(1)    /* 加工ID */,
    BPDD_PRD_ID varchar(4)    /* 製品ID */,
    BPDD_LOT decimal(6)    /* LOT */,
    BPDD_QTY decimal(5)    /* 数量 */,
    BPDD_FLG decimal(1)    /* フラグ */,
    BPDD_CRT decimal(6)    /* コート日 */,
    PRIMARY KEY (BPDD_ID)
);