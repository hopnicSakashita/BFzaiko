/* 製造データ */
CREATE TABLE CPRD_DAT (
    CPDD_ID decimal(10) NOT NULL identity(1,1)    /* 製造ID */,
    CPDD_PRD_ID varchar(5)    /* 製品ID */,
    CPDD_LOT decimal(6)    /* LOT */,
    CPDD_SPRIT1 decimal(2)    /* 分割番号1 */,
    CPDD_SPRIT2 decimal(2)    /* 分割番号2 */,
    CPDD_RANK decimal(2)    /* ランク */,
    CPDD_QTY decimal(5)    /* 数量 */,
    CPDD_FLG decimal(1)    /* フラグ */,
    CPDD_PCD_ID decimal(10)    /* 加工データID */,    

    PRIMARY KEY (CPDD_ID)
);

