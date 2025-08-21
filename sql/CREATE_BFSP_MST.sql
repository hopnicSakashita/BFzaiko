/* 規格マスタ */
CREATE TABLE BFSP_MST (
    BFSP_PRD_ID varchar(4) NOT NULL    /* 製品ID */,
    BFSP_MONO decimal(2)    /* モノマー */,
    BFSP_BASE decimal(1)    /* ベース */,
    BFSP_ADP decimal(3)    /* 加入度数 */,
    BFSP_LR varchar(1)    /* L/R */,
    BFSP_CLR varchar(2)    /* 色 */,
    BFSP_SORT decimal(3)    /* ソート順 */,
    BFSP_S_NC varchar(15)    /* サンレーNC */,
    BFSP_S_HC varchar(15)    /* サンレーHC */,
    BFSP_Y_BCD varchar(15)    /* ヤンガーBCD */,
    BFSP_Y_GTIN varchar(15)    /* ヤンガーGTIN */,
    PRIMARY KEY (BFSP_PRD_ID)
);