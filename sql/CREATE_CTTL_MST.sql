/* 集計グループマスタ */
CREATE TABLE CTTL_MST (
    CTTL_ID decimal(5) NOT NULL    /* グループID */,
    CTTL_PRD_ID varchar(5) NOT NULL    /* 製品ID */,
    CTTL_G_NM nvarchar(20)    /* グループ名 */,
    CTTL_COL_NM nvarchar(20)    /* 列名 */,
    CTTL_ROW_NM nvarchar(20)    /* 行名 */,
    CTTL_COL_KEY decimal(2)    /* 列キー */,
    CTTL_ROW_KEY decimal(2)    /* 行キー */,

    PRIMARY KEY (CTTL_ID, CTTL_PRD_ID)
);