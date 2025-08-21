/* 加工集計グループマスタ */
CREATE TABLE CPRG_MST (
    CPRG_ID varchar(5) NOT NULL    /* グループID */,
    CPRG_PRD_ID varchar(5) NOT NULL    /* 製品ID */,
    CPRG_PRC_ID decimal(5) NOT NULL    /* 加工ID */,
    CPRG_G_NM nvarchar(20)    /* グループ名 */,
    CPRG_COL_NM nvarchar(20)    /* 列名 */,
    CPRG_ROW_NM nvarchar(20)    /* 行名 */,
    CPRG_AF_PRD_ID varchar(5)    /* 加工後製品ID */,
    CPRG_COL_KEY decimal(2)    /* 列キー */,
    CPRG_ROW_KEY decimal(2)    /* 行キー */,

    PRIMARY KEY (CPRG_ID, CPRG_PRD_ID, CPRG_PRC_ID)
);

alter table CPRG_MST add CPRG_COL_KEY decimal(2);
alter table CPRG_MST add CPRG_ROW_KEY decimal(2);