/* 加工マスタ */
CREATE TABLE CPRC_MST (
    CPRC_ID decimal(5) NOT NULL identity(1,1)   /* 加工ID */,
    CPRC_NM nvarchar(40)    /* 加工名 */,
    CPRC_PRD_NM nvarchar(40)    /* 加工前製品名 */,
    CPRC_TO decimal(3)    /* 加工依頼先 */,
    CPRC_TIME decimal(2)    /* 加工日数 */,
    CPRC_FLG decimal(1)    /* フラグ */,
    CPRC_PRD_ID varchar(5)    /* 製品ID */,
    CPRC_AF_PRD_ID varchar(5)    /* 加工後製品ID */,

    PRIMARY KEY (CPRC_ID)
);

alter table CPRC_MST add CPRC_AF_PRD_ID varchar(5);
alter table CPRC_MST add CPRC_PRD_NM nvarchar(40);
alter table CPRC_MST drop column CPRC_RYAKU;