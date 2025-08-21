/* 取引先製品マスタ */
CREATE TABLE CTPD_MST (
    CTPD_ID decimal(10) NOT NULL identity(1,1)    /* 取引先製品ID */,
    CTPD_ZTR_ID decimal(3)    /* 取引先ID */,
    CTPD_PRD_ID varchar(5)    /* 製品ID */,
    CTPD_RANK decimal(2)    /* ランク */,
    CTPD_NM nvarchar(100)    /* 製品名 */,
    CTPD_SPC nvarchar(100)    /* 規格 */,
    CTPD_FRG decimal(1)    /* フラグ */,

    PRIMARY KEY (CTPD_ID)
);

CREATE UNIQUE INDEX IX_CTPD_MST 
ON CTPD_MST(CTPD_ZTR_ID,CTPD_PRD_ID,CTPD_RANK);
