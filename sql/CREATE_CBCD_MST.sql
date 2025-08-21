/* バーコードマスタ */
CREATE TABLE CBCD_MST (
    CBCD_ID decimal(10) NOT NULL identity(1,1)   /* ID */,
    CBCD_PRD_ID varchar(5)    /* 製品ID */,
    CBCD_TO decimal(3)    /* 出荷先ID */,
    CBCD_NM nvarchar(100)    /* 製品名 */,
    CBCD_NO1 varchar(60)    /* バーコード１ */,
    CBCD_NO2 varchar(60)    /* バーコード２ */,

    PRIMARY KEY (CBCD_ID)
);