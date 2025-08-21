/* バーコード取込 */
CREATE TABLE BBCD_DAT (
    BBCD_ID nvarchar(3) NOT NULL    /* ID */,
    BBCD_NO varchar(60)    /* バーコード */,
    BBCD_NM nvarchar(30)    /* バーコード名 */,
    BBCD_KBN decimal(1)    /* 区分 */,

    PRIMARY KEY (BBCD_ID, BBCD_KBN)
);
