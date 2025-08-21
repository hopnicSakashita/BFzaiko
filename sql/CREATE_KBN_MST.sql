/* 区分マスタ */
CREATE TABLE KBN_MST (
    KBN_ID varchar(10)    /* 区分ID */,
    KBN_NO decimal(2)    /* 区分番号 */,
    KBN_NM nvarchar(50)    /* 区分名 */,
    KBN_FLG decimal(1)    /* フラグ */,

    PRIMARY KEY (KBN_ID, KBN_NO)

);