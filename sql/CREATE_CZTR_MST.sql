/* 取引先マスタ */
CREATE TABLE CZTR_MST (
    CZTR_ID decimal(3) NOT NULL    /* 取引先ID */,
    CZTR_NM nvarchar(40)    /* 取引先名 */,
    CZTR_FULL_NM nvarchar(80)    /* 取引先名正式名称 */,
    CZTR_TANTO_NM nvarchar(20)    /* 担当者名 */,
    CZTR_KBN decimal(2)    /* 区分 */,
    CZTR_FLG decimal(1)    /* フラグ */,
    CZTR_TYP decimal(2)    /* タイプ */,

    PRIMARY KEY (CZTR_ID)
);
