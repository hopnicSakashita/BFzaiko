/* 製品マスタ */
CREATE TABLE PRD_MST (
    PRD_ID varchar(5) NOT NULL    /* 製品ID */,
    PRD_MONOMER nvarchar(30)    /* モノマー */,
    PRD_NAME nvarchar(40)    /* 呼び名 */,
    PRD_LOWER_DIE nvarchar(20)    /* 下型 */,
    PRD_UPPER_DIE nvarchar(20)    /* 上型 */,
    PRD_FILM_COLOR nvarchar(20)    /* 膜カラー */,
    PRD_KBN decimal(1)    /* 商品分類 */,
    PRD_FLG decimal(1)    /* フラグ */,
    PRD_DSP_NM nvarchar(70)    /* 表示名 */,

    PRIMARY KEY (PRD_ID)
);