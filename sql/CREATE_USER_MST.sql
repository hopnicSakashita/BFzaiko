/* ユーザーマスタ */
CREATE TABLE USER_MST (
    USER_ID varchar(20) NOT NULL    /* ID */,
    USER_NM nvarchar(50) NOT NULL    /* ユーザー名 */,
    USER_PW varchar(255) NOT NULL    /* パスワード */,
    USER_FLG decimal(1)    /* フラグ */,

    PRIMARY KEY (USER_ID)
);