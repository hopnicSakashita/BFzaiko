/* グラデ加工依頼データ */
CREATE TABLE GPRR_DAT (
    GPRR_ID decimal(10)   identity(1,1) /* ID */,
    GPRR_SPEC decimal(2)    /* 規格 */,
    GPRR_COLOR decimal(2)    /* 色 */,
    GPRR_REQ_TO decimal(1)    /* 依頼先 */,
    GPRR_REQ_DATE datetime2    /* 依頼日 */,
    GPRR_QTY decimal(5)    /* 数量 */,


    PRIMARY KEY (GPRR_ID)

);

CREATE UNIQUE INDEX IX_GPRR_DAT 
ON GPRR_DAT(GPRR_SPEC,GPRR_COLOR,GPRR_REQ_TO,GPRR_REQ_DATE);

