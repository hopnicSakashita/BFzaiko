CREATE FUNCTION [dbo].[Get_GRD_PRC_ZAN_Qty] (@GPRR_ID decimal(10))
RETURNS decimal(5)
AS
BEGIN
    DECLARE @GPRR_QTY decimal(5);
    DECLARE @GPRC_SUM_QTY decimal(5);
    DECLARE @Remaining_Qty decimal(5);

    -- GPRR_QTYの取得
    SELECT @GPRR_QTY = GPRR_QTY
    FROM GPRR_DAT
    WHERE GPRR_ID = @GPRR_ID;

    -- 出荷数量の合計取得（NULL対策としてISNULL使用）
    SELECT @GPRC_SUM_QTY = ISNULL(SUM(GPRC_QTY), 0)
    FROM GPRC_DAT
    WHERE GPRC_REQ_ID = @GPRR_ID
    AND GPRC_REQ_TO = 1;

    -- 差分を計算
    SET @Remaining_Qty = @GPRR_QTY - @GPRC_SUM_QTY;

    RETURN @Remaining_Qty;
END;


