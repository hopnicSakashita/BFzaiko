CREATE FUNCTION [dbo].[Get_GRD_SHK_ZAN_Qty] (@GPRC_ID decimal(10))
RETURNS decimal(5)
AS
BEGIN
    DECLARE @GPRC_PASS_QTY decimal(5);
    DECLARE @GSHK_SUM_QTY decimal(5);
    DECLARE @Remaining_Qty decimal(5);

    -- GPRC_PASS_QTYの取得
    SELECT @GPRC_PASS_QTY = ISNULL(GPRC_PASS_QTY, 0)
    FROM GPRC_DAT
    WHERE GPRC_ID = @GPRC_ID;

    -- 出庫数量の合計取得（GSHK_STC_ID = GPRC_IDで関連付け）
    SELECT @GSHK_SUM_QTY = ISNULL(SUM(GSHK_QTY), 0)
    FROM GSHK_DAT
    WHERE GSHK_STC_ID = @GPRC_ID;

    -- 差分を計算
    SET @Remaining_Qty = @GPRC_PASS_QTY - @GSHK_SUM_QTY;

    RETURN @Remaining_Qty;
END; 