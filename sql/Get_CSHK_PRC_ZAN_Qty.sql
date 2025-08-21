CREATE FUNCTION [dbo].[Get_CSHK_PRC_ZAN_Qty] (@CSHK_ID decimal(10))
RETURNS decimal(5)
AS
BEGIN
    DECLARE @CSHK_QTY decimal(5);
    DECLARE @CPRC_SUM_QTY decimal(5);
    DECLARE @Remaining_Qty decimal(5);

    -- CSHK_QTYの取得
    SELECT @CSHK_QTY = CSHK_QTY
    FROM CSHK_DAT
    WHERE CSHK_ID = @CSHK_ID;

    -- 戻り数量の合計取得（NULL対策としてISNULL使用）
    SELECT @CPRC_SUM_QTY = ISNULL(SUM(CPCD_QTY), 0)
    FROM CPRC_DAT
    WHERE CPCD_SHK_ID = @CSHK_ID;

    -- 差分を計算（加工から戻ってきていない残数）
    SET @Remaining_Qty = @CSHK_QTY - @CPRC_SUM_QTY;

    RETURN @Remaining_Qty;
END; 