CREATE FUNCTION [dbo].[Get_CPRD_ZAN_Qty] (@CPDD_ID decimal(10))
RETURNS decimal(5)
AS
BEGIN
    DECLARE @CPDD_QTY decimal(5);
    DECLARE @CSHK_SUM_QTY decimal(5);
    DECLARE @Remaining_Qty decimal(5);

    -- CPDD_QTYの取得（入庫数量）
    SELECT @CPDD_QTY = CPDD_QTY
    FROM CPRD_DAT
    WHERE CPDD_ID = @CPDD_ID;

    -- 出荷数量の合計取得（NULL対策としてISNULL使用）
    SELECT @CSHK_SUM_QTY = ISNULL(SUM(CSHK_QTY), 0)
    FROM CSHK_DAT
    WHERE CSHK_PDD_ID = @CPDD_ID;

    -- 差分を計算（入庫数量 - 出荷数量 = 在庫残数量）
    SET @Remaining_Qty = @CPDD_QTY - @CSHK_SUM_QTY;

    RETURN @Remaining_Qty;
END; 