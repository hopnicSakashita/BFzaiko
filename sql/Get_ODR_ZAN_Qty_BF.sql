CREATE FUNCTION [dbo].[Get_ODR_ZAN_Qty_BF] (@BRCP_ID decimal(10))
RETURNS decimal(5)
AS
BEGIN
    DECLARE @BRCP_QTY decimal(5);
    DECLARE @BSHK_SUM_QTY decimal(5);
    DECLARE @Remaining_Qty decimal(5);

    -- BRCP_QTYの取得
    SELECT @BRCP_QTY = BRCP_QTY
    FROM BRCP_DAT
    WHERE BRCP_ID = @BRCP_ID;

    -- 出荷数量の合計取得（NULL対策としてISNULL使用）
    SELECT @BSHK_SUM_QTY = ISNULL(SUM(BSHK_QTY), 0)
    FROM BSHK_DAT
    WHERE BSHK_RCP_ID = @BRCP_ID;

    -- 差分を計算
    SET @Remaining_Qty = @BRCP_QTY - @BSHK_SUM_QTY;

    RETURN @Remaining_Qty;
END;


