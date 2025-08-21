CREATE FUNCTION [dbo].[Get_Zaiko_Qty_BF] (@BPDD_ID decimal(8))
RETURNS decimal(5)
AS
BEGIN
    DECLARE @BPDD_QTY decimal(5);
    DECLARE @BSHK_SUM_QTY decimal(5);
    DECLARE @Remaining_Qty decimal(5);

    -- BPDD_QTYの取得
    SELECT @BPDD_QTY = BPDD_QTY
    FROM BPRD_DAT
    WHERE BPDD_ID = @BPDD_ID;

    -- 出荷数量の合計取得（NULL対策としてISNULL使用）
    SELECT @BSHK_SUM_QTY = ISNULL(SUM(BSHK_QTY), 0)
    FROM BSHK_DAT
    WHERE BSHK_PDD_ID = @BPDD_ID;

    -- 差分を計算
    SET @Remaining_Qty = @BPDD_QTY - @BSHK_SUM_QTY;

    RETURN @Remaining_Qty;
END;

