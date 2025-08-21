CREATE FUNCTION [dbo].[Get_GSHK_GPRC_Diff] (@GSHK_ID decimal(10))
RETURNS decimal(10)
AS
BEGIN
    DECLARE @GSHK_QTY decimal(10);
    DECLARE @GPRC_SUM_QTY decimal(10);
    DECLARE @Difference decimal(10);

    -- GSHK_QTYの取得
    SELECT @GSHK_QTY = GSHK_QTY
    FROM GSHK_DAT
    WHERE GSHK_ID = @GSHK_ID;

    -- 関連するGPRCデータの戻り数合計を取得
    -- GSHK_ID = GPRC_SHK_IDで関連付け
    SELECT @GPRC_SUM_QTY = ISNULL(SUM(GPRC_QTY), 0)
    FROM GPRC_DAT
    WHERE GPRC_SHK_ID = @GSHK_ID;

    -- 差分を計算（GSHK_QTY - SUM(GPRC_QTY)）
    SET @Difference = @GSHK_QTY - @GPRC_SUM_QTY;

    RETURN @Difference;
END; 