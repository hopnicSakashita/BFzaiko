INSERT INTO CZTR_MST 
    (CZTR_ID, CZTR_NM, CZTR_FULL_NM, CZTR_TANTO_NM, CZTR_KBN, CZTR_FLG, CZTR_TYP)
VALUES
    -- 加工宛
    (601, N'自社', N'石田第２工場', NULL, 2, 0, 2),
    
    -- コロンバス
    (501, N'コロンバス', N'サンレーコロンバス', NULL, 1, 0, 2),
    
    -- ダラス
    (502, N'ダラス', N'サンレーダラス', NULL, 1, 0, 2),
    
    -- ヤンガー
    (503, N'ヤンガーUS', N'ヤンガーアメリカ', NULL, 1, 0, 2),
    
    -- ヤンガーヨーロッパ
    (504, N'ヤンガーEU', N'ヤンガーヨーロッパ', NULL, 1, 0, 2),
    
    -- 欠損
    (999, N'欠損', N'欠損', NULL, 3, 0, 2);