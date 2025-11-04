SELECT Heat_Map_Output_Name,
       Age_Low,
       Age_High,
       Heat_Map_Start_Date,
       Heat_Map_End_Date,
       PRODUCT_NAME,
       GENDER,
       RISK_CLASS,
       FACE_AMOUNT_LOW,
       FACE_AMOUNT_HIGH,
       IFRS17_PRFTBLY,
       PREMIUM_MODE
FROM TIME_SCH.LKP_HT_MP_IFRS17_PRFTBLY
WHERE Heat_Map_Output_Name LIKE 'FE'
