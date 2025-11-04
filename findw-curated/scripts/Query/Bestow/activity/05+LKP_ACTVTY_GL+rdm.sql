SELECT ACTVTY_GL_SRC_CD AS activitygeneralledgersourcecode,
       ACTVTY_GL_APP_AREA_CD AS activitygeneralledgerapplicationareacode,
       ACTVTY_SRC_SYS_NM
FROM TIME_SCH.LKP_ACTVTY_GL
WHERE ACTVTY_SRC_SYS_NM = 'Bestow'
