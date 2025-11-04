SELECT a.*
FROM (SELECT *
      FROM {source_database}.gdqvantageoneacctctr
      WHERE batchid = '{batchidvantage}') a
LEFT JOIN (SELECT *
           FROM {source_database}.gdqvantageoneacctctr
           WHERE batchid = '{batchidvantagetwo}') b
       ON a.company = b.company
      AND a.plan = b.plan
      AND a.mgt_code = b.mgt_code
WHERE generatesameuuid(concat(coalesce(a.center_number,'NULL'), coalesce(a.c_center_number,'NULL'), coalesce(a.center_number_isvalid,'NULL'), coalesce(a.center_number_override,'NULL')))
      <>
      generatesameuuid(concat(coalesce(b.center_number,'NULL'), coalesce(b.c_center_number,'NULL'), coalesce(b.center_number_isvalid,'NULL'), coalesce(b.center_number_override,'NULL')));
