%sql
-- 1. テーブル作成
CREATE OR REPLACE TABLE default.redshift_count AS
SELECT
  COUNT(*) AS redshift_count
FROM default.joined_cur_master
WHERE `product/ProductName` LIKE '%Redshift%';