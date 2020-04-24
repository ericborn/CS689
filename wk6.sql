-- Week 6 assignment
-- eborn
-- CS 689

-- 4.1.1
DROP INDEX IF EXISTS cal_mfr_1 ON calendar_manufacture_dim;
CREATE INDEX cal_mfr_1 ON calendar_manufacture_dim (manufacture_yearmonth) INCLUDE (manufacture_cal_key);

-- 4.1.2
DROP INDEX IF EXISTS cal_mfr_2 ON calendar_manufacture_dim;
CREATE INDEX cal_mfr_2 ON calendar_manufacture_dim (manufacture_cal_key) INCLUDE (manufacture_yearmonth);

-- 4.2.1
DROP INDEX IF EXISTS supply_1 ON supply_fact;
CREATE INDEX supply_1 ON supply_fact (supply_qty) INCLUDE (manufacture_cal_key);

-- 4.2.2
DROP INDEX IF EXISTS supply_2 ON supply_fact;
CREATE INDEX supply_2 ON supply_fact (manufacture_cal_key) INCLUDE (supply_qty);

-- 4.3.1
DROP INDEX IF EXISTS mfr_fact_1 ON manufacture_fact;
CREATE INDEX mfr_fact_1 ON manufacture_fact (qty_passed) INCLUDE (manufacture_cal_key);

-- 4.3.2
DROP INDEX IF EXISTS mfr_fact_2 ON manufacture_fact;
CREATE INDEX mfr_fact_2 ON manufacture_fact (manufacture_cal_key) INCLUDE (qty_passed);

-- 4.4.1
DROP INDEX IF EXISTS cal_mfr_2 ON calendar_manufacture_dim;
DROP INDEX IF EXISTS mfr_fact_2 ON manufacture_fact;
CREATE INDEX cal_mfr_2 ON calendar_manufacture_dim (manufacture_cal_key) INCLUDE (manufacture_yearmonth);
CREATE INDEX mfr_fact_2 ON manufacture_fact (manufacture_cal_key) INCLUDE (qty_passed);

-- 4.4.2
DROP INDEX IF EXISTS supply_2 ON supply_fact;
DROP INDEX IF EXISTS mfr_fact_2 ON manufacture_fact;
CREATE INDEX supply_2 ON supply_fact (manufacture_cal_key) INCLUDE (supply_qty);
CREATE INDEX mfr_fact_2 ON manufacture_fact (manufacture_cal_key) INCLUDE (qty_passed);


SELECT *
FROM
   (
      SELECT cal.manufacture_cal_key, sum(sfac.supply_qty) AS sum_supplies
      FROM supply_fact sfac
      JOIN calendar_manufacture_dim cal on sfac.manufacture_cal_key = cal.manufacture_cal_key
      WHERE cal.manufacture_yearmonth = 'mYM201904'
      GROUP BY cal.manufacture_cal_key
   ) agsupp
      FULL OUTER JOIN
   (
      SELECT cal.manufacture_cal_key, sum(mfac.qty_passed) AS sum_passed
      FROM manufacture_fact mfac
      JOIN calendar_manufacture_dim cal on mfac.manufacture_cal_key = cal.manufacture_cal_key
      WHERE cal.manufacture_yearmonth = 'mYM201904'
      GROUP BY cal.manufacture_cal_key
   ) agmfg on agsupp.manufacture_cal_key = agmfg.manufacture_cal_key;