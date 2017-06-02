--HQL=hivedb
add jar /search/hadoop/hive/lib/sogouudf.jar;
-- UDTF ------------------------------------------------------------------------------
CREATE TEMPORARY FUNCTION s_explode AS 'com.sogou.support.hive.udtf.GBKExplode';
CREATE TEMPORARY FUNCTION s_putregn_id AS 'com.sogou.support.hive.udtf.RegionExplode';
CREATE TEMPORARY FUNCTION s_puthour_id AS 'com.sogou.support.hive.udtf.TimeExplode';

-- UDF -------------------------------------------------------------------------------
CREATE TEMPORARY FUNCTION s_andshift AS 'com.sogou.support.hive.udf.AndShift';
CREATE TEMPORARY FUNCTION s_domain AS 'com.sogou.support.hive.udf.Domain';
CREATE TEMPORARY FUNCTION s_rshift AS 'com.sogou.support.hive.udf.RShift';
CREATE TEMPORARY FUNCTION s_showmatch_id AS 'com.sogou.support.hive.udf.ShowMatch';
CREATE TEMPORARY FUNCTION s_putpl_code_lead AS 'com.sogou.support.hive.udf.PutPLCodeLead';
CREATE TEMPORARY FUNCTION s_putpl_code_xml  AS 'com.sogou.support.hive.udf.PutPLCodeXml';
CREATE TEMPORARY FUNCTION s_putpl_code_cpc  AS 'com.sogou.support.hive.udf.PutPLCodeCpc';
CREATE TEMPORARY FUNCTION s_putpl_code_lu   AS 'com.sogou.support.hive.udf.PutPLCodeLu';
CREATE TEMPORARY FUNCTION s_putpl_code_all  AS 'com.sogou.support.hive.udf.PutPLCode';
CREATE TEMPORARY FUNCTION s_md5 AS 'com.sogou.support.hive.udf.MD5';
CREATE TEMPORARY FUNCTION s_black_pid AS 'com.sogou.support.hive.udf.IsBlackPid';
CREATE TEMPORARY FUNCTION s_black_pid_pro AS 'com.sogou.support.hive.udf.IsBlackPidPro';
CREATE TEMPORARY FUNCTION s_cpcstatus AS 'com.sogou.support.hive.udf.CpcStatus';
CREATE TEMPORARY FUNCTION s_putregn_int AS 'com.sogou.support.hive.udf.PutRegnInt';
CREATE TEMPORARY FUNCTION s_puthour_char AS 'com.sogou.support.hive.udf.PutHourChar';

--END-HQL