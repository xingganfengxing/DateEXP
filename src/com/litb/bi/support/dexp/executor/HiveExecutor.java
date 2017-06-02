package com.litb.bi.support.dexp.executor;

import com.litb.bi.support.dexp.common.DBModel;
import com.litb.bi.support.dexp.common.DBPool;
import com.litb.bi.support.dexp.sql.SQL;
import com.litb.bi.support.dexp.sql.SQL.EXE_TYPE;
import com.litb.bi.support.dexp.util.Log;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveExecutor
{
  public static void executeQuerySQL(SQL sql)
    throws Exception
  {
    if ((sql.getSql() == null) || (sql.getSql().equals(""))) {
      Log.warn("SQL is null or empty!");
      return;
    }

    if (sql.getExetype() != SQL.EXE_TYPE.QUERY)
    {
      throw new Exception("Only support query sql for Hive!");
    }

    Log.log("Executing SQL: " + sql.getSql());

    ResultSet rs = DBPool.getStatement(sql.getDbmodel().getDbname()).executeQuery(sql.getSql());
    rs.close();
  }

  public static void executeSQL(SQL sql) throws Exception {
    if ((sql.getSql() == null) || (sql.getSql().equals(""))) {
      Log.warn("SQL is null or empty!");
      return;
    }

    if (sql.getExetype() != SQL.EXE_TYPE.QUERY)
    {
      throw new Exception("Only support query sql for Hive!");
    }

    Log.log("Executing SQL: " + sql.getSql());

    DBPool.getStatement(sql.getDbmodel().getDbname()).execute(sql.getSql());
  }
}