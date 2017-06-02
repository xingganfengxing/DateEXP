package com.litb.bi.support.dexp.executor;

import com.litb.bi.support.dexp.common.DBModel;
import com.litb.bi.support.dexp.common.DBPool;
import com.litb.bi.support.dexp.common.DBTYPE;
import com.litb.bi.support.dexp.exporter.Exporter;
import com.litb.bi.support.dexp.exporter.ExporterOracle;
import com.litb.bi.support.dexp.importer.Importer;
import com.litb.bi.support.dexp.importer.ImporterOracle;
import com.litb.bi.support.dexp.sql.SQL;
import com.litb.bi.support.dexp.sql.SQL.EXE_TYPE;
import com.litb.bi.support.dexp.util.Log;
import java.sql.Statement;

public class DBExecutor
{
  public static void executeSQLExport(SQL sql)
    throws Exception
  {
    Exporter export = null;
    if (sql.getDbmodel().getDbtype() == DBTYPE.ORACLE)
      export = new ExporterOracle();
    else if (sql.getDbmodel().getDbtype() == DBTYPE.MYSQL)
    {
      export = new ExporterOracle();
    }
    else throw new Exception("Not support db type to export!");

    Log.log("Export data from path: [" + sql.getSrc() + "]");
    export.init(sql);
    export.export();
  }

  public static void executeSQLImport(SQL sql) throws Exception {
    Importer importer = null;
    if (sql.getDbmodel().getDbtype() == DBTYPE.ORACLE)
      importer = new ImporterOracle();
    else if (sql.getDbmodel().getDbtype() == DBTYPE.MYSQL)
    {
      importer = new ImporterOracle();
    }
    else throw new Exception("Not support db type to import!");

    Log.log("Import data to path: [" + sql.getDest() + "]");
    importer.init(sql);
    importer.importData();
  }

  public static void executeSQLQuery(SQL sql) throws Exception
  {
    DBPool.getStatement(sql.getDbmodel().getDbname()).executeUpdate(
      sql.getSql());
  }

  public static void executeSQL(SQL sql) throws Exception {
    if ((sql.getSql() == null) || (sql.getSql().equals(""))) {
      Log.warn("SQL is null or empty!");
      return;
    }

    Log.log("Executing SQL: " + sql.getSql());
    if (sql.getExetype() == SQL.EXE_TYPE.QUERY)
      executeSQLQuery(sql);
    else if (sql.getExetype() == SQL.EXE_TYPE.EXPORT)
      executeSQLExport(sql);
    else if (sql.getExetype() == SQL.EXE_TYPE.IMPORT)
      executeSQLImport(sql);
    else
      throw new Exception("Not a correct execute type!");
  }
}