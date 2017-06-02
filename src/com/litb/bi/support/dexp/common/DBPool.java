package com.litb.bi.support.dexp.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DBPool
{
  public static Map<String, DBConnector> conMap = new HashMap();
  public static Map<String, Statement> stmtMap = new HashMap();

  public static Connection getConnection(String dbname)
    throws SQLException
  {
    if ((conMap.isEmpty()) || (conMap.get(dbname) == null)) {
      if (DBModel.getDBModel(dbname) == null) {
        throw new SQLException("The dbname:[" + dbname + 
          "] DBModel is not exists");
      }
      DBConnector contor = new DBConnector(DBModel.getDBModel(dbname));
      conMap.put(dbname, contor);
    }
    return ((DBConnector)conMap.get(dbname)).getConnection();
  }

  public static Statement getStatement(String dbname)
    throws SQLException
  {
    if ((stmtMap.isEmpty()) || (stmtMap.get(dbname) == null)) {
      Statement stmt = getConnection(dbname).createStatement();
      stmtMap.put(dbname, stmt);
    }
    return (Statement)stmtMap.get(dbname);
  }
}