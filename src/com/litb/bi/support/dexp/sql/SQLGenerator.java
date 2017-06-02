package com.litb.bi.support.dexp.sql;

public class SQLGenerator
{
  public static String generate(SQL sql)
  {
    if (sql.getToken() < 1) {
      return sql.getSql();
    }
    StringBuilder sbr = new StringBuilder();

    return sbr.toString();
  }
}