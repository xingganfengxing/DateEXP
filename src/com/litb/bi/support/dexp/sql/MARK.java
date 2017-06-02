package com.litb.bi.support.dexp.sql;

import java.util.HashMap;
import java.util.Map;

public class MARK
{
  public static final String HQL_START = "--HQL=";
  public static final String HQL_END = "--END-HQL";
  public static final String SQL_START = "--SQL=";
  public static final String SQL_END = "--END-SQL";
  public static final String IGNORE = "--[ignore]";
  public static final String TERMINATER = "--[terminater]=";
  public static final String ENCODING = "--[encoding]=";
  public static final String FLUSH = "--[flush]=";
  public static final String SOURCE = "--[src]=";
  public static final String DEST = "--[dest]=";
  public static final String TOKEN = "--[token]=";
  public static final String HEADER = "--[header]=";
  public static final String OVERWRITE = "--[overwrite]=";
  public static final String VARFILE = "--[varfile]=";
  public static final String OTHER = "--";
  public static final String SEMICOLON = ";";
  public static final Map<String, String> terminaterMap = new HashMap() { private static final long serialVersionUID = 1L; } ;

  public static String getTerminater(String key)
  {
    if (terminaterMap.containsKey(key)) {
      return (String)terminaterMap.get(key);
    }
    return key;
  }
}