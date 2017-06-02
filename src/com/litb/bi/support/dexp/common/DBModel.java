package com.litb.bi.support.dexp.common;

import com.litb.bi.support.dexp.util.PropertiesTool;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class DBModel
{
  private String dbname;
  private String driver;
  private String url;
  private String username;
  private String password;
  private DBTYPE dbtype = DBTYPE.ORACLE;

  public static Map<String, DBModel> DBMap = new HashMap();
  public static String dbconfig = "/dbjdbc.properties";

  public static void parsePropertiesFile()
    throws Exception
  {
    DBMap.clear();
    Properties pro = PropertiesTool.parseFile(dbconfig);
    Iterator it = pro.entrySet().iterator();
    String key = "";
    String prefix = "";
    while (it.hasNext()) {
      Map.Entry e = (Map.Entry)it.next();
      key = e.getKey().toString();
      prefix = key.split("\\.")[0];
      if (key.indexOf(".jdbc.url") > 1) {
        DBModel dm = new DBModel();
        dm.setDbname(prefix);
        dm.setDriver(pro.getProperty(prefix + ".jdbc.driver"));
        dm.setUrl(e.getValue().toString());
        dm.setUsername(pro.getProperty(prefix + ".jdbc.username"));
        dm.setPassword(pro.getProperty(prefix + ".jdbc.password"));
        dm.setDbtype(DBTYPE.valueOf(pro.getProperty(
          prefix + ".jdbc.type").toUpperCase()));
        DBMap.put(prefix, dm);
      }
    }
  }

  public static DBModel getDBModel(String dbname)
  {
    if (DBMap.isEmpty()) {
      try {
        parsePropertiesFile();
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }
    return (DBModel)DBMap.get(dbname);
  }

  public String getUrl() {
    return this.url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return this.username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return this.password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public DBTYPE getDbtype() {
    return this.dbtype;
  }

  public void setDbtype(DBTYPE dbtype) {
    this.dbtype = dbtype;
  }

  public String getDbname() {
    return this.dbname;
  }

  public void setDbname(String dbname) {
    this.dbname = dbname;
  }

  public String getDriver() {
    return this.driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }
}