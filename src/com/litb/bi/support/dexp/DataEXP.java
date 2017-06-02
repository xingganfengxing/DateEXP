package com.litb.bi.support.dexp;

import com.litb.bi.support.dexp.common.DBModel;
import com.litb.bi.support.dexp.executor.DBExecutor;
import com.litb.bi.support.dexp.executor.HiveExecutor;
import com.litb.bi.support.dexp.sql.SQL;
import com.litb.bi.support.dexp.sql.SQL.SQL_TYPE;
import com.litb.bi.support.dexp.sql.SQLParser;
import com.litb.bi.support.dexp.util.Log;
import com.litb.bi.support.dexp.util.TimeUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataEXP
{
  private String hqlfile = null;
  private static String initfile = "";

  public static Map<String, String> paramMap = new HashMap();

  public void run()
    throws Exception
  {
    run(this.hqlfile);
  }

  public void run(String hql)
    throws Exception
  {
    if ((initfile != null) && (!initfile.equals("")))
    {
      List<SQL> hlist = new SQLParser().parseFile(initfile, paramMap);

      Log.p("============================================");
      Log.p("HQL File:\t" + initfile);
      Log.p("SQL Size:\t" + hlist.size());
      Log.p("============================================");
      for (SQL sql : hlist)
      {
        try
        {
          HiveExecutor.executeSQL(sql);
        } catch (Exception e) {
          if (sql.isIgnore())
            Log.warn(e.getMessage());
          else {
            throw e;
          }
        }
        TimeUtil.showTime();
      }

    }

    List <SQL> slist = new SQLParser().parseFile(this.hqlfile, paramMap);

    Log.p("============================================");
    Log.p("HQL File:\t" + this.hqlfile);
    Log.p("SQL Size:\t" + slist.size());
    Log.p("============================================");

    for (SQL sql : slist) {
      Log.debug("DBname=" + sql.getDbmodel().getDbname());
      Log.debug("DB url=" + sql.getDbmodel().getUrl());
      try {
        if (sql.getSqltype() == SQL.SQL_TYPE.DBSQL) {
          DBExecutor.executeSQL(sql);
        }
        else if (sql.getSqltype() == SQL.SQL_TYPE.HQL) {
          HiveExecutor.executeQuerySQL(sql);
        }
        else
          throw new Exception("Unknow sql type");
      }
      catch (Exception e) {
        if (sql.isIgnore())
          Log.warn(e.getMessage());
        else {
          throw e;
        }
      }
      TimeUtil.showTime();
    }
  }

  public static void main(String[] args)
    throws Exception
  {
    TimeUtil.init();
    String hqlfile = null;

    boolean flag = false;

    for (int i = 0; i < args.length; i++)
    {
      if (args[i].endsWith("-hfile")) {
        i++; hqlfile = args[i];
        flag = true;
      } else if (args[i].endsWith("-ifile")) {
        i++; initfile = args[i];
      } else if (args[i].endsWith("-hiveconf")) {
        i++; String para = args[i];
        if (para.indexOf("=") < 0) {
          Log.error("Wrong args: " + para);
          System.exit(2);
        }
        String[] paraArray = para.split("=");
        paramMap.put(paraArray[0], paraArray[1]);
      }
    }

    if (!flag) {
      Log.error("Wrong args:");
      System.exit(2);
    }

    DataEXP dataExp = new DataEXP();
    dataExp.setHqlfile(hqlfile);
    dataExp.run();
  }

  public String getHqlfile() {
    return this.hqlfile;
  }

  public void setHqlfile(String hqlfile) {
    this.hqlfile = hqlfile;
  }
}