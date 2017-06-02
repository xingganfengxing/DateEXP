package com.litb.bi.support.dexp.sql;

import com.litb.bi.support.dexp.DataEXP;
import com.litb.bi.support.dexp.common.DBModel;
import com.litb.bi.support.dexp.util.FileUtil;
import com.litb.bi.support.dexp.util.Log;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SQLParser
{
  private SQL.SQL_TYPE sqltype = SQL.SQL_TYPE.DBSQL;
  private boolean ignore = false;
  private String encoding = "GBK";
  private String terminater = "\001";
  private String src = null;
  private String dest = null;
  private boolean header = false;
  private int token = 0;
  private String varfile = null;
  private int flushsize = 5000;
  private SQL.EXE_TYPE exetype = SQL.EXE_TYPE.QUERY;
  private boolean overwrite = false;

  private StringBuffer sb = new StringBuffer();

  public SQLParser() {
    init();
  }

  public void init()
  {
    this.sb.setLength(0);
    this.sqltype = SQL.SQL_TYPE.DBSQL;
    this.ignore = false;
    this.encoding = "GBK";
    this.terminater = "\001";
    this.src = null;
    this.dest = null;
    this.header = false;
    this.token = 0;
    this.varfile = null;
    this.flushsize = 5000;
    this.exetype = SQL.EXE_TYPE.QUERY;
    this.overwrite = false;
  }

  public List<SQL> parseFile(String hfile, Map<String, String> paraMap)
    throws Exception
  {
    BufferedReader br = null;

    if ((hfile.toLowerCase().indexOf("hdfs://") > -1) || (hfile.toLowerCase().indexOf("s3://") > -1))
    {
      Path filePath = new Path(hfile);
      Configuration config = new Configuration();
      FileSystem fs = filePath.getFileSystem(config);
      FSDataInputStream fsin = fs.open(filePath);
      br = new BufferedReader(new InputStreamReader(fsin));
    }
    else
    {
      File file = FileUtil.getLocalFile(
        hfile, DataEXP.class);
      if ((file == null) || (!file.exists())) {
        throw new Exception("SQL file not exists!");
      }
      br = new BufferedReader(new FileReader(file));
    }

    List slist = new ArrayList();
    List lines = new ArrayList();

    DBModel dbmodel = null;
    try
    {
      String expect = null;
      String dbname = "";

      int count = 0;
      String line;
      while ((line = br.readLine()) != null)
      {
        count++;

        if (line.indexOf("--[terminater]=") < 0) {
          line = line.trim();
        }

        if (line.equals(""))
        {
          continue;
        }
        if (expect != null) {
          if ((expect != null) && (line.indexOf(expect) == 0)) {
            if (expect.equals("--END-HQL"))
            {
              parseHQL(slist, lines, dbmodel, paraMap);
            } else if (expect.equals("--END-SQL"))
            {
              parseDBSQL(slist, lines, dbmodel, paraMap);
            }
            else throw new Exception("Error expect token: " + expect);

            lines.clear();
            expect = null;
          }
          else {
            lines.add(line);
          }

        }
        else if (line.indexOf("--HQL=") == 0)
        {
          expect = "--END-HQL";
          dbname = line.substring("--HQL=".length());
          dbmodel = DBModel.getDBModel(dbname);
          if (dbmodel == null)
            throw new Exception("the dbmodel is not exists! line=" + 
              count + "; dbname=" + dbname);
        }
        else if (line.indexOf("--SQL=") == 0)
        {
          expect = "--END-SQL";
          dbname = line.substring("--SQL=".length());
          dbmodel = DBModel.getDBModel(dbname);
          if (dbmodel == null)
            throw new Exception("the dbmodel is not exists! line=" + 
              count + "; dbname=" + dbname);
        }
        else {
          expect = null;
        }
      }
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
      Log.error("File Not Found!");
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      Log.error("IO Exception!");
      return null;
    } finally {
      if (br != null)
        br.close();
    }
    if (br != null) {
      br.close();
    }

    return slist;
  }

  public void parseHQL(List<SQL> slist, List<String> lines, DBModel dbmodel, Map<String, String> paraMap)
    throws Exception
  {
    init();
    for (String line : lines) {
      if (line.indexOf("--[ignore]") == 0)
      {
        this.ignore = true;
      } else {
        if (line.indexOf("--") == 0) {
          continue;
        }
        if (line.indexOf(";") > -1)
        {
          this.sb.append(line.subSequence(0, line.indexOf(";")));

          String sqlstr = this.sb.toString();
          sqlstr = replaceVar(sqlstr, paraMap);

          this.dest = replaceVar(this.dest, paraMap);
          this.src = replaceVar(this.src, paraMap);

          this.sqltype = SQL.SQL_TYPE.HQL;
          SQL sql = new SQL(sqlstr, dbmodel, this.sqltype, this.ignore, 
            this.encoding, this.terminater, this.src, this.dest, this.header, this.token, 
            this.varfile, this.flushsize, this.exetype, this.overwrite);
          slist.add(sql);
          init();

          line = line.substring(line.indexOf(";") + 1);
        }

        if (!line.trim().equals("")) {
          if (line.indexOf("--") > 0)
            this.sb.append(line.substring(0, line.indexOf("--")))
              .append(" ");
          else {
            this.sb.append(line).append(" ");
          }
        }
      }
    }

    if (this.sb.length() > 0) {
      Log.warn("Ignore HQL:" + this.sb.toString());
      Log.warn("May you are missing a semicolon(;) at the end!");
    }
  }

  public void parseDBSQL(List<SQL> slist, List<String> lines, DBModel dbmodel, Map<String, String> paraMap)
    throws Exception
  {
    init();
    for (String line : lines) {
      if (line.indexOf("--[header]=") == 0)
      {
        String headerStr = line.substring("--[header]=".length());
        if (headerStr.toLowerCase().equals("true")) {
          this.header = true;
        }
      }
      else if (line.indexOf("--[overwrite]=") == 0)
      {
        String overStr = line.substring("--[overwrite]=".length());
        if (overStr.toLowerCase().equals("true")) {
          this.overwrite = true;
        }
      }
      else if (line.indexOf("--[encoding]=") == 0)
      {
        this.encoding = line.substring("--[encoding]=".length());
      }
      else if (line.indexOf("--[terminater]=") == 0)
      {
        this.terminater = line.substring("--[terminater]=".length());
        this.terminater = MARK.getTerminater(this.terminater);
      }
      else if (line.indexOf("--[flush]=") == 0)
      {
        this.flushsize = 
          Integer.parseInt(line.substring("--[flush]=".length()));
      }
      else if (line.indexOf("--[ignore]") == 0)
      {
        this.ignore = true;
      }
      else if (line.indexOf("--[src]=") == 0)
      {
        this.exetype = SQL.EXE_TYPE.EXPORT;
        this.src = line.substring("--[src]=".length());
      }
      else if (line.indexOf("--[dest]=") == 0)
      {
        this.exetype = SQL.EXE_TYPE.IMPORT;
        this.dest = line.substring("--[dest]=".length());
      } else {
        if (line.indexOf("--") == 0) {
          continue;
        }
        if (line.indexOf(";") > -1)
        {
          this.sb.append(line.subSequence(0, line.indexOf(";")));

          String sqlstr = this.sb.toString();
          sqlstr = replaceVar(sqlstr, paraMap);

          this.dest = replaceVar(this.dest, paraMap);
          this.src = replaceVar(this.src, paraMap);

          this.sqltype = SQL.SQL_TYPE.DBSQL;
          SQL sql = new SQL(sqlstr, dbmodel, this.sqltype, this.ignore, 
            this.encoding, this.terminater, this.src, this.dest, this.header, this.token, 
            this.varfile, this.flushsize, this.exetype, this.overwrite);
          slist.add(sql);
          init();

          line = line.substring(line.indexOf(";") + 1);
        }

        if (!line.trim().equals("")) {
          if (line.indexOf("--") > 0)
            this.sb.append(line.substring(0, line.indexOf("--")))
              .append(" ");
          else {
            this.sb.append(line).append(" ");
          }
        }
      }
    }
    if (this.sb.length() > 0) {
      Log.warn("Ignore DBSQL:" + this.sb.toString());
      Log.warn("May you are missing a semicolon(;) at the end!");
    }
  }

  private String replaceVar(String sql, Map<String, String> paramMap)
  {
    if (sql == null) {
      return null;
    }

    for (String key : paramMap.keySet()) {
      String repl = "\\u0024\\{hiveconf\\:" + key + "\\}";

      sql = sql.replaceAll(repl, (String)paramMap.get(key));

      repl = "\\u0024\\{" + key + "\\}";
      sql = sql.replaceAll(repl, (String)paramMap.get(key));
    }
    return sql;
  }
}