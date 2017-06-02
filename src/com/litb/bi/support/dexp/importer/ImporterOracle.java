package com.litb.bi.support.dexp.importer;

import com.litb.bi.support.dexp.common.DBModel;
import com.litb.bi.support.dexp.common.DBPool;
import com.litb.bi.support.dexp.sql.SQL;
import com.litb.bi.support.dexp.util.FileUtil;
import com.litb.bi.support.dexp.util.Log;
import com.litb.bi.support.dexp.writer.ResultWriter;
import com.litb.bi.support.dexp.writer.ResultWriterHDFS;
import com.litb.bi.support.dexp.writer.ResultWriterLocal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ImporterOracle
  implements Importer
{
  private SQL sql;
  private ResultWriter rw;

  public void init(SQL sql)
    throws Exception
  {
    this.sql = sql;
    if ((sql.getSql() == null) || (sql.getSql().equals("")) || 
      (sql.getDest() == null) || (sql.getDest().equals(""))) {
      throw new Exception("Import sql is empty or dest path is empty!");
    }
    if ((sql.getDest().indexOf("hdfs://") > -1) || (sql.getDest().indexOf("s3://") > -1))
      this.rw = new ResultWriterHDFS();
    else {
      this.rw = new ResultWriterLocal();
    }
    this.rw.init(sql.getDest(), sql.getEncoding(), sql.isOverwrite());
  }

  public void importData() throws Exception
  {
    ResultSet rs = null;
    try {
      Statement stmt = DBPool.getStatement(this.sql.getDbmodel().getDbname());
      rs = stmt.executeQuery(this.sql.getSql());
      ResultSetMetaData rsmd = rs.getMetaData();
      int colcount = rsmd.getColumnCount();
      int i = 0;
      StringBuilder sbr = new StringBuilder();

      if (this.sql.getDest().endsWith(".csv")) {
        FileUtil.generateCSV(rs, this.sql.getDest(), ',', 
          '"', '"', "\n", 
          this.sql.isHeader());
      }
      else {
        if (this.sql.isHeader()) {
          for (i = 1; i <= colcount; i++) {
            sbr.append(rsmd.getColumnName(i) + this.sql.getTerminater());
          }
          sbr.setLength(sbr.length() - this.sql.getTerminater().length());
          sbr.append('\n');
        }

        int count = 0;
        while (rs.next()) {
          count++;
          for (i = 1; i <= colcount; i++) {
            sbr.append(rs.getString(i) + this.sql.getTerminater());
          }
          sbr.setLength(sbr.length() - this.sql.getTerminater().length());
          sbr.append('\n');
          if (count % this.sql.getFlushsize() == 0) {
            this.rw.write(sbr.toString());
            sbr.setLength(0);
          }
        }
        this.rw.write(sbr.toString());
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (this.rw != null) {
        try {
          this.rw.close();
        } catch (Exception e) {
          Log.warn(e.getMessage());
        }
      }
      if (rs != null)
        try {
          rs.close();
        } catch (SQLException e) {
          Log.warn(e.getMessage());
        }
    }
  }
}