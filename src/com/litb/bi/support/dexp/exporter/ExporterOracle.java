package com.litb.bi.support.dexp.exporter;

import com.litb.bi.support.dexp.common.DBModel;
import com.litb.bi.support.dexp.common.DBPool;
import com.litb.bi.support.dexp.reader.ResultReader;
import com.litb.bi.support.dexp.reader.ResultReaderHDFS;
import com.litb.bi.support.dexp.reader.ResultReaderLocal;
import com.litb.bi.support.dexp.reader.ResultReaderS3;
import com.litb.bi.support.dexp.sql.SQL;
import com.litb.bi.support.dexp.util.Log;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ExporterOracle
  implements Exporter
{
  private PreparedStatement pstmt;
  private Connection con;
  private SQL sql;
  private ResultReader rr;

  public void init(SQL dbsql)
    throws Exception
  {
    this.sql = dbsql;
    if ((this.sql == null) || (this.sql.getSrc() == null) || (this.sql.getSrc().equals(""))) {
      Log.warn("Not a availiable DBSQL for loading data! Output is empty!");
      return;
    }

    if (this.sql.getSrc().toLowerCase().indexOf("hdfs://") > -1)
    {
      this.rr = new ResultReaderHDFS();
      this.rr.init(this.sql.getSrc(), this.sql.getEncoding());
    } else if (this.sql.getSrc().toLowerCase().indexOf("s3://") > -1) {
      this.rr = new ResultReaderS3();
      this.rr.init(this.sql.getSrc().replaceAll("s3://pajhdf/", ""), this.sql.getEncoding());//修改
      System.out.println(this.sql.getSrc());
    }
    else {
      this.rr = new ResultReaderLocal();
      this.rr.init(this.sql.getSrc(), this.sql.getEncoding());
    }

    createPreparedStatement();
  }

  public void export() throws Exception
  {
    try {
      String tmp = null;
      String[] values = null;
      int size = 0;
      int count = 0;
      while ((tmp = this.rr.readerLine()) != null) {
        values = tmp.split(this.sql.getTerminater());
        try {
          imporData(values);
          size++;
          count++;
          values = null;
        } catch (SQLException e) {
          Log.error("[Array Size: " + values.length + "] " + tmp);
          if (this.sql.isIgnore())
            e.printStackTrace();
          else {
            throw e;
          }
        }
        if (size >= this.sql.getFlushsize()) {
          size = 0;
          flush();
          Log.log("Commit: " + count);
        }
      }
      flush();
      Log.log("Commit: " + count);
    } catch (SQLException e) {
      throw e;
    }
    finally {
      this.pstmt.close();
    }
  }

  public void createPreparedStatement() throws SQLException {
    this.con = DBPool.getConnection(this.sql.getDbmodel().getDbname());
    this.con.setAutoCommit(false);
    this.pstmt = this.con.prepareStatement(this.sql.getSql());
  }

  public void flush() throws SQLException {
    try {
      this.pstmt.executeBatch();
    } catch (SQLException e) {
      if (!this.sql.isIgnore()) {
        throw e;
      }
    }
    this.con.commit();
  }

  public void imporData(String[] values) throws SQLException {
    for (int i = 0; i < values.length; i++)
    {
      if ((values[i] == null) || (values[i].equals("\\N"))) {
        this.pstmt.setString(i + 1, null);
      }
      else
        this.pstmt.setString(i + 1, values[i]);
    }
    this.pstmt.addBatch();
  }
}