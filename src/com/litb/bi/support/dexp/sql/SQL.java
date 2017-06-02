package com.litb.bi.support.dexp.sql;

import com.litb.bi.support.dexp.common.DBModel;

public class SQL
{
  private String sql = null;
  private DBModel dbmodel;
  private SQL_TYPE sqltype = SQL_TYPE.DBSQL;

  private boolean ignore = false;

  private String encoding = "GBK";

  private String terminater = "\001";

  private String src = null;

  private String dest = null;

  private boolean header = false;

  private boolean overwrite = false;

  private int token = 0;

  private String varfile = null;
  private int flushsize = 5000;

  private EXE_TYPE exetype = EXE_TYPE.QUERY;

  public SQL(String sql, DBModel dbmodel, SQL_TYPE sqltype, boolean ignore, String encoding, String terminater, String src, String dest, boolean header, int token, String varfile, int flushsize, EXE_TYPE exetype, boolean overwrite)
  {
    this.sql = sql;
    this.dbmodel = dbmodel;
    this.sqltype = sqltype;
    this.ignore = ignore;
    this.encoding = encoding;
    this.terminater = terminater;
    this.src = src;
    this.dest = dest;
    this.header = header;
    this.token = token;
    this.varfile = varfile;
    this.flushsize = flushsize;
    this.exetype = exetype;
    this.overwrite = overwrite;
  }

  public String getEncoding() {
    return this.encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public boolean isIgnore() {
    return this.ignore;
  }

  public void setIgnore(boolean ignore) {
    this.ignore = ignore;
  }

  public DBModel getDbmodel() {
    return this.dbmodel;
  }

  public void setDbmodel(DBModel dbmodel) {
    this.dbmodel = dbmodel;
  }

  public String getSql() {
    return this.sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public SQL_TYPE getSqltype() {
    return this.sqltype;
  }

  public void setSqltype(SQL_TYPE sqltype) {
    this.sqltype = sqltype;
  }

  public String getTerminater() {
    return this.terminater;
  }

  public void setTerminater(String terminater) {
    this.terminater = terminater;
  }

  public String getSrc() {
    return this.src;
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public String getDest() {
    return this.dest;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public boolean isHeader() {
    return this.header;
  }

  public void setHeader(boolean header) {
    this.header = header;
  }

  public int getToken() {
    return this.token;
  }

  public void setToken(int token) {
    this.token = token;
  }

  public String getVarfile() {
    return this.varfile;
  }

  public void setVarfile(String varfile) {
    this.varfile = varfile;
  }

  public int getFlushsize() {
    return this.flushsize;
  }

  public void setFlushsize(int flushsize) {
    this.flushsize = flushsize;
  }

  public EXE_TYPE getExetype() {
    return this.exetype;
  }

  public void setExetype(EXE_TYPE exetype) {
    this.exetype = exetype;
  }
  public boolean isOverwrite() {
    return this.overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public static enum EXE_TYPE
  {
    IMPORT, EXPORT, QUERY;
  }

  public static enum SQL_TYPE {
    HQL, DBSQL;
  }
}