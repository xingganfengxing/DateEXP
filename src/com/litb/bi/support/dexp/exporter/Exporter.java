package com.litb.bi.support.dexp.exporter;

import com.litb.bi.support.dexp.sql.SQL;

public abstract interface Exporter
{
  public abstract void init(SQL paramSQL)
    throws Exception;

  public abstract void export()
    throws Exception;
}