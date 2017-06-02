package com.litb.bi.support.dexp.importer;

import com.litb.bi.support.dexp.sql.SQL;

public abstract interface Importer
{
  public abstract void init(SQL paramSQL)
    throws Exception;

  public abstract void importData()
    throws Exception;
}

/* Location:           C:\Users\Administrator\Desktop\NGE-work\dateto\DataEXP-2.0\
 * Qualified Name:     com.litb.bi.support.dexp.importer.Importer
 * JD-Core Version:    0.6.0
 */