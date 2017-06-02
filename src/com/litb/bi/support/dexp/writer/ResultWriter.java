package com.litb.bi.support.dexp.writer;

import java.io.IOException;

public abstract interface ResultWriter
{
  public abstract void init(String paramString1, String paramString2, boolean paramBoolean)
    throws IOException;

  public abstract void write(String paramString)
    throws IOException;

  public abstract void close();
}