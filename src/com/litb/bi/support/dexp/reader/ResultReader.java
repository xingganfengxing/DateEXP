package com.litb.bi.support.dexp.reader;

import java.io.IOException;

public abstract interface ResultReader
{
  public abstract void init(String paramString1, String paramString2)
    throws IOException;

  public abstract String readerLine()
    throws IOException;

  public abstract void close();
}

