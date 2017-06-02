package com.litb.bi.support.dexp.util;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesTool
{
  public static Properties parseFile(String path)
    throws Exception
  {
    InputStream is = PropertiesTool.class.getClass().getResourceAsStream(
      path);
    if (is == null) {
      throw new Exception("Properties属性文件路径错误！path=" + path);
    }

    Properties mpro = new Properties();
    mpro.load(is);

    return mpro;
  }
}