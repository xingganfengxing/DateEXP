package com.litb.bi.support.dexp.util;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log
{
  private static final Logger logger = LoggerFactory.getLogger(Log.class);

  public static boolean debug = true;

  public static void error(String log)
  {
    logger.error(log);
  }

  public static void log(String log)
  {
    logger.info(log);
  }

  public static void warn(String log)
  {
    logger.warn(log);
  }

  public static void debug(String log) {
    if (debug)
    {
      logger.debug(log);
    }
  }

  public static void p(String log)
  {
    logger.info(log);
  }

  public static void main(String[] args) {
    List s = new ArrayList();
    s.add("1");
    s.add("2");
    System.out.println((String)s.get(1));
  }
}