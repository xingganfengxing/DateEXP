package com.litb.bi.support.dexp.util;

public class TimeUtil
{
  private static long starttime = System.currentTimeMillis();
  private static long between = System.currentTimeMillis();
  private static long sbetween = 0L;
  private static long day = 0L;
  private static long hour = 0L;
  private static long minute = 0L;
  private static long second = 0L;

  public static void init() {
    starttime = System.currentTimeMillis();
    between = System.currentTimeMillis();
  }

  public static void showTime() {
    between = (System.currentTimeMillis() - between) / 1000L;
    sbetween = (System.currentTimeMillis() - starttime) / 1000L;
    setTime(between);
    Log.log("Time: " + day + "d-" + hour + "h-" + minute + "m-" + second + 
      "s");
    setTime(sbetween);
    Log.log("Totle Time: " + day + "d-" + hour + "h-" + minute + "m-" + 
      second + "s");
    between = System.currentTimeMillis();
  }

  public static void setTime(long time) {
    day = time / 86400L;
    hour = time % 86400L / 3600L;
    minute = time % 3600L / 60L;
    second = time % 60L;
  }
}