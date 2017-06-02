package com.litb.bi.support.dexp;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestHiveJDBC
{
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws Exception {
    try {
      System.out.println("----------------11");

      Class.forName(driverName);
    }
    catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    Connection con = DriverManager.getConnection("jdbc:hive://localhost:10004/default", "", "");

    System.out.println("----------------conn\t" + con);
    Statement stmt = con.createStatement();
    String tableName = "wordpay_litb";

    String sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    while (res.next())
      System.out.println(res.getString(1));
  }
}