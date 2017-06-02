package com.litb.bi.support.dexp.common;

import com.litb.bi.support.dexp.util.Log;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnector
{
  private Connection con;
  private String driver;
  private String url;
  private String username;
  private String password;

  public DBConnector(String driver, String url, String username, String password)
  {
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public DBConnector(DBModel dbmodel) {
    this.driver = dbmodel.getDriver();
    this.url = dbmodel.getUrl();
    this.username = dbmodel.getUsername();
    this.password = dbmodel.getPassword();
  }

  public DBConnector(String dbname) {
    this(DBModel.getDBModel(dbname));
  }

  public Connection getConnection() throws SQLException {
    if ((this.con == null) || (this.con.isClosed())) {
      try {
        Class.forName(this.driver);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        throw new SQLException("[ERROR] Can not find the JDBC driver=" + 
          this.driver);
      }
      this.con = DriverManager.getConnection(this.url, this.username, this.password);
    }
    return this.con;
  }

  public void closeConnection() {
    try {
      if ((this.con != null) && (!this.con.isClosed()))
        this.con.close();
    }
    catch (SQLException e) {
      Log.error("Can not close DB connection!");
      e.printStackTrace();
    }
  }

  public Statement getStatement() throws SQLException {
    Statement stmt = getConnection().createStatement();
    return stmt;
  }

  public String getUrl() {
    return this.url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return this.username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return this.password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDriver() {
    return this.driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }
}