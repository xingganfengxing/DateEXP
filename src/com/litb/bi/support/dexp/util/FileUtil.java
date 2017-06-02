package com.litb.bi.support.dexp.util;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;

public class FileUtil
{
  public static LineReader getHDFSPath(Path hpath, Configuration config)
  {
    if (hpath == null) {
      return null;
    }
    if (config == null) {
      config = new Configuration();
    }

    LineReader lr = null;
    try {
      FileSystem hdfs = FileSystem.get(config);
      if (!hdfs.exists(hpath)) {
        return null;
      }
      FileStatus fs = hdfs.getFileStatus(hpath);
      if (fs.isDir()) {
        return null;
      }
      FSDataInputStream fsin = hdfs.open(hpath);
      lr = new LineReader(fsin);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return lr;
  }

  public static File getLocalFile(String path) {
    if ((path == null) || (path.equals(""))) {
      return null;
    }
    File file = new File(path);
    if (isFile(file)) {
      return file;
    }
    return null;
  }

  public static File getLocalFile(String path, Class obj)
  {
    if ((path == null) || (path.equals(""))) {
      return null;
    }
    File file = new File(path);
    if (isFile(file)) {
      return file;
    }
    file = new File(obj.getResource("/").getPath() + File.separator + path);
    if (isFile(file)) {
      return file;
    }
    file = new File(obj.getResource("").getPath() + File.separator + path);
    if (isFile(file)) {
      return file;
    }
    file = new File(System.getProperty("user.dir") + File.separator + path);
    if (isFile(file)) {
      return file;
    }
    return null;
  }

  public static File getLocalDir(String path) {
    if ((path == null) || (path.equals(""))) {
      return null;
    }
    File file = new File(path);
    if (isDir(file)) {
      return file;
    }
    return null;
  }

  public static File createEmptyLocalDir(String path) throws IOException {
    if ((path == null) || (path.equals(""))) {
      throw new IOException("Directory path is NULL of empty! path=" + 
        path);
    }
    File file = new File(path);
    if (file.exists()) {
      throw new IOException("Directory path exists! path=" + path);
    }
    if (!file.mkdirs()) {
      throw new IOException("Make dir failed! path=" + path);
    }
    return file;
  }

  public static File getLocalDir(String path, Class obj)
  {
    if ((path == null) || (path.equals(""))) {
      return null;
    }
    File file = new File(path);
    if (isDir(file)) {
      return file;
    }
    file = new File(obj.getResource("/").getPath() + File.separator + path);
    if (isDir(file)) {
      return file;
    }
    file = new File(obj.getResource("").getPath() + File.separator + path);
    if (isDir(file)) {
      return file;
    }
    file = new File(System.getProperty("user.dir") + File.separator + path);
    if (isDir(file)) {
      return file;
    }

    return null;
  }

  public static File getLocal(String path, Class obj)
  {
    if ((path == null) || (path.equals(""))) {
      return null;
    }
    File file = new File(path);
    if (file.exists()) {
      return file;
    }
    file = new File(obj.getResource("/").getPath() + File.separator + path);
    if (file.exists()) {
      return file;
    }
    file = new File(obj.getResource("").getPath() + File.separator + path);
    if (file.exists()) {
      return file;
    }
    file = new File(System.getProperty("user.dir") + File.separator + path);
    if (file.exists()) {
      return file;
    }

    return null;
  }

  public static boolean isFile(File file) {
    if ((file == null) || (!file.exists())) {
      return false;
    }
    return (file.exists()) && (file.isFile());
  }

  public static boolean isDir(File file)
  {
    if ((file == null) || (!file.exists())) {
      return false;
    }
    return (file.exists()) && (file.isDirectory());
  }

  public static void generateCSV(ResultSet rs, String path, char separator, char quotechar, char escapechar, String lineEnd, boolean header)
    throws Exception
  {
    Log.log("Start to write csv!");
    CSVWriter writer = new CSVWriter(new FileWriter(path), separator, quotechar, 
      escapechar, lineEnd);
    if (header)
      writer.writeAll(rs, true);
    else {
      writer.writeAll(rs, false);
    }
    writer.close();
    Log.log("End write csv!");
  }

  public static void writeCSV(CSVWriter writer, ResultSet rs, boolean header) {
    try {
      if (header)
        writer.writeAll(rs, true);
      else {
        writer.writeAll(rs, false);
      }
      writer.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}