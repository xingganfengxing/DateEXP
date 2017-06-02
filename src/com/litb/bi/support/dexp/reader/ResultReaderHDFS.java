package com.litb.bi.support.dexp.reader;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class ResultReaderHDFS
  implements ResultReader
{
  private String encoding = "GBK";
  private FileStatus[] flist;
  private boolean isdir = false;
  private int filepos = 0;
  private LineReader lr;
  private Text line = new Text();
  private int readbytes = 0;
  FileSystem filesystem;
  Configuration config;

  public void init(String path, String encoding)
    throws IOException
  {
    this.config = new Configuration();
    Path hpath = new Path(path);
    this.filesystem = hpath.getFileSystem(this.config);

    this.encoding = encoding;
    if (!this.filesystem.exists(hpath)) {
      throw new IOException("HDFS File don't exists! " + path);
    }
    FileStatus fs = this.filesystem.getFileStatus(hpath);
    if (!fs.isDir()) {
      this.lr = getReader(hpath);
    } else if (fs.isDir()) {
      this.isdir = true;
      this.flist = this.filesystem.listStatus(hpath);
      if ((this.flist != null) && (this.flist.length > 0)) {
        this.lr = getReader(this.flist[0].getPath());
        this.filepos += 1;
      } else {
        throw new IOException("No HDFS File under path! " + path);
      }
    } else {
      throw new IOException("HDFS File init failes! " + path);
    }
  }

  public String readerLine()
    throws IOException
  {
    String tmp = null;
    this.line = new Text();
    if (this.lr == null) {
      return null;
    }
    if (this.isdir) {
      this.readbytes = this.lr.readLine(this.line);
      do {
        this.lr = getReader(this.flist[this.filepos].getPath());
        this.filepos += 1;
        if (this.lr != null)
        {
          this.readbytes = this.lr.readLine(this.line);
        }
        if (this.readbytes >= 1) break; 
      }while (this.filepos < this.flist.length);
    }
    else
    {
      this.readbytes = this.lr.readLine(this.line);
    }
    if (this.readbytes < 1)
      tmp = null;
    else if (this.line.getLength() < 1)
      tmp = "";
    else {
      tmp = new String(this.line.getBytes(), this.encoding);
    }
    return tmp;
  }

  private LineReader getReader(Path hpath) throws IOException {
    FSDataInputStream fsin = this.filesystem.open(hpath);
    return new LineReader(fsin);
  }

  public void close()
  {
    try {
      this.lr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}