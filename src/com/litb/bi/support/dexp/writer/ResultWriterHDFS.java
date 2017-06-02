package com.litb.bi.support.dexp.writer;

import com.litb.bi.support.dexp.util.Log;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultWriterHDFS
  implements ResultWriter
{
  private String encoding = "GBK";
  private FileSystem hdfs;
  private Configuration config;
  private FSDataOutputStream fsout;
  private BufferedWriter bw;
  private int BUFFER_SIZE = 8192;

  public void init(String path, String encoding, boolean overwite) throws IOException
  {
    this.config = new Configuration();
    Path hpath = new Path(path);

    this.hdfs = hpath.getFileSystem(this.config);
    this.encoding = encoding;
    if (this.hdfs.exists(hpath)) {
      throw new IOException("HDFS File is exists! " + path);
    }
    this.hdfs.mkdirs(hpath.getParent());

    this.fsout = this.hdfs.create(hpath, true, this.BUFFER_SIZE);
    this.bw = 
      new BufferedWriter(new OutputStreamWriter(this.fsout, this.encoding), 
      this.BUFFER_SIZE);
  }

  public void write(String content) throws IOException
  {
    this.bw.write(content);
    this.bw.flush();
  }

  public void close()
  {
    try {
      this.bw.close();
    } catch (IOException e) {
      Log.warn(e.getMessage());
    }
  }
}