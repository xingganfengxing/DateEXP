package com.litb.bi.support.dexp.writer;

import com.litb.bi.support.dexp.util.Log;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ResultWriterLocal
  implements ResultWriter
{
  private String encoding = "GBK";
  private FileOutputStream fos;
  private OutputStreamWriter osw;
  private File outfile;

  public void init(String path, String encoding, boolean overwrite)
    throws IOException
  {
    this.outfile = new File(path);
    this.encoding = encoding;

    if ((!overwrite) && (
      (this.outfile == null) || (this.outfile.exists()))) {
      throw new IOException("Out file path is exists! " + path);
    }

    this.outfile.getParentFile().mkdirs();
    this.fos = new FileOutputStream(this.outfile);
    this.osw = new OutputStreamWriter(this.fos, this.encoding);
  }

  public void write(String content) throws IOException
  {
    this.osw.write(content);
    this.osw.flush();
  }

  public void close()
  {
    try {
      this.osw.close();
    } catch (IOException e) {
      Log.warn(e.getMessage());
    }
  }
}