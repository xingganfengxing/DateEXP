package com.litb.bi.support.dexp.reader;

import com.litb.bi.support.dexp.util.FileUtil;
import com.litb.bi.support.dexp.util.Log;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class ResultReaderLocal
  implements ResultReader
{
  private String encoding = "GBK";
  private BufferedReader br;
  private File[] flist;
  private boolean isdir = false;
  private int filepos = 0;

  public String readerLine() throws IOException
  {
    if (this.br == null) {
      return null;
    }
    String tmp = null;
    if (this.isdir) {
      tmp = this.br.readLine();
      while ((tmp == null) && (this.filepos < this.flist.length)) {
        this.br = getReader(this.flist[this.filepos]);
        this.filepos += 1;
        if (this.br == null) {
          continue;
        }
        tmp = this.br.readLine();
      }
      return tmp;
    }
    tmp = this.br.readLine();
    return tmp;
  }

  public void close()
  {
    try {
      this.br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void init(String path, String encoding) throws IOException
  {
    this.encoding = encoding;
    File file = FileUtil.getLocal(path, ResultReaderLocal.class);
    if (FileUtil.isFile(file)) {
      this.br = getReader(file);
    } else if (FileUtil.isDir(file)) {
      this.isdir = true;
      this.flist = file.listFiles();
      if ((this.flist != null) && (this.flist.length > 0)) {
        this.br = getReader(this.flist[this.filepos]);
        this.filepos += 1;
      } else {
        this.br = null;
        throw new IOException("No file under dir: " + 
          file.getAbsolutePath());
      }
    } else {
      throw new IOException("File doesn't exists: " + path);
    }
  }

  private BufferedReader getReader(File file) throws IOException
  {
    if ((file == null) || (!file.exists()) || (file.isHidden())) {
      return null;
    }
    InputStreamReader sr = new InputStreamReader(new FileInputStream(file), 
      this.encoding);
    BufferedReader br = new BufferedReader(sr);
    Log.log("Loading file: " + file);
    return br;
  }
}