package com.litb.bi.support.dexp.reader;

import com.litb.bi.support.dexp.util.S3Util;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public class ResultReaderS3
  implements ResultReader
{
  private String encoding = "GBK";
  private BufferedReader br;
  private int filepos = 0;
  private String path = "";

  public String readerLine() throws IOException
  {
    if (this.br == null) {
      return null;
    }
    String tmp = null;

    List objectList = S3Util.getObjectList("pajhdf", this.path);//bucketName按照实际环境更改
    tmp = this.br.readLine();
    while ((tmp == null) && (this.filepos < objectList.size())) {
      this.br = getReader((String)objectList.get(this.filepos));
      this.filepos += 1;
      if (this.br == null) {
        continue;
      }
      tmp = this.br.readLine();
    }

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
    this.path = path;

    List objectList = S3Util.getObjectList("pajhdf", path);
    if ((objectList != null) && (objectList.size() > 0)) {
      this.br = getReader(objectList.get(this.filepos).toString());
      this.filepos += 1;
    } else {
      this.br = null;
      throw new IOException("No file under dir: "+objectList);
    }
  }

  private BufferedReader getReader(String path)
    throws IOException
  {
    this.br = S3Util.s3reader("pajhdf", path);
    return this.br;
  }
}