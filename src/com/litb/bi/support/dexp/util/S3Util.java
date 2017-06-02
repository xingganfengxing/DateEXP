package com.litb.bi.support.dexp.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class S3Util
{
  private static AmazonS3 s3 = null;

  static
  {
    try {
      AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
      s3 = new AmazonS3Client(credentials);
    }
    catch (Exception e) {
      throw new AmazonClientException(
        "Cannot load the credentials from the credential profiles file. Please make sure that your credentials file is at the correct location (~/.aws/credentials), and is in valid format.", 
        e);
    }
  }

  public static AmazonS3 getS3()
  {
    return s3;
  }

  private static void displayTextInputStream(InputStream input)
    throws IOException
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    while (true)
    {
      String line = reader.readLine();
      if (line == null)
        break;
      System.out.println("    " + line);
    }
    System.out.println();
  }

  public static BufferedReader s3reader(String bucketName, String objectName)
  {
    S3Object object = s3.getObject(new GetObjectRequest(bucketName, objectName));
    BufferedReader reader = new BufferedReader(
      new InputStreamReader(object.getObjectContent()));
    return reader;
  }

  public static List<String> getObjectList(String BucketName, String prefix)
  {
    ObjectListing o = s3.listObjects(new ListObjectsRequest()
      .withBucketName(BucketName)
      .withPrefix(prefix));

    List<S3ObjectSummary> list = o.getObjectSummaries();
    List objectList = new ArrayList();

    for (S3ObjectSummary objectSummary : list) {
      objectList.add(objectSummary.getKey());
    }
    return objectList;
  }
}