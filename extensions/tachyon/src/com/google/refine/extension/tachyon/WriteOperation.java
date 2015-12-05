package com.google.refine.extension.tachyon;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

public class WriteOperation implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final WriteType mWriteType;
  private ByteBuffer mBuffer = null;

  public WriteOperation(TachyonURI masterLocation, TachyonURI filePath, WriteType writeType, ByteBuffer buffer) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mWriteType = writeType;
    mBuffer = buffer;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mMasterLocation, new TachyonConf());
    createFile(tachyonClient);
    writeFile(tachyonClient);
    return Boolean.TRUE;
//    return readFile(tachyonClient);
  }

  private void createFile(TachyonFS tachyonClient) throws IOException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = tachyonClient.createFile(mFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  private void writeFile(TachyonFS tachyonClient) throws IOException {
    /**
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    */
    LOG.debug("Writing data...");
    mBuffer.flip();
    
      
    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    OutStream os = file.getOutStream(mWriteType);
    os.write(mBuffer.array());
    os.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "writeFile to file " + mFilePath);
  }

  /**
  private boolean readFile(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;
    LOG.debug("Reading data...");

    final long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    TachyonByteBuffer buf = file.readByteBuffer(0);
    if (buf == null) {
      file.recache();
      buf = file.readByteBuffer(0);
    }
    buf.mData.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      pass = pass && (buf.mData.getInt() == k);
    }
    buf.close();

    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "readFile file " + mFilePath);
    return pass;
  }
  */

  /**
  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicOperations <TachyonMasterAddress> <FilePath> <WriteType>");
      System.exit(-1);
    }

    Utils.runExample(new WriteOperation(new TachyonURI(args[0]), new TachyonURI(args[1]),
        WriteType.valueOf(args[2])));
  }
  */
}