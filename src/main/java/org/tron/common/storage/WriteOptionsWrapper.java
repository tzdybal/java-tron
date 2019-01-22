package org.tron.common.storage;

import org.tron.core.config.args.Args;

public class WriteOptionsWrapper {

  public volatile static WriteOptionsWrapper wapper;
  public org.rocksdb.WriteOptions rocks = null;
  public org.iq80.leveldb.WriteOptions level = null;
  private static String dbEngine = "";

  private WriteOptionsWrapper(){

  }

  public static WriteOptionsWrapper getInstance() {
    if (wapper == null) {
      synchronized (WriteOptionsWrapper.class) {
        if (wapper == null) {
          createInstance();
        }
      }
    }
    return wapper;
  }

  private static void createInstance() {
    wapper = new WriteOptionsWrapper();
    if (Args.getInstance().getStorage().getDbVersion() == 2) {
      dbEngine = "levelDb";
      wapper.level = new org.iq80.leveldb.WriteOptions();
    } else if (Args.getInstance().getStorage().getDbVersion() == 3) {
      wapper.rocks = new org.rocksdb.WriteOptions();
      dbEngine = "rocksDb";
    }
  }


  public WriteOptionsWrapper sync(boolean bool) {
    if (dbEngine.equals("levelDb")) {
      wapper.level.sync(bool);
    } else if (dbEngine.equals("rocksDb")) {
      wapper.rocks.setSync(bool);
    }
    return wapper;
  }
}