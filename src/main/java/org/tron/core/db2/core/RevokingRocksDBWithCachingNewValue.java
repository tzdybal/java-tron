package org.tron.core.db2.core;


import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.tron.common.utils.ByteUtil;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.WrappedByteArray;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.IRevokingDB;
import org.tron.core.db2.common.LevelDB;
import org.tron.core.db2.common.Value;
import org.tron.core.exception.ItemNotFoundException;

public class RevokingRocksDBWithCachingNewValue implements IRevokingDB {

  private ThreadLocal<Boolean> mode = new ThreadLocal<>();
  private Snapshot head;
  @Getter
  private String dbName;
  private Class<? extends DB> clz;

  public RevokingRocksDBWithCachingNewValue(String dbName) {
    this.dbName = dbName;
    head = new SnapshotRoot(Args.getInstance().getOutputDirectoryByDbName(dbName), dbName, "rocksDb");
    mode.set(true);
  }

  public RevokingRocksDBWithCachingNewValue(String dbName, Class<? extends DB> clz) {
    this.dbName = dbName;
    this.clz = clz;
    head = new SnapshotRoot(Args.getInstance().getOutputDirectoryByDbName(dbName), dbName, clz);
    mode.set(true);
  }

  @Override
  public synchronized void put(byte[] key, byte[] value) {
    head().put(key, value);
  }

  @Override
  public synchronized void delete(byte[] key) {
    head().remove(key);
  }

  @Override
  public synchronized boolean has(byte[] key) {
    return getUnchecked(key) != null;
  }

  @Override
  public synchronized byte[] get(byte[] key) throws ItemNotFoundException {
    byte[] value = getUnchecked(key);
    if (value == null) {
      throw new ItemNotFoundException();
    }

    return value;
  }

  @Override
  public synchronized byte[] getUnchecked(byte[] key) {
    return head().get(key);
  }

  @Override
  public synchronized void close() {
    head.close();
  }

  @Override
  public synchronized void reset() {
    head().reset();
    head().close();
    if (clz == null) {
      head = new SnapshotRoot(Args.getInstance().getOutputDirectoryByDbName(dbName), dbName, "rocksDb");
    } else {
      head = new SnapshotRoot(Args.getInstance().getOutputDirectoryByDbName(dbName), dbName, clz);
    }
  }

  private Snapshot head() {
    if (mode.get() == null || mode.get()) {
      return head;
    } else {
      return head.getSolidity();
    }
  }

  @Override
  public void setMode(boolean mode) {
    this.mode.set(mode);
  }

  public synchronized Snapshot getHead() {
    return head();
  }

  public synchronized void setHead(Snapshot head) {
    this.head = head;
  }

  //for blockstore
  @Override
  public Set<byte[]> getlatestValues(long limit) {
    return getlatestValues(head(), limit);
  }

  //for blockstore
  private synchronized Set<byte[]> getlatestValues(Snapshot head, long limit) {
    if (limit <= 0) {
      return Collections.emptySet();
    }

    Set<byte[]> result = new HashSet<>();
    Snapshot snapshot = head;
    long tmp = limit;
    for (; tmp > 0 && snapshot.getPrevious() != null; snapshot = snapshot.getPrevious()) {
      if (!((SnapshotImpl) snapshot).db.isEmpty()) {
        --tmp;
        Streams.stream(((SnapshotImpl) snapshot).db)
            .map(Map.Entry::getValue)
            .map(Value::getBytes)
            .forEach(result::add);
      }
    }

    if (snapshot.getPrevious() == null && tmp != 0) {
      result.addAll(((LevelDB) ((SnapshotRoot) snapshot).db).getDb().getlatestValues(tmp));
    }

    return result;
  }

  //for blockstore
  public Set<byte[]> getValuesNext(Snapshot head, byte[] key, long limit) {
    if (limit <= 0) {
      return Collections.emptySet();
    }

    Map<WrappedByteArray, WrappedByteArray> collection = new HashMap<>();
    if (head.getPrevious() != null) {
      ((SnapshotImpl) head).collect(collection);
    }

    Map<WrappedByteArray, WrappedByteArray> rocksDBMap = new HashMap<>();

    ((LevelDB) ((SnapshotRoot) head.getRoot()).db).getDb().getNext(key, limit).entrySet().stream()
        .map(e -> Maps.immutableEntry(WrappedByteArray.of(e.getKey()), WrappedByteArray.of(e.getValue())))
        .forEach(e -> rocksDBMap.put(e.getKey(), e.getValue()));

    rocksDBMap.putAll(collection);

    return rocksDBMap.entrySet().stream()
        .sorted((e1, e2) -> ByteUtil.compare(e1.getKey().getBytes(), e2.getKey().getBytes()))
        .filter(e -> ByteUtil.greaterOrEquals(e.getKey().getBytes(), key))
        .limit(limit)
        .map(Map.Entry::getValue)
        .map(WrappedByteArray::getBytes)
        .collect(Collectors.toSet());
  }

  @Override
  public Set<byte[]> getValuesNext(byte[] key, long limit) {
    return getValuesNext(head(), key, limit);
  }

  @Override
  public synchronized Iterator<Entry<byte[], byte[]>> iterator() {
    return head().iterator();
  }
}