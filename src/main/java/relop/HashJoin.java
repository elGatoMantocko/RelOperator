package relop;

import global.SearchKey;
import index.HashIndex;
import index.BucketScan;
import heap.HeapFile;
import relop.IndexScan;

public class HashJoin extends Iterator {

  private IndexScan outerScan, innerScan;
  private BucketScan outerBucketScan, innerBucketScan;
  private HashIndex outerHash, innerHash;

  private int outercolnum, innercolnum;

  private Schema schema;

  private Tuple next;

  public HashJoin(FileScan outer, FileScan inner, int outercolnum, int innercolnum) {
    // partitioning phase
    this.outercolnum = outercolnum;
    this.innercolnum = innercolnum;

    this.schema = Schema.join(outer.getSchema(), inner.getSchema());

    next = new Tuple(schema);

    // create the hash index on the scans given
    outerHash = getHashIndex(outer, outercolnum);
    innerHash = getHashIndex(inner, innercolnum);

    // create the index scans on the hash indexs
    outerScan = getIndexScan(outer, outerHash);
    innerScan = getIndexScan(inner, innerHash);

    outerBucketScan = outerHash.openScan();
    innerBucketScan = innerHash.openScan();
  }

  public HashJoin(HashJoin hj, IndexScan scan, int outercolnum, int innercolnum) {
    // TODO HashJoin copy constructor
    // not sure how to initialize this yet
  }

  private HashIndex getHashIndex(FileScan scan, int colnum) {
    HashIndex hash = new HashIndex(null);
    scan.restart();

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      hash.insertEntry(new SearchKey(t.getField(colnum)), scan.getLastRID());
    }

    return hash;
  }

  private IndexScan getIndexScan(FileScan scan, HashIndex index) {
    HeapFile heap = new HeapFile(null);

    // make sure we are at the top of the scan
    scan.restart();

    // have to build a heapfile
    while (scan.hasNext()) {
      heap.insertRecord(scan.getNext().getData());
    }

    // create the index scan on the hashindex
    return new IndexScan(scan.getSchema(), index, heap);
  }

  @Override
  public void restart() {
    outerScan.restart();
    innerScan.restart();
  }

  @Override
  public void explain(int depth) {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() {
    outerScan.close();
    innerScan.close();
  }

  @Override
  public boolean isOpen() {
    return outerScan.isOpen() && innerScan.isOpen();
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Tuple getNext() {
    if (next == null) {
      throw new IllegalStateException();
    }
    
    return next;
  }
}

