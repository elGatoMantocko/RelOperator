package relop;

import global.SearchKey;
import index.HashIndex;
import heap.HeapFile;
import relop.IndexScan;

import java.util.HashMap;
import java.util.ArrayList;

public class HashJoin extends Iterator {

  private IndexScan outerScan, innerScan;
  private int outercolnum, innercolnum, currentHash;
  private HashMap<SearchKey, ArrayList<Tuple>> mMap;
  private Tuple next;

  public HashJoin(FileScan outer, FileScan inner, int outercolnum, int innercolnum) {
    // partitioning phase
    this.outercolnum = outercolnum;
    this.innercolnum = innercolnum;

    this.setSchema(Schema.join(outer.getSchema(), inner.getSchema()));

    next = new Tuple(getSchema());

    mMap = new HashMap<SearchKey, ArrayList<Tuple>>();

    currentHash = -1;

    // h1
    // create the index scans on the hash indexs
    outerScan = getIndexScan(outer, getHashIndex(outer, outercolnum));
    innerScan = getIndexScan(inner, getHashIndex(inner, innercolnum));
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
      // System.out.println(t.getField(colnum));
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

    // at some point here we have to clear the in memory hashmap

    // first we have to build a memory hash table on
    int innerHashValue = innerScan.getNextHash();
    // the hash table is built on the buckets in outerScan that are equal to anything on innerScan
    if (innerHashValue != currentHash) {
      currentHash = innerHashValue;
      outerScan.restart();
      mMap.clear();
      // we first have to find our bucket that we are on
      while (outerScan.hasNext() && outerScan.getNextHash() != currentHash) {
        outerScan.getNext();
      }

      // h2
      // insert any tuples in the current bucket into the memory hash
      while (outerScan.getNextHash() == currentHash && outerScan.hasNext()) {
        // we are on the correct partition
        Tuple tup = outerScan.getNext();
        SearchKey k = new SearchKey(tup.getField(outercolnum));
        if (!mMap.containsKey(k)) {
          mMap.put(k, new ArrayList<Tuple>());
        }

        // not sure but this doesn't handle duplicates
        mMap.get(k).add(tup);
      }
    }

    if (innerScan.hasNext()) {
      Tuple innerTuple = innerScan.getNext();
      ArrayList<Tuple> tuples = mMap.get(new SearchKey(innerTuple.getField(innercolnum)));
      if (!tuples.isEmpty()) {
        for (Tuple tuple : tuples) {
          if (innerTuple.getField(innercolnum).equals(tuple.getField(outercolnum))) {
            next = Tuple.join(tuple, innerTuple, this.getSchema());
            return true;
          }
        }
      }
    }

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

