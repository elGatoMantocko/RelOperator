package relop;

import java.util.Map;

import global.SearchKey;
import index.HashIndex;
import heap.HeapFile;
import relop.IndexScan;

public class HashJoin extends Iterator {

  private Map<Object, Tuple> mMap;
  private IndexScan hash;

  public HashJoin(FileScan outer, FileScan inner, int outercolnum, int innercolnum) {
    // TODO HashJoin constructor with two filescans
    // partitioning phase
    HashIndex index = new HashIndex(null);
    HeapFile heap = new HeapFile(null);

    // have to build a heapfile
    while (outer.hasNext()) {
      heap.insertRecord(outer.getNext().getData());
    }

    // restart the scan
    outer.restart();

    // build the index scan
    while (outer.hasNext()) {
      // build a hashindex object
      index.insertEntry(new SearchKey(outer.getNext().getField(outercolnum)), outer.getLastRID());
    }
    hash = new IndexScan(outer.getSchema(), index, heap);

    // not sure if we will be able to use the map object
    //  need methods like getNextHash and getLastKey in index scan
    // mMap = new MultipleValueTreeMap<Object, Tuple>();
    // while (outer.hasNext()) {
    //   Tuple next = outer.getNext();
    //   mMap.put(next.getField(outercolnum), next);
    // }
    // while (inner.hasNext()) {
    //   Tuple next = outer.getNext();
    //   mMap.put(next.getField(innercolnum), next);
    // }
  }

  public HashJoin(HashJoin hj, IndexScan scan, int outercolnum, int innercolnum) {
    // TODO HashJoin copy constructor
    // not sure how to initialize this yet
  }

  @Override
  public void restart() {
    // TODO Auto-generated method stub

  }

  @Override
  public void explain(int depth) {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isOpen() {
    // TODO Auto-generated method stub
      return false;
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
      return false;
  }

  @Override
  public Tuple getNext() {
    // TODO Auto-generated method stub
      return null;
  }
}

