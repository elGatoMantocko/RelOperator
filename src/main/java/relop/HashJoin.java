package relop;

import global.SearchKey;
import index.HashIndex;
import heap.HeapFile;
import relop.IndexScan;

public class HashJoin extends Iterator {

  private IndexScan outerScan, innerScan;
  private int outercolnum, innercolnum, currentHash;
  private HashTableDup hashTable;
  private Tuple next;

  public HashJoin(Iterator outer, Iterator inner, int outercolnum, int innercolnum) {
    // partitioning phase
    this.outercolnum = outercolnum;
    this.innercolnum = innercolnum;

    this.setSchema(Schema.join(outer.getSchema(), inner.getSchema()));

    next = new Tuple(getSchema());

    hashTable = new HashTableDup();

    // h1
    // create the index scans on the hash indexs
    if (outer instanceof IndexScan) {
      outerScan = (IndexScan)outer;
    } else if (outer instanceof FileScan) {
      outerScan = getIndexScan((FileScan)outer, getHashIndex((FileScan)outer, outercolnum));
    }

    if (inner instanceof IndexScan) {
      innerScan = (IndexScan)inner;
    } else if (inner instanceof FileScan) {
      innerScan = getIndexScan((FileScan)inner, getHashIndex((FileScan)inner, innercolnum));
    }
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
      hashTable.clear();
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
        if (!hashTable.containsKey(k)) {
          hashTable.add(k, tup);
        }
      }
    }

    if (innerScan.hasNext()) {
      Tuple innerTuple = innerScan.getNext();
      Tuple[] tuples = hashTable.getAll(new SearchKey(innerTuple.getField(innercolnum)));
      if (tuples!= null) {
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

