package relop;

import global.SearchKey;
import global.RID;
import index.HashIndex;
import heap.HeapFile;
import relop.IndexScan;

public class HashJoin extends Iterator {

  private IndexScan outerScan, innerScan;
  private int outercolnum, innercolnum, currentHash;

  private HashTableDup hashTable;
  private Tuple[] tupsInBucket;
  private Tuple currentInnerTup;
  private int posInTupsArray;
  
  private Tuple next;

  public HashJoin(Iterator outer, Iterator inner, int outercolnum, int innercolnum) {
    // partitioning phase
    this.outercolnum = outercolnum;
    this.innercolnum = innercolnum;

    this.setSchema(Schema.join(outer.getSchema(), inner.getSchema()));
    
    // these will be used to determine the next tuples in the hash
    this.next = new Tuple(getSchema());
    this.hashTable = new HashTableDup();

    this.currentHash = -1;

    // build the outer index scan
    if (outer instanceof IndexScan) {
      this.outerScan = (IndexScan)outer;
    } else if (outer instanceof FileScan || outer instanceof HashJoin) {
      this.outerScan = getIndexScan(outer, outercolnum);
    }

    // build the inner index scan
    if (inner instanceof IndexScan) {
      this.innerScan = (IndexScan)inner;
    } else if (inner instanceof FileScan || inner instanceof HashJoin) {
      this.innerScan = getIndexScan(inner, innercolnum);
    }
  }

  private IndexScan getIndexScan(Iterator scan, int colnum) {
    HashIndex index = new HashIndex(null);
    HeapFile heap = new HeapFile(null);
    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      RID rid = heap.insertRecord(t.getData());
      index.insertEntry(new SearchKey(t.getField(colnum)), rid);
    }

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

  private boolean findNextTuple() {
    while (posInTupsArray < tupsInBucket.length) {
      if (currentInnerTup.getField(innercolnum).equals(tupsInBucket[posInTupsArray].getField(outercolnum))) {
        next = Tuple.join(tupsInBucket[posInTupsArray++], currentInnerTup, getSchema());
        return true;
      }
      posInTupsArray++;
    }
    posInTupsArray = 0;
    tupsInBucket = null;
    return hasNext();
  }

  @Override
  public boolean hasNext() {

    // if we already have tuples and haven't reached the end
    //  continue checking through that bucket
    if (tupsInBucket != null && posInTupsArray == tupsInBucket.length - 1) { // if we are at the end, reset
      posInTupsArray = 0;
      tupsInBucket = null;
      return hasNext();
    } else if (tupsInBucket != null){ // if we aren't at the end, lets find the tuple in that bucket
      return findNextTuple();
    } else {

      // lets first check that we are on the right bucket
      int innerHashValue = innerScan.getNextHash();
      if (innerHashValue != currentHash) {
        // we aren't on the right bucket and need to create the hashTable
        currentHash = innerHashValue;
        outerScan.restart();
        hashTable.clear();

        while (outerScan.hasNext() && outerScan.getNextHash() != currentHash) {
          outerScan.getNext();
        }

        while (outerScan.getNextHash() == currentHash && outerScan.hasNext()) {
          // currentHash is now correctly set to the right bucket
          Tuple outerTup = outerScan.getNext();
          hashTable.add(new SearchKey(outerTup.getField(outercolnum)), outerTup);
        }
      }

      while (innerScan.hasNext()) {
        currentInnerTup = innerScan.getNext();
        tupsInBucket = hashTable.getAll(new SearchKey(currentInnerTup.getField(innercolnum)));
        if (tupsInBucket != null) {
          return findNextTuple();
        }
      }
    }
    tupsInBucket = null;
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

