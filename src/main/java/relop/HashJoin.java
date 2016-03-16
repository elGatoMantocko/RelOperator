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
  // we need to save the state of the current bucket search
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
    this.indent(depth);
    System.out.println("HashJoin");

    outerScan.explain(depth + 1);
    innerScan.explain(depth + 1);
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

    // check if the hashtable has elements in it or not
    if (tupsInBucket == null) {
      // we need to find the tuples in the current bucket
      int hashValue = innerScan.getNextHash();
      // the outerBucket is not on the correct hash so we need to rebuild the hashTable
      if (hashValue != currentHash) {
        currentHash = hashValue;
        outerScan.restart();
        hashTable.clear();

        // we need to find the correct bucket on the outer scan
        while (outerScan.hasNext() && outerScan.getNextHash() != currentHash) {
          outerScan.getNext();
        }
        
        // find all of the tuples in the current hash index and add them to the hashTable
        while (outerScan.getNextHash() == currentHash && outerScan.hasNext()) {
          Tuple outerTup = outerScan.getNext();
          hashTable.add(new SearchKey(outerTup.getField(outercolnum)), outerTup);
        }
      }

      if (innerScan.hasNext()) {
        currentInnerTup = innerScan.getNext();
        tupsInBucket = hashTable.getAll(new SearchKey(currentInnerTup.getField(innercolnum)));
        if (tupsInBucket != null) {
          while (posInTupsArray < tupsInBucket.length) {
            if (currentInnerTup.getField(innercolnum).equals(tupsInBucket[posInTupsArray].getField(outercolnum))) {
              next = Tuple.join(tupsInBucket[posInTupsArray++], currentInnerTup, getSchema());
              return true;
            }
            posInTupsArray++;
          }
        }
        posInTupsArray = 0;
        tupsInBucket = null;
        return hasNext();
      } else {
        tupsInBucket = null;
        return false;
      }
    } else if (posInTupsArray == tupsInBucket.length - 1) { // the hashtable has been initialized, but the position is at the end
      posInTupsArray = 0;
      tupsInBucket = null;
      return hasNext();
    } else {
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
  }

  @Override
  public Tuple getNext() {
    if (next == null) {
      throw new IllegalStateException();
    }
    
    return next;
  }
}

