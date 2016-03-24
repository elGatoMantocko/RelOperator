package relop;

import global.SearchKey;
import global.RID;
import index.HashIndex;
import heap.HeapFile;
import relop.IndexScan;

public class HashJoin extends Iterator {

  private IndexScan left, right;
  private int leftColNum, rightColNum, currentHash;

  private HashTableDup hashTable;
  private Tuple[] tupsInBucket;
  private Tuple currentRightTup;
  // we need to save the state of the current bucket search
  private int posInTupsArray;
  
  private Tuple next;

  public HashJoin(Iterator left, Iterator right, int leftColNum, int rightColNum) {
    // partitioning phase
    this.leftColNum = leftColNum;
    this.rightColNum = rightColNum;

    this.setSchema(Schema.join(left.getSchema(), right.getSchema()));
    
    // these will be used to determine the next tuples in the hash
    this.next = null;
    this.hashTable = new HashTableDup();

    this.currentHash = -1;

    // build the right index scan
    if (left instanceof IndexScan) {
      this.left = (IndexScan)left;
    } else {
      this.left = getIndexScan(left, leftColNum);
    }

    // build the right index scan
    if (right instanceof IndexScan) {
      this.right = (IndexScan)right;
    } else {
      this.right = getIndexScan(right, rightColNum);
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
    left.restart();
    right.restart();
  }

  @Override
  public void explain(int depth) {
    this.indent(depth);
    System.out.println("HashJoin");

    left.explain(depth + 1);
    right.explain(depth + 1);
  }

  @Override
  public void close() {
    left.close();
    right.close();
  }

  @Override
  public boolean isOpen() {
    return left.isOpen() && right.isOpen();
  }

  @Override
  public boolean hasNext() {

    if (next != null) {
      return true;
    }

    // check if the hashtable has elements in it or not
    if (tupsInBucket == null) {
      // we need to find the tuples in the current bucket
      int hashValue = right.getNextHash();
      // the right Bucket is not on the correct hash so we need to rebuild the hashTable
      if (hashValue != currentHash) {
        currentHash = hashValue;
        left.restart();
        hashTable.clear();

        // we need to find the correct bucket on the right scan
        while (left.hasNext() && left.getNextHash() != currentHash) {
          left.getNext();
        }
        
        // find all of the tuples in the current hash index and add them to the hashTable
        while (left.getNextHash() == currentHash && left.hasNext()) {
          Tuple leftTup = left.getNext();
          hashTable.add(new SearchKey(leftTup.getField(leftColNum)), leftTup);
        }
      }

      if (right.hasNext()) {
        currentRightTup = right.getNext();
        tupsInBucket = hashTable.getAll(new SearchKey(currentRightTup.getField(rightColNum)));
        if (tupsInBucket != null) {
          while (posInTupsArray < tupsInBucket.length) {
            if (currentRightTup.getField(rightColNum).equals(tupsInBucket[posInTupsArray].getField(leftColNum))) {
              next = Tuple.join(tupsInBucket[posInTupsArray++], currentRightTup, getSchema());
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
        if (currentRightTup.getField(rightColNum).equals(tupsInBucket[posInTupsArray].getField(leftColNum))) {
          next = Tuple.join(tupsInBucket[posInTupsArray++], currentRightTup, getSchema());
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
    if (next == null && !hasNext()) {
      throw new IllegalStateException();
    } else {
      Tuple ret = next;
      next = null;
      return ret;
    }
  }
}

