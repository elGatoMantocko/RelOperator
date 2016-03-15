package relop;

import global.SearchKey;
import index.HashIndex;
import heap.HeapFile;
import relop.IndexScan;

public class HashJoin extends Iterator {

  private IndexScan outerScan, innerScan;
  private int outercolnum, innercolnum, currentHash;

  private HashTableDup hashTable;
  private Tuple[] tuples;
  private Tuple currentInnerTup;
  private int position;
  
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

  public HashJoin(HashJoin outer, IndexScan innerScan, int outercolnum, int innercolnum) {
    this.outercolnum = outercolnum;
    this.innercolnum = innercolnum;

    this.setSchema(Schema.join(outer.getSchema(), innerScan.getSchema()));

    next = new Tuple(getSchema());

    hashTable = new HashTableDup();

    // i think we need to actually perform the join here
    //  then build an index on the resulting table
    HeapFile heap = new HeapFile(null);
    // first build a heapfile on the outer join
    while (outer.hasNext()) {
      Tuple t = outer.getNext();
      heap.insertRecord(t.getData());
    }

    outer.restart();

    // then build a hash index
    FileScan file = new FileScan(getSchema(), heap);
    HashIndex hash = new HashIndex(null);
    while (file.hasNext()) {
      Tuple t = file.getNext();
      hash.insertEntry(new SearchKey(t.getField(outercolnum)), file.getLastRID());
    }

    this.outerScan = new IndexScan(getSchema(), hash, heap);
    this.innerScan = innerScan;
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
    if (tuples != null) {
      if (position == tuples.length - 1) {
        position = 0;
        tuples = null;
        return hasNext();
      } else {
        while (position < tuples.length) {
          if (currentInnerTup.getField(innercolnum).equals(tuples[position].getField(outercolnum))) {
            next = Tuple.join(tuples[position++], currentInnerTup, getSchema());
            return true;
          }
          position++;
        }
        position = 0;
        tuples = null;
        return hasNext();
      }

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

      if (innerScan.hasNext()) {
        currentInnerTup = innerScan.getNext();
        tuples = hashTable.getAll(new SearchKey(currentInnerTup.getField(innercolnum)));
        if (tuples != null) {
          while (position < tuples.length) {
            if (currentInnerTup.getField(innercolnum).equals(tuples[position].getField(outercolnum))) {
              next = Tuple.join(tuples[position++], currentInnerTup, getSchema());
              return true;
            }
            position++;
          }
          position = 0;
          tuples = null;
          return hasNext();
        }
      }
      else {
        tuples = null;
        return false;
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

