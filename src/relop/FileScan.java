package relop;

import global.RID;
import global.PageId;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  private HeapFile file;
  private HeapScan scan;

  private RID rid;

  private boolean isopen;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    this.setSchema(schema);
    this.file = file;
    this.scan = file.openScan();
    this.isopen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return isopen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    scan.close();
    isopen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {

    if (rid == null) {
      rid = new RID(new PageId(file.FIRST_PAGEID), 0);
    }

    try {
      return new Tuple(this.getSchema(), scan.getNext(rid));
    } catch(Exception e){
      throw new IllegalStateException();
    }
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return rid;
  }

} // public class FileScan extends Iterator
