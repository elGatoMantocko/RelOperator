package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  private Predicate[] preds;
  private Iterator scan;

  private Tuple next;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.scan = iter;
    this.preds = preds;
    this.setSchema(iter.getSchema());

    // should we actually find the first tuple matching the predicate here?
    boolean passes;
    do {
      passes = false;
      next = scan.getNext();
      for (Predicate pred : preds) {
        passes = passes || pred.evaluate(next);
      }
    } while (scan.hasNext() && !passes);
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
    return scan.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    scan.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    // is there another tuple in the file
    //  that matches all of the predicates?
    // return scan.hasNext();
    if (next != null) {
      return true;
    }
    
    return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    // get the next tuple that matches the predicates
    // return new Tuple(scan.getSchema(), scan.getNext());
    Tuple ret = next;

    if (!scan.hasNext()) {
      next = null;
      return ret;
    }

    boolean passes;
    do {
      passes = false;
      next = scan.getNext();
      // System.out.println(next.toString());
      for (Predicate pred : preds) {
        passes = passes || pred.evaluate(next);
      }
    } while (scan.hasNext() && !passes);

    return ret;
  }

} // public class Selection extends Iterator
