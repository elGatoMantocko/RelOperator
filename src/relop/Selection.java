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
    scan.restart();
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

    if (!scan.hasNext()) {
      return false;
    }

    boolean passes;
    do {
      passes = false;
      next = scan.getNext();
      for (Predicate pred : preds) {
        passes = passes || pred.evaluate(next);
      }
    } while (!passes && scan.hasNext()); // exit the loop when we find a matching tuple

    // the state of the 'next' tuple should be stored in passes
    return passes;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    // get the next tuple that matches the predicates
    Tuple ret = next;

    if (next != null && !scan.hasNext()) { // next is the last element in the set of tuples
      next = null;
    }
    else if (next == null && !scan.hasNext()) { // there are no more tuples and next has been set to null
      // this means we are at the end of the scan after evaluating the last tuple
      // and the user still called getNext without checking if there were any more tuples in the scan.
      //  What a dick.
      throw new IllegalStateException();
    }

    return ret;
  }

} // public class Selection extends Iterator
