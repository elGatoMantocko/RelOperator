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
    this.indent(depth);
    System.out.println("Selection");

    scan.explain(depth + 1);
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
    // if there is already a tuple in next, return true
    if (next != null) {
      return true;
    } else if (!scan.hasNext()) { // no more tuples in the iterator
      return false;
    } else { // the typical case that we need to find the next tuple
      boolean passes = false;
      // while there are still tuples left in the scan
      while (scan.hasNext() && !passes) {
        // get the next tuple
        next = scan.getNext();
        // if the tuple passes a predicate then we can return true
        for (Predicate pred : preds) {
          if (passes = pred.evaluate(next)) {
            break;
          }
        }
      }

      // the state of the 'next' tuple should be stored in passes
      return passes;
    }
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    // return the next tuple that matches the predicates
    if (next != null) {
      Tuple ret = next;
      next = null;
      return ret;
    } else {
      throw new IllegalStateException();
    }
  }

} // public class Selection extends Iterator

