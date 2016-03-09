package relop;

public class HashJoin extends Iterator {

  public HashJoin(FileScan outer, FileScan inner, int outercolnum, int innercolnum) { 
    // TODO HashJoin constructor with two filescans
    // not sure how to initialize this yet
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
