package relop;

public class HashJoin extends Iterator {

  public Schema schema;

  public HashJoin() {
    // TODO Empty HashJoin constructor
    // not sure how to initialize this yet
  }
    
  public HashJoin(FileScan scan1, FileScan scan2, int noidea, int noclue) { 
    // TODO HashJoin constructor with two filescans
    // not sure how to initialize this yet
  }

  public HashJoin(HashJoin hj, IndexScan scan, int noidea, int noclue) {
    // TODO HashJoin copy constructor
    // not sure how to initialize this yet
  }

  @Override
  public void restart() {
    // TODO Auto-generated method stub

  }

  @Override
  public Tuple getNext() {
    // TODO Auto-generated method stub
    return null;
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

}
