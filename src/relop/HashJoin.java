package relop;

public class HashJoin extends Iterator {

  public Schema schema;

  public HashJoin() {
    schema = null;
  }
    
  public HashJoin(Schema schema) { 
    this.schema = schema;
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
