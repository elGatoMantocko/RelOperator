package relop;


/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  private Iterator iter;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    this.iter = iter;

    Schema schema = iter.getSchema();
    Schema newSchema = new Schema(fields.length);
    int fieldno = 0;
    for (Integer field : fields) {
      newSchema.initField(fieldno++, schema.fieldType(field), schema.fieldLength(field), schema.fieldName(field));
    }
    this.setSchema(newSchema);
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
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    Tuple next = iter.getNext();
    Tuple out = new Tuple(getSchema());

    Object[] allFields = next.getAllFields();
    for (int i = 0; i < allFields.length; i++) {
      int newSchemaFieldNum = getSchema().fieldNumber(next.schema.fieldName(i));
      if(newSchemaFieldNum > -1) { //If the field exists in the new schema.
        out.setField(newSchemaFieldNum, next.getField(i));
      }
    }

    return out;
  }

} // public class Projection extends Iterator
