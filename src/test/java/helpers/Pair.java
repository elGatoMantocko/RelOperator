package helpers;


/**
 * Created by david on 3/9/16.
 */
public class Pair<T, K> {
    private T value1;
    private K value2;

    public Pair(T value1, K value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public T getValue1() {
        return value1;
    }

    public K getValue2() {
        return value2;
    }
}
