/**
 * Tuple container class
 **/

package qp.utils;

import java_cup.runtime.Symbol;
import qp.operators.Debug;

import java.util.*;
import java.io.*;

/**
 * Tuple - a simple object which holds an ArrayList of data
 */
public class Tuple implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;

    public ArrayList<Object> _data;

    public Tuple(ArrayList<Object> d) {
        _data = d;
    }

    /**
     * Accessor for data
     */
    public ArrayList<Object> data() {
        return _data;
    }

    public Object dataAt(int index) {
        return _data.get(index);
    }

    /**
     * Checks whether the join condition is satisfied or not with one condition
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, int leftindex, int rightindex) {
        Object leftData = dataAt(leftindex);
        Object rightData = right.dataAt(rightindex);
        if (leftData.equals(rightData))
            return true;
        else
            return false;
    }

    /**
     * Checks whether the join condition is satisfied or not with multiple conditions
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, ArrayList<Integer> leftindex, ArrayList<Integer> rightindex) {
        if (leftindex.size() != rightindex.size())
            return false;
        for (int i = 0; i < leftindex.size(); ++i) {
            Object leftData = dataAt(leftindex.get(i));
            Object rightData = right.dataAt(rightindex.get(i));
            if (!leftData.equals(rightData)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Joining two tuples without duplicate column elimination
     **/
    public Tuple joinWith(Tuple right) {
        ArrayList<Object> newData = new ArrayList<>(this.data());
        newData.addAll(right.data());
        return new Tuple(newData);
    }

    /**
     * Compare two tuples in the same table on given attribute
     **/
    public static int compareTuples(Tuple left, Tuple right, int index) {
        return compareTuples(left, right, index, index);
    }

    /**
     * Compare two tuples in the same table on a list of given attributes
     */
    public static int compareTuples(Tuple left, Tuple right, ArrayList<Integer> indexes) {
        return compareTuples(left, right, indexes, indexes);
    }

    /**
     * Comparing tuples in different tables, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, int leftIndex, int rightIndex) {
        Object leftdata = left.dataAt(leftIndex);
        Object rightdata = right.dataAt(rightIndex);
        if (leftdata instanceof Integer) {
            return ((Integer) leftdata).compareTo((Integer) rightdata);
        } else if (leftdata instanceof String) {
            return ((String) leftdata).compareTo((String) rightdata);
        } else if (leftdata instanceof Float) {
            return ((Float) leftdata).compareTo((Float) rightdata);
        } else {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
    }

    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, ArrayList<Integer> leftIndex, ArrayList<Integer> rightIndex) {
        if (leftIndex.size() != rightIndex.size()) {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
        for (int i = 0; i < leftIndex.size(); ++i) {
            Object leftdata = left.dataAt(leftIndex.get(i));
            Object rightdata = right.dataAt(rightIndex.get(i));
            if (leftdata.equals(rightdata)) continue;
            if (leftdata instanceof Integer) {
                return ((Integer) leftdata).compareTo((Integer) rightdata);
            } else if (leftdata instanceof String) {
                return ((String) leftdata).compareTo((String) rightdata);
            } else if (leftdata instanceof Float) {
                return ((Float) leftdata).compareTo((Float) rightdata);
            } else {
                System.out.println("Tuple: Unknown comparision of the tuples");
                System.exit(1);
                return 0;
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        // IntelliJ's suggested hashCode function.
        int result = (int) (_data.hashCode() ^ (_data.hashCode() >>> 32));
        for (Object obj : _data) {
            if (obj instanceof Integer) {
                result += 31 * result + ((Integer) obj).hashCode();
            } else if (obj instanceof String) {
                result += 31 * result + ((String) obj).hashCode();
            } else if (obj instanceof Float) {
                result += 31 * result + ((Float) obj).hashCode();
            } else {
                System.out.println("Tuple: hashCode() unsupported for unknown data type");
                System.exit(1);
                return 0;
            }
        }
//        System.out.println("Hashcode: "  + result);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof Tuple)) return false;
        return o.hashCode() == this.hashCode();
    }
}
