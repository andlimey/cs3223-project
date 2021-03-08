package qp.utils;

import java.util.*;
import java.util.stream.Collectors;

import static qp.utils.Tuple.compareTuples;

public class TupleComparator implements Comparator<Tuple> {
    ArrayList<Integer> attrList;

    public TupleComparator(int[] attrList) {
        this.attrList = new ArrayList<Integer>(Arrays.stream(attrList).boxed().collect(Collectors.toList()));
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        return compareTuples(t1, t2, this.attrList);
    }
}