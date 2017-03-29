package qp.sorter.util;

import java.util.Comparator;

import qp.utils.Tuple;

public class TupleComparator implements Comparator<Tuple>
{
    public int compare(Tuple t1, Tuple t2)
    {
        return ((Integer) t1.dataAt(0)).compareTo((Integer) t2.dataAt(0));
    }
}