package qp.utils;

import java.util.Comparator;
import java.util.Vector;

public class TupleComparator implements Comparator<Tuple> {

    Vector<AttributeHelper> attrOps;

    public TupleComparator(Vector<AttributeHelper> attrOps) {
	this.attrOps = attrOps;
    }


    @Override
    public int compare(Tuple o1, Tuple o2) {
	if (Tuple.goodOrder(o1, o2, attrOps)) {
	    // o1 is before o2.
	    return -1;
	} else if (Tuple.goodOrder(o2, o1, attrOps)) {
	    // o1 is after o2.
	    return 1;
	} else {
	    // o1 and o2 has the same value for the sort attributes.
	    return 0;
	}
    }
}
