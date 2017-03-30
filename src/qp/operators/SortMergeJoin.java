package qp.operators;

import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.OrderByEnum;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
	//same parater setting as nestedjoin
	int batchsize;
	int leftindex;
	int rightindex;
	Batch leftbatch;
	Batch rightbatch;
	Batch outbatch;
	Batch outbatchOverflow;
	//specify the order as ascending;
	OrderByEnum orderByEnum = OrderByEnum.ASC;
	Tuple leftTuple;
	Tuple rightTuple;
	//Lists to store resultant tuples on the join condition
	//from left and rigt tables
	List<Tuple> resultRightTuples;
	List<Tuple> resultLeftTuples;

	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}
	//methods to extract next tuple
	private Tuple getNextTuple(Batch batch, Operator node) {
		if (batch != null) {
			Tuple tuple = batch.getTuples().get(0);
			batch.getTuples().remove(0);
			return tuple;
		} else {
			return null;
		}
	}

	private Tuple nextLeftTuple() {
		if (leftbatch.isEmpty()) {
			leftbatch = left.next();
		}
		return getNextTuple(leftbatch, left);
	}

	private Tuple nextRightTuple() {
		if (rightbatch.isEmpty()) {
			rightbatch = right.next();
		}
		return getNextTuple(rightbatch, right);
	}

	public boolean open() {
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);

		if (!left.open()) {
			return false;
		}
		if (!right.open()) {
			return false;
		}		
		leftbatch = left.next();
		rightbatch = right.next();
		resultRightTuples = new ArrayList<Tuple>();
		resultLeftTuples = new ArrayList<Tuple>();
		leftTuple = getNextTuple(leftbatch, left);
		rightTuple = getNextTuple(rightbatch, right);
		outbatchOverflow = new Batch(batchsize);
		return true;
	}

	public Batch next() {
		if (joinIsCompleted()) {
			return null;
		}
		outbatch = new Batch(batchsize);

		// add the one of the previous work that would have been overflowed
		while (!outbatch.isFull() && !outbatchOverflow.isEmpty()) {
			outbatch.add(outbatchOverflow.elementAt(0));
			outbatchOverflow.remove(0);
		}

		// We continue the join
		while (!outbatch.isFull() && !joinIsCompleted()) {
			int compare = compareTuplesForJoin(leftTuple, rightTuple);
			if (compare == 0) {
				resultLeftTuples.add(leftTuple);
				resultRightTuples.add(rightTuple);
				// get all the tuple from the right that are equal
				rightTuple = nextRightTuple();
				while (rightTuple != null && compareTuplesForJoin(resultLeftTuples.get(0), rightTuple) == 0) {
					resultRightTuples.add(rightTuple);
					rightTuple = nextRightTuple();
				}
				// get all the tuple from the left that are equal
				leftTuple = nextLeftTuple();
				while (leftTuple != null && compareTuplesForJoin(leftTuple, resultRightTuples.get(0)) == 0) {
					resultLeftTuples.add(leftTuple);
					leftTuple = nextLeftTuple();
				}
				
				for (Tuple leftTuple : resultLeftTuples) {
					for (Tuple rightTuple : resultRightTuples) {
						if (!outbatch.isFull()) {
							// System.out.println("add: " + leftTuple.dataAt(leftindex) + "=" +
							// rightTuple.dataAt(rightindex));
							outbatch.add(leftTuple.joinWith(rightTuple));
						} else {
							// System.out.println("addT: " + leftTuple.dataAt(leftindex) + "=" +
							// rightTuple.dataAt(rightindex));
							outbatchOverflow.add(leftTuple.joinWith(rightTuple));
						}
					}
				}
				resultRightTuples = new ArrayList<Tuple>();
				resultLeftTuples = new ArrayList<Tuple>();

			} else if (compare < 0) {
				leftTuple = nextLeftTuple();
			} else if (compare > 0) {
				rightTuple = nextRightTuple();
			}

		}
		return outbatch;
	}

	public boolean close() {
		if (left.close() && right.close()) {

			return true;
		} else
			return false;
	}

	/**
	 * To know when we should stop the operator.
	 * 
	 * @return
	 */
	private boolean joinIsCompleted() {
		return leftbatch == null || rightbatch == null;
	}

	/**
	 * To know from which table the next tuple should be taken.
	 * 
	 * @throws Exception
	 */
	private int compareTuplesForJoin(Tuple leftTuple, Tuple rightTuple) throws RuntimeException {
		// System.out.println(leftTuple.dataAt(leftindex) + "?=" + rightTuple.dataAt(rightindex));
		int compareTuples = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
		switch (orderByEnum) {
		case ASC:
			return compareTuples;
		case DESC:
			return -compareTuples;
		default:
			throw new RuntimeException("No orderByOption");
		}
	}



	/**
	 * @param orderByEnum
	 *        the orderByOption to set
	 */
	public void setOrderByOption(OrderByEnum orderByEnum) {
		this.orderByEnum = orderByEnum;
	}
}