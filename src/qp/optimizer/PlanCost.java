/** This method calculates the cost of the generated plans **/
/** also estimates the statistics of the result relation **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.*;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Enumeration;
import java.io.*;

public class PlanCost {
	long cost;
	int numtuple;

	/** If buffers are not enough for a selected join
	 ** then this plan is not feasible and return
	 ** a cost of infinity
	 **/
	boolean isFeasible;

	/** Hashtable stores mapping from Attribute name to
	 ** number of distinct values of that attribute
	 **/
	Hashtable ht;

	public PlanCost() {
		ht = new Hashtable();
		cost = 0;
	}

	/** returns the cost of the plan **/
	public long getCost(Operator root){
		isFeasible = true;
		numtuple = calculateCost(root);
		if (isFeasible==true) {
			return cost;
		} else {
			return Integer.MAX_VALUE;
		}
	}

	/** get number of tuples in estimated results **/
	public int getNumTuples(){
		return numtuple;
	}

	/** returns number of tuples in the root **/
	protected int calculateCost(Operator node) {
		if (node.getOpType()==OpType.JOIN) {
			return getStatistics((Join)node);
		} else if (node.getOpType() == OpType.SELECT) {
			//System.out.println("PlanCost: line 40");
			return getStatistics((Select)node);
		} else if (node.getOpType() == OpType.PROJECT) {
			return getStatistics((Project)node);
		} else if (node.getOpType() == OpType.SCAN) {
			return getStatistics((Scan)node);
		}
		return -1;
	}

	/** projection will not change any statistics
	 ** No cost involved as done on the fly
	 **/
	protected int getStatistics(Project node){
		return calculateCost(node.getBase());
	}

	/** calculates the statistics, and cost of join operation **/
	protected int getStatistics(Join node){
		int lefttuples = calculateCost(node.getLeft());
		int righttuples= calculateCost(node.getRight());

		if(isFeasible==false){
			return -1;
		}

		Condition con = node.getCondition();
		Schema leftschema = node.getLeft().getSchema();
		Schema rightschema = node.getRight().getSchema();

		/** get size of the tuple in output & correspondigly calculate
		 ** buffer capacity, i.e., number of tuples per page **/
		int tuplesize = node.getSchema().getTupleSize();
		int outcapacity = Batch.getPageSize()/tuplesize;
		int leftuplesize= leftschema.getTupleSize();
		int leftcapacity = Batch.getPageSize()/leftuplesize;
		int righttuplesize = rightschema.getTupleSize();
		int rightcapacity = Batch.getPageSize()/righttuplesize;

		/** number of pages -> used to calculate joinCost **/
		int leftPages = (int) Math.ceil(((double)lefttuples) / (double)leftcapacity);
		int rightPages = (int) Math.ceil(((double)righttuples) / (double) rightcapacity);

		Attribute leftjoinAttr = con.getLhs();
		Attribute rightjoinAttr = (Attribute)con.getRhs();
		int leftattrind = leftschema.indexOf(leftjoinAttr);
		int rightattrind = rightschema.indexOf(rightjoinAttr);
		leftjoinAttr = leftschema.getAttribute(leftattrind);
		rightjoinAttr = rightschema.getAttribute(rightattrind);

		/** number of distinct values of left and right join attribute **/
		int leftattrdistn = ((Integer)ht.get(leftjoinAttr)).intValue();
		int rightattrdistn = ((Integer)ht.get(rightjoinAttr)).intValue();

		int outtuples = (int) Math.ceil(((double) lefttuples*righttuples) / (double) Math.max(leftattrdistn,rightattrdistn));

		int mindistinct = Math.min(leftattrdistn, rightattrdistn);
		ht.put(leftjoinAttr, new Integer(mindistinct));
		ht.put(rightjoinAttr, new Integer(mindistinct));

		/** now calculate the cost of the operation**/
		int joinType = node.getJoinType();
		long joinCost = PlanCost.getJoinCost(joinType, leftPages, rightPages);
		//System.out.println("PlanCost: jointype = " + joinType);

		cost = cost + joinCost;
		return outtuples;
	}

	public static long getJoinCost(int joinType, int leftPages, int rightPages) {
		long joinCost;
		int numBuff = BufferManager.getBuffersPerJoin();

		switch(joinType) {
			case JoinType.NESTEDJOIN:
				joinCost = Math.min(leftPages, rightPages) + ((long)leftPages) * rightPages;
				break;
			case JoinType.BLOCKNESTED:
                long numOfIteration = ((long) Math.ceil(1.0 * Math.min(rightPages, leftPages) / (numBuff - 2)));
				joinCost = numOfIteration * Math.max(rightPages, leftPages);
				break;
			case JoinType.SORTMERGE:
				joinCost = leftPages + rightPages + getSortCost(numBuff, leftPages) + getSortCost(numBuff, rightPages);
				break;
			case JoinType.HASHJOIN:
				joinCost = 3 * (leftPages + rightPages);
				break;
			default:
				joinCost = 0;
				break;
		}
		return joinCost;
	}

	protected static long getSortCost(int numBuff, int numOfPages) {
		long numOfIO = 0;
		int numOfSortedRuns = (int) Math.ceil((double) numOfPages/numBuff);
		int numOfPasses = (int) Math.ceil(Math.log(numOfSortedRuns) /Math.log(numBuff - 1));
		numOfIO = 2 * (numOfPasses + 1) * numOfPages;
		return numOfIO;
	}

	/** Find number of incoming tuples, Using the selectivity find # of output tuples
	 ** And statistics about the attributes
	 ** Selection is performed on the fly, so no cost involved
	 **/
	protected int getStatistics(Select node){
		//System.out.println("PlanCost: here at line 127");
		int intuples = calculateCost(node.getBase());

		if(isFeasible==false){
			return Integer.MAX_VALUE;
		}

		Condition con = node.getCondition();
		Schema schema = node.getSchema();

		Attribute attr = con.getLhs();

		int index = schema.indexOf(attr);
		Attribute fullattr = schema.getAttribute(index);

		int exprtype = con.getExprType();

		/** Get number of distinct values of selection attributes **/

		Integer temp =(Integer)ht.get(fullattr);
		int numdistinct = temp.intValue();
		//int numdistinct = ((Integer)ht.get(fullattr)).intValue();
		int outtuples;

		/** calculate the number of tuples in result **/
		if(exprtype==Condition.EQUAL){
			outtuples = (int) Math.ceil((double)intuples/(double)numdistinct);
		}else if(exprtype==Condition.NOTEQUAL){
			outtuples= (int) Math.ceil(intuples - ((double) intuples/(double) numdistinct));
		}else{
			outtuples=(int) Math.ceil(0.5*intuples);
		}

		/** Modify the number of distinct values of each attribute
		 ** Assuming the values are distributed uniformly along entire
		 ** relation
		 **/
		for(int i=0;i<schema.getNumCols();i++){
			Attribute attri = schema.getAttribute(i);
			int oldvalue = ((Integer)ht.get(attri)).intValue();
			int newvalue = (int) Math.ceil(((double) outtuples/(double) intuples)*oldvalue);
			ht.put(attri,new Integer(outtuples));
		}
		//System.out.println("PlanCost: line 164: outtuples="+outtuples);
		return outtuples;
	}

	/**  the statistics file <tablename>.stat to find the statistics
	 ** about that table;
	 ** This table contains number of tuples in the table
	 ** number of distinct values of each attribute
	 **/
	protected int getStatistics(Scan node) {
		String tablename = node.getTabName();
		String filename = tablename + ".stat";
		Schema schema = node.getSchema();
		int numAttr = schema.getNumCols();
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(filename));
		} catch(IOException io) {
			System.out.println("Error in opening file " + filename);
			System.exit(1);
		}
		String line = null;

		// First line = number of tuples
		try {
			line = in.readLine();
		} catch(IOException io) {
			System.out.println("Error in readin first line of " + filename);
			System.exit(1);
		}
		StringTokenizer tokenizer = new StringTokenizer(line);
		if (tokenizer.countTokens() != 1) {
			System.out.println("incorrect format of statistics file " + filename);
			System.exit(1);
		}

		String temp = tokenizer.nextToken();
		/** number of tuples in this table; **/
		int numtuples = Integer.parseInt(temp);

		try {
			line = in.readLine();
		} catch(IOException io) {
			System.out.println("error in reading second line of " + filename);
			System.exit(1);
		}
		tokenizer = new StringTokenizer(line);
		if (tokenizer.countTokens() != numAttr) {
			System.out.println("incorrect format of statistics file " + filename);
			System.exit(1);
		}

		for (int i=0; i<numAttr; i++) {
			Attribute attr = schema.getAttribute(i);
			temp = tokenizer.nextToken();
			Integer distinctValues = Integer.valueOf(temp);
			ht.put(attr, distinctValues);
		}

		/** number of tuples per page**/
		int tuplesize = schema.getTupleSize();
		int pagesize = Batch.getPageSize() / tuplesize;
		//Batch.capacity();
		int numpages = (int) Math.ceil((double) numtuples / (double) pagesize);
		cost = cost + numpages;
		try {
			in.close();
		} catch(IOException io) {
			System.out.println("error in closing the file " + filename);
			System.exit(1);
		}

		//System.out.println("Scan: tablename="+tablename+"pres cost="+numpages+"total cost="+cost);
		return numtuples;
	}
}