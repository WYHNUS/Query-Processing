/** prepares a random initial plan for the given SQL query **/
/** see the ReadMe file to understand this **/
package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.util.Vector;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.*;

public class RandomInitialPlan {
	SQLQuery sqlquery;

	Vector projectList;
	Vector fromList;
	Vector selectionlist;     	// List of select conditions
	Vector joinlist;          	// List of join conditions
	Vector groupbylist;
	int numJoin;    			// Number of joins in this query

	Hashtable tab_op_hash;      // table name to the Operator
	Operator root; 				// root of the query plan tree

	public RandomInitialPlan(SQLQuery sqlquery) {
		this.sqlquery = sqlquery;

		projectList = (Vector) sqlquery.getProjectList();
		fromList = (Vector) sqlquery.getFromList();
		selectionlist = sqlquery.getSelectionList();
		joinlist = sqlquery.getJoinList();
		groupbylist = sqlquery.getGroupByList();
		numJoin = joinlist.size();
	}

	/** number of join conditions **/
	public int getNumJoins(){
		return numJoin;
	}

	/** prepare initial plan for the query **/
	public Operator prepareInitialPlan() {
		tab_op_hash = new Hashtable();

		createScanOp();
		createSelectOp();
		if (numJoin != 0) {
			createJoinOp();
		}
		createProjectOp();
		return root;
	}

	/** Create Scan Operator for each of the table
	 ** mentioned in from list
	 **/
	public void createScanOp() {
		int numTab = fromList.size();
		Scan temPop = null;

		for (int i=0; i<numTab; i++) {  // For each table in from list
			String tabName = (String) fromList.elementAt(i);
			Scan op1 = new Scan(tabName, OpType.SCAN);
			temPop = op1;

			/** Read the schema of the table from tablename.md file
			 ** md stands for metadata
			 **/
			String filename = tabName + ".md";
			try {
				ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(filename));
				Schema schm = (Schema) inputStream.readObject();
				op1.setSchema(schm);
				inputStream.close();
			} catch (Exception e) {
				System.err.println("RandomInitialPlan:Error reading Schema of the table " + filename);
				System.exit(1);
			}
			tab_op_hash.put(tabName, op1);
		}

		// 12 July 2003 (whtok)
		// To handle the case where there is no where clause
		// selectionlist is empty, hence we set the root to be
		// the scan operator. the projectOp would be put on top of
		// this later in CreateProjectOp
		if (selectionlist.size() == 0) {
			root = temPop;
			return;
		}
	}

	/** Create Selection Operators for each of the
	 ** selection condition mentioned in Condition list
	 **/
	public void createSelectOp() {
		Select op1 = null;

		for (int j=0; j<selectionlist.size(); j++) {
			Condition cn = (Condition) selectionlist.elementAt(j);
			if (cn.getOpType() == Condition.SELECT) {
				String tabName = cn.getLhs().getTabName();
				//System.out.println("RandomInitial:-------------Select-------:"+tabname);

				Operator temPop = (Operator)tab_op_hash.get(tabName);
				op1 = new Select(temPop, cn, OpType.SELECT);
				/** set the schema same as base relation **/
				op1.setSchema(temPop.getSchema());

				modifyHashtable(temPop, op1);
				//tab_op_hash.put(tabname,op1);
			}
		}
		/** The last selection is the root of the plan tree
		 ** constructed thus far
		 **/
		if (selectionlist.size() != 0) {
			root = op1;
		}
	}

	/** create join operators **/
	public void createJoinOp(){
		BitSet bitCList = new BitSet(numJoin);
		int jnNum = RandNumb.randInt(0,numJoin-1);
		Join jn = null;

		/** Repeat until all the join conditions are considered **/
		while (bitCList.cardinality() != numJoin) {
			/** If this condition is already consider chose
			 ** another join condition
			 **/
			while (bitCList.get(jnNum)) {
				jnNum = RandNumb.randInt(0,numJoin-1);
			}

			Condition cn = (Condition) joinlist.elementAt(jnNum);
			String lefttab = cn.getLhs().getTabName();
			String righttab = ((Attribute) cn.getRhs()).getTabName();

//			System.out.println(cn.getExprType());
//			System.out.println("---------JOIN:---------left: " + lefttab + " and right: " + righttab);

			Operator left = (Operator) tab_op_hash.get(lefttab);
			Operator right = (Operator) tab_op_hash.get(righttab);
			jn = new Join(left, right, cn, OpType.JOIN);
			jn.setNodeIndex(jnNum);
			Schema newsche = left.getSchema().joinWith(right.getSchema());
			jn.setSchema(newsche);
			/** randomly select a join type**/
			int numJMeth = JoinType.numJoinTypes();
			int joinMeth = RandNumb.randInt(0,numJMeth-1);
			jn.setJoinType(joinMeth);

			modifyHashtable(left, jn);
			modifyHashtable(right, jn);
			//tab_op_hash.put(lefttab,jn);
			//tab_op_hash.put(righttab,jn);

			bitCList.set(jnNum);
		}

		/** The last join operation is the root for the
		 ** constructed till now
		 **/
		if (numJoin != 0) {
			root = jn;
		}
	}

	public void createProjectOp(){
		Operator base = root;
		if (projectList == null) {
			projectList = new Vector();
		}

		if (!projectList.isEmpty()) {
			root = new Project(base, projectList, OpType.PROJECT);
			Schema newSchema = base.getSchema().subSchema(projectList);
			root.setSchema(newSchema);
		}
	}

	private void modifyHashtable(Operator old, Operator newop) {
		Enumeration e = tab_op_hash.keys();
		while (e.hasMoreElements()) {
			String key = (String)e.nextElement();
			Operator temp = (Operator)tab_op_hash.get(key);
			if (temp == old) {
				tab_op_hash.put(key, newop);
			}
		}
	}
}