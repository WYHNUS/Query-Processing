/** prepares a random initial plan for the given SQL query **/
/** see the ReadMe file to understand this **/
package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.util.BitSet;

public class RandomInitialPlan extends BasicPlan {
	public RandomInitialPlan(SQLQuery sqlquery) {
		super(sqlquery);
	}

	/** create join operators **/
	public void createJoinOp(){
		BitSet bitCList = new BitSet(numJoin);
		int jnNum = RandNumb.randInt(0,numJoin-1);
		Join jn = null;

		/** Repeat until all the join conditions are considered **/
		while (bitCList.cardinality() != numJoin) {
			/** If this condition is already considered, choose another join condition **/
			while (bitCList.get(jnNum)) {
				jnNum = RandNumb.randInt(0,numJoin-1);
			}

			Condition cn = (Condition) joinlist.elementAt(jnNum);
			String lefttab = cn.getLhs().getTabName();
			String righttab = ((Attribute) cn.getRhs()).getTabName();

//			System.out.println(cn.getExprType());
//			System.out.println("---------JOIN:---------left: " + lefttab + " and right: " + righttab);

			Operator left = (Operator) tabOpHash.get(lefttab);
			Operator right = (Operator) tabOpHash.get(righttab);
			jn = new Join(left, right, cn, OpType.JOIN);
			jn.setNodeIndex(jnNum);
			Schema newsche = left.getSchema().joinWith(right.getSchema());
			jn.setSchema(newsche);
			/** randomly select a join type**/
			int numJMeth = JoinType.numJoinTypes();
			int joinMeth = 1;//RandNumb.randInt(0,numJMeth-1);
			jn.setJoinType(joinMeth);

			modifyHashtable(left, jn);
			modifyHashtable(right, jn);
			//tab_op_hash.put(lefttab,jn);
			//tab_op_hash.put(righttab,jn);

			bitCList.set(jnNum);
		}

		/** The last join operation is the root for the constructed till now **/
		if (numJoin != 0) {
			root = jn;
		}
	}
}