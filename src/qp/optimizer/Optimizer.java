package qp.optimizer;

import qp.operators.*;

public abstract class Optimizer {
    /**
     * Implementation for Query Plan can be Randomization / DP / Greedy Heuristics
     **/
    public abstract Operator getOptimizedPlan();

    /**
     *  After finding a choice of method for each operator
     *  prepare an execution plan by replacing the methods with
     *  corresponding join operator implementation
     **/
    public Operator makeExecPlan(Operator node){
        if (node.getOpType()== OpType.JOIN) {
            Operator left = makeExecPlan(((Join)node).getLeft());
            Operator right = makeExecPlan(((Join)node).getRight());
            int joinType = ((Join)node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();

            switch(joinType){
                case JoinType.NESTEDJOIN:
                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                /** Temporarity used simple nested join,
                 replace with hasjoin, if implemented **/
                case JoinType.BLOCKNESTED:
                    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;

                case JoinType.SORTMERGE:
                    NestedJoin sm = new NestedJoin((Join) node);
	                /* + other code */
                    return sm;

                case JoinType.HASHJOIN:
                    HashJoin hj = new HashJoin((Join) node);
	                /* + other code */
                    hj.setLeft(left);
                    hj.setRight(right);
                    hj.setNumBuff(numbuff);
                    return hj;

                default:
                    return node;
            }
        } else if(node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select)node).getBase());
            ((Select)node).setBase(base);
            return node;
        } else if(node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project)node).getBase());
            ((Project)node).setBase(base);
            return node;
        } else {
            return node;
        }
    }
}
