package qp.optimizer;

import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.utils.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class DPPlan extends BasicPlan {
    private ArrayList<String> joinTablesList;       // list of distinct tables that are involved in join query
    private ArrayList<String> currentResult;        // helper variable for generating combination

    // store the <lhsTable, rhsTable, Condition> relationship
    // Note: there might be multiple conditions between two tables
    private HashMap<String, HashMap<String, ArrayList<Condition>>> conditionDict;
    private HashMap<String, Integer> costMemo;      // store <TableCombination, cost>
    private HashMap<String, Operator> joinMemo;     // store <TableCombination, JoinNode>

    public DPPlan(SQLQuery sqlquery) {
        super(sqlquery);
        joinTablesList = new ArrayList<>();
        currentResult = new ArrayList<>();

        conditionDict = new HashMap<>();
        costMemo = new HashMap<>();
        joinMemo = new HashMap<>();
    }

    /**
     * Create join operators:
     * Implementation only apply standard DP procedure to join operation
     **/
    public void createJoinOp() {
        HashSet<String> joinTablesSet = new HashSet<>();

        /**
         *  1. Construct condition chain (conditionDict) to record down join conditions
         *  2. HashMap (costMemo) to store minimum costs for each plan
         *  3. HashMap to store meta data (number of tuples; capacity; attributes) for each table
         *          -> to compute cost
         *  4. HashMap (joinMemo) to store Join node for each suboptimal plan
         *          -> to construct node
         **/

        // bottom-up DP initialization
        for (int i=0; i<numJoin; i++) {
            Condition cn = (Condition) joinlist.elementAt(i);
            String leftTableName = cn.getLhs().getTabName();
            String rightTableName = ((Attribute) cn.getRhs()).getTabName();

            /** store all join conditions **/
            HashMap<String, ArrayList<Condition>> nestedDict = new HashMap<>();
            ArrayList<Condition> conditionList = new ArrayList<>();
            // check if already exists
            if (conditionDict.containsKey(leftTableName)) {
                nestedDict = conditionDict.get(leftTableName);
                if (nestedDict.containsKey(rightTableName)) {
                    conditionList = nestedDict.get(rightTableName);
                }

            }
            conditionList.add(cn);
            nestedDict.put(rightTableName, conditionList);
            conditionDict.put(leftTableName, nestedDict);

            /** store distinct tables involved in join operation **/
            if (!joinTablesSet.contains(leftTableName)) {
                joinTablesSet.add(leftTableName);
                joinTablesList.add(leftTableName);
                costMemo.put(leftTableName, accessPlanCost((Operator)tabOpHash.get(leftTableName)));
            }
            if (!joinTablesSet.contains(rightTableName)) {
                joinTablesSet.add(rightTableName);
                joinTablesList.add(rightTableName);
                costMemo.put(rightTableName, accessPlanCost((Operator)tabOpHash.get(rightTableName)));
            }
        }

        for (int i=2; i<=joinTablesList.size(); i++) {
            /** calculate cost for each i-pair-wise-tables, and record down the minimum one **/
            ArrayList<ArrayList<String>> permutations = generateCombination(joinTablesList, i);

            for (ArrayList<String> permutation : permutations) {
                int minCost = Integer.MAX_VALUE;
                ArrayList<String> minLhsJoin = new ArrayList<>();
                ArrayList<String> minRhsJoin = new ArrayList<>();
                ArrayList<ArrayList<ArrayList<String>>> planList = generatePlans(permutation);

                for (int j=0; j<planList.size(); j++) {
                    ArrayList<String> lhsJoin = planList.get(j).get(0);
                    ArrayList<String> rhsJoin = planList.get(j).get(1);

                    int cost = joinPlanCost(lhsJoin, rhsJoin);

                    if (cost < minCost) {
                        minCost = cost;
                        // record down the lhs and rhs as well
                        minLhsJoin = lhsJoin;
                        minRhsJoin = rhsJoin;
                    }
                }

                // concatenate String together to generate key
                costMemo.put(convertLstToString(permutation), minCost);

                /** construct corresponding join node for sub-optimal solution **/
                Operator left = joinMemo.get(convertLstToString(minLhsJoin));
                Operator right = joinMemo.get(convertLstToString(minRhsJoin));

                // clone left and right operators before performing any action
                Operator minLeft = (Operator) left.clone();
                minLeft.setSchema((Schema) left.getSchema().clone());
                Operator minRight = (Operator) right.clone();
                minRight.setSchema((Schema) right.getSchema().clone());

                // check for applicable conditions, and construct new Join Operator if possible
                joinTables(minLhsJoin, minRhsJoin, minLeft, minRight);
                joinTables(minRhsJoin, minLhsJoin, minLeft, minRight);

                // store the result Join Operator into memo table (minRight and minLeft should have the same structure)
                joinMemo.put(convertLstToString(permutation), minRight);
            }
        }
    }

    private void joinTables(ArrayList<String> minLhsJoin, ArrayList<String> minRhsJoin,
                            Operator minLeft, Operator minRight) {
        for (int j=0; j<minLhsJoin.size(); j++) {
            for (int k=0; k<minRhsJoin.size(); k++) {
                ArrayList<Condition> conditions = getRelation(minLhsJoin.get(j), minRhsJoin.get(k));
                if (conditions != null) {

                    for (Condition condition: conditions) {
                        Join minJoinNode = new Join(minLeft, minRight, condition, OpType.JOIN);
                        minJoinNode.setSchema(minLeft.getSchema().joinWith(minRight.getSchema()));

                        /** randomly select a join type **/
                        int numJMeth = JoinType.numJoinTypes();
                        int joinMeth = RandNumb.randInt(0,numJMeth-1);
                        minJoinNode.setJoinType(joinMeth);

                        // reassign
                        minLeft = minJoinNode;
                        minRight = (Operator) minJoinNode.clone();
                        minRight.setSchema((Schema) minJoinNode.getSchema().clone());
                    }
                }
            }
        }
    }

    private ArrayList<Condition> getRelation(String lTable, String rTable) {
        if (conditionDict.containsKey(lTable))
            if (conditionDict.get(lTable).containsKey(rTable))
                return conditionDict.get(lTable).get(rTable);
        return null;
    }

    private int joinPlanCost(ArrayList<String> leftPlanName, ArrayList<String> rightPlanName) {
        // sub-plan must always inside the memo table
        int leftPlanCost = 0;
        int rightPlanCost = 0;

        if (costMemo.containsKey(leftPlanName)) {
            leftPlanCost = costMemo.get(leftPlanName);
        } else {
            System.out.print("Error in calculating join plan cost!");
        }

        if (costMemo.containsKey(rightPlanName)) {
            rightPlanCost = costMemo.get(rightPlanName);
        } else {
            System.out.print("Error in calculating join plan cost!");
        }

        // compute the join cost for combining the two plans
        /** Stub **/
        int joinCost = 0;

        return leftPlanCost + rightPlanCost + joinCost;
    }

    private int accessPlanCost(Operator table) {
        /** Stub **/
        return 0;
    }

    private ArrayList<ArrayList<ArrayList<String>>> generatePlans(ArrayList<String> sources) {
        if (sources.size() <= 1) {
            System.out.println("Invalid source in generatePlan method!");
            return new ArrayList<>();
        }

        ArrayList<ArrayList<ArrayList<String>>> resultList = new ArrayList<>();
        for (int i=1; i<sources.size(); i++) {
            ArrayList<ArrayList<String>> allLHS = generateCombination(sources, i);

            for (int j=0; j<allLHS.size(); j++) {
                // for each lhs, find corresponding rhs
                ArrayList<String> lhs = allLHS.get(j);
                ArrayList<String> rhs = new ArrayList<String>();

                // traverse through sources to construct rhs
                for (int k=0; k<sources.size(); k++) {
                    if (!lhs.contains(sources.get(k))) {
                        rhs.add(sources.get(k));
                    }
                }
                ArrayList<ArrayList<String>> tmp = new ArrayList<>();
                tmp.add(lhs);
                tmp.add(rhs);
                resultList.add(tmp);
            }
        }
        return resultList;
    }

    /** Generate all possible combination of choosing k distinct elements from joinTablesList **/
    private ArrayList<ArrayList<String>> generateCombination(ArrayList<String> sourceList, int k) {
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        recursiveCombine(sourceList, result, 0, k);
        return result;
    }

    private void recursiveCombine(ArrayList<String> sourceList, ArrayList<ArrayList<String>> result,
                                  int offset, int sizeNeeded) {
        if (sizeNeeded == 0) {
            result.add(new ArrayList(currentResult));
            return;
        }

        for (int i=offset; i<=sourceList.size()-sizeNeeded; i++) {
            // choose or not choose
            System.out.println(i + "  " + sizeNeeded);
            currentResult.add(sourceList.get(i));
            recursiveCombine(sourceList, result, i + 1, sizeNeeded - 1);
            currentResult.remove(currentResult.size() - 1);
        }
    }

    private String convertLstToString(ArrayList<String> source) {
        String result = "";
        for (int i=0; i<source.size(); i++) {
            result += source.get(i);
        }
        return result;
    }
}
