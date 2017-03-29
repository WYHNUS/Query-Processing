package qp.optimizer;

import qp.operators.Operator;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class DPPlan extends BasicPlan {
    private ArrayList<String> joinTablesList;       // list of distinct tables that are involved in join query
    private ArrayList<String> currentResult;        // helper variable for generating combination
    private HashMap<String, Integer> memo;          // memo table of <TableCombination, cost>

    public DPPlan(SQLQuery sqlquery) {
        super(sqlquery);
        joinTablesList = new ArrayList<>();
        currentResult = new ArrayList<>();
        memo = new HashMap<>();
    }

    /**
     * Create join operators:
     * Implementation only apply standard DP procedure to join operation
     **/
    public void createJoinOp(){
        HashSet<String> joinTablesSet = new HashSet<>();

        // bottom-up DP initialization
        for (int i=0; i<numJoin; i++) {
            Condition cn = (Condition) joinlist.elementAt(i);
            String leftTableName = cn.getLhs().getTabName();
            String rightTableName = ((Attribute) cn.getRhs()).getTabName();

            if (!joinTablesSet.contains(leftTableName)) {
                joinTablesSet.add(leftTableName);
                joinTablesList.add(leftTableName);
                memo.put(leftTableName, accessPlanCost((Operator)tabOpHash.get(leftTableName)));
            }
            if (!joinTablesSet.contains(rightTableName)) {
                joinTablesSet.add(rightTableName);
                joinTablesList.add(rightTableName);
                memo.put(rightTableName, accessPlanCost((Operator)tabOpHash.get(rightTableName)));
            }
        }

        for (int i=2; i<=joinTablesList.size(); i++) {
            // calculate cost for each i-pair-wise-tables, and select the minimum one
            ArrayList<ArrayList<String>> permutations = generateCombination(joinTablesList, i);

            for (ArrayList<String> permutation : permutations) {
                int minCost = Integer.MAX_VALUE;
                ArrayList<ArrayList<ArrayList<String>>> planList = generatePlans(permutation);

                for (int j=0; j<planList.size(); j++) {
                    ArrayList<String> lhsJoin = planList.get(j).get(0);
                    ArrayList<String> rhsJoin = planList.get(j).get(1);

                    int cost = joinPlanCost(lhsJoin, rhsJoin);

                    if (cost < minCost) {
                        minCost = cost;
                    }
                }

                // concatenate String together to generate key
                String planKey = "";
                for (int j=0; j<permutation.size(); j++) {
                    planKey += permutation.get(j);
                }
                memo.put(planKey, minCost);
            }
        }
    }

    private int joinPlanCost(ArrayList<String> leftPlanName, ArrayList<String> rightPlanName) {
        // sub-plan must always inside the memo table
        int leftPlanCost = 0;
        int rightPlanCost = 0;

        if (memo.containsKey(leftPlanName)) {
            leftPlanCost = memo.get(leftPlanName);
        } else {
            System.out.print("Error in calculating join plan cost!");
        }

        if (memo.containsKey(rightPlanName)) {
            rightPlanCost = memo.get(rightPlanName);
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
