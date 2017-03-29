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
    private ArrayList<String> currentResult;                   // helper variable for generating combination
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
            ArrayList<ArrayList<String>> permutations = generateCombination(i);

            for (ArrayList<String> permutation : permutations) {
                int minCost = Integer.MAX_VALUE;
                ArrayList<String[]> planList = generatePlans(permutation);

                for (int j=0; j<planList.size(); j++) {
                    String lhsJoin = planList.get(j)[0];
                    String rhsJoin = planList.get(j)[1];
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

    private int joinPlanCost(String leftPlanName, String rightPlanName) {
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

    private ArrayList<String[]> generatePlans(ArrayList<String> sources) {
        if (sources.size() <= 1) {
            System.out.println("Invalid source in generatePlan method!");
            return new ArrayList<>();
        }

        ArrayList<String[]> resultList = new ArrayList<>();
        for (int i=1; i<sources.size()-1; i++) {
            String[] tmp = new String[2];
            for (int j=0; j<i; j++) {
                tmp[0] += sources.get(j);
            }
            for (int j=i; j<sources.size(); j++) {
                tmp[1] += sources.get(j);
            }
            resultList.add(tmp);
        }
        return resultList;
    }

    private ArrayList<ArrayList<String>> generateCombination(int number) {
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        recursiveCombine(result, 0, number);
        return result;
    }

    private void recursiveCombine(ArrayList<ArrayList<String>> result, int offset, int sizeNeeded) {
        if (sizeNeeded == 0) {
            result.add(currentResult);
        }

        for (int i=offset; i<joinTablesList.size()-sizeNeeded; i++) {
            // choose or not choose
            currentResult.add(joinTablesList.get(i));
            recursiveCombine(result, i + 1, sizeNeeded - 1);
            currentResult.remove(currentResult.size() - 1);
        }
    }
}
