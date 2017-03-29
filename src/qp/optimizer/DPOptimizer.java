package qp.optimizer;

import qp.utils.*;
import qp.operators.*;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class DPOptimizer extends Optimizer {
    private SQLQuery sqlQuery;              // Vector of Vectors of Select + From + Where + GroupBy
    private Vector projectList;
    private Vector fromList;
    private Vector selectionlist;     	    // List of select conditions
    private Vector joinlist;          	    // List of join conditions
    private Vector groupbylist;
    private int numJoin;    			    // Number of joins in this query

    private ArrayList<String> joinTablesList;       // list of distinct tables that are involved in join query
    private ArrayList<String> currentResult;                   // helper variable for generating combination
    private HashMap<String, Operator> tabOpHash;    // table name to the Operator
    private HashMap<String, Integer> memo;          // memo table of <TableCombination, cost>

    /** constructor **/
    public DPOptimizer(SQLQuery sqlquery) {
        this.sqlQuery = sqlquery;
        this.projectList = (Vector) sqlquery.getProjectList();
        this.fromList = (Vector) sqlquery.getFromList();
        this.selectionlist = sqlquery.getSelectionList();
        this.joinlist = sqlquery.getJoinList();
        this.groupbylist = sqlquery.getGroupByList();
        this.numJoin = joinlist.size();

        joinTablesList = new ArrayList<>();
        currentResult = new ArrayList<>();
        tabOpHash = new HashMap<>();
        readBaseTables();
    }

    /**
     * Implementation only apply standard DP procedure to join operation
     **/
    public Operator getOptimizedPlan() {
        memo = new HashMap<>();
        HashSet<String> joinTablesSet = new HashSet<>();

        // bottom-up DP initialization
        for (int i=0; i<numJoin; i++) {
            Condition cn = (Condition) joinlist.elementAt(i);
            String leftTableName = cn.getLhs().getTabName();
            String rightTableName = ((Attribute) cn.getRhs()).getTabName();

            if (!joinTablesSet.contains(leftTableName)) {
                joinTablesSet.add(leftTableName);
                joinTablesList.add(leftTableName);
                memo.put(leftTableName, accessPlanCost(tabOpHash.get(leftTableName)));
            }
            if (!joinTablesSet.contains(rightTableName)) {
                joinTablesSet.add(rightTableName);
                joinTablesList.add(rightTableName);
                memo.put(rightTableName, accessPlanCost(tabOpHash.get(rightTableName)));
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

        return null;
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

    private  ArrayList<String[]> generatePlans(ArrayList<String> sources) {
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

     /**
      * Create Scan Operator for each of the table mentioned in from list
     **/
     private void readBaseTables() {
        for (int i=0; i<fromList.size(); i++) {  // For each table in from list
            String tabName = (String) fromList.elementAt(i);
            Scan op1 = new Scan(tabName, OpType.SCAN);

            /**
             * Read the schema of the table from tablename.md file where md stands for metadata
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
            tabOpHash.put(tabName, op1);
        }
    }
}
