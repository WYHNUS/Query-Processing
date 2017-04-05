package qp.optimizer;

import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.utils.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

// helper class to facilitate cost computation
class Metadata {
    int numTuples;
    int tupleSize;
    ArrayList<Attribute> attributeList;

    public  Metadata() {}

    public Metadata(int numTuples, int tupleSize, ArrayList<Attribute> attributeList) {
        this.numTuples = numTuples;
        this.tupleSize = tupleSize;
        this.attributeList = new ArrayList<> ();
        for (int i=0; i<attributeList.size(); i++) {
            this.attributeList.add(attributeList.get(i));
        }
    }
}


/**
 *  Some explanation on data structure:
 *  1. Construct condition chain (conditionDict) to record down join conditions
 *  2. HashMap (costMemo) to store minimum costs for each plan
 *  3. HashMap (metaMemo) to store meta data (number of tuples; capacity; attributes) for each table
 *          -> to compute cost
 *  4. HashMap (joinMemo) to store Join node for each suboptimal plan
 *          -> to construct node
 **/
public class DPPlan extends BasicPlan {
    private ArrayList<String> joinTablesList;       // list of distinct tables that are involved in join query
    private ArrayList<String> currentResult;        // helper variable for generating combination

    // store the <lhsTable, rhsTable, Condition> relationship
    // Note: there might be multiple conditions between two tables
    private HashMap<String, HashMap<String, ArrayList<Condition>>> conditionDict;
    private HashMap<String, Integer> costMemo;      // store <TableCombination, cost>
    private HashMap<String, Metadata> metaMemo;     // store <TableCombination, Metadata>
    private HashMap<String, Operator> joinMemo;     // store <TableCombination, JoinNode>

    public DPPlan(SQLQuery sqlquery) {
        super(sqlquery);
        joinTablesList = new ArrayList<>();
        currentResult = new ArrayList<>();

        conditionDict = new HashMap<>();
        costMemo = new HashMap<>();
        metaMemo = new HashMap<>();
        joinMemo = new HashMap<>();
    }

    /**
     * Create join operators:
     * Implementation only apply standard DP procedure to join operation
     **/
    public void createJoinOp() {
        HashSet<String> joinTablesSet = new HashSet<>();

        /**
         * randomly select a join type, or other choices
         **/
        int numJMeth = JoinType.numJoinTypes();
        int joinMethod = 1;// RandNumb.randInt(0, numJMeth - 1);

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
                initializePlan(leftTableName);
            }
            if (!joinTablesSet.contains(rightTableName)) {
                joinTablesSet.add(rightTableName);
                initializePlan(rightTableName);
            }
        }

        for (int i=2; i<=joinTablesList.size(); i++) {
            /** calculate cost for each i-pair-wise-tables, and record down the minimum one **/
            ArrayList<ArrayList<String>> combinations = generateCombination(joinTablesList, i);

            for (ArrayList<String> combination : combinations) {
                String fullPlanName = convertLstToString(combination);
                int minCost = Integer.MAX_VALUE;

                ArrayList<String> minLhsJoin = new ArrayList<>();
                ArrayList<String> minRhsJoin = new ArrayList<>();
                ArrayList<ArrayList<ArrayList<String>>> planList = generatePlans(combination);

                // compute cost for all possible plans of a given combination
                for (int j=0; j<planList.size(); j++) {
                    ArrayList<String> lhsJoin = planList.get(j).get(0);
                    ArrayList<String> rhsJoin = planList.get(j).get(1);

                    int cost = joinPlanCost(lhsJoin, rhsJoin, joinMethod);

                    if (cost < minCost) {
                        minCost = cost;
                        // record down the lhs and rhs as well
                        minLhsJoin = lhsJoin;
                        minRhsJoin = rhsJoin;
                    }
                }

                /** construct corresponding join node for sub-optimal solution **/
                String leftPlanName = convertLstToString(minLhsJoin);
                String rightPlanName = convertLstToString(minRhsJoin);
                Operator left = joinMemo.get(leftPlanName);
                Operator right = joinMemo.get(rightPlanName);

                /** temporary check for skipping cross product **/
                if (left == null || right == null) {
                    continue;
                }

                // clone left and right operators before performing any action
                Operator minLeft = (Operator) left.clone();
                minLeft.setSchema((Schema) left.getSchema().clone());
                Operator minRight = (Operator) right.clone();
                minRight.setSchema((Schema) right.getSchema().clone());

                // check for applicable conditions, and construct new Join Operator if possible
                ArrayList triple = joinTables(minLhsJoin, minRhsJoin, minLeft, minRight, joinMethod);
                boolean shouldCrossProduct = (boolean) triple.get(0);
                minLeft = (Operator) triple.get(1);
                minRight = (Operator) triple.get(2);

                // need to check A x B and B x A
                triple = joinTables(minRhsJoin, minLhsJoin, minLeft, minRight, joinMethod);
                shouldCrossProduct = (shouldCrossProduct && (boolean) triple.get(0));
                minLeft = (Operator) triple.get(1);
                minRight = (Operator) triple.get(2);

                if (shouldCrossProduct) {
                    continue;   /** Temporarily skip cross production... **/
                    // perform cross product between minLeft and minRight
//                    Join minJoinNode = new Join(minLeft, minRight, new Condition(<what should be filled in here...>), OpType.JOIN);
//                    // reassign
//                    minLeft = minJoinNode;
//                    minRight = (Operator) minJoinNode.clone();
//                    minRight.setSchema((Schema) minJoinNode.getSchema().clone());
                }

                // store the result Join Operator into memo table (minRight and minLeft should have the same structure)
                joinMemo.put(fullPlanName, minRight);
                // update cost
                costMemo.put(fullPlanName, minCost);
                // assign metadata
                assignMetaData(fullPlanName, leftPlanName, rightPlanName);
            }
        }

        // print out the final join cost
        System.out.println("Final join plan cost: " + costMemo.get(convertLstToString(joinTablesList)));

        // assign the root
        root = joinMemo.get(convertLstToString(joinTablesList));
    }

    private void initializePlan(String tableName) {
        joinTablesList.add(tableName);
        joinMemo.put(tableName, (Operator) tabOpHash.get(tableName));
        try {
            costMemo.put(tableName, accessPlanCost(tableName));
        } catch(Exception e) {
            System.out.println("error in accessPlanCost");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void assignMetaData(String fullPlanName, String leftPlanKeyName, String rightPlanKeyName) {
        Metadata leftMetadata = metaMemo.get(leftPlanKeyName);
        Metadata rightMetadata = metaMemo.get(rightPlanKeyName);

        /** Stub: still problematic **/
        ArrayList<Attribute>  leftJoinAttr = leftMetadata.attributeList;
        ArrayList<Attribute>  rightJoinAttr = rightMetadata.attributeList;

        /** number of distinct values of left and right join attribute **/
        int leftAttrDistTuple = leftJoinAttr.size();
        int rightAttrDistTuple = rightJoinAttr.size();

        /** update metadata memo table **/
        int outNumTuples = (int) Math.ceil(1.0 * leftMetadata.numTuples * rightMetadata.numTuples
                /  Math.max(leftAttrDistTuple, rightAttrDistTuple));
        int outTupleSize = leftMetadata.tupleSize + rightMetadata.tupleSize;

        ArrayList<Attribute> outAttributes = new ArrayList<> ();
        for (Attribute attribute : leftJoinAttr) {
            outAttributes.add((Attribute) attribute.clone());
        }
        for (Attribute attribute : rightJoinAttr) {
            if (!outAttributes.contains(attribute)) {
                outAttributes.add((Attribute) attribute.clone());
            }
        }

        metaMemo.put(fullPlanName, new Metadata(outNumTuples, outTupleSize, outAttributes));
    }

    private ArrayList joinTables(ArrayList<String> minLhsJoin, ArrayList<String> minRhsJoin,
                            Operator minLeft, Operator minRight, int joinMethod) {
        boolean shouldCrossProduct = true;
        for (int j=0; j<minLhsJoin.size(); j++) {
            for (int k=0; k<minRhsJoin.size(); k++) {
                ArrayList<Condition> conditions = getRelation(minLhsJoin.get(j), minRhsJoin.get(k));
                if (conditions != null) {
                    for (Condition condition: conditions) {
                        shouldCrossProduct = false;     // has join condition between the two tables

                        Join minJoinNode = new Join(minLeft, minRight, condition, OpType.JOIN);
                        minJoinNode.setSchema(minLeft.getSchema().joinWith(minRight.getSchema()));
                        minJoinNode.setJoinType(joinMethod);

                        // reassign
                        minLeft = minJoinNode;
                        minRight = (Operator) minJoinNode.clone();
                        minRight.setSchema((Schema) minJoinNode.getSchema().clone());
                    }
                }
            }
        }
        ArrayList result = new ArrayList<>();
        result.add(new Boolean(shouldCrossProduct));
        result.add(minLeft);
        result.add(minRight);
        return result;
    }

    private ArrayList<Condition> getRelation(String lTable, String rTable) {
        if (conditionDict.containsKey(lTable))
            if (conditionDict.get(lTable).containsKey(rTable))
                return conditionDict.get(lTable).get(rTable);
        return null;
    }

    private int joinPlanCost(ArrayList<String> leftPlanName, ArrayList<String> rightPlanName, int joinMethod) {
        // sub-plan must always inside the memo table
        int leftPlanCost = 0;
        int rightPlanCost = 0;
        String leftPlanKeyName = convertLstToString(leftPlanName);
        String rightPlanKeyName = convertLstToString(rightPlanName);

        if (!costMemo.containsKey(leftPlanKeyName) || !costMemo.containsKey(rightPlanKeyName)) {
//            System.out.print("Error in calculating join plan cost! Left plan can't be empty!");
            return 100000000;  /** hard-coded number to indicate INF for cross-product **/
        }
        leftPlanCost = costMemo.get(leftPlanKeyName);
        rightPlanCost = costMemo.get(rightPlanKeyName);

        // compute the join cost for combining the two plans
        int pageSize = Batch.getPageSize();

        /** Get metadata from memo table **/
        Metadata leftMetadata = metaMemo.get(leftPlanKeyName);
        Metadata rightMetadata = metaMemo.get(rightPlanKeyName);

        int leftCapacity = pageSize / leftMetadata.tupleSize;
        int rightCapacity = pageSize / rightMetadata.tupleSize;

        int leftPages = (int) Math.ceil(1.0 * leftMetadata.numTuples / leftCapacity);
        int rightPages = (int) Math.ceil(1.0 * rightMetadata.numTuples / rightCapacity);

        int joinCost = PlanCost.getJoinCost(joinMethod, leftPages, rightPages);

        return leftPlanCost + rightPlanCost + joinCost;
    }

    private int accessPlanCost(String tableName) throws Exception {
        String fileName = tableName + ".stat";
        Schema schema = ((Operator) tabOpHash.get(tableName)).getSchema();

//        int numAttr = schema.getNumCols();

        BufferedReader in = new BufferedReader(new FileReader(fileName));
        String line = in.readLine();   // First line = number of tuples

        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statistics file " + fileName);
            System.exit(1);
        }
        /** number of tuples in this table **/
        String temp = tokenizer.nextToken();
        int numTuples = Integer.parseInt(temp);

        in.close();

        /** number of tuples per page**/
        int tupleSize = schema.getTupleSize();
        int pageSize = Batch.getPageSize() / tupleSize;
        int numPages = (int) Math.ceil(1.0 * numTuples / pageSize);

        Vector attrList = schema.getAttList();
        ArrayList<Attribute> attributeList = new ArrayList<>();
        for (int i=0; i<attrList.size(); i++) {
            attributeList.add((Attribute) attrList.get(i));
        }

        metaMemo.put(tableName, new Metadata(numTuples, tupleSize, attributeList));

        return numPages;
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
        currentResult = new ArrayList<>();  // re-initialize global helper variable
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