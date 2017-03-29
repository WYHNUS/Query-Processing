package qp.optimizer;

import qp.operators.*;
import qp.utils.*;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public abstract class BasicPlan {
    protected SQLQuery sqlquery;

    protected Vector projectList;
    protected Vector fromList;
    protected Vector selectionlist;     	// List of select conditions
    protected Vector joinlist;          	// List of join conditions
    protected Vector groupbylist;
    protected int numJoin;    			// Number of joins in this query

    protected Hashtable tabOpHash;      // table name to the Operator
    protected Operator root; 				// root of the query plan tree

    public BasicPlan(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;

        projectList = sqlquery.getProjectList();
        fromList = sqlquery.getFromList();
        selectionlist = sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();

        numJoin = joinlist.size();
        tabOpHash = new Hashtable();
    }

    /** number of join conditions **/
    public int getNumJoins(){
        return numJoin;
    }

    /** prepare initial plan for the query **/
    public Operator prepareInitialPlan() {
        createScanOp();
        createSelectOp();
        if (numJoin != 0) {
            createJoinOp();
        }
        createProjectOp();
        return root;
    }

    /** create join operators **/
    public abstract void createJoinOp();

    /**
     * Create Scan Operator for each of the table mentioned in from list
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
            tabOpHash.put(tabName, op1);
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

    /**
     * Create Selection Operators for each of the
     * selection condition mentioned in Condition list
     **/
    public void createSelectOp() {
        Select op1 = null;

        for (int j=0; j<selectionlist.size(); j++) {
            Condition cn = (Condition) selectionlist.elementAt(j);
            if (cn.getOpType() == Condition.SELECT) {
                String tabName = cn.getLhs().getTabName();
                //System.out.println("RandomInitial:-------------Select-------:"+tabname);

                Operator temPop = (Operator)tabOpHash.get(tabName);
                op1 = new Select(temPop, cn, OpType.SELECT);
                /** set the schema same as base relation **/
                op1.setSchema(temPop.getSchema());

                modifyHashtable(temPop, op1);
                //tab_op_hash.put(tabname,op1);
            }
        }
        /** The last selection is the root of the plan tree constructed thus far **/
        if (selectionlist.size() != 0) {
            root = op1;
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

    public void modifyHashtable(Operator old, Operator newop) {
        Enumeration e = tabOpHash.keys();
        while (e.hasMoreElements()) {
            String key = (String)e.nextElement();
            Operator temp = (Operator)tabOpHash.get(key);
            if (temp == old) {
                tabOpHash.put(key, newop);
            }
        }
    }
}
