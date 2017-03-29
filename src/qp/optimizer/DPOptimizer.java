package qp.optimizer;

import qp.utils.*;
import qp.operators.*;

public class DPOptimizer extends Optimizer {
    private SQLQuery sqlQuery;              // Vector of Vectors of Select + From + Where + GroupBy
    /** constructor **/
    public DPOptimizer(SQLQuery sqlquery) {
        this.sqlQuery = sqlquery;
    }

    public Operator getOptimizedPlan() {
        DPPlan dpPlan = new DPPlan(sqlQuery);
        return dpPlan.prepareInitialPlan();
    }
}
