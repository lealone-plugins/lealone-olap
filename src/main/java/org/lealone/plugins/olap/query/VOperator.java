/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.olap.query;

import java.util.ArrayList;

import org.lealone.db.result.Row;
import org.lealone.plugins.olap.expression.evaluator.HotSpotEvaluator;
import org.lealone.plugins.olap.expression.visitor.GetValueVectorVisitor;
import org.lealone.plugins.olap.vector.ValueVector;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.evaluator.ExpressionEvaluator;
import org.lealone.sql.operator.Operator;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.QOperator;
import org.lealone.sql.query.Select;

public class VOperator extends QOperator {

    // protected static final int MAX_BATCH_SIZE = 1024;

    protected ArrayList<Row> batch;
    protected final int batchSize;
    protected final TableFilter topTableFilter;

    VOperator(Select select) {
        super(select);
        // 批量大小为128时经过实测更优，见VectorPerfTest
        batchSize = session.getOlapBatchSize() <= 0 ? 128 : session.getOlapBatchSize();
        batch = new ArrayList<>(batchSize);
        topTableFilter = select.getTopTableFilter();
    }

    @Override
    public ExpressionEvaluator createConditionEvaluator(Expression c) {
        return new HotSpotEvaluator(session, c);
    }

    @Override
    public void copyStatus(Operator old) {
        if (old instanceof QOperator) {
            QOperator q = (QOperator) old;
            q.copyStatusTo(this);
            rowCount++; // 调用YieldableSelect.yieldIfNeeded后，oltp转olap之前，QOperator的子类还会执行一次
        }
    }

    protected boolean nextBatch() {
        batch.clear();
        // 不能把topTableFilter.next()放在前面，会丢掉一条记录
        for (int i = 0; i < batchSize && topTableFilter.next(); i++) {
            batch.add(topTableFilter.get());
        }
        return !batch.isEmpty();
    }

    protected ValueVector getConditionValueVector() {
        ValueVector conditionValueVector = null;
        if (select.getCondition() != null) {
            GetValueVectorVisitor visitor = new GetValueVectorVisitor(topTableFilter, session, null,
                    batch);
            conditionValueVector = select.getCondition().accept(visitor);
        }
        return conditionValueVector;
    }

    protected int getBatchSize(ValueVector conditionValueVector) {
        return conditionValueVector == null ? batch.size() : conditionValueVector.trueCount();
    }
}
