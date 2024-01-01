/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.query;

import java.util.ArrayList;

import com.lealone.db.result.Row;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.evaluator.ExpressionEvaluator;
import com.lealone.sql.operator.Operator;
import com.lealone.sql.optimizer.TableFilter;
import com.lealone.sql.query.QOperator;
import com.lealone.sql.query.Select;

import com.lealone.plugins.olap.expression.evaluator.HotSpotEvaluator;
import com.lealone.plugins.olap.expression.visitor.GetValueVectorVisitor;
import com.lealone.plugins.olap.vector.ValueVector;

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
