/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.olap.query;

import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.plugins.olap.expression.visitor.UpdateVectorizedAggregateVisitor;
import org.lealone.plugins.olap.vector.ValueVector;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.query.QGroup;
import org.lealone.sql.query.Select;

// 除了QuickAggregateQuery之外的聚合函数，没有group by
class VAggregate extends VOperator {

    VAggregate(Select select) {
        super(select);
        if (select.getCurrentGroup() == null)
            select.setCurrentGroup(new HashMap<>());
    }

    @Override
    public void run() {
        while (nextBatch()) {
            boolean yield = yieldIfNeeded(++loopCount);
            topTableFilter.setBatchSize(batch.size());
            ValueVector conditionValueVector = getConditionValueVector();
            UpdateVectorizedAggregateVisitor visitor = new UpdateVectorizedAggregateVisitor(
                    topTableFilter, session, conditionValueVector, batch);
            select.incrementCurrentGroupRowId();
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.getExpressions().get(i);
                expr.accept(visitor);
            }
            rowCount += getBatchSize(conditionValueVector);
            if (sampleSize > 0 && rowCount >= sampleSize) {
                break;
            }
            if (yield)
                return;
        }
        // 最后把聚合后的结果增加到结果集中
        Value[] row = createRow();
        row = QGroup.toResultRow(row, columnCount, select.getResultColumnCount());
        result.addRow(row);
        loopEnd = true;
    }
}
