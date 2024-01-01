/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.query;

import com.lealone.db.value.Value;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.query.Select;

import com.lealone.plugins.olap.expression.visitor.GetValueVectorVisitor;
import com.lealone.plugins.olap.vector.ValueVector;

// 最普通的查询
class VFlat extends VOperator {

    VFlat(Select select) {
        super(select);
    }

    @Override
    public void run() {
        while (nextBatch()) {
            boolean yield = yieldIfNeeded(++loopCount);
            ValueVector conditionValueVector = getConditionValueVector();
            GetValueVectorVisitor visitor = new GetValueVectorVisitor(topTableFilter, session,
                    conditionValueVector, batch);
            ValueVector[] rows = new ValueVector[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.getExpressions().get(i);
                rows[i] = expr.accept(visitor);
            }
            for (int i = 0, szie = rows[0].size(); i < szie; i++) {
                Value[] row = new Value[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    ValueVector vv = rows[j];
                    row[j] = vv.getValue(i);
                }
                result.addRow(row);
            }
            rowCount += getBatchSize(conditionValueVector);
            if (canBreakLoop()) {
                break;
            }
            if (yield)
                return;
        }
        loopEnd = true;
    }
}
