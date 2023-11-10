/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.olap.query;

import java.util.ArrayList;
import java.util.HashMap;

import org.lealone.db.result.Row;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.plugins.olap.expression.visitor.UpdateVectorizedAggregateVisitor;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.operator.Operator;
import org.lealone.sql.query.QGroup;
import org.lealone.sql.query.Select;

// 只处理group by，且group by的字段没有索引
class VGroup extends VOperator {

    private ValueHashMap<HashMap<Expression, Object>> groups;
    private ValueHashMap<ArrayList<Row>> batchMap;

    VGroup(Select select) {
        super(select);
        groups = ValueHashMap.newInstance();
        batchMap = ValueHashMap.newInstance();
    }

    @Override
    public void copyStatus(Operator old) {
        super.copyStatus(old);
        if (old instanceof QGroup) {
            QGroup q = (QGroup) old;
            groups = q.getGroups();
        }
    }

    @Override
    public void run() {
        while (topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate() && !tryLockRow())
                    return; // 锁记录失败
                rowCount++;
                Value key = QGroup.getKey(select);
                batch = batchMap.get(key);
                if (batch == null) {
                    batch = new ArrayList<>(batchSize);
                    batchMap.put(key, batch);
                }
                batch.add(topTableFilter.get());
                if (batch.size() >= batchSize) {
                    updateVectorizedAggregate(key);
                }
                if (sampleSize > 0 && rowCount >= sampleSize) {
                    break;
                }
            }
            if (yield)
                return;
        }
        for (Value key : batchMap.keys()) {
            batch = batchMap.get(key);
            if (!batch.isEmpty()) {
                updateVectorizedAggregate(key);
            }
        }
        QGroup.addGroupRows(groups, select, columnCount, result);
        loopEnd = true;
    }

    private void updateVectorizedAggregate(Value key) {
        select.setCurrentGroup(QGroup.getOrCreateGroup(groups, key));
        updateVectorizedAggregate(select, columnCount, batch);
    }

    static void updateVectorizedAggregate(Select select, int columnCount, ArrayList<Row> batch) {
        select.incrementCurrentGroupRowId();
        UpdateVectorizedAggregateVisitor visitor = new UpdateVectorizedAggregateVisitor(
                select.getTopTableFilter(), select.getSession(), null, batch);
        for (int i = 0; i < columnCount; i++) {
            if (select.getGroupByExpression() == null || !select.getGroupByExpression()[i]) {
                Expression expr = select.getExpressions().get(i);
                expr.accept(visitor);
            }
        }
        batch.clear();
    }
}
