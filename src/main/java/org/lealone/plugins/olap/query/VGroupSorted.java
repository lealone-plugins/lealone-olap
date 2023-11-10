/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.olap.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.sql.operator.Operator;
import org.lealone.sql.query.QGroup;
import org.lealone.sql.query.QGroupSorted;
import org.lealone.sql.query.Select;

// 只处理group by，且group by的字段有对应的索引
class VGroupSorted extends VOperator {

    private Value[] previousKeyValues;

    VGroupSorted(Select select) {
        super(select);
    }

    @Override
    public void copyStatus(Operator old) {
        super.copyStatus(old);
        if (old instanceof QGroupSorted) {
            QGroupSorted q = (QGroupSorted) old;
            previousKeyValues = q.getPreviousKeyValues();
        }
    }

    @Override
    public void run() {
        batch = new ArrayList<>(batchSize);
        while (topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate() && !tryLockRow())
                    return; // 锁记录失败
                rowCount++;
                Value[] keyValues = QGroup.getKeyValues(select);
                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    select.setCurrentGroup(new HashMap<>());
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    VGroup.updateVectorizedAggregate(select, columnCount, batch);
                    QGroup.addGroupRow(select, previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    select.setCurrentGroup(new HashMap<>());
                }
                batch.add(topTableFilter.get());
                if (yield)
                    return;
            }
        }
        if (previousKeyValues != null && !batch.isEmpty()) {
            VGroup.updateVectorizedAggregate(select, columnCount, batch);
            QGroup.addGroupRow(select, previousKeyValues, columnCount, result);
        }
        loopEnd = true;
    }
}
