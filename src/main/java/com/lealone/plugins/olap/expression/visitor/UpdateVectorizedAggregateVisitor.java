/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.expression.visitor;

import java.util.ArrayList;

import com.lealone.db.result.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.util.ValueHashMap;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.aggregate.ACount.AggregateDataCount;
import com.lealone.sql.expression.aggregate.ACountAll.AggregateDataCountAll;
import com.lealone.sql.expression.aggregate.ADefault.AggregateDataDefault;
import com.lealone.sql.expression.visitor.VoidExpressionVisitor;
import com.lealone.sql.expression.aggregate.AGroupConcat;
import com.lealone.sql.expression.aggregate.Aggregate;
import com.lealone.sql.expression.aggregate.AggregateData;
import com.lealone.sql.expression.aggregate.BuiltInAggregate;
import com.lealone.sql.expression.aggregate.JavaAggregate;
import com.lealone.sql.optimizer.TableFilter;

import com.lealone.plugins.olap.vector.ValueVector;

public class UpdateVectorizedAggregateVisitor extends VoidExpressionVisitor {

    private final ServerSession session;
    private final ValueVector bvv;
    private final GetValueVectorVisitor getValueVectorVisitor;

    public UpdateVectorizedAggregateVisitor(TableFilter tableFilter, ServerSession session,
            ValueVector bvv, ArrayList<Row> batch) {
        this.session = session;
        this.bvv = bvv;
        this.getValueVectorVisitor = new GetValueVectorVisitor(tableFilter, session, bvv, batch);
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        e.updateAggregate(session); // 直接更新单行即可
        return null;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        BuiltInAggregate bia = (BuiltInAggregate) e;
        AggregateData data = bia.getAggregateData();
        if (data == null) {
            return null;
        }
        ValueVector vv = e.getOn() == null ? null : e.getOn().accept(getValueVectorVisitor);

        switch (bia.getAType()) {
        case Aggregate.COUNT:
            updateVectorizedCount(session, bvv, vv, (AggregateDataCount) data);
            break;
        case Aggregate.COUNT_ALL:
            updateVectorizedCountAll(session, bvv, vv, (AggregateDataCountAll) data);
            break;
        case Aggregate.GROUP_CONCAT:
            updateVectorizedAggregate(session, bvv, vv, data);
            break;
        case Aggregate.HISTOGRAM:
            updateVectorizedAggregate(session, bvv, vv, data);
            break;
        case Aggregate.SELECTIVITY:
            updateVectorizedAggregate(session, bvv, vv, data);
            break;
        default:
            updateVectorizedDefault(session, bvv, vv, (AggregateDataDefault) data);
        }
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        visitAggregate(e);
        return null;
    }

    private void updateVectorizedAggregate(ServerSession session, ValueVector bvv, ValueVector vv,
            AggregateData data) {
        for (Value v : vv.getValues(bvv)) {
            data.add(session, v);
        }
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        visitAggregate(e);
        return null;
    }

    private void updateVectorizedCount(ServerSession session, ValueVector bvv, ValueVector vv,
            AggregateDataCount a) {
        long count = a.getCount();
        if (bvv == null)
            count += vv.size();
        else
            count += bvv.trueCount();
        a.setCount(count);
        if (a.isDistinct()) {
            ValueHashMap<AggregateDataCount> distinctValues = a.getDistinctValues();
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
                a.setDistinctValues(distinctValues);
            }
            for (Value v : vv.getValues(bvv))
                distinctValues.put(v, a);
        }
    }

    private void updateVectorizedCountAll(ServerSession session, ValueVector bvv, ValueVector vv,
            AggregateDataCountAll a) {
        long count = a.getCount();
        if (bvv == null)
            count += a.getSelect().getTopTableFilter().getBatchSize();
        else
            count += bvv.trueCount();
        a.setCount(count);
    }

    private void updateVectorizedDefault(ServerSession session, ValueVector bvv, ValueVector vv,
            AggregateDataDefault a) {
        long count = a.getCount();
        if (bvv == null)
            count += vv.size();
        else
            count += bvv.trueCount();
        a.setCount(count);
        if (a.isDistinct()) {
            ValueHashMap<AggregateDataDefault> distinctValues = a.getDistinctValues();
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
                a.setDistinctValues(distinctValues);
            }
            for (Value v0 : vv.getValues(bvv))
                distinctValues.put(v0, a);
            return;
        }
        Value value = a.getValue();
        int dataType = a.getDataType();
        switch (a.getAType()) {
        case Aggregate.SUM:
            if (value == null) {
                value = vv.sum(bvv);
            } else {
                value = value.add(vv.sum().convertTo(dataType));
            }
            // if (this.vv == null) {
            // // value = v.convertTo(dataType);
            // this.vv = vv;
            // this.bvv = bvv;
            // } else {
            // // v = v.convertTo(value.getType());
            // this.vv = this.vv.add(this.bvv, vv, bvv);
            // }
            a.setValue(value);
            return;
        case Aggregate.AVG:
            if (value == null) {
                value = vv.sum(bvv);
            } else {
                value = value.add(vv.sum(bvv));
            }
            a.setValue(value);
            return;
        case Aggregate.MIN:
            if (value == null) {
                value = vv.min(bvv);
            } else {
                Value min = vv.min(bvv);
                if (session.getDatabase().compare(min, value) < 0)
                    value = min;
            }
            a.setValue(value);
            return;
        case Aggregate.MAX:
            if (value == null) {
                value = vv.max(bvv);
            } else {
                Value max = vv.max(bvv);
                if (session.getDatabase().compare(max, value) > 0)
                    value = max;
            }
            a.setValue(value);
            return;
        }
        for (Value v : vv.getValues(bvv)) {
            if (v == ValueNull.INSTANCE) {
                continue;
            }
            a.addOther(session, v);
        }
    }
}
