/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import java.util.ArrayList;
import java.util.HashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.Utils;
import org.lealone.db.Mode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Alias;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionList;
import org.lealone.sql.expression.Operation;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Variable;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;
import org.lealone.sql.expression.condition.CompareLike;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionExists;
import org.lealone.sql.expression.condition.ConditionIn;
import org.lealone.sql.expression.condition.ConditionInConstantSet;
import org.lealone.sql.expression.condition.ConditionInSelect;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.expression.function.JavaFunction;
import org.lealone.sql.expression.function.TableFunction;
import org.lealone.sql.expression.subquery.SubQuery;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;
import org.lealone.sql.query.SelectUnion;
import org.lealone.sql.vector.BooleanVector;
import org.lealone.sql.vector.DefaultValueVector;
import org.lealone.sql.vector.DefaultValueVectorFactory;
import org.lealone.sql.vector.SingleValueVector;
import org.lealone.sql.vector.ValueVector;
import org.lealone.sql.vector.ValueVectorArray;
import org.lealone.sql.vector.ValueVectorFactory;

public class GetValueVectorVisitor extends ExpressionVisitorBase<ValueVector> {

    private final TableFilter tableFilter;
    private final ServerSession session;
    private final ValueVector bvv;
    private final ArrayList<Row> batch;
    private final ValueVectorFactory valueVectorFactory;
    private final HashMap<Column, ValueVector> vvMap = new HashMap<>();

    public GetValueVectorVisitor(TableFilter tableFilter, ServerSession session, ValueVector bvv,
            ArrayList<Row> batch) {
        this.tableFilter = tableFilter;
        this.session = session;
        this.bvv = bvv;
        this.batch = batch;
        this.valueVectorFactory = createValueVectorFactory(session);
    }

    private static ValueVectorFactory createValueVectorFactory(ServerSession session) {
        String valueVectorFactoryName = session.getValueVectorFactoryName();
        if (valueVectorFactoryName == null) {
            return DefaultValueVectorFactory.INSTANCE;
        } else {
            return Utils.newInstance(valueVectorFactoryName);
        }
    }

    private ValueVector getSingleValueVector(Expression e) {
        return new SingleValueVector(e.getValue(session));
    }

    @Override
    public ValueVector visitExpressionColumn(ExpressionColumn e) {
        // 缓存ExpressionColumn对应的ValueVector，避免重复构建
        ValueVector vv = vvMap.get(e.getColumn());
        if (vv == null) {
            vv = valueVectorFactory.createValueVector(batch, e.getColumn());
            if (vv == null) {
                throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, e.getSQL());
            }
            vvMap.put(e.getColumn(), vv);
        }
        return vv.filter(bvv);
    }

    @Override
    public ValueVector visitAggregate(Aggregate e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitAGroupConcat(AGroupConcat e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitJavaAggregate(JavaAggregate e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitAlias(Alias e) {
        return e.getNonAliasExpression().accept(this);
    }

    @Override
    public ValueVector visitExpressionList(ExpressionList e) {
        Expression[] list = e.getList();
        ValueVector[] a = new ValueVector[list.length];
        for (int i = 0; i < list.length; i++) {
            a[i] = list[i].accept(this);
        }
        return new ValueVectorArray(a);
    }

    @Override
    public ValueVector visitOperation(Operation e) {
        Expression left = e.getLeft(), right = e.getRight();
        ValueVector l = left.accept(this).convertTo(e.getDataType());
        ValueVector r;
        if (right == null) {
            r = null;
        } else {
            r = right.accept(this);
            if (e.isConvertRight()) {
                r = r.convertTo(e.getDataType());
            }
        }
        switch (e.getOpType()) {
        case Operation.NEGATE:
            return l.negate();
        case Operation.CONCAT: {
            Mode mode = session.getDatabase().getMode();
            return l.concat(r, mode.nullConcatIsNull);
        }
        case Operation.PLUS:
            return l.add(r);
        case Operation.MINUS:
            return l.subtract(r);
        case Operation.MULTIPLY:
            return l.multiply(r);
        case Operation.DIVIDE:
            return l.divide(r);
        case Operation.MODULUS:
            return l.modulus(r);
        default:
            throw DbException.getInternalError("type=" + e.getOpType());
        }
    }

    @Override
    public ValueVector visitParameter(Parameter e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitRownum(Rownum e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitSequenceValue(SequenceValue e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitSubQuery(SubQuery e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitValueExpression(ValueExpression e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitVariable(Variable e) {
        return getSingleValueVector(e);
    }

    @Override
    public ValueVector visitWildcard(Wildcard e) {
        throw DbException.getInternalError();
    }

    @Override
    public ValueVector visitCompareLike(CompareLike e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitComparison(Comparison e) {
        Expression left = e.getLeft(), right = e.getRight();
        ValueVector l = left.accept(this);
        if (right == null) {
            BooleanVector result;
            switch (e.getCompareType()) {
            case Comparison.IS_NULL:
                result = l.isNull();
                break;
            case Comparison.IS_NOT_NULL:
                result = l.isNotNull();
                break;
            default:
                throw DbException.getInternalError("type=" + e.getCompareType());
            }
            return result;
        }
        ValueVector r = right.accept(this);
        return l.compare(r, e.getCompareType());
    }

    @Override
    public ValueVector visitConditionAndOr(ConditionAndOr e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitConditionExists(ConditionExists e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitConditionIn(ConditionIn e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitConditionInConstantSet(ConditionInConstantSet e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitConditionInSelect(ConditionInSelect e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitConditionNot(ConditionNot e) {
        return e.getCondition().accept(this).negate();
    }

    @Override
    public ValueVector visitFunction(Function e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitJavaFunction(JavaFunction e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitTableFunction(TableFunction e) {
        return visitExpression(e);
    }

    @Override
    public ValueVector visitSelect(Select s) {
        throw DbException.getInternalError(); // TODO
    }

    @Override
    public ValueVector visitSelectUnion(SelectUnion su) {
        throw DbException.getInternalError(); // TODO
    }

    @Override
    public ValueVector visitExpression(Expression e) { // 回退到逐行遍历模式
        Row old = tableFilter.get();
        try {
            int size = batch.size();
            Value[] values = new Value[size];
            for (int i = 0; i < size; i++) {
                tableFilter.set(batch.get(i));
                values[i] = e.getValue(session);
            }
            ValueVector vv = new DefaultValueVector(values);
            if (bvv != null) {
                vv = vv.filter(bvv);
            }
            return vv;
        } finally {
            tableFilter.set(old);
        }
    }
}
