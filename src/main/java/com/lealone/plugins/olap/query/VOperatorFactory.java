/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.query;

import com.lealone.sql.operator.OperatorFactoryBase;
import com.lealone.sql.query.Select;

import com.lealone.plugins.olap.OlapPlugin;

public class VOperatorFactory extends OperatorFactoryBase {

    public VOperatorFactory() {
        super(OlapPlugin.NAME);
    }

    @Override
    public VOperator createOperator(Select select) {
        if (select.isQuickAggregateQuery()) {
            return null;
        } else if (select.isGroupQuery()) {
            if (select.isGroupSortedQuery()) {
                return new VGroupSorted(select);
            } else {
                if (select.getGroupIndex() == null) { // 忽视select.havingIndex
                    return new VAggregate(select);
                } else {
                    return new VGroup(select);
                }
            }
        } else if (select.isDistinctQuery()) {
            return null;
        } else {
            return new VFlat(select);
        }
    }
}
