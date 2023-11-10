/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.olap.query;

import org.lealone.plugins.olap.OlapPlugin;
import org.lealone.sql.operator.OperatorFactoryBase;
import org.lealone.sql.query.Select;

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
