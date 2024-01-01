/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.vector;

import java.util.List;

import com.lealone.db.result.Row;
import com.lealone.db.table.Column;

public interface ValueVectorFactory {

    public ValueVector createValueVector(List<Row> batch, Column column);

}
