/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.vector.jdk16;

import java.util.List;

import com.lealone.db.result.Row;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;

import com.lealone.plugins.olap.vector.DefaultValueVectorFactory;
import com.lealone.plugins.olap.vector.ValueVector;
import com.lealone.plugins.olap.vector.ValueVectorFactory;

public class Jdk16ValueVectorFactory implements ValueVectorFactory {

    @Override
    public ValueVector createValueVector(List<Row> batch, Column column) {
        int size = batch.size();
        int columnId = column.getColumnId();
        switch (column.getType()) {
        case Value.INT: {
            int[] values = new int[size];
            for (int i = 0; i < size; i++) {
                values[i] = batch.get(i).getValue(columnId).getInt();
            }
            return new Jdk16IntVector(values);
        }
        default:
            return DefaultValueVectorFactory.createDefaultValueVector(batch, column);
        }
    }

}
