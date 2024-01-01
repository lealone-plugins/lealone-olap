/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.vector.jdk16;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBoolean;

public class Jdk16BooleanVector extends Jdk16ValueVector {

    private boolean[] values;

    public Jdk16BooleanVector() {
    }

    public Jdk16BooleanVector(boolean[] values) {
        this.values = values;
    }

    public boolean[] getValues() {
        return values;
    }

    public void setValues(boolean[] values) {
        this.values = values;
    }

    @Override
    public boolean isTrue(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value getValue(int index) {
        return ValueBoolean.get(values[index]);
    }
}
