/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.olap.expression.evaluator;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.expression.evaluator.ExpressionEvaluator;

public abstract class JitEvaluator implements ExpressionEvaluator {

    protected HotSpotEvaluator evaluator;
    protected ServerSession session;

    public void setHotSpotEvaluator(HotSpotEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    public void setSession(ServerSession session) {
        this.session = session;
    }
}
