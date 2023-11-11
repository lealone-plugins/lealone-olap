/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.olap;

import java.util.Map;

import org.lealone.db.Plugin;
import org.lealone.db.PluginBase;
import org.lealone.db.PluginManager;
import org.lealone.plugins.olap.query.VOperatorFactory;
import org.lealone.sql.operator.OperatorFactory;

public class OlapPlugin extends PluginBase {

    public static final String NAME = "olap";

    public OlapPlugin() {
        super(NAME);
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        PluginManager.register(OperatorFactory.class, new VOperatorFactory(), getName());
    }

    @Override
    public void close() {
        OperatorFactory p = PluginManager.getPlugin(OperatorFactory.class, getName());
        if (p != null)
            PluginManager.deregister(OperatorFactory.class, p);
        super.close();
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return OperatorFactory.class;
    }
}
