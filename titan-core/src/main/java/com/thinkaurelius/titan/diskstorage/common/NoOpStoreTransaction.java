package com.thinkaurelius.titan.diskstorage.common;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;

/**
 * Dummy transaction object that does nothing
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class NoOpStoreTransaction extends AbstractStoreTransaction {

    public NoOpStoreTransaction(Configuration config) {
        super(config);
    }
}
