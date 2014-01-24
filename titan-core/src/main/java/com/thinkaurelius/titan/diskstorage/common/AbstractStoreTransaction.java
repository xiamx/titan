package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;
import static com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration.TIMESTAMP_OVERRIDE;

/**
 * Abstract implementation of {@link StoreTransaction} to be used as the basis for more specific implementations.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractStoreTransaction implements StoreTransaction {

    private final Configuration config;
    private long ts;

    public AbstractStoreTransaction(Configuration config) {
        Preconditions.checkNotNull(config);
        this.config = config;
        Long l = config.get(TIMESTAMP_OVERRIDE);
        if (null != l)
            ts = l;
    }

    @Override
    public void commit() throws StorageException {
    }

    @Override
    public void rollback() throws StorageException {
    }

    @Override
    public void flush() throws StorageException {
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    public long getTimestamp() {
        return ts;
    }

    public void setTimestamp(long ts) {
        this.ts = ts;
    }

    @Override
    public String getMetricsPrefix() {
        return config.get(METRICS_PREFIX);
    }

}
