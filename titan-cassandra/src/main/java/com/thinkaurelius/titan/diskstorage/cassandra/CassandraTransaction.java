package com.thinkaurelius.titan.diskstorage.cassandra;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY;
import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class CassandraTransaction extends AbstractStoreTransaction {

    public CassandraTransaction(Configuration c) {
        super(c);
    }

    public CLevel getReadConsistencyLevel() {
        return CLevel.parse(getConfiguration().get(CASSANDRA_READ_CONSISTENCY));
    }

    public CLevel getWriteConsistencyLevel() {
        return CLevel.parse(getConfiguration().get(CASSANDRA_WRITE_CONSISTENCY));
    }

    public static CassandraTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions.checkArgument(txh instanceof CassandraTransaction, "Unexpected transaction type %s", txh.getClass().getName());
        return (CassandraTransaction) txh;
    }
}
