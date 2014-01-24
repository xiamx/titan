package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;

/**
 * Used to configure a {@link com.thinkaurelius.titan.core.TitanTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see com.thinkaurelius.titan.core.TitanTransaction
 */
public class StandardTransactionBuilder implements TransactionConfiguration, TransactionBuilder {

    private boolean isReadOnly = false;

    private boolean assignIDsImmediately = false;

    private DefaultTypeMaker defaultTypeMaker;

    private boolean verifyExternalVertexExistence = true;

    private boolean verifyInternalVertexExistence = false;

    private boolean verifyUniqueness = true;

    private boolean acquireLocks = true;

    private boolean propertyPrefetching = true;

    private boolean singleThreaded = false;

    private boolean threadBound = false;

    private int vertexCacheSize;

    private long indexCacheWeight;

    private Long timestamp = null;

    private String metricsPrefix;

    private ModifiableConfiguration storageConfiguration;

    private final StandardTitanGraph graph;

    /**
     * Constructs a new TitanTransaction configuration with default configuration parameters.
     */
    public StandardTransactionBuilder(GraphDatabaseConfiguration graphConfig, StandardTitanGraph graph) {
        Preconditions.checkNotNull(graphConfig);
        Preconditions.checkNotNull(graph);
        this.graph = graph;
        this.defaultTypeMaker = graphConfig.getDefaultTypeMaker();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.metricsPrefix = graphConfig.getMetricsPrefix();
        this.propertyPrefetching = graphConfig.hasPropertyPrefetching();
        this.storageConfiguration = GraphDatabaseConfiguration.buildConfiguration();
        if (graphConfig.isReadOnly()) readOnly();
        setCacheSize(graphConfig.getTxCacheSize());
        if (graphConfig.isBatchLoading()) enableBatchLoading();
    }

    public StandardTransactionBuilder threadBound() {
        this.threadBound = true;
        this.singleThreaded = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder readOnly() {
        this.isReadOnly = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder enableBatchLoading() {
        verifyUniqueness = false;
        verifyExternalVertexExistence = false;
        acquireLocks = false;
        return this;
    }

    @Override
    public StandardTransactionBuilder setCacheSize(int size) {
        Preconditions.checkArgument(size >= 0);
        this.vertexCacheSize = size;
        this.indexCacheWeight = size / 2;
        return this;
    }

    @Override
    public StandardTransactionBuilder checkInternalVertexExistence() {
        this.verifyInternalVertexExistence = true;
        return this;
    }

    @Override
    public StandardTransactionBuilder setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public StandardTransactionBuilder setMetricsPrefix(String p) {
        this.metricsPrefix = p;
        return this;
    }

    @Override
    public TitanTransaction start() {
        // TODO copy storageConfiguration to an immutable equivalent

        if (null != timestamp)
            storageConfiguration.set(TIMESTAMP_OVERRIDE, getTimestamp());
        else
            storageConfiguration.set(TIMESTAMP_OVERRIDE, TimeUtility.INSTANCE.getApproxNSSinceEpoch());

        if (null != metricsPrefix)
            storageConfiguration.set(METRICS_PREFIX, metricsPrefix);
        else
            storageConfiguration.set(METRICS_PREFIX, METRICS_PREFIX.getDefaultValue());

        TransactionConfiguration immutable = new ImmutableTxCfg(isReadOnly,
                assignIDsImmediately, verifyExternalVertexExistence,
                verifyInternalVertexExistence, acquireLocks,
                verifyUniqueness, propertyPrefetching, singleThreaded,
                threadBound, hasTimestamp(), hasTimestamp() ? timestamp : 0, indexCacheWeight,
                vertexCacheSize, metricsPrefix, defaultTypeMaker, storageConfiguration);
        return graph.newTransaction(immutable);
    }


    /* ##############################################
                    TransactionConfig
    ############################################## */


    @Override
    public final boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public final boolean hasAssignIDsImmediately() {
        return assignIDsImmediately;
    }

    @Override
    public final boolean hasVerifyExternalVertexExistence() {
        return verifyExternalVertexExistence;
    }

    @Override
    public final boolean hasVerifyInternalVertexExistence() {
        return verifyInternalVertexExistence;
    }

    @Override
    public final boolean hasAcquireLocks() {
        return acquireLocks;
    }

    @Override
    public final DefaultTypeMaker getAutoEdgeTypeMaker() {
        return defaultTypeMaker;
    }

    @Override
    public final boolean hasVerifyUniqueness() {
        return verifyUniqueness;
    }

    public boolean hasPropertyPrefetching() {
        return propertyPrefetching;
    }

    @Override
    public final boolean isSingleThreaded() {
        return singleThreaded;
    }

    @Override
    public final boolean isThreadBound() {
        return threadBound;
    }

    @Override
    public final int getVertexCacheSize() {
        return vertexCacheSize;
    }

    @Override
    public final long getIndexCacheWeight() {
        return indexCacheWeight;
    }

    @Override
    public boolean hasTimestamp() {
        return timestamp != null;
    }

    @Override
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    @Override
    public ModifiableConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    @Override
    public long getTimestamp() {
        Preconditions.checkState(timestamp != null, "A timestamp has not been configured");
        return timestamp;
    }

    private static class ImmutableTxCfg implements TransactionConfiguration {

        private final boolean isReadOnly;
        private final boolean hasAssignIDsImmediately;
        private final boolean hasVerifyExternalVertexExistence;
        private final boolean hasVerifyInternalVertexExistence;
        private final boolean hasAcquireLocks;
        private final boolean hasVerifyUniqueness;
        private final boolean hasPropertyPrefetching;
        private final boolean isSingleThreaded;
        private final boolean isThreadBound;
        private final boolean hasTimestamp;

        private final long timestamp;
        private final long indexCacheWeight;

        private final int vertexCacheSize;

        private final String metricsPrefix;

        private final DefaultTypeMaker defaultTypeMaker;

        private final Configuration storageConfiguration;

        public ImmutableTxCfg(boolean isReadOnly,
                boolean hasAssignIDsImmediately,
                boolean hasVerifyExternalVertexExistence,
                boolean hasVerifyInternalVertexExistence,
                boolean hasAcquireLocks, boolean hasVerifyUniqueness,
                boolean hasPropertyPrefetching, boolean isSingleThreaded,
                boolean isThreadBound, boolean hasTimestamp, long timestamp,
                long indexCacheWeight, int vertexCacheSize, String metricsPrefix, DefaultTypeMaker defaultTypeMaker, Configuration storageConfiguration) {
            this.isReadOnly = isReadOnly;
            this.hasAssignIDsImmediately = hasAssignIDsImmediately;
            this.hasVerifyExternalVertexExistence = hasVerifyExternalVertexExistence;
            this.hasVerifyInternalVertexExistence = hasVerifyInternalVertexExistence;
            this.hasAcquireLocks = hasAcquireLocks;
            this.hasVerifyUniqueness = hasVerifyUniqueness;
            this.hasPropertyPrefetching = hasPropertyPrefetching;
            this.isSingleThreaded = isSingleThreaded;
            this.isThreadBound = isThreadBound;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
            this.indexCacheWeight = indexCacheWeight;
            this.vertexCacheSize = vertexCacheSize;
            this.metricsPrefix = metricsPrefix;
            this.defaultTypeMaker = defaultTypeMaker;
            this.storageConfiguration = storageConfiguration;
        }

        @Override
        public boolean isReadOnly() {
            return isReadOnly;
        }

        @Override
        public boolean hasAssignIDsImmediately() {
            return hasAssignIDsImmediately;
        }

        @Override
        public boolean hasVerifyExternalVertexExistence() {
            return hasVerifyExternalVertexExistence;
        }

        @Override
        public boolean hasVerifyInternalVertexExistence() {
            return hasVerifyInternalVertexExistence;
        }

        @Override
        public boolean hasAcquireLocks() {
            return hasAcquireLocks;
        }

        @Override
        public DefaultTypeMaker getAutoEdgeTypeMaker() {
            return defaultTypeMaker;
        }

        @Override
        public boolean hasVerifyUniqueness() {
            return hasVerifyUniqueness;
        }

        @Override
        public boolean hasPropertyPrefetching() {
            return hasPropertyPrefetching;
        }

        @Override
        public boolean isSingleThreaded() {
            return isSingleThreaded;
        }

        @Override
        public boolean isThreadBound() {
            return isThreadBound;
        }

        @Override
        public int getVertexCacheSize() {
            return vertexCacheSize;
        }

        @Override
        public long getIndexCacheWeight() {
            return indexCacheWeight;
        }

        @Override
        public boolean hasTimestamp() {
            return hasTimestamp;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String getMetricsPrefix() {
            return metricsPrefix;
        }

        @Override
        public Configuration getStorageConfiguration() {
            return storageConfiguration;
        }

    }
}
