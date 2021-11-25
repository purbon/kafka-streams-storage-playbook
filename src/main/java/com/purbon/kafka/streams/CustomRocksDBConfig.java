package com.purbon.kafka.streams;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

import java.util.Map;

public class CustomRocksDBConfig implements RocksDBConfigSetter {

    private org.rocksdb.Cache cache = new org.rocksdb.LRUCache(16 * 1024L * 1024L);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        options.
        tableConfig.setBlockCache(cache);
        // See #2 below.
        tableConfig.setBlockSize(16 * 1024L);
        // See #3 below.
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setTableFormatConfig(tableConfig);
        // See #4 below.
        options.setMaxWriteBufferNumber(2);
    }


    @Override
    public void close(final String storeName, final Options options) {
        // See #5 below.
        cache.close();
    }

}
