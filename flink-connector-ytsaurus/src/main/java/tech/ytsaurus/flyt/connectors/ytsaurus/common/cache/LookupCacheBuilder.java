package tech.ytsaurus.flyt.connectors.ytsaurus.common.cache;

import java.util.function.BiFunction;

import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.functions.UserDefinedFunction;

public interface LookupCacheBuilder extends BiFunction<UserDefinedFunction, ScanTableSource.ScanRuntimeProvider,
        LookupTableSource.LookupRuntimeProvider> {
}
