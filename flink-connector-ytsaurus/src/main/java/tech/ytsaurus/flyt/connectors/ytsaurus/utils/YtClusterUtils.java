package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

@Slf4j
@UtilityClass
public class YtClusterUtils {
    private static final Cache<String, Boolean> CLUSTER_AVAILABILITY_CACHE = CacheBuilder
            .newBuilder()
            // TODO: maybe add a property in the future
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build();

    public static boolean isAvailable(String cluster, boolean useTls) {
        try {
            return CLUSTER_AVAILABILITY_CACHE.get(cluster, () -> isAvailableNonCached(cluster, useTls));
        } catch (ExecutionException e) {
            log.error("Failed to check cluster availability", e);
            throw new RuntimeException(e);
        }
    }

    private static boolean isAvailableNonCached(String cluster, boolean useTls) {
        log.info("Performing non-cached liveness check of YT cluster {} (tls={})...", cluster, useTls);
        try (YTsaurusClient client = YTsaurusClient.builder()
                .setAuth(YTsaurusClientAuth.empty())
                .setCluster(cluster)
                .setConfig(YtUtils.makeYtClientConfig(useTls))
                .build()) {
            CompletableFuture<Void> result = client.waitProxies();
            try {
                result.get();
                return true;
            } catch (ExecutionException e) {
                if (e.getMessage().contains("Cannot get rpc proxies;")) {
                    return false;
                }
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
