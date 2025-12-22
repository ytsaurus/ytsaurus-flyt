package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

@Slf4j
@UtilityClass
public class YtClusterUtils {
    private static final LoadingCache<String, Boolean> CLUSTER_AVAILABILITY_CACHE = CacheBuilder
            .newBuilder()
            // TODO: maybe add a property in the future
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public Boolean load(@Nonnull String cluster) {
                    return isAvailableNonCached(cluster);
                }
            });

    public static boolean isAvailable(String cluster) {
        try {
            return CLUSTER_AVAILABILITY_CACHE.get(cluster);
        } catch (ExecutionException e) {
            log.error("Failed to check cluster availability", e);
            throw new RuntimeException(e);
        }
    }

    private static boolean isAvailableNonCached(String cluster) {
        log.info("Performing non-cached liveness check of YT cluster {}...", cluster);
        try (YTsaurusClient client = YTsaurusClient.builder()
                .setAuth(YTsaurusClientAuth.empty())
                .setCluster(cluster)
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
