package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import static org.apache.flink.shaded.guava31.com.google.common.base.Preconditions.checkNotNull;

public class YtClientConfig implements Serializable {
    @Getter
    private final String proxy;
    @Getter
    private final String user;
    @Getter
    private final String token;
    @Getter
    private final String proxyRole;
    @Getter
    private final Duration globalYtTimeout;
    @Getter
    private final int batchSize;
    @Getter
    private final Duration transactionTimeout;
    @Getter
    private final Duration pendingTimeout;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private YtClientConfig(String proxy, String user, String token, String proxyRole, Duration globalYtTimeout,
                           int batchSize, Duration transactionTimeout, Duration pendingTimeout) {
        this.proxy = proxy;
        this.user = user;
        this.token = token;
        this.proxyRole = proxyRole;
        this.globalYtTimeout = globalYtTimeout;
        this.batchSize = batchSize;
        this.transactionTimeout = transactionTimeout;
        this.pendingTimeout = pendingTimeout;
    }

    @Accessors(chain = true)
    public static class Builder {
        @Setter
        private String proxy;
        @Setter
        private String user;
        @Setter
        private String token;
        @Setter
        private String proxyRole;
        @Setter
        private Duration globalTimeout = Duration.ofMinutes(5);
        @Setter
        private int batchSize = 10;
        @Setter
        private Duration transactionTimeout = Duration.ofMinutes(5);
        @Setter
        private Duration pendingTimeout = Duration.ofMinutes(5);

        public YtClientConfig build() {
            checkNotNull(proxy);

            return new YtClientConfig(
                    proxy,
                    Optional.ofNullable(user).orElseGet(YtClientConfig::localYtUser),
                    Optional.ofNullable(token).orElseGet(YtClientConfig::localYtToken),
                    proxyRole,
                    globalTimeout,
                    batchSize,
                    transactionTimeout,
                    pendingTimeout
            );
        }
    }

    private static String localYtUser() {
        return Optional.ofNullable(System.getenv("YT_USER"))
                .orElseGet(() -> System.getProperty("user.name"));
    }

    private static String localYtToken() {
        return Optional.ofNullable(System.getenv("YT_TOKEN"))
                .orElseGet(() -> readFileFromHome(".yt", "token"));
    }

    private static String readFileFromHome(String... path) {
        return readFile(Paths.get(System.getProperty("user.home"), path));
    }

    private static String readFile(Path path) {
        try (var reader = Files.newBufferedReader(path)) {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
