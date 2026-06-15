package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster;

import java.time.Duration;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH_MAP;

class FirstAvailableClusterPickStrategyTest {

    private static final String CANONICAL_PRIORITIES = "cluster-pick-strategy.firstAvailable.priorities";
    private static final String DEPRECATED_PRIORITIES = "clusterPickStrategy.firstAvailable.priorities";
    private static final String CANONICAL_PERIOD = "cluster-pick-strategy.firstAvailable.period";
    private static final String DEPRECATED_PERIOD = "clusterPickStrategy.firstAvailable.period";

    private static Configuration baseOptions() {
        Configuration options = new Configuration();
        options.set(PATH_MAP, Map.of(
                "first", "first://home/table",
                "second", "second://home/table"));
        return options;
    }

    private static FirstAvailableClusterPickStrategy open(Configuration options) {
        FirstAvailableClusterPickStrategy strategy = new FirstAvailableClusterPickStrategy();
        strategy.open(options);
        return strategy;
    }

    @Test
    void usesDefaultPeriodWhenUnset() {
        Assertions.assertThat(open(baseOptions()).getPollingPeriod()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void readsPeriodFromCanonicalKey() {
        Configuration options = baseOptions();
        options.setString(CANONICAL_PERIOD, "5 min");
        Assertions.assertThat(open(options).getPollingPeriod()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void readsPeriodFromDeprecatedKey() {
        Configuration options = baseOptions();
        options.setString(DEPRECATED_PERIOD, "5 min");
        Assertions.assertThat(open(options).getPollingPeriod()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void prioritiesDefaultToLookupClusters() {
        assertDoesNotThrow(() -> open(baseOptions()));
    }

    @Test
    void readsPrioritiesFromCanonicalKey() {
        Configuration options = baseOptions();
        options.setString(CANONICAL_PRIORITIES, "FIRST;SECOND");
        assertDoesNotThrow(() -> open(options));
    }

    @Test
    void readsPrioritiesFromDeprecatedKey() {
        Configuration options = baseOptions();
        options.setString(DEPRECATED_PRIORITIES, "FIRST;SECOND");
        assertDoesNotThrow(() -> open(options));
    }

    @Test
    void validationFailsWhenCanonicalPrioritiesMissCluster() {
        Configuration options = baseOptions();
        options.setString(CANONICAL_PRIORITIES, "FIRST");
        Assertions.assertThatThrownBy(() -> open(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Priorities differ from lookup clusters");
    }

    @Test
    void validationFailsWhenDeprecatedPrioritiesMissCluster() {
        Configuration options = baseOptions();
        options.setString(DEPRECATED_PRIORITIES, "FIRST");
        Assertions.assertThatThrownBy(() -> open(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Priorities differ from lookup clusters");
    }

    @Test
    void validationFailsOnDuplicatePriorities() {
        Configuration options = baseOptions();
        options.setString(CANONICAL_PRIORITIES, "FIRST;FIRST;SECOND");
        Assertions.assertThatThrownBy(() -> open(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Priorities contain duplicates");
    }
}
