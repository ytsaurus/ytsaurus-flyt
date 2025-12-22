package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;
import org.apache.flink.util.Preconditions;

@Slf4j
public class FirstAvailableClusterPickStrategy extends SimpleClusterPickStrategy {
    public static final String NAME = "firstAvailable";

    private List<String> priorities;

    public FirstAvailableClusterPickStrategy() {
        super(NAME);
    }

    @Override
    public void open(ReadableConfig options) {
        super.open(options);
        this.priorities = mandatory(priorities());
        this.validatePriorities();
    }

    @Override
    public String pickCluster() {
        for (String ytCluster : priorities) {
            log.info("Checking if cluster {} is available...", ytCluster);
            boolean available = isAvailable(ytCluster);
            log.info("Cluster {}'s availability = {}", ytCluster, available);
            if (available) {
                return ytCluster;
            }
        }
        throw new IllegalStateException("No available clusters");
    }

    private ConfigOption<List<String>> priorities() {
        return makeOption("priorities")
                .stringType()
                .asList()
                .defaultValues(getPathMap().keySet().toArray(String[]::new));
    }

    private void validatePriorities() {
        Set<String> actualSet = new HashSet<>(priorities);
        Preconditions.checkArgument(actualSet.size() == priorities.size(),
                "Priorities contain duplicates: " + priorities);

        Set<String> expectedSet = new HashSet<>(getPathMap().keySet());
        Set<String> difference = Sets.difference(expectedSet, actualSet);

        Preconditions.checkArgument(difference.isEmpty(),
                String.format("Priorities differ from lookup clusters: %s (expected: %s, actual: %s)",
                        difference, expectedSet, actualSet));
    }
}
