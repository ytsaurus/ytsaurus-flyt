package tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info;

import java.util.Optional;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.metrics.GaugeLong;

@Slf4j
public class ProjectInfoUtils {
    public static final int REQUIRED_VERSION_PARTS_COUNT = 3;
    public static final int MAX_VERSION_PARTS_COUNT = 4;

    public static final int RELEASE_PRERANK = 999;
    public static final int RC_PRERANK = 300;
    public static final int SNAPSHOT_AND_DEV_PRERANK = 200;
    public static final int UNKNOWN_PRERANK = 100;


    public static void registerProjectInFlinkMetrics(String projectName,
                                                     String projectVersion,
                                                     Supplier<MetricGroup> metricGroupSupplier) {
        log.info("Registering project metrics: {} v{}", projectName, projectVersion);
        try {
            MetricGroup metricGroup = metricGroupSupplier.get();
            parseVersion(projectVersion).ifPresentOrElse(
                    version -> registerProjectInFlinkMetrics(projectName, version, metricGroup),
                    () -> log.warn("Unable to parse project[name={}] version: {}", projectName, projectVersion)
            );
        } catch (IllegalStateException e) {
            log.warn("Unable to register project: {} in Flink metrics", projectName, e);
        }
    }

    private static void registerProjectInFlinkMetrics(String projectName,
                                                      Long projectVersion,
                                                      MetricGroup metricGroup) {
        metricGroup = metricGroup.addGroup(projectName);
        GaugeLong gauge = new GaugeLong(() -> projectVersion);
        metricGroup.gauge("version", gauge);
    }

    /**
     * Parses a version string into a single numeric representation using the formula:
     *
     * <p><b>releaseNumber = major·B⁴ + minor·B³ + patch·B² + preRank·B + preNum</b>
     * <br>where B = 1000, and:
     * <ul>
     *   <li>major, minor, patch ∈ [0, 999] - version components</li>
     *   <li>preRank ∈ [0, 999] - version stability marker</li>
     * </ul>
     *
     * <p><b>Version mapping rules:</b>
     * <ul>
     *   <li><b>release</b> (1.2.3): preRank = 999, preNum = 0</li>
     *   <li><b>rc</b> (1.2.3-rc.2): preRank = 300, preNum = 2</li>
     *   <li><b>SNAPSHOT/dev</b> (1.2.3-SNAPSHOT.1): preRank = 200, preNum = 1</li>
     *   <li><b>/Unknown tag</b> (1.2.3-unknown.5): preRank = 100, preNum = 5</li>
     * </ul>
     *
     * <p><b>Calculation examples:</b>
     * <ul>
     *   <li>1.2.3 → 1×1000⁴ + 2×1000³ + 3×1000² + 999×1000 + 0 = 1,002,003,999,000</li>
     *   <li>1.2.3-rc.2 → 1×1000⁴ + 2×1000³ + 3×1000² + 400×1000 + 2 = 1,002,003,400,002</li>
     *   <li>1.0.0-alpha.5 → 1×1000⁴ + 0×1000³ + 0×1000² + 200×1000 + 5 = 1,000,000,200,005</li>
     * </ul>
     *
     * @param version version string to parse (e.g. "1.2.3", "1.5.0-rc.2")
     * @return numeric representation of version or empty Optional if:
     * <ul>
     *   <li>version is null/empty</li>
     *   <li>has invalid format</li>
     *   <li>any component ∉ [0, 999]</li>
     * </ul>
     */
    @VisibleForTesting
    static Optional<Long> parseVersion(String version) {
        if (version == null || version.isEmpty()) {
            return Optional.empty();
        }
        long preRank = getPreRank(version);
        version = version.replaceAll("[^0-9.]", "");

        String[] parts = version.split("\\.");
        if (parts.length < REQUIRED_VERSION_PARTS_COUNT || parts.length > MAX_VERSION_PARTS_COUNT) {
            return Optional.empty();
        }

        try {
            long result = 0;

            for (int i = 0; i < REQUIRED_VERSION_PARTS_COUNT; i++) {
                long num = Long.parseLong(parts[i]);
                if (num < 0 || num > 999) {
                    return Optional.empty();
                }
                result += num * (long) Math.pow(1000, 4 - i);
            }

            result += preRank * 1000;

            if (parts.length > REQUIRED_VERSION_PARTS_COUNT) {
                long preNum = Long.parseLong(parts[REQUIRED_VERSION_PARTS_COUNT]);
                if (preNum < 0 || preNum > 999) {
                    return Optional.empty();
                }
                result += preNum;
            }

            return Optional.of(result);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }


    private static long getPreRank(String version) {
        if (version.matches("(\\d{1,3}\\.){2,3}\\d{1,3}")) {
            return RELEASE_PRERANK;
        } else if (version.contains("rc")) {
            return RC_PRERANK;
        } else if (version.contains("SNAPSHOT") || version.contains("dev")) {
            return SNAPSHOT_AND_DEV_PRERANK;
        } else {
            return UNKNOWN_PRERANK;
        }
    }


}
