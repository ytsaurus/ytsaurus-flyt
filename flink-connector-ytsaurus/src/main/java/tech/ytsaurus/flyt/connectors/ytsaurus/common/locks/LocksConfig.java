package tech.ytsaurus.flyt.connectors.ytsaurus.common.locks;

import java.io.Serializable;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.ReadableConfig;

@Getter
@RequiredArgsConstructor
public class LocksConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String locksProviderName;

    private final ReadableConfig config;

}
