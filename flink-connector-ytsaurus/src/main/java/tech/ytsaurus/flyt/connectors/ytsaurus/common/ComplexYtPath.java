package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;

import javax.annotation.Nullable;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_ATTRIBUTE_SYMBOL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_PATH_SEP;

@Data
@Builder
@Accessors(chain = true)
public class ComplexYtPath implements Serializable {

    private String clusterName;

    private String basePath;

    @Nullable
    private String tableName;

    private boolean isPartitioned;

    private boolean enableDynamicStoreRead;

    public String getFullPath() {
        if (tableName == null) {
            throw new IllegalStateException("Table name is not defined for this ComplexYtPath");
        }
        return basePath + YT_PATH_SEP + tableName;
    }

    public String getFullPathWithAttribute(String attribute) {
        return getFullPath() + YT_PATH_SEP + YT_ATTRIBUTE_SYMBOL + attribute;
    }

    public String getBaseTableName() {
        String[] pathParts = basePath.split(YT_PATH_SEP);
        return pathParts[pathParts.length - 1];
    }

    public ComplexYtPath copy() {
        return ComplexYtPath.builder()
                .clusterName(clusterName)
                .basePath(basePath)
                .tableName(tableName)
                .isPartitioned(isPartitioned)
                .enableDynamicStoreRead(enableDynamicStoreRead)
                .build();
    }
}
