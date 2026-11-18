package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;

/** Small test boundary: the lifecycle writer never needs to emulate SDK/network internals. */
interface StaticTableClient extends AutoCloseable {
    CompletableFuture<Void> ensureTable();
    CompletableFuture<TableWriter<YTreeMapNode>> openWriter();
    @Override
    void close();
}

final class YtStaticTableClient implements StaticTableClient {
    private static final Set<String> PROTECTED_ATTRIBUTES = Set.of(
            "dynamic", "schema", "sorted", "sorted_by", "unique_keys", "type");
    private final YTsaurusClient client;
    private final YtStaticSink<?> config;
    private final TableSchema schema;

    YtStaticTableClient(YtStaticSink<?> config) {
        this(config, createClient(config));
    }

    YtStaticTableClient(YtStaticSink<?> config, YTsaurusClient client) {
        this.config = config;
        schema = parseSchema(config.ysonSchema);
        this.client = client;
    }

    private static YTsaurusClient createClient(YtStaticSink<?> config) {
        OAuthCredentialsConfig credentials = config.credentialsProvider.getCredentials(config.proxy);
        // A streaming RPC clears the ordinary request deadline. Configure its inactivity timers
        // explicitly; the writer also bounds each open/readiness/close wait independently.
        YTsaurusClientConfig clientConfig = YtUtils.makeYtClientConfig(config.useTls);
        clientConfig.getRpcOptions()
                .setGlobalTimeout(config.writeTimeout)
                .setStreamingReadTimeout(config.writeTimeout)
                .setStreamingWriteTimeout(config.writeTimeout);
        YTsaurusClient.ClientBuilder<? extends YTsaurusClient, ?> clientBuilder =
                YtUtils.makeYtClientBuilder(config.proxy, credentials, config.useTls);
        if (config.proxyRole != null) {
            clientBuilder.setProxyRole(config.proxyRole);
        }
        return clientBuilder.setConfig(clientConfig).build();
    }

    @Override
    public CompletableFuture<Void> ensureTable() {
        Map<String, YTreeNode> attributes = creationAttributes(config.ysonSchema, config.customAttributes);
        // Every subtask can race here. ignoreExisting does not change an existing table.
        return client.createNode(CreateNode.builder().setPath(YPath.simple(config.path)).setType(CypressNodeType.TABLE)
                        .setIgnoreExisting(true).setAttributes(attributes).build())
                .thenCompose(id -> client.getNode(GetNode.builder().setPath(YPath.simple(config.path))
                        .setAttributes(java.util.List.of("type", "dynamic", "sorted", "schema")).build()))
                .thenAccept(actual -> validateTable(YTree.builder().value(actual.getAttributes()).build().mapNode(), schema));
    }

    @Override
    public CompletableFuture<TableWriter<YTreeMapNode>> openWriter() {
        return client.writeTable(WriteTable.builder(YTreeMapNode.class)
                .setPath(YPath.simple(config.path).append(true))
                .setNeedRetries(false)
                .build());
    }

    @Override
    public void close() { client.close(); }

    static TableSchema parseSchema(String ysonSchema) {
        YTreeNode node = YTreeTextSerializer.deserialize(ysonSchema);
        if (!node.isListNode()) {
            throw new IllegalArgumentException("schema must be a YSON list");
        }
        TableSchema schema = TableSchema.fromYTree(node);
        if (schema.getKeyColumnsCount() != 0) {
            throw new IllegalArgumentException("Static YT sink requires an unsorted schema (no sort_order)");
        }
        return schema;
    }

    static void validateCustomAttributes(Map<String, String> attributes) {
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            if (PROTECTED_ATTRIBUTES.contains(entry.getKey())) {
                throw new IllegalArgumentException("Static sink controls attribute: " + entry.getKey());
            }
            YTreeTextSerializer.deserialize(entry.getValue());
        }
    }

    static Map<String, YTreeNode> creationAttributes(String schema, Map<String, String> custom) {
        validateCustomAttributes(custom);
        Map<String, YTreeNode> attributes = new HashMap<>();
        attributes.put("dynamic", YTree.booleanNode(false));
        attributes.put("schema", parseSchema(schema).toYTree());
        attributes.put("optimize_for", YTree.stringNode("scan"));
        attributes.put("chunk_merger_mode", YTree.stringNode("auto"));
        custom.forEach((key, value) -> attributes.put(key, YTreeTextSerializer.deserialize(value)));
        return attributes;
    }

    static void validateTable(YTreeMapNode attributes, TableSchema expected) {
        if (!"table".equals(attributes.getString("type"))) {
            throw new IllegalArgumentException("Static sink destination must be a table");
        }
        if (attributes.getBool("dynamic") || attributes.getBool("sorted")) {
            throw new IllegalArgumentException("Static sink destination must be an unsorted static table");
        }
        TableSchema actual = TableSchema.fromYTree(attributes.getOrThrow("schema"));
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("Existing YT table schema differs from the configured schema");
        }
    }
}
