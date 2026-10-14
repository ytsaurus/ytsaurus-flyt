package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YtStaticTableClientTest {
    private static final String SCHEMA = YtStaticSinkWriterTest.SCHEMA;

    @Test
    void createsStaticScanTableWithAutomaticChunkMergingAndCustomCompression() {
        Map<String, YTreeNode> attributes = YtStaticTableClient.creationAttributes(
                SCHEMA, Map.of("compression_codec", "lz4"));
        assertThat(attributes.get("dynamic").boolValue()).isFalse();
        assertThat(attributes.get("optimize_for").stringValue()).isEqualTo("scan");
        assertThat(attributes.get("chunk_merger_mode").stringValue()).isEqualTo("auto");
        assertThat(attributes.get("compression_codec").stringValue()).isEqualTo("lz4");
        assertThat(attributes).doesNotContainKeys("tablet_cell_bundle", "min_tablet_count");
    }

    @ParameterizedTest
    @ValueSource(strings = {"dynamic", "schema", "sorted", "sorted_by", "unique_keys", "type"})
    void preventsCustomAttributesFromChangingTableKindOrSchema(String name) {
        assertThrows(IllegalArgumentException.class,
                () -> YtStaticTableClient.creationAttributes(SCHEMA, Map.of(name, "%true")));
    }

    @Test
    void acceptsEquivalentLegacyAndTypeV3Schemas() {
        TableSchema expected = YtStaticTableClient.parseSchema(SCHEMA);
        YTreeMapNode actual = attributes("[{name=id;type_v3={type_name=optional;item=int64}}]");
        YtStaticTableClient.validateTable(actual, expected);
    }

    @Test
    void nativeTemporalNullabilityComesFromTypeV3RatherThanLegacyRequired() {
        TableSchema expected = YtStaticTableClient.parseSchema(
                "[{name=ts;type=timestamp;required=%false;"
                        + "type_v3={type_name=optional;item=timestamp}}]");
        assertThat(expected.getColumnSchema(0).isRequired()).isFalse();
        YtStaticTableClient.validateTable(attributes(
                "[{name=ts;type_v3={type_name=optional;item=timestamp}}]"), expected);
        assertThrows(IllegalArgumentException.class, () -> YtStaticTableClient.validateTable(attributes(
                "[{name=ts;type=timestamp;required=%false;type_v3=timestamp}]"), expected));
    }

    @Test
    void rejectsDynamicSortedWrongTypeAndMismatchingColumns() {
        TableSchema expected = YtStaticTableClient.parseSchema(SCHEMA);
        for (String key : new String[]{"dynamic", "sorted"}) {
            YTreeMapNode node = attributes(SCHEMA);
            node.put(key, YTree.booleanNode(true));
            assertThrows(IllegalArgumentException.class, () -> YtStaticTableClient.validateTable(node, expected));
        }
        YTreeMapNode node = attributes(SCHEMA);
        node.put("type", YTree.stringNode("map_node"));
        assertThrows(IllegalArgumentException.class, () -> YtStaticTableClient.validateTable(node, expected));
        assertThrows(IllegalArgumentException.class, () -> YtStaticTableClient.validateTable(
                attributes("[{name=id;type=string}]"), expected));
        assertThrows(IllegalArgumentException.class, () -> YtStaticTableClient.validateTable(
                attributes("[{name=another;type=int64}]"), expected));
    }

    @Test
    void rejectsSortedConfiguredSchemaBeforeAnyClientIsCreated() {
        assertThrows(IllegalArgumentException.class, () -> YtStaticTableClient.parseSchema(
                "[{name=id;type=int64;sort_order=ascending}]"));
    }

    @Test
    void concurrentCreateProtocolValidatesExistingTableAndOpensExactAppendPathWithoutRetries() throws Exception {
        YTsaurusClient sdk = mock(YTsaurusClient.class);
        when(sdk.createNode(any(CreateNode.class))).thenReturn(CompletableFuture.completedFuture(GUID.create()));
        YTreeNode metadata = YTree.builder().beginAttributes()
                .key("type").value("table").key("dynamic").value(false).key("sorted").value(false)
                .key("schema").value(YTreeTextSerializer.deserialize(SCHEMA))
                .endAttributes().entity().build();
        when(sdk.getNode(any(GetNode.class))).thenReturn(CompletableFuture.completedFuture(metadata));
        doReturn(CompletableFuture.completedFuture(mock(TableWriter.class))).when(sdk).writeTable(any(WriteTable.class));
        YtStaticTableClient client = new YtStaticTableClient(YtStaticSinkWriterTest.builder().build(), sdk);
        client.ensureTable().get();
        client.openWriter().get();

        ArgumentCaptor<CreateNode> create = ArgumentCaptor.forClass(CreateNode.class);
        verify(sdk).createNode(create.capture());
        assertThat(create.getValue()).extracting("ignoreExisting", "recursive").containsExactly(true, false);
        ArgumentCaptor<GetNode> get = ArgumentCaptor.forClass(GetNode.class);
        verify(sdk).getNode(get.capture());
        assertThat(get.getValue()).extracting("path").asString().isEqualTo("//tmp/static");
        ArgumentCaptor<WriteTable> write = ArgumentCaptor.forClass(WriteTable.class);
        verify(sdk).writeTable(write.capture());
        assertThat(write.getValue().getNeedRetries()).isFalse();
        assertThat(write.getValue().getPath()).contains("append", "//tmp/static").doesNotContain("//tmp/static/static");
        client.close();
        verify(sdk).close();
    }

    private static YTreeMapNode attributes(String schema) {
        return YTree.mapBuilder().key("type").value("table").key("dynamic").value(false)
                .key("sorted").value(false).key("schema").value(YTreeTextSerializer.deserialize(schema)).buildMap();
    }
}
