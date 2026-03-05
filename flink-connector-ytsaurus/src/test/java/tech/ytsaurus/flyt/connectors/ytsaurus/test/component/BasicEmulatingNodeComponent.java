package tech.ytsaurus.flyt.connectors.ytsaurus.test.component;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.platform.commons.util.StringUtils;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_TABLET_STATE_ATTRIBUTE_NAME;

public class BasicEmulatingNodeComponent implements NodeComponent {

    protected enum NodeType {
        DIR, TABLE, VALUE
    }

    @Data
    @AllArgsConstructor
    protected static class Node {
        String name;
        Map<String, Node> children;
        NodeType nodeType;
        Map<String, Node> attributes = new HashMap<>();
        YTreeNode value;

        public Node(String name, Map<String, Node> children, NodeType nodeType) {
            this.name = name;
            this.children = children;
            this.nodeType = nodeType;
        }

        public static Node value(String name, YTreeNode value) {
            return new Node(name, Map.of(), NodeType.VALUE, new HashMap<>(), value);
        }
    }

    private final Node root = new Node("/", new HashMap<>(), NodeType.DIR);

    @Override
    public CompletableFuture<Boolean> existsNode(String path) {
        return CompletableFuture.supplyAsync(() -> get(path).isPresent());
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return CompletableFuture.supplyAsync(() ->
                get(req.toBuilder().getPath().toStableString())
                        .map(Node::getValue)
                        .orElse(YTree.entityNode()));
    }

    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        Node current = root;
        String path = req.toBuilder().getPath().toStableString();
        String[] parts = Arrays.stream(path.split("/")).filter(StringUtils::isNotBlank).toArray(String[]::new);
        int index = 0;
        for (String s : parts) {
            if (s.isEmpty()) {
                continue;
            }
            if (current.getChildren().containsKey(s)) {
                current = current.getChildren().get(s);
            } else {
                NodeType type;
                if (index != parts.length - 1) {
                    type = NodeType.DIR;
                } else {
                    if (req.toBuilder().getType() == CypressNodeType.TABLE) {
                        type = NodeType.TABLE;
                    } else {
                        type = NodeType.VALUE;
                    }
                }

                Node child = new Node(s, new HashMap<>(), type);
                if (type == NodeType.TABLE) {
                    fillTable(child);
                }

                current.getChildren().put(s, child);
                current = child;
            }
            index++;
        }
        return CompletableFuture.completedFuture(GUID.create());
    }

    private void fillTable(Node node) {
        node.getAttributes().put(YT_TABLET_STATE_ATTRIBUTE_NAME,
                Node.value(YT_TABLET_STATE_ATTRIBUTE_NAME, YTree.stringNode("unmounted")));
    }

    public CompletableFuture<Void> mountTable(String path) {
        Optional<Node> nodeOptional = get(path);
        if (nodeOptional.isEmpty()) {
            throw new IllegalStateException("Mount on unknown node '" + path + "'");
        }
        Node node = nodeOptional.get();
        if (node.nodeType != NodeType.TABLE) {
            throw new IllegalStateException("Cannot mount non-table at '" + path + "'");
        }
        setNode(path + "/@" + YT_TABLET_STATE_ATTRIBUTE_NAME, YTree.stringNode("mounted"));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> mountTable(String path,
                                              GUID cellId,
                                              boolean freeze,
                                              boolean waitMounted,
                                              @Nullable Duration requestTimeout) {
        return mountTable(path);
    }

    @Override
    public CompletableFuture<Void> mountTableAndWaitTablets(MountTable req) {
        return mountTable(req.getPath());
    }

    @Override
    public CompletableFuture<Void> setNode(String path, YTreeNode data) {
        Optional<Node> nodeOptional = get(path, true);
        if (nodeOptional.isEmpty()) {
            throw new IllegalStateException("Set on unknown node '" + path + "'");
        }
        Node node = nodeOptional.get();
        if (node.nodeType != NodeType.VALUE) {
            throw new IllegalStateException("Cannot set non-value at '" + path + "'");
        }
        node.setValue(data);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(String path) {
        return CompletableFuture.completedFuture(get(path).map(Node::getValue).orElse(YTree.entityNode()));
    }

    private Optional<Node> get(String path, boolean createAttributes) {
        Node current = root;
        String[] originalParts = Arrays.stream(path.split("/"))
                .filter(StringUtils::isNotBlank)
                .toArray(String[]::new);
        for (String s : originalParts) {
            if (s.isEmpty()) {
                continue;
            }
            if (s.contains("@")) {
                String[] parts = s.split("@", 2);
                String key = parts[0];
                String attribute = parts[1];
                Node pass = current;
                if (!key.isEmpty()) {
                    pass = current.getChildren().get(key);
                }
                if (pass == null) {
                    return Optional.empty();
                }
                if (createAttributes && !pass.attributes.containsKey(attribute)) {
                    pass.attributes.put(attribute, Node.value(attribute, null));
                }
                current = pass.attributes.get(attribute);
                continue;
            }
            if (!current.children.containsKey(s)) {
                return Optional.empty();
            }
            current = current.getChildren().get(s);
        }
        return Optional.ofNullable(current);
    }

    private Optional<Node> get(String path) {
        return get(path, false);
    }
}
