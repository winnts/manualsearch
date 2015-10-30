import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by adyachenko on 21.10.15.
 */
public class NodeToES implements AutoCloseable {
    public static Node connectASNode () {
        Node node = nodeBuilder()
                .settings(ImmutableSettings.settingsBuilder()
                        .put("http.enabled", false)
                        .loadFromSource("config/elasticsearch.yml"))
                .client(true)
                .node();
        return node;
    }
    public void close() throws IOException {
        connectASNode().close();
    }
}
