package model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Message implements Serializable {
    private final List<String> columns;
    private final List data;
    private final Map metadata;

    public Message(List<String> columns, List data, Map metadata) {
        this.columns = columns;
        this.data = data;
        this.metadata = metadata;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List getData() {
        return data;
    }

    public Map getMetadata() {
        return metadata;
    }
}
