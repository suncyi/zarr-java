package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;


import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

final class CoordinateSystemRefSerde {
    private CoordinateSystemRefSerde() {
    }

    static final class Deserializer extends StdDeserializer<String> {
        Deserializer() {
            super(String.class);
        }

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) {
            JsonToken token = p.currentToken();
            if (token == JsonToken.VALUE_STRING) {
                return p.getValueAsString();
            }
            if (token == JsonToken.START_OBJECT) {
                JsonNode node = p.readValueAsTree();
                JsonNode pathNode = node.get("path");
                JsonNode nameNode = node.get("name");
                String path = pathNode != null && pathNode.isTextual() ? pathNode.asText() : null;
                String name = nameNode != null && nameNode.isTextual() ? nameNode.asText() : null;
                if (name == null) {
                    return path;
                }
                String canonicalPath = (path == null || path.isEmpty()) ? "." : path;
                return canonicalPath + "#" + name;
            }
            return (String) ctxt.handleUnexpectedToken(String.class, p);
        }
    }
}
