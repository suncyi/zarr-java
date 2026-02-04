package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.codec.CodecRegistry;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;


public interface Node extends dev.zarr.zarrjava.core.Node {

    static ObjectMapper makeObjectMapper() {
        /*ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        return objectMapper;*/
        ObjectMapper objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .registerSubtypes(CodecRegistry.getNamedTypes())
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .changeDefaultPropertyInclusion(incl -> incl.withValueInclusion(JsonInclude.Include.NON_NULL))
                .build();

        return objectMapper;
    }

    static ObjectWriter makeObjectWriter() {
        return makeObjectMapper().writerWithDefaultPrettyPrinter();
    }

    /**
     * Opens an existing Zarr array or group at a specified storage location.
     *
     * @param storeHandle the storage location of the Zarr array or group
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr array or group cannot be opened
     */
    static Node open(StoreHandle storeHandle) throws IOException, ZarrException {
        ObjectMapper objectMapper = makeObjectMapper();
        ByteBuffer metadataBytes = storeHandle.resolve(ZARR_JSON).readNonNull();
        byte[] metadataBytearray = Utils.toArray(metadataBytes);
        String nodeType = objectMapper.readTree(metadataBytearray)
                .get("node_type")
                .asText();
        switch (nodeType) {
            case ArrayMetadata.NODE_TYPE:
                return new Array(storeHandle,
                        objectMapper.readValue(metadataBytearray, ArrayMetadata.class));
            case GroupMetadata.NODE_TYPE:
                return new Group(storeHandle,
                        objectMapper.readValue(metadataBytearray, GroupMetadata.class));
            default:
                throw new ZarrException("Unsupported node_type '" + nodeType + "' at " + storeHandle);
        }
    }

    static Node open(Path path) throws IOException, ZarrException {
        return open(new StoreHandle(new FilesystemStore(path)));
    }

    static Node open(String path) throws IOException, ZarrException {
        return open(Paths.get(path));
    }
}
