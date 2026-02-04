package dev.zarr.zarrjava.v2;


import com.fasterxml.jackson.annotation.JsonInclude;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.codec.CodecRegistry;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface Node extends dev.zarr.zarrjava.core.Node {

    static ObjectMapper makeObjectMapper() {
        /*ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
        return objectMapper;*/
        ObjectMapper objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .registerSubtypes(CodecRegistry.getNamedTypes())
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES) // 添加这一行
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
     * @param storeHandle the storage location of the Zarr array
     * @throws IOException   throws IOException if the metadata cannot be read
     * @throws ZarrException throws ZarrException if the Zarr array or group cannot be opened
     */
    static Node open(StoreHandle storeHandle) throws IOException, ZarrException {
        boolean isGroup = storeHandle.resolve(ZGROUP).exists();
        boolean isArray = storeHandle.resolve(ZARRAY).exists();

        if (isGroup && isArray) {
            throw new ZarrException("Store handle '" + storeHandle + "' contains both a " + ZGROUP + " and a " + ZARRAY + " file.");
        } else if (isGroup) {
            return Group.open(storeHandle);
        } else if (isArray) {
            try {
                return Array.open(storeHandle);
            } catch (IOException e) {
                throw new ZarrException("Failed to read array metadata for store handle '" + storeHandle + "'.", e);
            }
        }
        throw new NoSuchFileException("Store handle '" + storeHandle + "' does not contain a " + ZGROUP + " or a " + ZARRAY + " file.");
    }

    static Node open(Path path) throws IOException, ZarrException {
        return open(new StoreHandle(new FilesystemStore(path)));
    }

    static Node open(String path) throws IOException, ZarrException {
        return open(Paths.get(path));
    }
}
