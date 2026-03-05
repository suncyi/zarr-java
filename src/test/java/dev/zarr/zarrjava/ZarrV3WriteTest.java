package dev.zarr.zarrjava;

import java.io.IOException;

import com.scalableminds.bloscjava.Blosc;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;
import lombok.extern.slf4j.Slf4j;

/**
 * @author suncy
 * @date 2026/2/18 22:35
 */
@Slf4j
public class ZarrV3WriteTest {


    // ORIG: 53*1441*2880
   static long[] DATA_SHAPE_SINGLE_LEVEL = {1441, 2880, 53};

    static int[] CHUNK_SHAPE_SINGLE_LEVEL = {100, 200, 32};

    static int[] SHARD_SHAPE_SINGLE_LEVEL = {500, 600, 64};

    public static void main(String[] args) throws ZarrException, IOException {

        String varName = "10_metre_U_wind_component_surface";

        Group rootGroup = createZarrGroup("D:/meter-data/EC20251215.zarr");

        Array array = createArray(rootGroup, varName, DATA_SHAPE_SINGLE_LEVEL, CHUNK_SHAPE_SINGLE_LEVEL, SHARD_SHAPE_SINGLE_LEVEL);

    }


    public static Group createZarrGroup(String rootPath) {
        // Implement Zarr reading logic here
        log.info("=== 打开 Zarr group 文件 {}  ===", rootPath);
        try {
            return Group.create(new FilesystemStore(rootPath).resolve(""));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static Array createArray(Group rootGroup, String arrayName,
                                    long[] dataShape,
                                    int[] chunkShape,
                                    int[] shardShape) throws ZarrException, IOException {
        // 如果 arrayName 存在则终止
        if (rootGroup.get(arrayName) != null) {
            return (Array) rootGroup.get(arrayName);
        }

        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(dataShape)
                .withChunkShape(shardShape)
                .withDataType(DataType.FLOAT32)
                .withFillValue((float) 0.0)
                //  配置压缩，zstd 解压极快，处理补齐（Padding）产生的 NaN效率高；
                .withCodecs(cn -> cn.withSharding(chunkShape)
                        .withBlosc(Blosc.Compressor.ZSTD.getValue(), "byteshuffle", 5)
                );
        Array array = rootGroup.createArray(arrayName, builder.build());
        return array;
    }
}
