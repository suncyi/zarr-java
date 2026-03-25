package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.utils.JsonUtil;
import dev.zarr.zarrjava.v2.Group;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

/**
 * @author suncy
 * @date 2026/3/25 10:10
 */
@Slf4j
public class EcZarrTest {

    @Test
    public void readMetaTest() throws IOException, ZarrException {
        Group group = Group.open("D:\\tds\\mnt\\mini-s3\\ec-zarr\\EC-20260323_08");
        String varName = "pv";
        ucar.ma2.Array aryVal = readZarrVar(group, varName);
        Map<String, Object> meta = getMeta(group, varName);
        log.info("=== 读取变量 {} 元数据:{},datashape:{} ===", varName, JsonUtil.toJson(meta), JsonUtil.toJson(aryVal.getShape()));
    }


    public static ucar.ma2.Array readZarrVar(Group rootGroup, String varName) throws ZarrException, IOException {
        Array array = (Array) rootGroup.get(varName);
        if (array == null) {
            throw new RuntimeException(String.format("=== 变量 %s 不存在 ===", varName));
        }
        long startTime = System.currentTimeMillis();
        ucar.ma2.Array varArray = array.read();

        log.info("=== 变量 {}读取完毕,耗时:{}ms, shape: {} ===", varName,
                (System.currentTimeMillis() - startTime), JsonUtil.toJson(varArray.getShape()));
        return varArray;
    }

    public static Map<String, Object> getMeta(Group rootGroup, String varName) throws ZarrException, IOException {
        Array array = (Array) rootGroup.get(varName);
        if (array == null) {
            throw new RuntimeException(String.format("=== 变量 %s 不存在 ===", varName));
        }
        return array.metadata().attributes();
    }
}
