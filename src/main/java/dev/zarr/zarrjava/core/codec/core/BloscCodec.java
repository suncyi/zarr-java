package dev.zarr.zarrjava.core.codec.core;

import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;
import dev.zarr.zarrjava.utils.Utils;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.exc.StreamReadException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class BloscCodec extends BytesBytesCodec {

    @Override
    public ByteBuffer decode(ByteBuffer chunkBytes)
            throws ZarrException {
        try {
            return ByteBuffer.wrap(Blosc.decompress(Utils.toArray(chunkBytes)));
        } catch (Exception ex) {
            throw new ZarrException("Error in decoding blosc.", ex);
        }
    }

    public static final class CustomCompressorDeserializer extends StdDeserializer<Blosc.Compressor> {

        public CustomCompressorDeserializer() {
            this(null);
        }

        public CustomCompressorDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public Blosc.Compressor deserialize(JsonParser jsonParser, DeserializationContext ctxt) {
//            String cname = jsonParser.getCodec().readValue(jsonParser, String.class);
//            Blosc.Compressor compressor = Blosc.Compressor.fromString(cname);
//            if (compressor == null) {
//                throw new JsonParseException(
//                        jsonParser,
//                        String.format("Could not parse the Blosc.Compressor. Got '%s'", cname)
//                );
//            }
//            return compressor;
            String cname = jsonParser.getValueAsString();
            Blosc.Compressor compressor = Blosc.Compressor.fromString(cname);
            if (compressor == null) {
                throw new StreamReadException(
                        jsonParser,
                        String.format("Could not parse the Blosc.Compressor. Got '%s'", cname)
                );
            }
            return compressor;
        }
    }

    public static final class CustomCompressorSerializer extends StdSerializer<Blosc.Compressor> {

        public CustomCompressorSerializer() {
            super(Blosc.Compressor.class);
        }

        public CustomCompressorSerializer(Class t) {
            super(t);
        }

        @Override
        public void serialize(Blosc.Compressor compressor, JsonGenerator generator,
                              SerializationContext provider) {
            generator.writeString(compressor.getValue());
        }
    }
}
