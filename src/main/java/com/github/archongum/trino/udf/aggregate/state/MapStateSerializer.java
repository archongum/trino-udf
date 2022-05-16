package com.github.archongum.trino.udf.aggregate.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.HashMap;

import static io.trino.spi.type.VarcharType.VARCHAR;


/**
 * @author Archon  8/30/19
 * @since
 */
public class MapStateSerializer implements AccumulatorStateSerializer<MapState>
{

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Type getSerializedType() {
        return VARCHAR;
    }

    @Override
    public void serialize(MapState state, BlockBuilder out) {
        try {
            String jsonResult = mapper.writeValueAsString(state.getMap());
            VARCHAR.writeSlice(out, Slices.utf8Slice(jsonResult));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(Block block, int index, MapState state) {
        try {
            TypeReference<HashMap<String, Long>> typeRef = new TypeReference<>() {};
            state.setMap(mapper.readValue(VARCHAR.getSlice(block, index).toStringUtf8(), typeRef));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

