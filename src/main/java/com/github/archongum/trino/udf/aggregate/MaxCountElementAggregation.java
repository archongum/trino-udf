package com.github.archongum.trino.udf.aggregate;

import com.github.archongum.trino.udf.aggregate.state.MapState;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static io.trino.spi.type.VarcharType.VARCHAR;


/**
 * @author Archon 2019年8月30日
 */
@AggregationFunction("max_count_element")
@Description("Get maximum count element (null is not counting; if has multiple return one of them)")
public class MaxCountElementAggregation {
    @InputFunction
    public static void input(MapState state, @SqlType(StandardTypes.VARCHAR) Slice value) {
        Map<String, Long> map = state.getMap();
        if (map == null) {
            map = new HashMap<>(16);
            state.setMap(map);
        }
        String v = value.toStringUtf8();
        Long cnt = map.get(v);
        if (cnt == null) {
            map.put(v, 1L);
        } else {
            map.put(v, cnt+1);
        }
    }

    @CombineFunction
    public static void combine(MapState state, MapState otherState) {
        if (state.getMap() == null && otherState.getMap() == null) {
            return;
        }
        if (otherState.getMap() == null && state.getMap() != null) {
            otherState.setMap(state.getMap());
            return;
        }
        if (state.getMap() == null && otherState.getMap() != null) {
            state.setMap(otherState.getMap());
            return;
        }

        otherState.getMap().forEach((k, v) -> state.getMap().merge(k, v, Long::sum));
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(MapState state, BlockBuilder out)
    {
        if (state.getMap().isEmpty()) {
            out.appendNull();
        } else {
            VARCHAR.writeSlice(out,
                Slices.utf8Slice(state.getMap().entrySet().stream().max(Entry.comparingByValue()).get().getKey()));
        }
    }
}
