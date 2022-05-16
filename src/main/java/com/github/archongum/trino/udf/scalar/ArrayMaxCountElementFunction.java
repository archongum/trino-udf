package com.github.archongum.trino.udf.scalar;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;


/**
 * @author Archon 2019/8/30
 */
@ScalarFunction("array_max_count_element")
@Description("Get maximum count element of array (null is not counting; if has multiple return one of them)")
public final class ArrayMaxCountElementFunction {
    private ArrayMaxCountElementFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Slice sliceArrayMaxCountElement(
        @TypeParameter("T") Type elementType,
        @SqlType("array(T)") Block block
    ) {
        if (block.getPositionCount() == 0) {
            return null;
        }

        Map<Slice, Long> map = new HashMap<>(16);

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                continue;
            }
            Slice curElement = elementType.getSlice(block, i);
            Long c = map.get(curElement);
            if (c == null) {
                map.put(curElement, 1L);
            } else {
                map.put(curElement, c+1);
            }
        }

        if (map.isEmpty()) {
            return null;
        }

        Optional<Entry<Slice, Long>> max = map.entrySet().stream().max(Entry.comparingByValue());

        if (max.isPresent()) {
            return max.get().getKey();
        }

        return Slices.EMPTY_SLICE;
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long longArrayMaxCountElement(
        @TypeParameter("T") Type elementType,
        @SqlType("array(T)") Block block
    ) {
        if (block.getPositionCount() == 0) {
            return null;
        }

        Map<Long, Long> map = new HashMap<>(16);

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                continue;
            }
            Long curElement = elementType.getLong(block, i);
            Long c = map.get(curElement);
            if (c == null) {
                map.put(curElement, 1L);
            } else {
                map.put(curElement, c+1);
            }
        }

        if (map.isEmpty()) {
            return null;
        }

        Optional<Entry<Long, Long>> max = map.entrySet().stream().max(Entry.comparingByValue());

        if (max.isPresent()) {
            return max.get().getKey();
        }

        return 0L;
    }
}
