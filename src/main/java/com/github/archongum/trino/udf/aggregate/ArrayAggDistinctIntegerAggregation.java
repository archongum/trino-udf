package com.github.archongum.trino.udf.aggregate;

import java.util.HashSet;
import java.util.Set;
import com.github.archongum.trino.udf.aggregate.state.SetStateLong;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;


/**
 * @author Archon 2021年10月21日
 */
@AggregationFunction("array_agg_distinct_integer")
@Description("Count distinct array elements. input: array(BIGINT), output: integer.")
public class ArrayAggDistinctIntegerAggregation {

    @InputFunction
    public static void input(SetStateLong state, @SqlType("array(BIGINT)") Block block) {
        if (block.getPositionCount() == 0) {
            return;
        }

        Set<Long> set = state.getSet();
        if (set == null) {
            set = new HashSet<>();
            state.setSet(set);
        }

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                continue;
            }
            Long curElement = BIGINT.getLong(block, i);
            set.add(curElement);
        }
    }

    @CombineFunction
    public static void combine(SetStateLong state, SetStateLong otherState) {
        Set<Long> prev  = state.getSet();
        Set<Long> input = otherState.getSet();
        if (prev == null) {
            state.setSet(input);
        } else {
            if (input != null && !input.isEmpty()) {
                prev.addAll(input);
            }
        }
    }

    @OutputFunction(StandardTypes.INTEGER)
    public static void output(SetStateLong state, BlockBuilder out) {
        Set<Long> set = state.getSet();
        if (set == null || set.isEmpty()) {
            out.appendNull();
        } else {
            INTEGER.writeLong(out, set.size());
        }
    }
}
