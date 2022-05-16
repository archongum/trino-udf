package com.github.archongum.trino.udf.aggregate;

import java.util.HashSet;
import java.util.Set;
import com.github.archongum.trino.udf.aggregate.state.SetState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;


/**
 * @author Archon 2021年10月21日
 */
@AggregationFunction("array_agg_distinct")
@Description("Count distinct array elements. input: array(VARCHAR), output: integer.")
public class ArrayAggDistinctAggregation {

    @InputFunction
    public static void input(SetState state, @SqlType("array(VARCHAR)") Block block) {
        if (block.getPositionCount() == 0) {
            return;
        }

        Set<String> set = state.getSet();
        if (set == null) {
            set = new HashSet<>();
            state.setSet(set);
        }

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                continue;
            }
            String curElement = VARCHAR.getSlice(block, i).toStringUtf8();
            set.add(curElement);
        }
    }

    @CombineFunction
    public static void combine(SetState state, SetState otherState) {
        Set<String> prev  = state.getSet();
        Set<String> input = otherState.getSet();
        if (prev == null) {
            state.setSet(input);
        } else {
            if (input != null && !input.isEmpty()) {
                prev.addAll(input);
            }
        }
    }

    @OutputFunction(StandardTypes.INTEGER)
    public static void output(SetState state, BlockBuilder out) {
        Set<String> set = state.getSet();
        if (set == null || set.isEmpty()) {
            out.appendNull();
        } else {
            INTEGER.writeLong(out, set.size());
        }
    }
}
