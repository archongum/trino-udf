package com.github.archongum.trino.udf.scalar;

import java.util.Random;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;


/**
 * Random with string type seed
 *
 * @author Archon  2018/9/20
 * @since
 */
public class CommonFunctions {

    @Description("rand(String seed)")
    @ScalarFunction("rand")
    @SqlType(StandardTypes.DOUBLE)
    public static double randomWithSeed(@SqlType(StandardTypes.VARCHAR) Slice seed) {
        return new Random(seed.toStringUtf8().hashCode()).nextDouble();
    }
}
