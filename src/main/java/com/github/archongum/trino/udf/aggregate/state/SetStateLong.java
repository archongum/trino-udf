package com.github.archongum.trino.udf.aggregate.state;

import java.util.Set;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;


/**
 * @author Archon 2021年10月21日
 */
@AccumulatorStateMetadata(stateSerializerClass = SetStateLongSerializer.class, stateFactoryClass = SetStateLongFactory.class)
public interface SetStateLong extends AccumulatorState {

    /**
     * get set
     * @return set
     */
    Set<Long> getSet();

    /**
     * set set
     * @param value set
     */
    void setSet(Set<Long> value);
}
