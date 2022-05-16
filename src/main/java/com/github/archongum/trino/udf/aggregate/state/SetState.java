package com.github.archongum.trino.udf.aggregate.state;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

import java.util.Set;


/**
 * @author Archon 2021年10月21日
 */
@AccumulatorStateMetadata(stateSerializerClass = SetStateSerializer.class, stateFactoryClass = SetStateFactory.class)
public interface SetState extends AccumulatorState {

    /**
     * get set
     * @return set
     */
    Set<String> getSet();

    /**
     * set set
     * @param value set
     */
    void setSet(Set<String> value);
}
