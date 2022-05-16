package com.github.archongum.trino.udf.aggregate.state;

import java.util.Map;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;


/**
 * @author Archon 2019年8月30日
 */
@AccumulatorStateMetadata(stateSerializerClass = MapStateSerializer.class, stateFactoryClass = MapStateFactory.class)
public interface MapState extends AccumulatorState {

    /**
     * get map
     * @return map
     */
    Map<String, Long> getMap();

    /**
     * set map
     * @param value map
     */
    void setMap(Map<String, Long> value);
}
