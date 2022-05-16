package com.github.archongum.trino.udf.aggregate.state;

import java.util.HashMap;
import java.util.Map;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;


/**
 * @author Archon  8/30/19
 * @since
 */
public class MapStateFactory implements AccumulatorStateFactory<MapState> {

    public static final class SingleMapState implements MapState {

        private Map<String, Long> map = new HashMap<>();

        @Override
        public Map<String, Long> getMap() {
            return map;
        }

        @Override
        public void setMap(Map<String, Long> value) {
            this.map = value;
        }

        @Override
        public long getEstimatedSize() {
            return map.size();
        }
    }

    public static class GroupedMapState implements GroupedAccumulatorState, MapState {

        private final ObjectBigArray<Map<String, Long>> maps = new ObjectBigArray<>();
        private long groupId;

        @Override
        public Map<String, Long> getMap() {
            return maps.get(groupId);
        }

        @Override
        public void setMap(Map<String, Long> value) {
            maps.set(groupId, value);
        }

        @Override
        public void setGroupId(long groupId) {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size) {
            maps.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize() {
            return maps.sizeOf();
        }
    }

    @Override
    public MapState createSingleState() {
        return new SingleMapState();
    }

    @Override
    public MapState createGroupedState() {
        return new GroupedMapState();
    }

}
