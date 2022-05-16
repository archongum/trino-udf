package com.github.archongum.trino.udf.aggregate.state;

import java.util.HashSet;
import java.util.Set;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;


/**
 * @author Archon  8/30/19
 * @since
 */
public class SetStateLongFactory implements AccumulatorStateFactory<SetStateLong> {

    public static final class SingleSetState implements SetStateLong {

        private Set<Long> set = new HashSet<>();

        @Override
        public Set<Long> getSet() {
            return set;
        }

        @Override
        public void setSet(Set<Long> value) {
            this.set = value;
        }

        @Override
        public long getEstimatedSize() {
            return set.size();
        }
    }

    public static class GroupedSetState implements GroupedAccumulatorState, SetStateLong {

        private final ObjectBigArray<Set<Long>> container = new ObjectBigArray<>();
        private long groupId;

        @Override
        public Set<Long> getSet() {
            return container.get(groupId);
        }

        @Override
        public void setSet(Set<Long> value) {
            container.set(groupId, value);
        }

        @Override
        public void setGroupId(long groupId) {
            this.groupId = groupId;
            if (this.getSet() == null) {
                this.setSet(new HashSet<>());
            }
        }

        @Override
        public void ensureCapacity(long size) {
            container.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize() {
            return container.sizeOf();
        }
    }

    @Override
    public SetStateLong createSingleState() {
        return new SingleSetState();
    }


    @Override
    public SetStateLong createGroupedState() {
        return new GroupedSetState();
    }
}
