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
public class SetStateFactory implements AccumulatorStateFactory<SetState> {

    public static final class SingleSetState implements SetState {

        private Set<String> set = new HashSet<>();

        @Override
        public Set<String> getSet() {
            return set;
        }

        @Override
        public void setSet(Set<String> value) {
            this.set = value;
        }

        @Override
        public long getEstimatedSize() {
            return set.size();
        }
    }

    public static class GroupedSetState implements GroupedAccumulatorState, SetState {

        private final ObjectBigArray<Set<String>> container = new ObjectBigArray<>();
        private long groupId;

        @Override
        public Set<String> getSet() {
            return container.get(groupId);
        }

        @Override
        public void setSet(Set<String> value) {
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
    public SetState createSingleState() {
        return new SingleSetState();
    }


    @Override
    public SetState createGroupedState() {
        return new GroupedSetState();
    }
}
