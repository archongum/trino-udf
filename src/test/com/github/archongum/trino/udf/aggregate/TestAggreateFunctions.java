package com.github.archongum.trino.udf.aggregate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;


/**
 * @author Archon  2019/8/29
 * @since
 */
class TestAggreateFunctions {

    @Test
    void testTopN() {
        List<String> list =  Arrays.asList(
            "a", "a", "a",
            "b", "b",
            "c",
            "a", "a"
        );

        Map<String, Long> map = new HashMap<>();

        for (String curElement : list) {
            Long c = map.get(curElement);
            if (c == null) {
                map.put(curElement, 1L);
            } else {
                map.put(curElement, c + 1);
            }
        }

        System.out.println(new HashMap<String, Long>().entrySet().stream().max(Entry.comparingByValue()).get());
    }
}
