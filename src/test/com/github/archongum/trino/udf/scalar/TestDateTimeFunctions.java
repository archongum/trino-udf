package com.github.archongum.trino.udf.scalar;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * @author Archon  2018/9/20
 * @since
 */
class TestDateTimeFunctions {

    @Test
    void testLastDay() {
        long millis = System.currentTimeMillis();
        System.out.println(DateTimeFunctions.lastDay(DateTimeUtils.toDays(millis)));
        System.out.println(MILLISECONDS.toDays(millis));
        System.out.println(LocalTime.parse("23:59:59.999"));
        LocalDateTime ts = LocalDateTime.of(LocalDate.now(), LocalTime.parse("23:59:59.999"));
        System.out.println(ts.toEpochSecond(ZoneOffset.UTC)*1000 + ts.getNano() / 1000_000);
    }

    @Test
    void testToDatetime() {
        long millis = System.currentTimeMillis();
        long d1 = DateTimeUtils.toLocalDateTime(millis).toLocalDate().toEpochDay();
        System.out.println(d1);
        System.out.println(millis/86400000);
    }

    @Test
    void testYesterday() {
        System.out.println(DateTimeFunctions.yesterday());
    }
}
