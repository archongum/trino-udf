package com.github.archongum.trino.udf.scalar;

import java.time.LocalDate;
import java.time.LocalDateTime;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.StandardTypes;
import static com.github.archongum.trino.udf.scalar.DateTimeUtils.OFFSET_MILLIS;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * Date Time Functions.
 *
 * @author Archon  2018/9/20
 * @since
 */
public class DateTimeFunctions {

    @Description("yesterday")
    @ScalarFunction("yesterday")
    @SqlType(StandardTypes.DATE)
    public static long yesterday() {
        return MILLISECONDS.toDays(System.currentTimeMillis()) - 1;
    }

    @Description("first day of month")
    @ScalarFunction("first_day")
    @SqlType(StandardTypes.DATE)
    public static long firstDay(@SqlType(StandardTypes.DATE) long days) {
        return DateTimeUtils.firstDayOfMonth((int) days).toEpochDay();
    }

    @Description("last day of month")
    @ScalarFunction("last_day")
    @SqlType(StandardTypes.DATE)
    public static long lastDay(@SqlType(StandardTypes.DATE) long days) {
        return DateTimeUtils.lastDayOfMonth((int) days).toEpochDay();
    }

    @Description("last second of the date")
    @ScalarFunction("last_second")
    @SqlType("timestamp(3) with time zone")
    public static long lastSecond(ConnectorSession session, @SqlType(StandardTypes.DATE) long days) {
        return DateTimeEncoding.packDateTimeWithZone(DateTimeUtils.toMillis(LocalDateTime.of(DateTimeUtils.toLocalDate((int) days), DateTimeUtils.LAST_SECOND)), session.getTimeZoneKey());
    }

    @Description("yesterday last second")
    @ScalarFunction("yesterday_last_second")
    @SqlType("timestamp(3) with time zone")
    public static long yesterdayLastSecond(ConnectorSession session) {
        return DateTimeEncoding.packDateTimeWithZone(DAYS.toMillis(MILLISECONDS.toDays(System.currentTimeMillis())) - OFFSET_MILLIS - 1, session.getTimeZoneKey());
    }

    @Description("to timestamp")
    @ScalarFunction("to_datetime")
    @SqlType("timestamp(3) with time zone")
    public static long toDatetime(ConnectorSession session, @SqlType(StandardTypes.DATE) long days, @SqlType(StandardTypes.VARCHAR) Slice time) {
        return DateTimeEncoding.packDateTimeWithZone(DateTimeUtils.toMillis(DateTimeUtils.toLocalDateTime(LocalDate.ofEpochDay(days), time.toStringUtf8())), session.getTimeZoneKey());
    }
}
