package com.github.archongum.trino.udf.scalar;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Calendar;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * @author Archon  2018/9/20
 * @since
 */
public class DateTimeUtils {
    public static final int OFFSET_MILLIS = Calendar.getInstance().getTimeZone().getRawOffset();

    public static final int OFFSET_SECOND = OFFSET_MILLIS/1000;

    public static final ZoneOffset ZONE_OFFSET = ZoneOffset.ofTotalSeconds(OFFSET_SECOND);

    public static final LocalTime LAST_SECOND = LocalTime.of(23, 59, 59, 999999999);

    // ----------------------------- base ------------------------ //
    public static long toMillis(LocalDateTime dateTime) {
        return dateTime.toEpochSecond(ZONE_OFFSET) * 1000 + dateTime.getNano() / 1000_000;
    }
    public static long toMillis(LocalDate date) {
        return date.toEpochDay() * 86400000;
    }
    public static long toSeconds(long millis) {
        return MILLISECONDS.toSeconds(millis);
    }
    public static long toMinutes(long millis) {
        return MILLISECONDS.toMinutes(millis);
    }
    public static long toHours(long millis) {
        return MILLISECONDS.toHours(millis);
    }
    public static int toDays(long millis) {
        return (int) MILLISECONDS.toDays(millis);
    }

    // base
    public static LocalDateTime toLocalDateTime(long millis) {
        return LocalDateTime.ofEpochSecond(toSeconds(millis), 0, ZONE_OFFSET);
    }

    public static LocalDateTime toLocalDateTime(LocalDate date, String time) {
        return LocalDateTime.of(date, LocalTime.parse(time));
    }

    public static LocalDate toLocalDate(int days) {
        return LocalDate.ofEpochDay(days);
    }

    public static LocalDate toLocalDate(long millis) {
        return toLocalDate(toDays(millis));
    }


    // --------------------- first day of month ---------------------------//
    public static LocalDate firstDayOfMonth(LocalDate date) {
        return date.withDayOfMonth(1);
    }

    public static LocalDate firstDayOfMonth(LocalDateTime dateTime) {
        return firstDayOfMonth(dateTime.toLocalDate());
    }

    public static LocalDate firstDayOfMonth(long millis) {
        return firstDayOfMonth(toLocalDate(millis));
    }

    public static LocalDate firstDayOfMonth(int days) {
        return firstDayOfMonth(toLocalDate(days));
    }

    // --------------------- last day of month ---------------------------//
    public static LocalDate lastDayOfMonth(LocalDate date) {
        int month = date.getMonthValue();
        if (month < 12) {
            return LocalDate.of(date.getYear(), month+1, 1).minusDays(1);
        } else {
            return LocalDate.of(date.getYear(), month, 31);
        }
    }

    public static LocalDate lastDayOfMonth(LocalDateTime dateTime) {
        return lastDayOfMonth(dateTime.toLocalDate());
    }

    public static LocalDate lastDayOfMonth(long millis) {
        return lastDayOfMonth(toLocalDate(millis));
    }

    public static LocalDate lastDayOfMonth(int days) {
        return lastDayOfMonth(toLocalDate(days));
    }

}
