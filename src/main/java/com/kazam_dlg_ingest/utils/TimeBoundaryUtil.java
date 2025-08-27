package com.kazam_dlg_ingest.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class TimeBoundaryUtil {
    public enum TimeBoundary {
        UNKNOWN,
        HOUR,
        DAY
    }

    private long epochMillis = 0;
    private int utcOffset = 0;
    public long startTime = 0;
    public long endTime = 0;

    public TimeBoundaryUtil(long epoch) {
        if (epoch == 0)
            return;

        epochMillis = epoch * 1000;
    }

    // public DateUtil (ZonedDateTime starTime, ZonedDateTime endTime) {
    // this.startTime = starTime;
    // this.endTime = endTime;
    // }

    private static long getHourStart(long epochMillis, int offsetSeconds) {
        return ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.ofTotalSeconds(offsetSeconds))
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toInstant().toEpochMilli();
    }

    private static long getHourEnd(long epochMillis, int offsetSeconds) {
        return ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.ofTotalSeconds(offsetSeconds))
                .plusHours(1)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toInstant().toEpochMilli();
    }

    public TimeBoundaryUtil getHourlyBoundaries() {
        this.startTime = getHourStart(this.epochMillis, this.utcOffset);
        this.endTime = getHourEnd(this.epochMillis, this.utcOffset);
        return this;
    }

    private static long getDayStart(long epochMillis, int offsetSeconds) {
        return ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.ofTotalSeconds(offsetSeconds))
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toInstant().toEpochMilli();
    }

    private static long getDayEnd(long epochMillis, int offsetSeconds) {
        return ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.ofTotalSeconds(offsetSeconds))
                .plusDays(1)
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toInstant().toEpochMilli();
    }

    public TimeBoundaryUtil utcOffset(int offsetSec) {
        this.utcOffset = offsetSec;
        return this;
    }

    public TimeBoundaryUtil getDayBoundaries() {
        this.startTime = getDayStart(this.epochMillis, this.utcOffset);
        this.endTime = getDayEnd(this.epochMillis, this.utcOffset);
        return this;
    }

    public TimeBoundaryUtil parseBoundaries(long startTime, long endTIme) {
        this.startTime = startTime;
        this.endTime = endTIme;
        return this;
    }

    public TimeBoundaryUtil getBoundaries(TimeBoundary boundaryUnit) {
        if (boundaryUnit == TimeBoundary.HOUR)
            return this.getHourlyBoundaries();

        return this.getDayBoundaries();
    }

}
