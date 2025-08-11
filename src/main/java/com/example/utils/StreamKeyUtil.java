package com.example.utils;

import com.example.utils.TimeBoundaryUtil.TimeBoundary;

public class StreamKeyUtil {
    private TimeBoundary boundaryUnit = TimeBoundary.HOUR;
    private String comb_id = "";
    private String org = "";
    private long timestamp = 0;
    private int utcOffset = 0;
    public TimeBoundaryUtil boundaries = new TimeBoundaryUtil(0);

    private String delimiter = "____";

    public StreamKeyUtil(String comb_id, String org, long timestamp, int offsetSec, TimeBoundary boundary) {
        if (comb_id != null)
            this.comb_id = comb_id;
        if (org != null)
            this.org = org;
        if (timestamp > 0)
            this.timestamp = timestamp;
        if (offsetSec > 0)
            this.utcOffset = offsetSec;
        if (boundary != null)
            this.boundaryUnit = boundary;
    }

    private static String streamKeyBuilder(String comb_id, String org, TimeBoundaryUtil boundaries, String delimiter) {
        String key = comb_id + delimiter + org + delimiter + boundaries.startTime + delimiter + boundaries.endTime;
        return key;
    }

    public String getStreamKey() {
        this.boundaries = new TimeBoundaryUtil(this.timestamp).utcOffset(utcOffset)
                .getBoundaries(this.boundaryUnit);
        // TimeBoundaryUtil BoundaryJson = new
        // TimeBoundaryUtil(this.timestamp).utcOffset(utcOffset);

        // if (this.boundaryUnit == TimeBoundary.HOUR) {
        // this.boundaries = BoundaryJson.getHourlyBoundaries();
        // } else if (this.boundaryUnit == TimeBoundary.DAY) {
        // this.boundaries = BoundaryJson.getDayBoundaries();
        // }

        return streamKeyBuilder(this.comb_id, this.org, this.boundaries, this.delimiter);
    }

    public StreamKeyUtil parseStreamKey(String key) {
        try {
            String[] splitted = key.split(delimiter);

            this.comb_id = splitted[0];
            this.org = splitted[1];

            long startTime = Long.parseLong(splitted[2]);
            long endTime = Long.parseLong(splitted[3]);
            this.boundaries = new TimeBoundaryUtil(0).parseBoundaries(startTime, endTime);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        return this;
    }

    public StreamKeyUtil getData() {
        return this;
    }
}
