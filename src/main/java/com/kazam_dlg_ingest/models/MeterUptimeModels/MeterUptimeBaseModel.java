package com.kazam_dlg_ingest.models.MeterUptimeModels;

import com.kazam_dlg_ingest.models.DlgPacketBaseModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

public class MeterUptimeBaseModel extends DlgPacketBaseModel {
    public Boolean network = true;
    public Long timestamp = null;
    public Long total_time = 0L;
    public Long online_time = 0L;

    private Integer TIME_TO_OFFLINE = 60000;

    public MeterUptimeBaseModel(JsonNode packet) {
        super(packet);

        if (packet == null)
            return;

        try {
            if (packet.has("timestamp") && packet.get("timestamp").isNumber()) {
                this.timestamp = packet.get("timestamp").asLong();
            }

            if (this.prevMeterUptimePkt != null) {
                Long time = (this.timestamp - this.prevMeterUptimePkt.timestamp) * 1000;
                if (time < TIME_TO_OFFLINE) {
                    this.online_time = time;
                }
            }

        } catch (Exception e) {
            System.err.println("UptimeBaseModel error " + e.getMessage());
        }

    }

    @JsonIgnore
    public MeterUptimeBaseModel getData() {
        if (this.timestamp == null)
            return null;
        else
            return this;
    }

}
