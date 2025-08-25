package com.example.topology;

import com.example.models.DlgPacketBaseModel;

public class AggregationBaseTopology {
    public String comb_id;
    public String meter_id;
    public String dlg_id;
    public String zone_id;
    public String org;
    public String app_id;
    public long from;
    public long to;

    public AggregationBaseTopology(DlgPacketBaseModel packet) {

        if (packet == null)
            return;

        this.comb_id = packet.comb_id;
        this.meter_id = packet.meter_id;
        this.dlg_id = packet.dlg_id;
        this.zone_id = packet.zone_id;
        this.org = packet.org;
        this.app_id = packet.app_id;
    }

}
