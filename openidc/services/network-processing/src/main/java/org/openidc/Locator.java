package org.openidc;

import java.util.List;

/**
 * Coarse event location and magnitude estimation.
 *
 * NOTE: this is a deliberately simplified locator. The real IDC uses
 * travel-time inversion across the network to solve for latitude, longitude,
 * depth and origin time. Here we estimate location as the centroid of the
 * detecting stations and magnitude from detection SNR. These estimates are good
 * enough to demonstrate the event-formation → screening flow; the screening
 * *logic* (see Screener) is the part that mirrors real CTBT verification.
 */
public class Locator {

    private final Stations stations;

    public Locator(Stations stations) {
        this.stations = stations;
    }

    public static final class Origin {
        public double lat;
        public double lon;
        public double depth;   // km (estimated)
        public double time;    // epoch seconds (estimated origin time)
        public double mb;      // body-wave magnitude proxy (from SNR)
        public double ms;      // surface-wave magnitude (not measured here -> -999)
        public int nass;       // number of associated detections
    }

    public Origin locate(List<Detection> cluster) {
        Origin o = new Origin();
        double sumLat = 0, sumLon = 0;
        int n = 0;
        double minTime = Double.MAX_VALUE, maxTime = -Double.MAX_VALUE, maxSnr = 1.0;

        for (Detection d : cluster) {
            Stations.Coord c = stations.get(d.sta);
            if (c != null) {
                sumLat += c.lat;
                sumLon += c.lon;
                n++;
            }
            minTime = Math.min(minTime, d.time);
            maxTime = Math.max(maxTime, d.time);
            maxSnr = Math.max(maxSnr, d.snr);
        }

        o.lat = n > 0 ? sumLat / n : 0.0;
        o.lon = n > 0 ? sumLon / n : 0.0;
        o.time = minTime - 30.0;                       // nominal P travel-time offset
        o.depth = Math.min(60.0, maxTime - minTime);   // crude: arrival-time spread as a depth proxy
        o.mb = 3.5 + Math.log10(maxSnr);               // body-wave magnitude proxy
        o.ms = -999.0;                                 // CSS "not measured" sentinel
        o.nass = cluster.size();
        return o;
    }
}
