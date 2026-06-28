package org.openidc;

import java.util.HashMap;
import java.util.Map;

/**
 * Station registry: maps station codes to geographic coordinates. Seeded with
 * the approximate locations of real IMS primary seismic arrays. New stations can
 * be added at runtime via a RabbitMQ control command ("configure new station").
 */
public class Stations {

    public static final class Coord {
        public final double lat;
        public final double lon;
        public Coord(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    private final Map<String, Coord> map = new HashMap<>();

    public Stations() {
        map.put("ARCES", new Coord(69.53, 25.51));    // Norway
        map.put("FINES", new Coord(61.44, 26.08));    // Finland
        map.put("GERES", new Coord(48.84, 13.70));    // Germany
        map.put("WRA",   new Coord(-19.94, 134.34));  // Australia
    }

    public synchronized void add(String sta, double lat, double lon) {
        map.put(sta, new Coord(lat, lon));
    }

    public synchronized Coord get(String sta) {
        return map.get(sta);
    }

    public synchronized int size() {
        return map.size();
    }
}
