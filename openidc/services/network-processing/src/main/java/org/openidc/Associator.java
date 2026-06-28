package org.openidc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Network association: groups detections from multiple stations that occur close
 * together in time into candidate events. This is the OpenIDC analogue of the
 * IDC's Global Association (GA) step.
 *
 * Detections are buffered as they arrive. A cluster is only emitted once its time
 * window has fully closed (window + grace older than "now"), so late-arriving
 * detections still have a chance to join. A cluster becomes an event only if it
 * contains detections from at least `minStations` distinct stations.
 *
 * `minStations` is volatile and updated via RabbitMQ control commands, so an
 * operator can tighten or loosen association sensitivity while the system runs.
 */
public class Associator {

    private final List<Detection> buffer = new ArrayList<>();
    private volatile int minStations;
    private final double windowSeconds;
    private final double graceSeconds;

    public Associator(int minStations, double windowSeconds, double graceSeconds) {
        this.minStations = minStations;
        this.windowSeconds = windowSeconds;
        this.graceSeconds = graceSeconds;
    }

    public void setMinStations(int minStations) {
        this.minStations = minStations;
    }

    public int getMinStations() {
        return minStations;
    }

    public synchronized void add(Detection d) {
        buffer.add(d);
    }

    /**
     * Return clusters whose association window has fully closed at time `now`
     * (epoch seconds). Consumed detections are removed from the buffer.
     */
    public synchronized List<List<Detection>> formEvents(double now) {
        List<List<Detection>> events = new ArrayList<>();
        buffer.sort(Comparator.comparingDouble(d -> d.time));
        Set<Detection> used = new HashSet<>();

        for (Detection anchor : buffer) {
            if (used.contains(anchor)) {
                continue;
            }
            // Buffer is sorted ascending, so once an anchor's window is still
            // open, every later anchor's window is too: stop scanning.
            if (anchor.time + windowSeconds + graceSeconds >= now) {
                break;
            }
            List<Detection> cluster = new ArrayList<>();
            for (Detection d : buffer) {
                if (!used.contains(d) && d.time >= anchor.time && d.time <= anchor.time + windowSeconds) {
                    cluster.add(d);
                }
            }
            long distinct = cluster.stream().map(d -> d.sta).distinct().count();
            if (distinct >= minStations) {
                events.add(cluster);
                used.addAll(cluster);
            } else {
                // Anchor cannot seed an event; drop it so it does not block others.
                used.add(anchor);
            }
        }

        buffer.removeAll(used);
        // Safety valve: discard stragglers far in the past so the buffer is bounded.
        buffer.removeIf(d -> d.time + 600 < now);
        return events;
    }
}
