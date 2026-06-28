package org.openidc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A single automatic detection (arrival) consumed from the Kafka `detections`
 * topic — the message the Phase 1 station-processing service publishes for every
 * STA/LTA trigger. Jackson maps the JSON fields onto the public fields below.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Detection {
    public long arid;      // arrival id (primary key in the CSS `arrival` table)
    public long wfid;      // waveform id
    public String sta;     // station code
    public String chan;    // channel code
    public double time;    // epoch seconds of the onset
    public double snr;     // signal-to-noise (STA/LTA peak ratio)
}
