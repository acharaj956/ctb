package org.openidc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * OpenIDC network-processing service (Phase 2).
 *
 * Consumes automatic detections from Kafka, associates them across stations into
 * events, locates and screens each event (earthquake vs explosion-like), and
 * writes origin/assoc/event rows to Postgres. A RabbitMQ control listener allows
 * the association parameters and station registry to be reconfigured at runtime.
 */
public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final Pattern STA_RE = Pattern.compile("^[A-Z0-9]{1,6}$");
    private static final Pattern CHAN_RE = Pattern.compile("^[A-Z0-9]{1,8}$");

    private static String env(String name, String dflt) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    private static boolean validDetection(Detection d) {
        return d != null
                && d.arid > 0
                && d.wfid > 0
                && d.sta != null
                && STA_RE.matcher(d.sta).matches()
                && d.chan != null
                && CHAN_RE.matcher(d.chan).matches()
                && Double.isFinite(d.time)
                && d.time > 0
                && Double.isFinite(d.snr)
                && d.snr >= 0.0
                && d.snr <= 1_000_000.0;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:29092");
        String detTopic = env("DET_TOPIC", "detections");
        int minStations = Integer.parseInt(env("MIN_STATIONS", "2"));
        double window = Double.parseDouble(env("WINDOW_SECONDS", "20"));
        double grace = Double.parseDouble(env("GRACE_SECONDS", "10"));
        double screenMb = Double.parseDouble(env("SCREEN_MB", "4.8"));
        double screenDepth = Double.parseDouble(env("SCREEN_DEPTH", "40"));

        Stations stations = new Stations();
        Associator associator = new Associator(minStations, window, grace);
        Locator locator = new Locator(stations);
        Screener screener = new Screener(screenMb, screenDepth);

        Db db = connectDb();

        // Control plane (RabbitMQ) runs in its own thread.
        Thread control = new Thread(
                new ControlListener(env("RABBITMQ_HOST", "rabbitmq"), associator, stations),
                "control");
        control.setDaemon(true);
        control.start();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "network-processing");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(detTopic));
            log.info("network-processing consuming '{}' (minStations={}, window={}s)",
                    detTopic, minStations, window);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        Detection detection = mapper.readValue(rec.value(), Detection.class);
                        if (!validDetection(detection)) {
                            log.warn("skipping invalid detection: {}", rec.value());
                            continue;
                        }
                        associator.add(detection);
                    } catch (Exception e) {
                        log.warn("skipping malformed detection: {}", e.getMessage());
                    }
                }

                double now = System.currentTimeMillis() / 1000.0;
                for (List<Detection> cluster : associator.formEvents(now)) {
                    Locator.Origin o = locator.locate(cluster);
                    String etype = screener.screen(o);
                    db.writeEvent(o, etype, cluster);
                    log.info("EVENT formed: nass={} lat={} lon={} depth={}km mb={} -> screening={}",
                            cluster.size(),
                            String.format("%.2f", o.lat),
                            String.format("%.2f", o.lon),
                            String.format("%.1f", o.depth),
                            String.format("%.2f", o.mb),
                            etype.equals("ex") ? "EXPLOSION-LIKE" : "earthquake");
                }
            }
        }
    }

    private static Db connectDb() throws Exception {
        String host = env("PGHOST", "postgres");
        String port = env("PGPORT", "5432");
        String name = env("PGDATABASE", "openidc");
        String user = env("PGUSER", "idc");
        String pass = env("PGPASSWORD", "idc");
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                Db db = new Db(host, port, name, user, pass);
                log.info("connected to Postgres");
                return db;
            } catch (Exception e) {
                log.warn("Postgres not ready ({}/30): {}", attempt, e.getMessage());
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("could not connect to Postgres");
    }
}
