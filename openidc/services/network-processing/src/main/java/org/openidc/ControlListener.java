package org.openidc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RabbitMQ control-plane listener.
 *
 * Separating the low-volume control plane (RabbitMQ) from the high-volume data
 * plane (Kafka) is the distinction the vacancy calls out. Operators publish JSON
 * commands to the `control.network` queue to reconfigure the running pipeline:
 *
 *   {"cmd":"set_min_stations","value":3}
 *   {"cmd":"add_station","sta":"NEWS","lat":12.3,"lon":45.6}
 *
 * The "add_station" command is the OpenIDC analogue of the duty "support the
 * connection configuration and testing of new stations".
 */
public class ControlListener implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ControlListener.class);
    private static final String QUEUE = "control.network";
    private static final Pattern STA_RE = Pattern.compile("^[A-Z0-9]{1,6}$");

    private final String host;
    private final Associator associator;
    private final Stations stations;
    private final ObjectMapper mapper = new ObjectMapper();

    public ControlListener(String host, Associator associator, Stations stations) {
        this.host = host;
        this.associator = associator;
        this.stations = stations;
    }

    @Override
    public void run() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        Connection connection = null;
        for (int attempt = 1; attempt <= 30 && connection == null; attempt++) {
            try {
                connection = factory.newConnection();
            } catch (Exception e) {
                log.warn("RabbitMQ not ready ({}/30): {}", attempt, e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        if (connection == null) {
            log.error("could not connect to RabbitMQ; control plane disabled");
            return;
        }

        try {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE, false, false, false, null);
            log.info("control plane ready; consuming '{}'", QUEUE);
            DeliverCallback cb = (tag, delivery) ->
                    handle(new String(delivery.getBody(), StandardCharsets.UTF_8));
            channel.basicConsume(QUEUE, true, cb, tag -> { });
        } catch (Exception e) {
            log.error("control listener failed: {}", e.getMessage());
        }
    }

    private void handle(String body) {
        try {
            JsonNode n = mapper.readTree(body);
            String cmd = n.path("cmd").asText("");
            switch (cmd) {
                case "set_min_stations" -> {
                    JsonNode value = n.get("value");
                    if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
                        log.warn("CONTROL invalid min-stations value: {}", body);
                        return;
                    }
                    int v = value.asInt();
                    if (v < 1 || v > 10) {
                        log.warn("CONTROL min-stations out of range: {}", v);
                        return;
                    }
                    associator.setMinStations(v);
                    log.info("CONTROL set_min_stations -> {}", v);
                }
                case "add_station" -> {
                    String sta = n.path("sta").asText();
                    JsonNode latNode = n.get("lat");
                    JsonNode lonNode = n.get("lon");
                    if (!STA_RE.matcher(sta).matches()) {
                        log.warn("CONTROL invalid station code: {}", sta);
                        return;
                    }
                    if (latNode == null || lonNode == null || !latNode.isNumber() || !lonNode.isNumber()) {
                        log.warn("CONTROL invalid station coordinates: {}", body);
                        return;
                    }
                    double lat = latNode.asDouble();
                    double lon = lonNode.asDouble();
                    if (!Double.isFinite(lat) || lat < -90.0 || lat > 90.0
                            || !Double.isFinite(lon) || lon < -180.0 || lon > 180.0) {
                        log.warn("CONTROL station coordinates out of range: {}", body);
                        return;
                    }
                    stations.add(sta, lat, lon);
                    log.info("CONTROL add_station -> {} ({}, {})", sta, lat, lon);
                }
                default -> log.warn("CONTROL unknown command: {}", body);
            }
        } catch (Exception e) {
            log.warn("CONTROL bad message '{}': {}", body, e.getMessage());
        }
    }
}
