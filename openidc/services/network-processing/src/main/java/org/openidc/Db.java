package org.openidc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * JDBC writer for the network-processing outputs. Populates the CSS 3.0
 * `origin`, `event` and `assoc` tables that Phase 1 leaves empty.
 */
public class Db {

    private final Connection conn;

    public Db(String host, String port, String db, String user, String pass) throws SQLException {
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
        this.conn = DriverManager.getConnection(url, user, pass);
        this.conn.setAutoCommit(true);
    }

    private long nextval(String seq) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet r = s.executeQuery("SELECT nextval('" + seq + "')")) {
            r.next();
            return r.getLong(1);
        }
    }

    private static int jdate(double epochSeconds) {
        ZonedDateTime z = Instant.ofEpochSecond((long) epochSeconds).atZone(ZoneOffset.UTC);
        return z.getYear() * 1000 + z.getDayOfYear();
    }

    /** Write one located, screened event: an origin, an event row, and assoc rows. */
    public synchronized void writeEvent(Locator.Origin o, String etype, List<Detection> cluster)
            throws SQLException {
        long orid = nextval("orid_seq");
        long evid = nextval("evid_seq");
        int jd = jdate(o.time);

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO origin (lat, lon, depth, time, orid, evid, jdate, nass, ndef, "
                        + "etype, mb, ms, algorithm, auth) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            ps.setDouble(1, o.lat);
            ps.setDouble(2, o.lon);
            ps.setDouble(3, o.depth);
            ps.setDouble(4, o.time);
            ps.setLong(5, orid);
            ps.setLong(6, evid);
            ps.setInt(7, jd);
            ps.setInt(8, o.nass);
            ps.setInt(9, o.nass);
            ps.setString(10, etype);
            ps.setDouble(11, o.mb);
            ps.setDouble(12, o.ms);
            ps.setString(13, "openidc-assoc");
            ps.setString(14, "OpenIDC-net");
            ps.execute();
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO event (evid, evname, prefor, auth) VALUES (?,?,?,?)")) {
            ps.setLong(1, evid);
            ps.setString(2, "ev" + evid);
            ps.setLong(3, orid);
            ps.setString(4, "OpenIDC-net");
            ps.execute();
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO assoc (arid, orid, sta, phase, timedef, auth) VALUES (?,?,?,?,?,?)")) {
            for (Detection d : cluster) {
                ps.setLong(1, d.arid);
                ps.setLong(2, orid);
                ps.setString(3, d.sta);
                ps.setString(4, "P");
                ps.setString(5, "d");
                ps.setString(6, "OpenIDC-net");
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}
