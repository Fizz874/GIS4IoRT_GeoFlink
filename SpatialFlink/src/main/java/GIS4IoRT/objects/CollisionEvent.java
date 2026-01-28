package GIS4IoRT.objects;

import java.io.Serializable;

public class CollisionEvent implements Serializable {
    public String pairKey;
    public String r1;
    public String r2;
    public double dist;
    public long alertTimestamp;
    public long t1;
    public long t2;

    public double lat1;
    public double lon1;
    public double lat2;
    public double lon2;

    public CollisionEvent() {}

    public CollisionEvent(String pairKey, String r1, String r2, double dist, long alertTimestamp,
                          long t1, long t2, double lat1, double lon1, double lat2, double lon2) {
        this.pairKey = pairKey;
        this.r1 = r1;
        this.r2 = r2;
        this.dist = dist;
        this.alertTimestamp = alertTimestamp;
        this.t1 = t1;
        this.t2 = t2;
        this.lat1 = lat1;
        this.lon1 = lon1;
        this.lat2 = lat2;
        this.lon2 = lon2;
    }

    @Override
    public String toString() {
        long skew = Math.abs(t1 - t2);
        return String.format("Collision: %s (Dist: %.2fm) [Skew: %dms, R1: %.6f,%.6f, R2: %.6f,%.6f]",
                pairKey, dist, skew, lat1, lon1, lat2, lon2);
    }
}