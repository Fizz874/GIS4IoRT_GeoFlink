package GIS4IoRT.objects;


import GeoFlink.spatialObjects.Polygon;

public class ZoneEvent {
    public enum Type { ADD, DELETE, CLEAR }

    public Type type;
    public String zoneID;
    public String gridID;
    public Polygon polygon;

    public ZoneEvent() {}

    public ZoneEvent(String zoneID, String gridID, Polygon polygon) {
        this.type = Type.ADD;
        this.zoneID = zoneID;
        this.gridID = gridID;
        this.polygon = polygon;
    }

    public ZoneEvent(Type type, String zoneID, String gridID) {
        this.type = type;
        this.zoneID = zoneID;
        this.gridID = gridID;
        this.polygon = null;
    }
}