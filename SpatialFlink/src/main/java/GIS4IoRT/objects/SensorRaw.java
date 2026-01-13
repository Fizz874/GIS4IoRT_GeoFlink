package GIS4IoRT.objects;


public class SensorRaw {
    public String id;
    public double humidity;
    public double lon;
    public double lat;
    public long timestamp;

    public SensorRaw() {}

    public SensorRaw(String id, double humidity, double lon, double lat, long timestamp) {
        this.id = id;
        this.humidity = humidity;
        this.lon = lon;
        this.lat = lat;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Raw{" + id + ", hum=" + humidity + ", loc=[" + lon + "," + lat + "]}";
    }
}