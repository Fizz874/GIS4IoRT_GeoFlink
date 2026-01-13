package GIS4IoRT.objects;


public class SensorConfig {
    public String id;
    public double radius;
    public double threshold;
    public String command;

    public SensorConfig() {}

    public SensorConfig(String id, double radius, double threshold, String command) {
        this.id = id;
        this.radius = radius;
        this.threshold = threshold;
        this.command = command;
    }

    @Override
    public String toString() {
        return "Config{id='" + id + "', r=" + radius + ", th=" + threshold + "}";
    }
}