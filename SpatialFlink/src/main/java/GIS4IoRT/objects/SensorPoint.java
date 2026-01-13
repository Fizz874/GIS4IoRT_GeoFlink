package GIS4IoRT.objects;
import GeoFlink.spatialObjects.Point;


public class SensorPoint extends Point {

    public double humidity;
    public double radius;
    public double threshold;

    public boolean isRetract = false;

    public SensorPoint() {
        super();
    }

    public SensorPoint(Point p, double humidity, double radius, double threshold) {
        super();

        this.point = p.point;
        this.gridID = p.gridID;
        this.ingestionTime = p.ingestionTime;
        this.objID = p.objID;
        this.timeStampMillisec = p.timeStampMillisec;


        this.deviceID = p.deviceID;
        this.eventID = p.eventID;

        this.humidity = humidity;
        this.radius = radius;
        this.threshold = threshold;
        this.isRetract = false;
    }

    @Override
    public String toString() {
        return super.toString() + " [Hum: " + humidity + ", R: " + radius + ", Retract: " + isRetract + "]";
    }
}