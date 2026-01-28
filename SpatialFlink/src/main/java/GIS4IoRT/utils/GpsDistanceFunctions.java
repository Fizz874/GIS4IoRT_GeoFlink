package GIS4IoRT.utils;

import GIS4IoRT.objects.SensorPoint;
import GeoFlink.spatialObjects.Point;

/* Geospatial Distance Utility Functions
 * This utility class provides static methods to calculate geodesic distances between
 * various spatial objects using the Haversine formula. It is specifically designed
 * to handle Earth's curvature by operating on latitude and longitude coordinates.
 */

public class GpsDistanceFunctions {
    private static final int EARTH_RADIUS = 6371000;


    public static double getDistance(Point p1, Point p2) {
        return getDistance(p1.point.getY(), p1.point.getX(), p2.point.getY(), p2.point.getX());
    }

    public static double getDistance(Point p1, SensorPoint p2) {
        return getDistance(p1.point.getY(), p1.point.getX(), p2.point.getY(), p2.point.getX());
    }

    private static double getDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c;
    }


}
