package GIS4IoRT.objects;

import GeoFlink.spatialObjects.Point;
import java.util.ArrayList;
import java.util.List;

public class AssignedPoint extends Point {

    public List<String> assignedZoneIDs;

    public AssignedPoint() {
        super();
        this.assignedZoneIDs = new ArrayList<>();
    }

    public AssignedPoint(Point p, List<String> assignedZoneIDs) {
        super();

        this.point = p.point;
        this.gridID = p.gridID;
        this.ingestionTime = p.ingestionTime;
        this.eventID = p.eventID;
        this.deviceID = p.deviceID;
        this.userID = p.userID;

        this.objID = p.objID;
        this.timeStampMillisec = p.timeStampMillisec;

        this.assignedZoneIDs = assignedZoneIDs != null ? assignedZoneIDs : new ArrayList<>();
    }

    @Override
    public String toString() {
        return super.toString() + " [Zones: " + assignedZoneIDs.size() + "]";
    }
}