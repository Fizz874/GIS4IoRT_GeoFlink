import rclpy
from rclpy.node import Node
from visualization_msgs.msg import Marker
from geometry_msgs.msg import Point
from builtin_interfaces.msg import Duration
from sensor_msgs.msg import NavSatFix
import pandas as pd
from shapely import wkb, geometry, affinity
from shapely.ops import unary_union
import binascii
from pyproj import Transformer
import os
from math import sqrt

class CsvToRvizMarkers(Node):
    def __init__(self):
        super().__init__('csv_to_rviz_markers')
        self.publisher = self.create_publisher(Marker, 'visualization_marker', 10)
        self.csv_path = os.path.expanduser('/app/data/parcelles.csv')
        self.transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)
        self.global_centroid = None

        # Parametr robot_name i subskrypcja GPS
        self.declare_parameter('robot_name', 'leader')
        self.robot_name = self.get_parameter('robot_name').get_parameter_value().string_value
        self.gps_topic = f'/{self.robot_name}/gps/fix'
        self.create_subscription(NavSatFix, self.gps_topic, self.gps_callback, 10)

        #Działka w centrum
        self.declare_parameter('centroid_shape_name', '1 MONT')
        self.centroid_shape_name = self.get_parameter('centroid_shape_name').get_parameter_value().string_value

        # Marker trasy robota
        self.path_marker = Marker()
        self.path_marker.header.frame_id = 'map'
        self.path_marker.type = Marker.LINE_STRIP
        self.path_marker.action = Marker.ADD
        self.path_marker.id = 9999
        self.path_marker.scale.x = 1.0
        self.path_marker.color.r = 0.0
        self.path_marker.color.g = 0.0
        self.path_marker.color.b = 1.0
        self.path_marker.color.a = 1.0
        self.path_marker.points = []

        # Timer do publikacji geometrii
        self.timer = self.create_timer(2.0, self.publish_markers)

    def publish_markers(self):
        df = pd.read_csv(self.csv_path)
        metric_geometries = []

        for i, row in df.iterrows():
            try:
                geom = wkb.loads(binascii.unhexlify(row['geom']))
                if geom.geom_type == 'Polygon':
                    exterior = [self.transformer.transform(y, x) for x, y in geom.exterior.coords]
                    metric_geometries.append(geometry.Polygon(exterior))
                elif geom.geom_type == 'MultiPolygon':
                    for poly in geom.geoms:
                        exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                        metric_geometries.append(geometry.Polygon(exterior))
            except Exception as e:
                self.get_logger().warn(f'Błąd przy geometrii {row["id"]}: {e}')

        if not metric_geometries:
            self.get_logger().warn("Brak poprawnych geometrii do przetworzenia.")
            return

        centroid_geometries = []

        for i, row in df.iterrows():
            try:
                geom = wkb.loads(binascii.unhexlify(row['geom']))
                if row['name'] == self.centroid_shape_name:
                    if geom.geom_type == 'Polygon':
                        exterior = [self.transformer.transform(y, x) for x, y in geom.exterior.coords]
                        centroid_geometries.append(geometry.Polygon(exterior))
                    elif geom.geom_type == 'MultiPolygon':
                        for poly in geom.geoms:
                            exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                            centroid_geometries.append(geometry.Polygon(exterior))
            except Exception as e:
                self.get_logger().warn(f'Błąd przy geometrii {row["id"]}: {e}')

        if not centroid_geometries:
            self.get_logger().warn(f"Nie znaleziono geometrii o nazwie '{self.centroid_shape_name}' — używam wszystkich.")
            combined = unary_union(metric_geometries)
        else:
            combined = unary_union(centroid_geometries)

        self.global_centroid = combined.centroid

        for i, row in df.iterrows():
            try:
                geom = wkb.loads(binascii.unhexlify(row['geom']))
                if geom.geom_type == 'Polygon':
                    exterior = [self.transformer.transform(y, x) for x, y in geom.exterior.coords]
                    metric_geom = geometry.Polygon(exterior)
                elif geom.geom_type == 'MultiPolygon':
                    polygons = []
                    for poly in geom.geoms:
                        exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                        polygons.append(geometry.Polygon(exterior))
                    metric_geom = geometry.MultiPolygon(polygons)
                else:
                    continue

                shifted = affinity.translate(metric_geom, xoff=-self.global_centroid.x, yoff=-self.global_centroid.y)

                marker = Marker()
                marker.lifetime = Duration(sec=0, nanosec=0)
                marker.header.frame_id = 'map'
                marker.type = Marker.LINE_STRIP
                marker.action = Marker.ADD
                marker.id = int(row['id'])
                marker.scale.x = 0.5
                marker.color.r = 1.0
                marker.color.g = 0.0
                marker.color.b = 0.0
                marker.color.a = 1.0

                if shifted.geom_type == 'MultiPolygon':
                    for polygon in shifted.geoms:
                        for x, y in polygon.exterior.coords:
                            marker.points.append(Point(x=x, y=y, z=0.0))
                elif shifted.geom_type == 'Polygon':
                    for x, y in shifted.exterior.coords:
                        marker.points.append(Point(x=x, y=y, z=0.0))
                self.publisher.publish(marker)
            except Exception as e:
                self.get_logger().warn(f'Błąd przy przesuwaniu {row["id"]}: {e}')

    def gps_callback(self, msg: NavSatFix):
        if self.global_centroid is None:
            self.get_logger().warn("Centroid niegotowy — pomijam GPS.")
            return

        mx, my = self.transformer.transform(msg.latitude, msg.longitude)
        dx = mx - self.global_centroid.x
        dy = my - self.global_centroid.y
        new_point = Point(x=dx, y=dy, z=0.0)


        # Sposób na ponawianie zapytań (aby nie trzeba było restartować programu)
        if self.path_marker.points:
            last_point = self.path_marker.points[-1]
            dist = sqrt((new_point.x - last_point.x)**2 + (new_point.y - last_point.y)**2)

            # Jeśli odległość większa niż próg (np. 5 metrów), wyczyść stare punkty
            if dist > 5.0:
                self.get_logger().info(f"Nowy punkt daleko ({dist:.2f} m) — resetuję ścieżkę.")
                self.path_marker.points = []




        # Dodaj punkt do trasy
        self.path_marker.points.append(new_point)
        self.path_marker.header.stamp = msg.header.stamp
        # Publikuj marker trasy
        self.publisher.publish(self.path_marker)

        self.get_logger().info(f'GPS względem centroidu: x={dx:.2f}, y={dy:.2f}, alt={msg.altitude:.2f}')

def main(args=None):
    rclpy.init(args=args)
    node = CsvToRvizMarkers()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info("Przerwano przez użytkownika (Ctrl+C)")
    except rclpy.executors.ExternalShutdownException:
        node.get_logger().info("ROS2 kontekst został zamknięty (SIGTERM)")
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()