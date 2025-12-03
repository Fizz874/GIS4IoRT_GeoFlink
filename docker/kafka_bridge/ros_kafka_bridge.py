import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
from kafka import KafkaProducer
import json
import os


#Version with raw GPS input

class ROSKafkaBridge(Node):
    def __init__(self):
        super().__init__('ros_kafka_bridge')

        # ROS2 parameters
        self.declare_parameter('robot_name', 'leader')
        self.declare_parameter('kafka_bootstrap_servers', 'localhost:9092')

        self.robot_name = self.get_parameter('robot_name').get_parameter_value().string_value
        self.kafka_servers = self.get_parameter('kafka_bootstrap_servers').get_parameter_value().string_value

        self.gps_topic = f'/{self.robot_name}/gps/fix'
        self.kafka_topic = f'{self.robot_name}_gps_fix'

        self.get_logger().info(f'Subscribing to {self.gps_topic}, publishing to Kafka topic {self.kafka_topic}')

        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks='all',
            retries=3,
            request_timeout_ms=30000,
            retry_backoff_ms=100,
            max_in_flight_requests_per_connection=1
        )

        # ROS2 subscription
        self.subscription = self.create_subscription(
            NavSatFix,
            self.gps_topic,
            self.gps_callback,
            10
        )

    def gps_callback(self, msg):
        data = {
            'timestamp': msg.header.stamp.sec * 1000 + msg.header.stamp.nanosec // 1_000_000, #self.get_clock().now().nanoseconds // 1_000_000,
            'frame_id': msg.header.frame_id,
            'latitude': msg.latitude,
            'longitude': msg.longitude,
            #'altitude': msg.altitude,
            #'source': source_label
        }

        
        csv_row = gps_dict_to_csv_row(data, self.robot_name)

        try:
            self.producer.send(self.kafka_topic, value=csv_row, key=self.robot_name)
            self.get_logger().info(f'Sent gps to Kafka: {self.kafka_topic}')
        except Exception as e:
            self.get_logger().error(f'Failed to send to Kafka: {e}')


def gps_dict_to_csv_row(data: dict, robot_name: str, delimiter: str = ',') -> str:
    timestamp = data.get('timestamp', 0)
    lat = data.get('latitude', 0.0)
    lon = data.get('longitude', 0.0)
    #alt = data.get('altitude', 0.0)
    #source = data.get('source', 'unknown')

    fields = [
        robot_name,
        str(timestamp),
        f"{lon:.8f}",
        f"{lat:.8f}"
        
    ]
    return delimiter.join(fields)


def main(args=None):
    rclpy.init(args=args)
    node = ROSKafkaBridge()
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