FROM ros:jazzy-ros-base
# Instalacja zależności ROS 2 i Pythona
RUN apt update && apt install -y \
    python3-pip \
    python3-venv \
    ros-jazzy-rmw-cyclonedds-cpp \
    ros-jazzy-sensor-msgs

# Utworzenie środowiska wirtualnego
RUN python3 -m venv /venv

# Aktywacja środowiska i instalacja klienta Kafka
ENV PATH="/venv/bin:$PATH"
RUN pip install --upgrade pip && \
    pip install kafka-python pyyaml numpy
