FROM ros:jazzy-ros-base
# Instalacja zależności ROS 2 i Pythona
RUN apt update && apt install -y \
    python3-pip \
    python3-venv \
    ros-jazzy-visualization-msgs \
    ros-jazzy-rmw-cyclonedds-cpp \
    ros-jazzy-geometry-msgs \
    ros-jazzy-sensor-msgs \
    ros-jazzy-builtin-interfaces

# Utworzenie środowiska wirtualnego
RUN python3 -m venv /venv

# Aktywacja środowiska i instalacja bibliotek Python
ENV PATH="/venv/bin:$PATH"
RUN pip install --upgrade pip && \
    pip install pandas shapely pyproj pyyaml

