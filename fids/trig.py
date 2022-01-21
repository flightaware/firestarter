from math import degrees, radians, sin, cos, atan2
from typing import Tuple


def get_bearing_degrees(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """Get bearing in degrees between 2 sets of coordinates"""
    r_lat1 = radians(coord1[0])
    r_lon1 = radians(coord1[1])
    r_lat2 = radians(coord2[0])
    r_lon2 = radians(coord2[1])
    d_lon = r_lon2 - r_lon1
    y = sin(d_lon) * cos(r_lat2)
    x = cos(r_lat1) * sin(r_lat2) - sin(r_lat1) * cos(r_lat2) * cos(d_lon)
    bearing = degrees(atan2(y, x))
    bearing = (bearing + 360) % 360
    return bearing


def get_cardinal_for_angle(angle_degrees: float) -> int:
    """Approximate an angle in degrees to 1 of 4 cardinal directions"""
    if 45 <= angle_degrees < 135:
        return 90
    elif 135 <= angle_degrees < 225:
        return 180
    elif 225 <= angle_degrees < 315:
        return 270
    else:
        return 0
