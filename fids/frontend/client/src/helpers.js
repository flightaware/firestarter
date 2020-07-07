const degreesToRadians = (degrees) => {
	return (degrees * Math.PI / 180);
}

const radiansToDegrees = (radians) => {
	return (radians * 180 / Math.PI);
}

export const getBearingDegrees = (d_lat1, d_lon1, d_lat2, d_lon2) => {
	let r_lat1 = degreesToRadians(d_lat1);
	let r_lon1 = degreesToRadians(d_lon1);
	let r_lat2 = degreesToRadians(d_lat2);
	let r_lon2 = degreesToRadians(d_lon2);
	let dLon = (r_lon2 - r_lon1);

	let y = Math.sin(dLon) * Math.cos(r_lat2);
	let x = Math.cos(r_lat1) * Math.sin(r_lat2) - Math.sin(r_lat1)
			* Math.cos(r_lat2) * Math.cos(dLon);

	let brng = Math.atan2(y, x);

	brng = radiansToDegrees(brng);
	brng = (brng + 360) % 360;

	return brng;
}

export const getClosestDegreeAngle = (input_degrees) => {
	if ((input_degrees >= 315 && input_degrees < 360) || (input_degrees >= 0 && input_degrees < 45)) {
		return 0;
	} else if (input_degrees >= 45 && input_degrees < 135) {
		return 90;
	} else if (input_degrees >= 135 && input_degrees < 225) {
		return 180;
	} else {
		return 270;
	}
}