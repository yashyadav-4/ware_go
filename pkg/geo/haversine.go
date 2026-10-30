package geo

import "math"

const EarthRadiusKm = 6371.0

func HaversineKm(lat1 , lon1 , lat2 , lon2 float64) float64{
	radLat1 := lat1 * math.Pi / 180.0
	radLon1 := lon1 * math.Pi / 180.0
	radLat2 := lat2 * math.Pi /180.0
	radLon2 := lon2 * math.Pi/ 180.0
	
	deltaLat := radLat2 - radLat1
	deltaLon := radLon2 - radLon1

	a := math.Sin(deltaLat/2.0)*math.Sin(deltaLat/2.0) + math.Cos(radLat1)*math.Cos(radLat2)* math.Sin(deltaLon/2.0)*math.Sin(deltaLon/2.0)

	c := 2.0 * math.Atan2(math.Sqrt(a), math.Sqrt(1.0-a))

	return EarthRadiusKm * c
}