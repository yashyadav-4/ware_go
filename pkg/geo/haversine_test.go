package geo

import (
	"math"
	"testing"
)

func TestHaversineKm(t *testing.T){
	cases := []struct{
		name string
		lat1 , lon1 float64	
		lat2 , lon2 float64
		wantKm float64
		toleranceKm float64
	}{
		{
			name:        "identical coordinates must have 0 km distance",
			lat1:        28.6139,
			lon1:        77.2090,
			lat2:        28.6139,
			lon2:        77.2090,
			wantKm:      0.0,
			toleranceKm: 0.001,
		},
		{
			name:        "Delhi to Jaipur approx distance",
			lat1:        28.6139,
			lon1:        77.2090,
			lat2:        26.9124,
			lon2:        75.7873,
			wantKm:      238.0,
			toleranceKm: 10.0,
		},
	}


	for _, c := range cases{
		t.Run(c.name , func	(t *testing.T){
			got := HaversineKm(c.lat1, c.lon1, c.lat2, c.lon2)
			diff := math.Abs(got - c.wantKm)

			if diff > c.toleranceKm{
				t.Fatalf("HaversineKm(%v, %v, %v, %v) = %.2f km; want %.2f ± %.2f km", c.lat1, c.lon1, c.lat2, c.lon2, got, c.wantKm, c.toleranceKm)
			}
		})
	}
}

// run -> go test -v ./pkg/geo/...