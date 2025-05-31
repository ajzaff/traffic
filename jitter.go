package traffic

import (
	"math"
	"math/rand/v2"
	"time"
)

type Jitter struct {
	Max   time.Duration
	Theta float64
}

func (j Jitter) UniformCall(r *rand.Rand, f func()) *time.Timer {
	var d time.Duration
	if j.Max > 0 {
		d = time.Duration(float64(j.Max) * r.Float64())
	}
	return time.AfterFunc(d, f)
}

func (j Jitter) NormalCall(r *rand.Rand, f func()) *time.Timer {
	var d time.Duration
	if j.Max > 0 {
		theta := 1.0
		if j.Theta > 0 {
			theta = j.Theta
		}
		d = time.Duration(float64(j.Max) * math.Abs(r.NormFloat64()) * theta)
	}
	return time.AfterFunc(d, f)
}
