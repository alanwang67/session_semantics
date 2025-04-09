package main

import (
	"fmt"
	"math"
	"math/rand/v2"
)

type probvals struct {
	prob     float64
	cum_prob float64
}

const IDX_SIZE = (1024 * 1024 * 100)

func init_zipf(zipf_table []int, theta float64, N int) {
	var sum = 0.0
	var c = 0.0
	var expo float64
	var sumc = 0.0
	var i int
	var j int

	g_zipf_dist := make([]*probvals, N)
	expo = 1 - theta

	i = 1
	for i <= N {
		sum += 1.0 / float64(math.Pow(float64(i), expo))
		i += 1
	}

	c = 1.0 / sum

	i = 0
	for i < N {
		g_zipf_dist[i] = &probvals{prob: c / (math.Pow(float64(i+1), expo))}
		// g_zipf_dist[i].prob = c / (math.Pow(float64(i+1), expo))
		sumc += g_zipf_dist[i].prob
		g_zipf_dist[i].cum_prob = sumc
		i += 1
	}

	var idx int
	var val int
	prev := 0
	i = 0
	for i < N {
		idx = int(g_zipf_dist[i].cum_prob * float64(IDX_SIZE))
		if idx >= IDX_SIZE {
			if prev < IDX_SIZE {
				val = zipf_table[prev-1]
				j = prev
				for j < IDX_SIZE {
					zipf_table[j] = val
					j += 1
				}
			}
			break
		}
		j = prev
		for j < idx {
			zipf_table[j] = i
			j += 1
		}
		prev = j
		i += 1
	}
}

func gen_zipf_blk(zipf_table []int) int {
	idx := int(rand.Float64() * (IDX_SIZE))
	return zipf_table[idx]
}

// this just generates the index of the record???
func main() {
	zipf_table := make([]int, IDX_SIZE)
	init_zipf(zipf_table, 0.55, 100)
	fmt.Println(zipf_table)
	fmt.Println(gen_zipf_blk(zipf_table))
}

// theta value - 0.99