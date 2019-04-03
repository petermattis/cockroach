// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpcc

import (
	"fmt"
	"math/bits"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/rand"
)

type pcg32 struct {
	state uint64
}

func newPCG32(seed uint64) *pcg32 {
	pcg := &pcg32{}
	pcg.Seed(seed)
	return pcg
}

// Seed uses the provided seed value to initialize the generator to a deterministic state.
func (pcg *pcg32) Seed(seed uint64) {
	pcg.state = 0
	pcg.Uint32()
	pcg.state += seed
	pcg.Uint32()
}

func (pcg *pcg32) Uint32() uint32 {
	oldstate := pcg.state
	pcg.state = oldstate*6364136223846793005 + 1
	xorshifted := uint32(((oldstate >> 18) ^ oldstate) >> 27)
	return bits.RotateLeft32(xorshifted, -int(oldstate>>59))
}

func TestPCG32(t *testing.T) {
	rng := newPCG32(uint64(timeutil.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		fmt.Println(rng.Uint32())
	}
}

func uint8n(r uint8, n uint32) uint32 {
	return (uint32(r) * n) >> 8
}

func randStringLetters2(rng rand.Source, buf []byte) {
	for len(buf) >= 8 {
		r := rng.Uint64()
		_ = buf[8] // preempt bounds checks below
		buf[0] = 'A' + byte(uint8n(uint8(r), 26))
		buf[1] = 'A' + byte(uint8n(uint8(r>>8), 26))
		buf[2] = 'A' + byte(uint8n(uint8(r>>16), 26))
		buf[3] = 'A' + byte(uint8n(uint8(r>>24), 26))
		buf[4] = 'A' + byte(uint8n(uint8(r>>32), 26))
		buf[5] = 'A' + byte(uint8n(uint8(r>>40), 26))
		buf[6] = 'A' + byte(uint8n(uint8(r>>48), 26))
		buf[7] = 'A' + byte(uint8n(uint8(r>>56), 26))
		buf = buf[8:]
	}

	if len(buf) > 0 {
		var r uint64
		n := 0
		for i := 0; i < len(buf); i++ {
			if n == 0 {
				r = rng.Uint64()
				n = 8
			}
			buf[i] = 'A' + byte(uint8n(uint8(r), 26))
			r >>= 8
		}
	}
}

func uint16n(r uint16, n uint) uint {
	return (uint(r) * n) >> 16
}

func randStringLetters3(rng *pcg32, buf []byte) {
	const high = 26
	const mask = 255
	const shift = 8

	for len(buf) >= 4 {
		r := rng.Uint32()
		buf[0] = 'A' + byte(uint8n(uint8(r), high))
		buf[1] = 'A' + byte(uint8n(uint8(r>>8), high))
		buf[2] = 'A' + byte(uint8n(uint8(r>>16), high))
		buf[3] = 'A' + byte(uint8n(uint8(r>>24), high))
		buf = buf[4:]
	}
	if len(buf) > 0 {
		r := rng.Uint32()
		for i := 0; i < len(buf); i++ {
			buf[i] = 'A' + byte(uint8n(uint8(r), high))
			r >>= shift
		}
	}
}

func randStringLetters4(rng *pcg32, buf []byte) {
	var r uint32
	n := 0
	for i := 0; i < len(buf); i++ {
		if n == 0 {
			r = rng.Uint32()
			n = 6
		}
		old := r
		r /= 26
		buf[i] = 'A' + byte(old-r)
		n--
	}
}

func BenchmarkRandStringFast(b *testing.B) {
	const strLen = 26
	rng := rand.NewSource(uint64(timeutil.Now().UnixNano()))
	buf := make([]byte, strLen)

	b.Run(`pcg64`, func(b *testing.B) {
		var r uint64
		for i := 0; i < b.N; i++ {
			r = rng.Uint64()
		}
		if testing.Verbose() {
			fmt.Println(r)
		}
	})
	b.Run(`pcg32`, func(b *testing.B) {
		rng := newPCG32(uint64(timeutil.Now().UnixNano()))
		b.ResetTimer()
		var r uint32
		for i := 0; i < b.N; i++ {
			r = rng.Uint32()
		}
		if testing.Verbose() {
			fmt.Println(r)
		}
	})

	b.Run(`letters`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringLetters(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`letters2`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringLetters2(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`letters3`, func(b *testing.B) {
		rng := newPCG32(uint64(timeutil.Now().UnixNano()))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			randStringLetters3(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`letters4`, func(b *testing.B) {
		rng := newPCG32(uint64(timeutil.Now().UnixNano()))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			randStringLetters4(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`numbers`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringNumbers(rng, buf)
		}
		b.SetBytes(strLen)
	})
	b.Run(`aChars`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			randStringAChars(rng, buf)
		}
		b.SetBytes(strLen)
	})
}
