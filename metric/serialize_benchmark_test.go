package metric

import (
	"bytes"
	"testing"
)

func BenchmarkSerialize(b *testing.B) {
	buf := new(bytes.Buffer)
	for n := 0; n < b.N; n++ {
		Serialize(name, tags, buf)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	buf := []byte(expected)
	for n := 0; n < b.N; n++ {
		Deserialize(buf)
	}
}
