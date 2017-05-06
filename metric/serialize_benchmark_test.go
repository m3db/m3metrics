package metric

import (
	"bytes"
	"testing"
)

func BenchmarkSerialize(b *testing.B) {
	buf := new(bytes.Buffer)
	for n := 0; n < b.N; n++ {
		Serialize(buf, name, commonTags, serviceTags)
	}
}

func BenchmarkDeserialize(b *testing.B) {
	buf := []byte(expected)
	for n := 0; n < b.N; n++ {
		Deserialize(buf)
	}
}
