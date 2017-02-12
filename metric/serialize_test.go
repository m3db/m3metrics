package metric

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	name = "foobar"
	tags = map[string]string{
		"service": "fizzbuzz",
		"dc":      "ewr1",
		"env":     "production",
	}
	expected = "foobar+service=fizzbuzz,dc=ewr1,env=production"
)

func TestSerializeName(t *testing.T) {
	testSerialize(t, name, nil, name)
}

func TestSerializeNameAndTags(t *testing.T) {
	// NB(jeromefroe): Since iterating over a map is randomized we can't rely on tags to be serialized
	// in the same order across calls to Serialize. Consequently we test with only one tag here.
	tags := map[string]string{
		"service": "fizzbuzz",
	}
	expected := "foobar+service=fizzbuzz"
	testSerialize(t, name, tags, expected)
}

func testSerialize(t *testing.T, name string, tags map[string]string, expected string) {
	buf := new(bytes.Buffer)
	Serialize(name, tags, buf)
	assert.Equal(t, expected, string(buf.Bytes()))
}

func TestDeserializeName(t *testing.T) {
	testDeserialize(t, []byte(name), name, nil, nil)
}

func TestDeserializeNameAndTags(t *testing.T) {
	buf := []byte(expected)
	testDeserialize(t, buf, name, tags, nil)
}

func TestDeserializeErrInvalidTags(t *testing.T) {
	buf := []byte("foobar+")
	testDeserialize(t, buf, "", nil, errParseTagFailure)
}

func TestDeserializeErrEmptyTagKey(t *testing.T) {
	buf := []byte("foobar+service=fizzbuzz,=ewr1,env=production")
	testDeserialize(t, buf, "", nil, errEmptyTagKey)
}

func TestDeserializeErrEmptyTagValue(t *testing.T) {
	buf := []byte("foobar+service=,dc=ewr1,env=production")
	testDeserialize(t, buf, "", nil, errEmptyTagValue)
}

func testDeserialize(t *testing.T, buf []byte, name string, tags map[string]string, err error) {
	actualName, actualTags, actualErr := Deserialize(buf)
	assert.Equal(t, name, actualName)
	assert.Equal(t, tags, actualTags)
	assert.Equal(t, err, actualErr)
}

func TestRoundtripName(t *testing.T) {
	testRoundtrip(t, "foobar", nil)
}

func TestRoundtripNameAndTags(t *testing.T) {
	tags := map[string]string{
		"service": "fizzbuzz",
		"dc":      "ewr1",
		"env":     "production",
	}
	testRoundtrip(t, "foobar", tags)
}

func testRoundtrip(t *testing.T, name string, tags map[string]string) {
	buf := new(bytes.Buffer)
	Serialize(name, tags, buf)
	actualName, actualTags, err := Deserialize(buf.Bytes())
	assert.Nil(t, err)
	assert.Equal(t, name, actualName)
	assert.Equal(t, tags, actualTags)
}
