package metric

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	name       = "foobar"
	commonTags = map[string]string{
		"service": "fizzbuzz",
		"dc":      "ewr1",
		"env":     "production",
	}
	serviceTags = map[string]string{
		"type": "requests",
		"city": "nyc",
	}
	expected = "foobar+service=fizzbuzz,dc=ewr1,env=production,type=requests,city=nyc"
)

func TestSerializeName(t *testing.T) {
	testSerialize(t, name, name)
}

func TestSerializeNameAndTags(t *testing.T) {
	// NB(jeromefroe): Since iterating over a map is randomized we can't rely on tags to be serialized
	// in the same order across calls to Serialize. Consequently we test with only one tag here.
	commonTags := map[string]string{
		"service": "fizzbuzz",
	}
	serviceTags := map[string]string{
		"type": "requests",
	}
	expected := "foobar+service=fizzbuzz,type=requests"
	testSerialize(t, expected, name, commonTags, serviceTags)
}

func testSerialize(t *testing.T, expected string, name string, tags ...map[string]string) {
	buf := new(bytes.Buffer)
	Serialize(buf, name, tags...)
	assert.Equal(t, expected, string(buf.Bytes()))
}

func TestDeserializeName(t *testing.T) {
	testDeserialize(t, []byte(name), nil, name)
}

func TestDeserializeNameAndTags(t *testing.T) {
	buf := []byte(expected)
	testDeserialize(t, buf, nil, name, commonTags, serviceTags)
}

func TestDeserializeErrInvalidTags(t *testing.T) {
	buf := []byte("foobar+")
	testDeserialize(t, buf, errParseTagFailure, "")
}

func TestDeserializeErrEmptyTagKey(t *testing.T) {
	buf := []byte("foobar+service=fizzbuzz,=ewr1,env=production")
	testDeserialize(t, buf, errEmptyTagKey, "")
}

func TestDeserializeErrEmptyTagValue(t *testing.T) {
	buf := []byte("foobar+service=,dc=ewr1,env=production")
	testDeserialize(t, buf, errEmptyTagValue, "")
}

func testDeserialize(t *testing.T, buf []byte, err error, name string, tags ...map[string]string) {
	allTags := combineTags(tags...)
	actualName, actualTags, actualErr := Deserialize(buf)

	assert.Equal(t, name, actualName)
	assert.Equal(t, allTags, actualTags)
	assert.Equal(t, err, actualErr)
}

func TestRoundtripName(t *testing.T) {
	testRoundtrip(t, "foobar")
}

func TestRoundtripNameAndTags(t *testing.T) {
	testRoundtrip(t, "foobar", commonTags, serviceTags)
}

func testRoundtrip(t *testing.T, name string, tags ...map[string]string) {
	buf := new(bytes.Buffer)
	Serialize(buf, name, tags...)

	actualName, actualTags, err := Deserialize(buf.Bytes())

	allTags := combineTags(tags...)
	assert.Nil(t, err)
	assert.Equal(t, name, actualName)
	assert.Equal(t, allTags, actualTags)
}

func combineTags(tags ...map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	allTags := make(map[string]string)
	for _, tagMap := range tags {
		for key, val := range tagMap {
			allTags[key] = val
		}
	}
	return allTags
}
