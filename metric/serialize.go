package metric

import (
	"bytes"
	"errors"
)

const (
	defaultTagsMapLen = 4

	// ComponentSplitter is the separator for different compoenents of the path
	ComponentSplitter = '+'
	// TagPairSplitter is the separator for tag pairs
	TagPairSplitter = ','
	// TagNameSplitter is the separator for tag name and values
	TagNameSplitter = '='
)

var (
	errParseTagFailure = errors.New("Unable to parse tags, unable to parse tags")
	errEmptyTagKey     = errors.New("Tag keys cannot be empty")
	errEmptyTagValue   = errors.New("Tag values cannot be empty")
)

// Serialize serializes the name and tags of a metric into a buffer of bytes
func Serialize(name string, tags map[string]string, buf *bytes.Buffer) {
	buf.WriteString(name)
	if len(tags) > 0 {
		buf.WriteRune(ComponentSplitter)
	}

	first := true
	for key, val := range tags {
		if !first {
			buf.WriteRune(TagPairSplitter)
		} else {
			first = false
		}

		buf.WriteString(key)
		buf.WriteRune(TagNameSplitter)
		buf.WriteString(val)
	}
}

// Deserialize extracts the name and tags for a metric from a buffer of bytes
func Deserialize(buf []byte) (string, map[string]string, error) {
	n := bytes.IndexByte(buf, ComponentSplitter)
	if n == -1 {
		return string(buf), nil, nil
	}

	name := string(buf[:n])
	buf = buf[(n + 1):]
	tags := make(map[string]string)

	for {
		n = bytes.IndexByte(buf, TagNameSplitter)
		switch n {
		case -1:
			return "", nil, errParseTagFailure
		case 0:
			return "", nil, errEmptyTagKey
		}

		key := string(buf[:n])
		buf = buf[(n + 1):]

		n = bytes.IndexByte(buf, TagPairSplitter)
		switch n {
		case -1:
			tags[key] = string(buf)
			return name, tags, nil
		case 0:
			return "", nil, errEmptyTagValue
		}

		tags[key] = string(buf[:n])
		buf = buf[(n + 1):]
	}
}
