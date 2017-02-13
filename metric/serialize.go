package metric

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	// ComponentSplitter is the separator for different compoenents of the path
	ComponentSplitter = '+'
	// TagPairSplitter is the separator for tag pairs
	TagPairSplitter = ','
	// TagNameSplitter is the separator for tag name and values
	TagNameSplitter = '='

	defaultTagsMapLen = 8
)

var (
	errParseTagFailure = fmt.Errorf("Parse failed, no tag splitter %q found", TagPairSplitter)
	errEmptyTagKey     = errors.New("Tag keys cannot be empty")
	errEmptyTagValue   = errors.New("Tag values cannot be empty")
)

// Serialize converts the name and tags of a metric into a sequence of bytes. It does not perform
// any input validation to avoid the performance penalty. Instead, all such validation, for example,
// ensuring that name and tags do not contain any invalid characters, must be performed by the
// caller.
func Serialize(buf *bytes.Buffer, name string, tags ...map[string]string) error {
	buf.WriteString(name)
	if len(tags) > 0 {
		buf.WriteRune(ComponentSplitter)
	}

	first := true
	for _, tagMap := range tags {
		for key, val := range tagMap {
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

	return nil
}

// Deserialize extracts the name and tags for a metric from a buffer of bytes. Tags are extracted
// in order so if a key is repeated, the last value will be returned in the map.
func Deserialize(buf []byte) (string, map[string]string, error) {
	n := bytes.IndexByte(buf, ComponentSplitter)
	if n == -1 {
		return string(buf), nil, nil
	}

	name := string(buf[:n])
	buf = buf[(n + 1):]
	tags := make(map[string]string, defaultTagsMapLen)

	for {
		if n = bytes.IndexByte(buf, TagNameSplitter); n == -1 {
			return "", nil, errParseTagFailure
		}

		key := string(buf[:n])
		buf = buf[(n + 1):]

		if n = bytes.IndexByte(buf, TagPairSplitter); n == -1 {
			tags[key] = string(buf)
			return name, tags, nil
		}

		tags[key] = string(buf[:n])
		buf = buf[(n + 1):]
	}
}
