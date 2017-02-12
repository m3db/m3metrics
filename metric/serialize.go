package metric

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	defaultTagsMapLen = 8

	// ComponentSplitter is the separator for different compoenents of the path
	ComponentSplitter = '+'
	// TagPairSplitter is the separator for tag pairs
	TagPairSplitter = ','
	// TagNameSplitter is the separator for tag name and values
	TagNameSplitter = '='
)

var (
	errParseTagFailure = fmt.Errorf("Parse failed, no tag splitter %q found", TagPairSplitter)
	errEmptyTagKey     = errors.New("Tag keys cannot be empty")
	errEmptyTagValue   = errors.New("Tag values cannot be empty")
)

// Serialize serializes the name and tags of a metric into a buffer of bytes
func Serialize(buf *bytes.Buffer, name string, tags ...map[string]string) error {
	buf.WriteString(name)
	if len(tags) > 0 {
		buf.WriteRune(ComponentSplitter)
	}

	// NB(jeromefroe): we do not check for duplicate tags here to avoid the performance penalty
	first := true
	for _, tagMap := range tags {
		for key, val := range tagMap {
			if len(key) == 0 {
				buf.Reset()
				return errEmptyTagKey
			}

			if len(val) == 0 {
				buf.Reset()
				return errEmptyTagValue
			}

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

// Deserialize extracts the name and tags for a metric from a buffer of bytes
func Deserialize(buf []byte) (string, map[string]string, error) {
	n := bytes.IndexByte(buf, ComponentSplitter)
	if n == -1 {
		return string(buf), nil, nil
	}

	name := string(buf[:n])
	buf = buf[(n + 1):]
	tags := make(map[string]string, defaultTagsMapLen)

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
