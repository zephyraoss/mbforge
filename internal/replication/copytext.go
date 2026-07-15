package replication

import "bytes"

var copyNullField = []byte(`\N`)

func decodeCopyField(raw []byte) (string, bool) {
	if bytes.Equal(raw, copyNullField) {
		return "", false
	}
	if !bytes.ContainsRune(raw, '\\') {
		return string(raw), true
	}

	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c != '\\' || i+1 >= len(raw) {
			out = append(out, c)
			continue
		}
		i++
		switch e := raw[i]; e {
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'v':
			out = append(out, '\v')
		case '\\':
			out = append(out, '\\')
		case 'x':
			value, width := parseHexEscape(raw[i+1:])
			if width == 0 {
				out = append(out, e)
				continue
			}
			out = append(out, value)
			i += width
		case '0', '1', '2', '3', '4', '5', '6', '7':
			value, width := parseOctalEscape(raw[i:])
			out = append(out, value)
			i += width - 1
		default:
			out = append(out, e)
		}
	}
	return string(out), true
}

func parseHexEscape(raw []byte) (byte, int) {
	var value byte
	width := 0
	for width < 2 && width < len(raw) {
		d, ok := hexDigit(raw[width])
		if !ok {
			break
		}
		value = value<<4 | d
		width++
	}
	return value, width
}

func parseOctalEscape(raw []byte) (byte, int) {
	var value byte
	width := 0
	for width < 3 && width < len(raw) && raw[width] >= '0' && raw[width] <= '7' {
		value = value<<3 | (raw[width] - '0')
		width++
	}
	return value, width
}

func hexDigit(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
