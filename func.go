package belastic

import (
	"strings"
)

func joinWithQuote(elems []string, sep string) string {
	switch len(elems) {
	case 0:
		return ""
	}

	n := (len(sep) * (len(elems) - 1)) + (len(elems) * 2)
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteByte('"')
	b.WriteString(elems[0])
	b.WriteByte('"')

	for _, s := range elems[1:] {
		b.WriteString(sep)
		b.WriteByte('"')
		b.WriteString(s)
		b.WriteByte('"')
	}
	return b.String()
}
