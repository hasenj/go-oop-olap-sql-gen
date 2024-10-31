package sql

import "strings"

type Writer interface {
	Write(b *strings.Builder)
}

func WriteString(w Writer) string {
	var b strings.Builder
	w.Write(&b)
	return b.String()
}

func WriteAll(b *strings.Builder, writers ...Writer) {
	for _, w := range writers {
		w.Write(b)
	}
}
