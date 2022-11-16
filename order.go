package belastic

import "strings"

const (
	asc  = `asc`
	desc = `desc`
)

type Order []OrderBy

type OrderBy struct {
	field string
	order string
}

func Desc(field string) OrderBy {
	return OrderBy{field: field, order: desc}
}

func Asc(field string) OrderBy {
	return OrderBy{field: field, order: asc}
}

func (o Order) toString() string {
	if len(o) == 0 {
		return ""
	}

	buf := strings.Builder{}

	n := 2 + len(o) - 1
	for i := 0; i < len(o); i++ {
		n += len(o[i].field) + len(o[i].order) + 43
	}

	buf.Grow(n)
	buf.WriteString(`{"`)

	buf.WriteString(o[0].field)
	buf.WriteString(`":{"unmapped_type": "keyword", "order":"`)
	buf.WriteString(o[0].order)
	buf.WriteString(`"}`)

	for index := 1; index < len(o); index++ {
		buf.WriteString(`,"`)
		buf.WriteString(o[index].field)
		buf.WriteString(`":{"unmapped_type": "keyword", "order":"`)
		buf.WriteString(o[index].order)
		buf.WriteString(`"}`)
	}

	buf.WriteByte('}')

	return buf.String()
}
