package elastic

import "strings"

type Condition struct {
	operator string
	items    []Operator
}

func AndCondition(items ...Operator) Condition {
	return Condition{operator: And, items: items}
}

func OrCondition(items ...Operator) Condition {
	return Condition{operator: Or, items: items}
}

func (c Condition) Build() string {
	if len(c.items) < 1 {
		return "*"
	}

	buf := strings.Builder{}

	buf.WriteByte('(')
	buf.WriteString(c.items[0].Build())
	for i := 1; i < len(c.items); i++ {
		buf.WriteString(c.operator)
		buf.WriteString(c.items[i].Build())
	}
	buf.WriteByte(')')

	return buf.String()
}
