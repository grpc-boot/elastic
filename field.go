package belastic

import "strings"

const (
	optContains = iota
	optLt
	optLte
	optGt
	optGte
	optNotNull
)

const (
	notNullPrefix = `_exists_:`
)

type Field struct {
	key      string
	operator uint8
	values   []string
}

func Contains(field string, values ...string) Field {
	return Field{key: field, operator: optContains, values: values}
}

func Terms(field string, values ...string) Field {
	return Contains(field, values...)
}

func In(field string, values ...string) Field {
	return Contains(field, values...)
}

func Equal(field string, value string) Field {
	return Term(field, value)
}

func Term(field string, value string) Field {
	return Field{key: field, operator: optContains, values: []string{value}}
}

func Lt(field string, value string) Field {
	return Field{key: field, operator: optLt, values: []string{value}}
}

func Lte(field string, value string) Field {
	return Field{key: field, operator: optLte, values: []string{value}}
}

func Gt(field string, value string) Field {
	return Field{key: field, operator: optGt, values: []string{value}}
}

func Gte(field string, value string) Field {
	return Field{key: field, operator: optGte, values: []string{value}}
}

func NotNil(field string) Field {
	return Field{key: field, operator: optNotNull}
}

func (f Field) Build() string {
	if len(f.values) < 1 && f.operator != optNotNull {
		return ""
	}

	switch f.operator {
	case optContains:
		return f.buildContains()
	case optLt:
		return f.buildLt()
	case optLte:
		return f.buildLte()
	case optGt:
		return f.buildGt()
	case optGte:
		return f.buildGte()
	case optNotNull:
		return f.buildNotNull()
	}

	return ""
}

func (f Field) buildContains() string {
	buf := strings.Builder{}

	n := len(f.key) + 1 + len(Or)*(len(f.values)-1)
	for i := 0; i < len(f.values); i++ {
		n += len(f.values[i])
	}

	if len(f.values) == 1 {
		buf.Grow(n)
		buf.WriteString(f.key)
		buf.WriteByte(':')
		buf.WriteString(f.values[0])
		return buf.String()
	}

	// ()长度
	n += 2
	buf.Grow(n)

	buf.WriteString(f.key)
	buf.WriteString(`:(`)
	buf.WriteString(f.values[0])

	for i := 1; i < len(f.values); i++ {
		buf.WriteString(Or)
		buf.WriteString(f.values[i])
	}

	buf.WriteByte(')')

	return buf.String()
}

func (f Field) buildLt() string {
	return f.key + ":<" + f.values[0]
}

func (f Field) buildLte() string {
	return f.key + ":<=" + f.values[0]
}

func (f Field) buildGt() string {
	return f.key + ":>" + f.values[0]
}

func (f Field) buildGte() string {
	return f.key + ":>=" + f.values[0]
}

func (f Field) buildNotNull() string {
	return notNullPrefix + f.key
}
