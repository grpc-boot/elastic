package belastic

import "github.com/grpc-boot/base"

const (
	TypeBinary  = `binary`
	TypeBoolean = `boolean`
	TypeByte    = `byte`
	TypeShort   = `short`
	TypeInteger = `integer`
	TypeLong    = `long`
	TypeUlong   = `unsigned_long`
	TypeFloat   = `float`
	TypeDouble  = `double`
	TypeKeyword = `keyword`
	TypeText    = `text`
	TypeDate    = `date`
	TypeIp      = `ip`
	TypeVersion = `version`
)

const (
	FormatDate                 = `yyyy-MM-dd`
	FormatDateTime             = `yyyy-MM-dd HH:mm:ss`
	FormatUnixTime             = `epoch_second`
	FormatTimestampSecond      = FormatUnixTime
	FormatTimestampMilliSecond = `epoch_millis`
)

type Mappings struct {
	Properties map[string]*Property `json:"properties"`
}

func (m *Mappings) Add(properties ...*Property) *Mappings {
	if m.Properties == nil {
		m.Properties = make(map[string]*Property)
	}

	for _, prop := range properties {
		m.Properties[prop.n] = prop
	}

	return m
}

func (m *Mappings) Marshal() []byte {
	data, _ := base.JsonMarshal(m)
	return data
}

type Property struct {
	n      string
	Type   string `json:"type"`
	Format string `json:"format,omitempty"`
}

func NewProperty(fieldName, fieldType string) *Property {
	return &Property{n: fieldName, Type: fieldType}
}

func (p *Property) FieldName() string {
	return p.n
}

func (p *Property) WithFormat(format string) *Property {
	p.Format = format
	return p
}
