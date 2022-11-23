package elastic

type Operator interface {
	Build() string
}
