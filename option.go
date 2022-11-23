package elastic

var (
	defaultOption = func() *Option {
		return &Option{
			DialTimeoutSecond:     1,
			KeepaliveSecond:       60,
			IdleConnTimeoutSecond: 30,
			MaxIdleConns:          8,
			MaxIdleConnsPerHost:   4,
			MaxConnsPerHost:       16,
		}
	}
)

type Option struct {
	BaseUrl               string `json:"baseUrl" yaml:"baseUrl"`
	UserName              string `json:"userName" yaml:"userName"`
	Password              string `json:"password" yaml:"password"`
	DialTimeoutSecond     int64  `json:"dialTimeoutSecond" yaml:"dialTimeoutSecond"`
	KeepaliveSecond       int64  `json:"keepaliveSecond" yaml:"keepaliveSecond"`
	IdleConnTimeoutSecond int64  `json:"idleConnTimeoutSecond" yaml:"idleConnTimeoutSecond"`
	MaxIdleConns          int    `json:"maxIdleConns" yaml:"maxIdleConns"`
	MaxIdleConnsPerHost   int    `json:"maxIdleConnsPerHost" yaml:"maxIdleConnsPerHost"`
	MaxConnsPerHost       int    `json:"maxConnsPerHost" yaml:"maxConnsPerHost"`
}

func loadOption(option Option) *Option {
	opt := defaultOption()
	opt.BaseUrl = option.BaseUrl
	opt.UserName = option.UserName
	opt.Password = option.Password

	if option.DialTimeoutSecond > 0 {
		opt.DialTimeoutSecond = option.DialTimeoutSecond
	}

	if option.KeepaliveSecond > 0 {
		opt.KeepaliveSecond = option.KeepaliveSecond
	}

	if option.IdleConnTimeoutSecond > 0 {
		opt.IdleConnTimeoutSecond = option.IdleConnTimeoutSecond
	}

	if option.MaxIdleConns > 0 {
		opt.MaxIdleConns = option.MaxIdleConns
	}

	if option.MaxIdleConnsPerHost > 0 {
		opt.MaxIdleConnsPerHost = option.MaxIdleConnsPerHost
	}

	if option.MaxConnsPerHost > 0 {
		opt.MaxConnsPerHost = option.MaxConnsPerHost
	}

	return opt
}
