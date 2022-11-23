package elastic

import "github.com/grpc-boot/base"

type Settings struct {
	NumberOfShards   int `json:"number_of_shards"`
	NumberOfReplicas int `json:"number_of_replicas,omitempty"`
}

func (s *Settings) Marshal() []byte {
	data, _ := base.JsonMarshal(s)
	return data
}
