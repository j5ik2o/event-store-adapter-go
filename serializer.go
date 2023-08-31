package event_store_adapter_go

import "encoding/json"

type JsonEventSerializer struct{}

func (s *JsonEventSerializer) Serialize(event Event) ([]byte, error) {
	return json.Marshal(event)
}

func (s *JsonEventSerializer) Deserialize(data []byte, eventMap *map[string]interface{}) error {
	return json.Unmarshal(data, eventMap)
}

type JsonSnapshotSerializer struct{}

func (s *JsonSnapshotSerializer) Serialize(aggregate Aggregate) ([]byte, error) {
	return json.Marshal(aggregate)
}

func (s *JsonSnapshotSerializer) Deserialize(data []byte, aggregateMap *map[string]interface{}) error {
	return json.Unmarshal(data, aggregateMap)
}
