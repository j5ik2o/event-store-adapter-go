package event_store_adapter_go

import "encoding/json"

type JsonEventSerializer struct{}

func (s *JsonEventSerializer) Serialize(event Event) ([]byte, error) {
	result, err := json.Marshal(event)
	if err != nil {
		return nil, &SerializationError{EventStoreBaseError{"Failed to serialize the event", err}}
	}
	return result, nil
}

func (s *JsonEventSerializer) Deserialize(data []byte, eventMap *map[string]interface{}) error {
	err := json.Unmarshal(data, eventMap)
	if err != nil {
		return &DeserializationError{EventStoreBaseError{"Failed to deserialize the event", err}}
	}
	return nil
}

type JsonSnapshotSerializer struct{}

func (s *JsonSnapshotSerializer) Serialize(aggregate Aggregate) ([]byte, error) {
	result, err := json.Marshal(aggregate)
	if err != nil {
		return nil, &SerializationError{EventStoreBaseError{"Failed to serialize the snapshot", err}}
	}
	return result, nil
}

func (s *JsonSnapshotSerializer) Deserialize(data []byte, aggregateMap *map[string]interface{}) error {
	err := json.Unmarshal(data, aggregateMap)
	if err != nil {
		return &DeserializationError{EventStoreBaseError{"Failed to deserialize the snapshot", err}}
	}
	return nil
}
