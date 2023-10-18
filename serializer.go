package event_store_adapter_go

import "encoding/json"

type DefaultEventSerializer struct{}

func (s *DefaultEventSerializer) Serialize(event Event) ([]byte, error) {
	result, err := json.Marshal(event)
	if err != nil {
		return nil, &SerializationError{EventStoreBaseError{"Failed to serialize the event", err}}
	}
	return result, nil
}

func (s *DefaultEventSerializer) Deserialize(data []byte, eventMap *map[string]interface{}) error {
	err := json.Unmarshal(data, eventMap)
	if err != nil {
		return &DeserializationError{EventStoreBaseError{"Failed to deserialize the event", err}}
	}
	return nil
}

type DefaultSnapshotSerializer struct{}

func (s *DefaultSnapshotSerializer) Serialize(aggregate Aggregate) ([]byte, error) {
	result, err := json.Marshal(aggregate)
	if err != nil {
		return nil, &SerializationError{EventStoreBaseError{"Failed to serialize the snapshot", err}}
	}
	return result, nil
}

func (s *DefaultSnapshotSerializer) Deserialize(data []byte, aggregateMap *map[string]interface{}) error {
	err := json.Unmarshal(data, aggregateMap)
	if err != nil {
		return &DeserializationError{EventStoreBaseError{"Failed to deserialize the snapshot", err}}
	}
	return nil
}
