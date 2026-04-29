package daemon

import "encoding/json"

type relayEventID string

func (id *relayEventID) UnmarshalJSON(data []byte) error {
	var stringID string
	if err := json.Unmarshal(data, &stringID); err == nil {
		*id = relayEventID(stringID)
		return nil
	}

	var numberID json.Number
	if err := json.Unmarshal(data, &numberID); err != nil {
		return err
	}
	*id = relayEventID(numberID.String())
	return nil
}
