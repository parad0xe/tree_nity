package protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

// Encode returns the JSON encoding of data after validation.
func Encode(data Data) ([]byte, error) {
	tmpData, err := data.toJSON()
	if err != nil {
		return nil, err
	}

	return json.Marshal(tmpData)
}

// Decode returns validated protocol data from a single JSON frame.
func Decode(frame []byte) (Data, error) {
	var tmpData dataJSON

	decoder := json.NewDecoder(bytes.NewReader(frame))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&tmpData); err != nil {
		return Data{}, err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			return Data{}, ErrAdditionalJSONValue
		}
		return Data{}, err
	}

	return tmpData.toData()
}
