package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
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
// Unknown JSON fields wrap ErrUnknownField.
func Decode(frame []byte) (Data, error) {
	var tmpData dataJSON

	decoder := json.NewDecoder(bytes.NewReader(frame))
	if err := decodeStrictJSON(decoder, &tmpData); err != nil {
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

func decodeStrictJSON(decoder *json.Decoder, target any) error {
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		// encoding/json exposes unknown fields only through its error text.
		if strings.HasPrefix(err.Error(), "json: unknown field ") {
			return fmt.Errorf("%w: %w", ErrUnknownField, err)
		}
		return err
	}
	return nil
}
