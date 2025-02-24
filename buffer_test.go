package lemonade

import (
	"testing"
)

func TestBufferEncode(t *testing.T) {
	buffer, err := EncodeStringToBuffer("Hello World")
	if err != nil {
		t.Error(err)
	}

	expectedBuffer := []int32{34,5,12,12,15,63,49,15,18,12,4} 

	for idx, value := range(expectedBuffer) {
		if buffer[idx] != value {
			t.Errorf("Expected (%d), got (%d)", value, buffer[idx])
		}
	}
}

func TestBufferDecode(t * testing.T) {
	buffer := &LemonadeBuffer{
		Buffer: []int32{34,5,12,12,15,63,49,15,18,12,4},
	}

	receivedString, err := buffer.DecodeToString()
	if err != nil {
		t.Error(err)
	}

	expectedString := "Hello World"
	if receivedString != expectedString {
		t.Errorf("Expected (%s), got (%s)", expectedString, receivedString)
	}
}

func TestInvalidEncode(t *testing.T) {
	_, err := EncodeStringToBuffer("Hello	World")
	if err == nil {
		t.Errorf("Expected an error, got none.")
	}
}
