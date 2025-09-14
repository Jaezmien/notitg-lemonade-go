package lemonade

import (
	"errors"
	"strings"
	"sync"
	"unicode"
)

type LemonadeBuffer struct {
	Buffer []int32
	Set    LemonadeBufferSet
}

type LemonadeBufferManager struct {
	Buffers []*LemonadeBuffer
	mutex sync.Mutex
}
func NewBufferManager() *LemonadeBufferManager {
	return &LemonadeBufferManager{
		Buffers: make([]*LemonadeBuffer, 0),
	}
}
func (m *LemonadeBufferManager) Queue(buffer *LemonadeBuffer) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Buffers = append(m.Buffers, buffer)
}
func (m *LemonadeBufferManager) Dequeue() *LemonadeBuffer {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.Buffers) <= 0 {
		return nil
	}

	buffer := m.Buffers[0]
	m.Buffers = m.Buffers[1:]

	return buffer
}

type LemonadeBufferSet uint

const (
	BUFFER_INDIVIDUAL LemonadeBufferSet = 0
	BUFFER_SET_CHUNK  LemonadeBufferSet = 1
	BUFFER_SET_END    LemonadeBufferSet = 2
)

func stringSatifiesEncoder(str string) bool {
	for _, char := range(str) {
		if char > unicode.MaxASCII {
			return false
		}
		if char < 0 {
			return false
		}
	}
	
	return true
}

func EncodeStringToBuffer(str string) ([]int32, error) {
	if(!stringSatifiesEncoder(str)) {
		return nil, errors.New("Invalid string")
	}

	buffer := make([]int32, len(str))

	for idx, char := range(str) {
		buffer[idx] = int32(char)
	}

	return buffer, nil
}

func (b *LemonadeBuffer) DecodeToString() (string, error) {
	return DecodeBufferToString(b.Buffer)
}
func DecodeBufferToString(buffer []int32) (string, error) {
	var sb strings.Builder

	for _, idx := range(buffer) {
		err := sb.WriteByte(byte(idx))

		if err != nil {
			return "", err
		}
	}

	return sb.String(), nil
}
