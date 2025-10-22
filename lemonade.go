package lemonade

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/Jaezmien/notitg-external-go"
)

const (
	HEARTBEAT_EXITED uint = iota
	HEARTBEAT_NOTFOUND
	HEARTBEAT_FOUND
)

const (
	INCOMING_ID = 0
	INCOMING_TYPE = 1
	INCOMING_LENGTH = 2
	INCOMING_DATA_START = 3
	INCOMING_DATA_END = 29
	INCOMING_STATE = 30

	OUTGOING_ID = 31
	OUTGOING_TYPE = 32
	OUTGOING_LENGTH = 33
	OUTGOING_DATA_START = 34
	OUTGOING_DATA_END = 60
	OUTGOING_STATE = 61

	INIT_STATE = 63
)

const (
	STATE_INCOMING_IDLE = 0
	STATE_INCOMING_BUSY = 1
	STATE_INCOMING_AVAILABLE = 2

	STATE_OUTGOING_IDLE = 0
	STATE_OUTGOING_AVAILABLE = 1
)

const (
	INIT_EXPECTED_VALUE = 56
	MAXIMUM_BUFFER_LENGTH = 29 - 3
)

type Lemonade struct {
	NotITG *notitg.NotITG

	AppID int32

	OnConnect    func(l *Lemonade)
	OnDisconnect func(l *Lemonade)
	OnExit       func(l *Lemonade)

	OnBufferRead func(l *Lemonade, buffer []int32)
	OnRead       func(l *Lemonade, buffer []int32)
	OnWrite      func(l *Lemonade, buffer []int32, set LemonadeBufferSet)

	MSTickRate int

	channel   chan struct{}
	waitGroup sync.WaitGroup

	initialized     bool
	heartbeatStatus uint

	readBuffers  []int32
	writeManager *LemonadeBufferManager

	Logger *log.Logger
}

func (l *Lemonade) IsInitialized() bool {
	return l.initialized
}

func (l *Lemonade) WriteString(str string) error {
	buffer, err := EncodeStringToBuffer(str)
	if err != nil {
		return err
	}

	return l.WriteBuffer(buffer)
}
func (l *Lemonade) WriteBuffer(buffer []int32) error {
	if len(buffer) <= MAXIMUM_BUFFER_LENGTH {
		l.writeManager.Queue(
			&LemonadeBuffer{
				Buffer: buffer,
				Set:    BUFFER_END,
			},
		)

		return nil
	}

	ChunkSlice(buffer, MAXIMUM_BUFFER_LENGTH, func(partialSlice []int32, isEnd bool) {
		lemonBuffer := LemonadeBuffer{
			Buffer: partialSlice,
		}

		if isEnd {
			lemonBuffer.Set = BUFFER_END
		} else {
			lemonBuffer.Set = BUFFER_PARTIAL
		}

		l.writeManager.Queue(&lemonBuffer)
	})

	return nil
}

type LemonadeInstanceConfig struct {
	DeepScan	bool
	ProcessID	int
	TickRate int
}

func New(AppID int32, config *LemonadeInstanceConfig) (*Lemonade, error) {
	if config == nil {
		config = &LemonadeInstanceConfig{
			DeepScan: false,
			ProcessID: 0,
			TickRate: 10,
		}
	}

	lemonadeInstance := &Lemonade{
		AppID:        AppID,
		MSTickRate:   config.TickRate,

		readBuffers:  make([]int32, 0),
		writeManager: NewBufferManager(),
		channel:      make(chan struct{}),
		
		Logger: log.New(os.Stdout, "Lemonade: ", log.Ldate|log.Ltime|log.Lshortfile),
	}

	go func(instance *Lemonade) {
		lemonadeTicker := time.NewTicker(time.Second * 2)

		for {
			select {
			case <-lemonadeTicker.C:

				// Check for process heartbeat
				hasHeartbeat := instance.NotITG != nil && instance.NotITG.Heartbeat()
				if !hasHeartbeat {
					if instance.heartbeatStatus != HEARTBEAT_FOUND {
						var nitg *notitg.NotITG
						var err error

						if config.ProcessID != 0 {
							nitg, err = notitg.ScanProcessID(config.ProcessID)
						} else {
							nitg, err = notitg.Scan(config.DeepScan)
						}

						if err != nil {
							panic(err)
						}

						if nitg == nil {
							instance.Logger.Println("NotITG not detected, retrying in 2 seconds...")
							break
						}

						if nitg.Version <= notitg.V2 {
							instance.Logger.Printf("Unsupported NotITG version! Expected V3 or higher.")
							break
						}

						instance.NotITG = nitg
						instance.heartbeatStatus = HEARTBEAT_FOUND
					} else if instance.heartbeatStatus == HEARTBEAT_EXITED {
						instance.heartbeatStatus = HEARTBEAT_NOTFOUND
						instance.initialized = false
					} else if instance.heartbeatStatus == HEARTBEAT_FOUND {
						instance.heartbeatStatus = HEARTBEAT_EXITED
						instance.initialized = false

						if instance.OnDisconnect != nil {
							instance.OnDisconnect(instance)
						}
						instance.NotITG = nil

						instance.Logger.Println("NotITG has exited!")
						lemonadeTicker.Reset(time.Second * 2)
					}

					break
				}

				// Check for Lemonade initialization
				if !instance.initialized {
					if instance.NotITG.GetExternal(INIT_STATE) != INIT_EXPECTED_VALUE {
						instance.Logger.Println("NotITG is currently initializing...")
						break
					}

					instance.initialized = true
					if instance.OnConnect != nil {
						instance.OnConnect(instance)
					}

					instance.Logger.Println("NotITG has initialized!")
					lemonadeTicker.Reset(time.Millisecond * time.Duration(instance.MSTickRate))
				}

				// Outgoing (Incoming from NotITG)
				if instance.NotITG.GetExternal(OUTGOING_STATE) == STATE_OUTGOING_AVAILABLE &&
					instance.NotITG.GetExternal(OUTGOING_ID) == instance.AppID {
					bufferLength := int(instance.NotITG.GetExternal(OUTGOING_LENGTH))
					buffer := make([]int32, bufferLength)

					for idx := range bufferLength {
						flagIdx := OUTGOING_DATA_START + idx
						buffer[idx] = instance.NotITG.GetExternal(flagIdx)
						instance.NotITG.SetExternal(flagIdx, 0)
					}

					isBufferEnd := instance.NotITG.GetExternal(OUTGOING_TYPE) == int32(BUFFER_END)

					instance.NotITG.SetExternal(OUTGOING_LENGTH, 0)
					instance.NotITG.SetExternal(OUTGOING_STATE, 0)
					instance.NotITG.SetExternal(OUTGOING_ID, 0)
					instance.NotITG.SetExternal(OUTGOING_STATE, STATE_OUTGOING_IDLE)

					if instance.OnRead != nil {
						instance.OnRead(instance, buffer)
					}

					if isBufferEnd {
						if len(instance.readBuffers) > 0 {
							buffer = append(instance.readBuffers, buffer...)
						}
						
						if instance.OnBufferRead != nil {
							instance.OnBufferRead(instance, buffer)
						}

						instance.readBuffers = nil
					} else {
						if instance.readBuffers == nil {
							instance.readBuffers = make([]int32, 0)
						}

						instance.readBuffers = append(instance.readBuffers, buffer...)
					}
				}

				// (Incoming) Outgoing to NotITG
				if len(instance.writeManager.Buffers) > 0 &&
					instance.NotITG.GetExternal(INCOMING_STATE) == STATE_INCOMING_IDLE {
					instance.NotITG.SetExternal(INCOMING_STATE, STATE_INCOMING_BUSY)
					buffer := instance.writeManager.Dequeue()

					for idx, value := range buffer.Buffer {
						flagIdx := INCOMING_DATA_START + idx
						instance.NotITG.SetExternal(flagIdx, value)
					}
					instance.NotITG.SetExternal(INCOMING_LENGTH, int32(len(buffer.Buffer)))

					instance.NotITG.SetExternal(INCOMING_TYPE, int32(buffer.Set))
					instance.NotITG.SetExternal(INCOMING_ID, instance.AppID)
					instance.NotITG.SetExternal(INCOMING_STATE, STATE_INCOMING_AVAILABLE)

					if instance.OnWrite != nil {
						instance.OnWrite(instance, buffer.Buffer, buffer.Set)
					}
				}

			case <-instance.channel:
				lemonadeTicker.Stop()
				return
			}
		}
	}(lemonadeInstance)

	return lemonadeInstance, nil
}
func (l *Lemonade) Close() {
	if l.channel == nil {
		return
	}

	if l.OnExit != nil {
		l.OnExit(l)
	}

	close(l.channel)
	l.channel = nil
}
