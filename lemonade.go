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

	READER_IDLE     int32 = 0
	READER_RESERVED int32 = 1
	READER_READY    int32 = 2

	WRITER_IDLE      int32 = 0
	WRITER_AVAILABLE int32 = 1

	BUFFER_MAX_SIZE = 26
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
	if len(buffer) <= BUFFER_MAX_SIZE {
		l.writeManager.Queue(
			&LemonadeBuffer{
				Buffer: buffer,
				Set:    BUFFER_INDIVIDUAL,
			},
		)

		return nil
	}

	ChunkSlice(buffer, BUFFER_MAX_SIZE, func(partialSlice []int32, isEnd bool) {
		lemonBuffer := LemonadeBuffer{}

		lemonBuffer.Buffer = partialSlice

		if isEnd {
			lemonBuffer.Set = BUFFER_SET_END
		} else {
			lemonBuffer.Set = BUFFER_SET_CHUNK
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
					if instance.NotITG.GetExternal(60) == 0 {
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

				if instance.NotITG.GetExternal(57) == WRITER_AVAILABLE && instance.NotITG.GetExternal(59) == instance.AppID {
					bufferLength := int(instance.NotITG.GetExternal(54))
					buffer := make([]int32, bufferLength)

					for idx := 0; idx < bufferLength; idx++ {
						buffer[idx] = instance.NotITG.GetExternal(28 + idx)
						instance.NotITG.SetExternal(28+idx, 0)
					}

					if instance.OnRead != nil {
						instance.OnRead(instance, buffer)
					}

					bufferStatus := instance.NotITG.GetExternal(55)
					if bufferStatus == int32(BUFFER_INDIVIDUAL) {
						if instance.OnBufferRead != nil {
							instance.OnBufferRead(instance, buffer)
						}
					} else {
						instance.readBuffers = append(instance.readBuffers, buffer...)
						if bufferStatus == int32(BUFFER_SET_END) {
							instance.OnBufferRead(instance, instance.readBuffers)
							instance.readBuffers = nil
						}
					}

					instance.NotITG.SetExternal(54, 0)
					instance.NotITG.SetExternal(55, 0)
					instance.NotITG.SetExternal(59, 0)
					instance.NotITG.SetExternal(57, 0)
				}
				if len(instance.writeManager.Buffers) > 0 && instance.NotITG.GetExternal(56) == READER_IDLE {
					instance.NotITG.SetExternal(56, READER_RESERVED)
					buffer := instance.writeManager.Dequeue()

					for idx, value := range buffer.Buffer {
						instance.NotITG.SetExternal(idx, value)
					}
					instance.NotITG.SetExternal(26, int32(len(buffer.Buffer)))
					instance.NotITG.SetExternal(27, int32(buffer.Set))
					instance.NotITG.SetExternal(58, instance.AppID)
					instance.NotITG.SetExternal(56, READER_READY)

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
