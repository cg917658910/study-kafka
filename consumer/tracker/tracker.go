package tracker

import (
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// 单个 offset 的状态结构体
type OffsetStatus struct {
	State     string    // "pending" or "done"
	Timestamp time.Time // 创建或完成时间
}

// OffsetState 表示单个分区的 offset 跟踪器
type OffsetState struct {
	mu       sync.Mutex
	status   map[int64]*OffsetStatus
	minReady int64
	ttl      time.Duration
}

type (
	TopicName   string
	PartitionID int32
)
type KafkaSafeConsumer struct {
	trackers   map[TopicName]map[PartitionID]*OffsetState
	trackersMu sync.Mutex
}

func NewKafkaSafeConsumer() *KafkaSafeConsumer {

	return &KafkaSafeConsumer{
		trackers: make(map[TopicName]map[PartitionID]*OffsetState),
	}
}

// get and set
func (c *KafkaSafeConsumer) GetTracker(topic TopicName, partition PartitionID) *OffsetState {
	c.trackersMu.Lock()
	defer c.trackersMu.Unlock()
	trackerTopic, ok := c.trackers[topic]
	if !ok {
		trackerTopic = make(map[PartitionID]*OffsetState)
		c.trackers[topic] = trackerTopic
	}
	tracker, ok := trackerTopic[partition]
	if !ok {
		tracker = NewOffsetState()
		c.trackers[topic][partition] = tracker
	}
	return tracker
}

// safe commit
func (c *KafkaSafeConsumer) SafeCommit(session sarama.ConsumerGroupSession) {
	if session == nil {
		return
	}
	c.trackersMu.Lock()
	defer c.trackersMu.Unlock()
	// mark offset
	for topic, partitions := range c.trackers {
		for partition, tracker := range partitions {
			safeOffset := tracker.CommitOffset()
			session.MarkOffset(string(topic), int32(partition), safeOffset, "")
		}
	}
	session.Commit()
}

// 构造一个新的 OffsetState
func NewOffsetState() *OffsetState {
	return &OffsetState{
		status:   make(map[int64]*OffsetStatus),
		minReady: -1,
		ttl:      10 * time.Minute,
		mu:       sync.Mutex{},
	}
}

// Init 标记 offset
func (s *OffsetState) SetInitOffset(initOffset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minReady = initOffset
}

// Start 标记 offset 开始处理
func (s *OffsetState) Start(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[offset] = &OffsetStatus{
		State:     "pending",
		Timestamp: time.Now(),
	}
}

// Done 标记 offset 完成
func (s *OffsetState) Done(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[offset] = &OffsetStatus{
		State:     "done",
		Timestamp: time.Now(),
	}
}

// CommitOffset 找出最大连续 done 的 offset，并清理过期状态
func (s *OffsetState) CommitOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: 清理 TTL 过期的 "done" 状态

	next := s.minReady + 1
	for {
		entry, ok := s.status[next]
		if !ok || entry.State != "done" {
			break
		}
		delete(s.status, next)
		s.minReady = next
		next++
	}
	return s.minReady + 1
}
