package tracker

import (
	"testing"
	"time"
)

func TestOffsetCommit_Sequential(t *testing.T) {
	s := NewOffsetState()
	s.minReady = 0

	s.Done(1)
	s.Done(2)
	s.Done(3)

	commit := s.CommitOffset()
	if commit != 4 {
		t.Errorf("expected 4, got %d", commit)
	}
}

func TestOffsetCommit_WithGap(t *testing.T) {
	s := NewOffsetState()
	s.minReady = 0

	s.Done(1)
	s.Done(2)
	s.Done(4)

	commit := s.CommitOffset()
	if commit != 3 {
		t.Errorf("expected 3, got %d", commit)
	}

	s.Done(3)
	commit = s.CommitOffset()
	if commit != 5 {
		t.Errorf("expected 5 after filling gap, got %d", commit)
	}
}

func TestOffsetCommit_OutOfOrder(t *testing.T) {
	s := NewOffsetState()
	s.minReady = 3

	s.Done(12)
	s.Done(5)
	s.Done(8)
	s.Done(4)
	s.Done(7)
	s.Done(6)

	commit := s.CommitOffset()
	if commit != 9 {
		t.Errorf("expected commit offset 9, got %d", commit)
	}
}

func TestOffsetCommit_TTLExpire(t *testing.T) {
	s := NewOffsetState()
	s.minReady = 1
	s.ttl = 1 * time.Second

	s.Done(2)
	s.Done(3)
	s.Done(4)

	time.Sleep(1500 * time.Millisecond)

	commit := s.CommitOffset()
	if commit != 2 {
		t.Errorf("expected commit offset 2 due to TTL cleanup, got %d", commit)
	}

	// Recover after re-setting done
	s.Done(2)
	s.Done(3)
	s.Done(4)
	commit = s.CommitOffset()
	if commit != 5 {
		t.Errorf("expected commit offset 5 after recovery, got %d", commit)
	}
}

func TestOffsetCommit_PartialOnly(t *testing.T) {
	s := NewOffsetState()
	s.minReady = 5

	s.Done(6)
	s.Done(8)
	s.Done(9)

	commit := s.CommitOffset()
	if commit != 7 {
		t.Errorf("expected commit offset 6 (only 6 is contiguous), got %d", commit)
	}

	s.Done(7)
	commit = s.CommitOffset()
	if commit != 10 {
		t.Errorf("expected commit offset 10 after filling gap, got %d", commit)
	}
}

func TestGetTracker_New(t *testing.T) {
	ctl := NewKafkaSafeConsumer()
	tracker := ctl.GetTracker(TopicName("test"), PartitionID(1))
	tracker.SetInitOffset(9)
	tracker.Start(9)
	tracker.Done(9)
	commit := tracker.CommitOffset()
	if commit != 10 {
		t.Errorf("expected commit offset 10 after filling gap, got %d", commit)
	}
}
