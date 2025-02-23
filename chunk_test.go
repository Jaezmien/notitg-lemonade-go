package lemonade

import (
	"testing"
)

func TestChunkSlice(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("should do nothing on minimal slice", func(t *testing.T) {
		ChunkSlice(slice, len(slice), func(partialSlice []int, isEnd bool) {
			if len(partialSlice) != len(slice) {
				t.Errorf("Expected same slice count (%d), got %d", len(slice), len(partialSlice))
				return
			}
			t.Logf("Got: %v", partialSlice)
		})
	})
	t.Run("should slice twice", func(t *testing.T) {
		sliceCount := 0

		ChunkSlice(slice, 4, func(partialSlice []int, isEnd bool) {
			if len(partialSlice) > 4 {
				t.Errorf("Expected a maximum chunk amount of 4, got %d", len(partialSlice))
				return
			}
			t.Logf("Got: %v", partialSlice)

			sliceCount += 1
		})

		if sliceCount != 2 {
			t.Errorf("Expected 2 slices, got %d", sliceCount)
			return
		}
	})
	t.Run("should slice thrice", func(t *testing.T) {
		sliceCount := 0

		ChunkSlice(slice, 3, func(partialSlice []int, isEnd bool) {
			if len(partialSlice) > 3 {
				t.Errorf("Expected a maximum chunk amount of 3, got %d", len(partialSlice))
				return
			}
			t.Logf("Got: %v", partialSlice)

			sliceCount += 1

			if sliceCount == 3 && !isEnd {
				t.Errorf("Expected last slice to be end")
				return
			}
		})

		if sliceCount != 3 {
			t.Errorf("Expected 3 slices, got %d", sliceCount)
			return
		}
	})
}
