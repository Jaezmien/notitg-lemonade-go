package lemonade

import (
	"math"
)

func ChunkSlice[T comparable](slice []T, count int, onChunk func(partialSlice []T, isEnd bool)) {
	ChunkCount := int(math.Ceil(float64(len(slice)) / float64(count)))

	chunkIndex := 0
	for chunkIndex < ChunkCount {
		chunkStartIndex := chunkIndex * count

		var chunkAmount int
		var isChunkEnd bool
		if chunkStartIndex+count < len(slice) {
			chunkAmount = count
			isChunkEnd = false
		} else {
			chunkAmount = len(slice) - chunkStartIndex
			isChunkEnd = true
		}

		chunk := slice[chunkStartIndex : chunkStartIndex+chunkAmount]

		onChunk(chunk, isChunkEnd)

		chunkIndex++
	}
}
