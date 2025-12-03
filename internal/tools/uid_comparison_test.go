package tools

import (
	"sync"
	"testing"
)

// TestUID_LengthComparison 测试三种方案生成的 ID 长度
func TestUID_LengthComparison(t *testing.T) {
	tests := []struct {
		name     string
		generate func() string
		wantLen  int
	}{
		{
			name:     "NanoID(16)",
			generate: func() string { return GetNanoIdBy(16) },
			wantLen:  16,
		},
		{
			name:     "ULID",
			generate: func() string { return GenerateULID() },
			wantLen:  26,
		},
		{
			name:     "KSUID",
			generate: func() string { return GenerateKSUID() },
			wantLen:  27,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 生成多个 ID 确保长度一致
			for i := 0; i < 10; i++ {
				id := tt.generate()
				if len(id) != tt.wantLen {
					t.Errorf("%s length = %v, want %v (ID: %s)", tt.name, len(id), tt.wantLen, id)
				}
			}
		})
	}
}

// BenchmarkUID_Comparison 对比测试，在同一基准下测试三种方案
func BenchmarkUID_Comparison(b *testing.B) {
	b.Run("NanoID_16", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			GetNanoIdBy(16)
		}
	})

	b.Run("ULID", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			GenerateULID()
		}
	})

	b.Run("KSUID", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			GenerateKSUID()
		}
	})
}

// BenchmarkUID_Parallel 并发性能测试
func BenchmarkUID_Parallel(b *testing.B) {
	b.Run("NanoID_16", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				GetNanoIdBy(16)
			}
		})
	})

	b.Run("ULID", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				GenerateULID()
			}
		})
	})

	b.Run("KSUID", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				GenerateKSUID()
			}
		})
	})
}

// TestUID_Uniqueness_Comparison 对比测试，生成 1,000,000 个 ID 进行唯一性对比
func TestUID_Uniqueness_Comparison(t *testing.T) {
	const count = 1000000

	tests := []struct {
		name     string
		generate func() string
	}{
		{
			name:     "NanoID(16)",
			generate: func() string { return GetNanoIdBy(16) },
		},
		{
			name:     "ULID",
			generate: func() string { return GenerateULID() },
		},
		{
			name:     "KSUID",
			generate: func() string { return GenerateKSUID() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := make(map[string]bool, count)
			duplicates := 0

			for i := 0; i < count; i++ {
				id := tt.generate()
				if ids[id] {
					duplicates++
					if duplicates == 1 {
						t.Errorf("%s found duplicate ID at iteration %d: %s", tt.name, i, id)
					}
				}
				ids[id] = true
			}

			uniqueCount := len(ids)
			if uniqueCount != count {
				t.Errorf("%s uniqueness: got %v unique IDs (expected %v), found %d duplicates", tt.name, uniqueCount, count, duplicates)
			} else {
				t.Logf("%s: Generated %d unique IDs, no duplicates found", tt.name, uniqueCount)
			}
		})
	}
}

// TestUID_ConcurrentSafety 测试三种方案在并发场景下的安全性
func TestUID_ConcurrentSafety(t *testing.T) {
	const goroutines = 100
	const idsPerGoroutine = 1000

	tests := []struct {
		name     string
		generate func() string
	}{
		{
			name:     "NanoID(16)",
			generate: func() string { return GetNanoIdBy(16) },
		},
		{
			name:     "ULID",
			generate: func() string { return GenerateULID() },
		},
		{
			name:     "KSUID",
			generate: func() string { return GenerateKSUID() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := make(chan string, goroutines*idsPerGoroutine)
			var wg sync.WaitGroup

			// 启动多个 goroutine 并发生成 ID
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < idsPerGoroutine; j++ {
						ids <- tt.generate()
					}
				}()
			}

			wg.Wait()
			close(ids)

			// 检查唯一性
			uniqueIds := make(map[string]bool)
			duplicates := 0
			totalCount := 0

			for id := range ids {
				totalCount++
				if uniqueIds[id] {
					duplicates++
					if duplicates == 1 {
						t.Errorf("%s concurrent test: duplicate ID found: %s", tt.name, id)
					}
				}
				uniqueIds[id] = true
			}

			expectedCount := goroutines * idsPerGoroutine
			if len(uniqueIds) != expectedCount {
				t.Errorf("%s concurrent uniqueness: got %v unique IDs (expected %v), found %d duplicates", tt.name, len(uniqueIds), expectedCount, duplicates)
			} else {
				t.Logf("%s: Concurrent test passed - Generated %d unique IDs from %d total, no duplicates", tt.name, len(uniqueIds), totalCount)
			}
		})
	}
}
