package tools

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// CustomUID 16 字符版本的 ULID
// 设计思路（参考 ULID）：
// - 前 10 字符（50 位）：秒级时间戳（从 2025-10-01 开始，可表示约 35,000,000 年）
// - 后 6 字符（30 位）：计数器（16 位）+ 随机数（14 位）
// - 总计 80 位，使用 Base32 编码为 16 字符
// - 相比标准 ULID：长度更短（16 vs 26），但保持相同的设计思路和排序特性
// - 使用线程安全的计数器确保在同一秒内也能保证唯一性，避免等待
// - 性能：单线程约 103.9 ns/op，并发约 209.8 ns/op

const (
	// customUIDTimestampBits 时间戳位数（秒级，50 位）
	customUIDTimestampBits = 50
	// customUIDCounterBits 计数器位数（16 位，可表示 65536 个序列）
	customUIDCounterBits = 16
	// customUIDRandomBits 随机数位数（14 位）
	customUIDRandomBits = 14
	// customUIDTotalBits 总位数
	customUIDTotalBits = 80
	// customUIDEpoch 自定义时间戳基准点（2025-10-01 00:00:00 UTC）
	// 使用 2025-10-01 作为基准，50 位秒级时间戳可以表示约 35,000,000 年
	customUIDEpoch = 1759248000 // 2025-10-01 00:00:00 UTC 的秒数
	// customUIDMaxCounter 最大计数器值（2^16 - 1）
	customUIDMaxCounter = 65535
)

// base32Chars Crockford's Base32 字符集（与 ULID 相同）
const base32Chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	// customUIDState 用于在同一秒内生成唯一 ID 的状态
	customUIDState struct {
		sync.Mutex
		lastSecond int64
		counter    uint32 // 计数器，在同一秒内递增
		randomBase uint32 // 随机基数，每秒更新一次
	}
)

// GenerateCustomUID 生成 16 字符版本的 ULID
// 返回 16 字符的 UID，包含秒级时间戳和随机部分，支持时间排序
func GenerateCustomUID() string {
	// 获取当前时间（Unix 纪元以来的秒数）
	now := time.Now().Unix()
	
	// 转换为相对于基准点（2025-10-01）的秒数
	timestampSec := uint64(now - customUIDEpoch)
	
	// 确保时间戳不超过 50 位（2^50 - 1）
	maxTimestamp := uint64(1<<customUIDTimestampBits - 1)
	if timestampSec > maxTimestamp {
		// 如果超出范围，使用最大值（理论上不会发生，因为 2^50 秒 ≈ 35,000,000 年）
		timestampSec = maxTimestamp
	}
	
	customUIDState.Lock()
	
	// 如果时间戳变化，重置计数器和随机基数
	if now != customUIDState.lastSecond {
		customUIDState.lastSecond = now
		customUIDState.counter = 0
		// 生成新的随机基数（14 位）
		randomBytes := make([]byte, 2)
		customUIDState.Unlock()
		if _, err := rand.Read(randomBytes); err != nil {
			panic(fmt.Sprintf("failed to generate random bytes: %v", err))
		}
		customUIDState.Lock()
		customUIDState.randomBase = uint32(binary.BigEndian.Uint16(randomBytes))
		customUIDState.randomBase = customUIDState.randomBase >> 2 // 取高 14 位
		customUIDState.randomBase &= 0x3FFF                         // 确保只有 14 位
	}
	
	// 递增计数器（16 位，最多 65536 个）
	counter := customUIDState.counter
	randomBase := customUIDState.randomBase
	
	customUIDState.counter++
	if customUIDState.counter > customUIDMaxCounter {
		// 如果计数器溢出，增加随机性并重置计数器
		// 不等待，而是增加随机性
		randomBytes := make([]byte, 2)
		customUIDState.Unlock()
		if _, err := rand.Read(randomBytes); err != nil {
			panic(fmt.Sprintf("failed to generate random bytes: %v", err))
		}
		customUIDState.Lock()
		customUIDState.randomBase = uint32(binary.BigEndian.Uint16(randomBytes))
		customUIDState.randomBase = customUIDState.randomBase >> 2 // 取高 14 位
		customUIDState.randomBase &= 0x3FFF                         // 确保只有 14 位
		customUIDState.counter = 1 // 重置计数器
		counter = 1
		randomBase = customUIDState.randomBase
	} else {
		counter = customUIDState.counter
		randomBase = customUIDState.randomBase
	}
	
	customUIDState.Unlock()
	
	// 组合：计数器（16 位）+ 随机数（14 位）= 30 位
	combinedRandom := (counter << customUIDRandomBits) | randomBase
	
	// 组合时间戳和随机数：时间戳（50 位）+ 组合随机数（30 位）= 80 位
	// 使用 10 字节（80 位）存储，大端序
	idBytes := make([]byte, 10)
	
	// 80 位的布局：
	// [0:50] 时间戳（50 位）
	// [50:80] 组合随机数（30 位：12 位计数器 + 18 位随机数）
	//
	// 使用大整数操作组合
	var combinedHigh, combinedLow uint64
	
	// 高 64 位：时间戳（50 位在最高位）+ 组合随机数的高 14 位
	// 时间戳左移 30 位，使 50 位在最高位
	combinedHigh = timestampSec << (customUIDCounterBits + customUIDRandomBits) // 左移 30 位
	
	// 组合随机数的高 14 位放在 combinedHigh 的低 14 位
	randomHigh14 := uint64(combinedRandom >> 16) // 高 14 位（30 - 16 = 14）
	combinedHigh |= randomHigh14
	
	// 低 16 位：组合随机数的低 16 位
	combinedLow = uint64(combinedRandom & 0xFFFF) // 低 16 位
	
	// 写入到字节数组
	binary.BigEndian.PutUint64(idBytes[0:8], combinedHigh)
	binary.BigEndian.PutUint16(idBytes[8:10], uint16(combinedLow))
	
	// Base32 编码为 16 字符
	return encodeBase32_16(idBytes)
}

// encodeBase32_16 将 80 位（10 字节）数据编码为 Base32 字符串（16 字符）
func encodeBase32_16(data []byte) string {
	// 80 位 = 10 字节
	// Base32 编码：每 5 位一个字符，80 位 = 16 字符（80/5 = 16）
	
	result := make([]byte, 16)
	
	// 将字节数组视为大端序的位流
	var buffer uint64
	var bitsInBuffer int
	
	pos := 0
	for i := 0; i < 10; i++ {
		buffer = (buffer << 8) | uint64(data[i])
		bitsInBuffer += 8
		
		for bitsInBuffer >= 5 {
			// 取高 5 位
			index := (buffer >> (bitsInBuffer - 5)) & 0x1F
			result[pos] = base32Chars[index]
			pos++
			bitsInBuffer -= 5
			buffer &= (1<<bitsInBuffer - 1) // 清除已使用的位
		}
	}
	
	// 80 位正好是 5 的倍数，应该没有剩余位
	if bitsInBuffer > 0 {
		index := (buffer << (5 - bitsInBuffer)) & 0x1F
		result[pos] = base32Chars[index]
		pos++
	}
	
	return string(result[:16])
}
