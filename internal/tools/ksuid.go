package tools

import "github.com/segmentio/ksuid"

// GenerateKSUID 生成 KSUID（K-Sortable Unique Identifier）
// 返回 27 字符的 KSUID，包含时间戳和随机部分，支持时间排序
func GenerateKSUID() string {
	return ksuid.New().String()
}
