package tools

import "github.com/oklog/ulid/v2"

// GenerateULID 生成 ULID（Universally Unique Lexicographically Sortable Identifier）
// 返回 26 字符的 ULID，包含时间戳和随机部分，支持时间排序
func GenerateULID() string {
	return ulid.Make().String()
}
