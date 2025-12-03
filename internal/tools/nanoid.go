package tools

import nanoid "github.com/matoous/go-nanoid/v2"

const (
	defaultAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz-"
	defaultSize     = 12
)

func GetNanoId() string {

	id, _ := nanoid.Generate(defaultAlphabet, defaultSize)
	return id
}

func GetNanoIdBy(length int) string {

	id, _ := nanoid.Generate(defaultAlphabet, length)
	return id
}
