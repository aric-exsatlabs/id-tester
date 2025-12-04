package uidstress

import (
	"bufio"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"

	"id-tester/internal/tools"
)

const (
	defaultApproxBytesPerID = 64
)

// Config controls how the stress test runs.
type Config struct {
	Schemes          []string
	Scale            int64
	ChunkSize        int64
	Workers          int
	TempDir          string
	KeepTempData     bool
	LogInterval      int64
	Verbose          bool
	ApproxBytesPerID int64
	MemGuardMB       float64
	DiskSafetyFactor float64
}

// Result captures the summary for each scheme.
type Result struct {
	Scheme       string
	Chunks       int
	Duration     time.Duration
	Generated    int64
	ChunkUnique  int64
	Unique       int64
	Duplicates   int64
	ManifestPath string
	OutputDir    string
}

type chunkMeta struct {
	Index         int       `json:"index"`
	Path          string    `json:"path"`
	UniqueCount   int64     `json:"unique_count"`
	OriginalCount int64     `json:"original_count"`
	Hash          string    `json:"hash"`
	SizeBytes     int64     `json:"size_bytes"`
	CreatedAt     time.Time `json:"created_at"`
}

type manifest struct {
	Scheme           string      `json:"scheme"`
	Scale            int64       `json:"scale"`
	ChunkSize        int64       `json:"chunk_size"`
	ApproxBytesPerID int64       `json:"approx_bytes_per_id"`
	CreatedAt        time.Time   `json:"created_at"`
	Chunks           []chunkMeta `json:"chunks"`
}

// Run runs the stress test for the configured schemes and returns results.
func Run(ctx context.Context, cfg Config) ([]Result, error) {
	if len(cfg.Schemes) == 0 {
		cfg.Schemes = []string{"nanoid16", "ulid", "ksuid"}
	}
	if cfg.Scale <= 0 {
		return nil, fmt.Errorf("scale must be > 0")
	}
	if cfg.ChunkSize <= 0 || cfg.ChunkSize > cfg.Scale {
		cfg.ChunkSize = minInt64(cfg.Scale, 1_000_000)
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.ApproxBytesPerID <= 0 {
		cfg.ApproxBytesPerID = defaultApproxBytesPerID
	}
	if cfg.LogInterval <= 0 {
		cfg.LogInterval = 1_000_000
	}
	if cfg.DiskSafetyFactor <= 0 {
		cfg.DiskSafetyFactor = 1.25
	}

	results := make([]Result, 0, len(cfg.Schemes))
	for _, scheme := range cfg.Schemes {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		start := time.Now()
		res, err := runScheme(ctx, scheme, cfg)
		if err != nil {
			return nil, err
		}
		res.Duration = time.Since(start)
		results = append(results, res)
	}
	return results, nil
}

func runScheme(ctx context.Context, scheme string, cfg Config) (Result, error) {
	gen, err := generatorFor(scheme)
	if err != nil {
		return Result{}, err
	}

	baseDir := cfg.TempDir
	if baseDir == "" {
		// 默认使用当前工作目录下的 tmp 目录
		cwd, err := os.Getwd()
		if err != nil {
			return Result{}, fmt.Errorf("get current directory: %w", err)
		}
		baseDir = filepath.Join(cwd, "tmp")
		// 确保 tmp 目录存在
		if err := os.MkdirAll(baseDir, 0o755); err != nil {
			return Result{}, fmt.Errorf("create tmp directory: %w", err)
		}
	}
	tempDir, err := os.MkdirTemp(baseDir, fmt.Sprintf("uidstress-%s-", scheme))
	if err != nil {
		return Result{}, fmt.Errorf("create temp dir: %w", err)
	}
	if !cfg.KeepTempData {
		defer os.RemoveAll(tempDir)
	}

	estimatedBytes := cfg.Scale * cfg.ApproxBytesPerID
	if err := ensureDisk(tempDir, estimatedBytes, cfg.DiskSafetyFactor); err != nil {
		return Result{}, err
	}

	man := &manifest{
		Scheme:           scheme,
		Scale:            cfg.Scale,
		ChunkSize:        cfg.ChunkSize,
		ApproxBytesPerID: cfg.ApproxBytesPerID,
		CreatedAt:        time.Now(),
	}

	var (
		totalGenerated int64
		totalUniqueSum int64
		chunkIndex     int
	)

	for totalGenerated < cfg.Scale {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		default:
		}

		chunkTarget := minInt64(cfg.ChunkSize, cfg.Scale-totalGenerated)
		if err := ensureMemory(cfg, chunkTarget); err != nil {
			return Result{}, err
		}
		if chunkTarget > int64(math.MaxInt) {
			return Result{}, fmt.Errorf("chunk size %d exceeds supported slice capacity", chunkTarget)
		}

		chunkIDs := make([]string, 0, int(chunkTarget))
		for int64(len(chunkIDs)) < chunkTarget {
			chunkIDs = append(chunkIDs, gen())
		}

		sort.Strings(chunkIDs)
		unique := dedupeSorted(chunkIDs)
		chunkHash := hashStrings(unique)

		chunkPath := filepath.Join(tempDir, fmt.Sprintf("%s-chunk-%05d.dat", scheme, chunkIndex))
		if err := writeChunkFile(chunkPath, unique); err != nil {
			return Result{}, err
		}
		fileHash, err := hashFile(chunkPath)
		if err != nil {
			return Result{}, err
		}
		if fileHash != chunkHash {
			return Result{}, fmt.Errorf("chunk hash mismatch: mem=%s file=%s", chunkHash, fileHash)
		}
		info, err := os.Stat(chunkPath)
		if err != nil {
			return Result{}, err
		}

		meta := chunkMeta{
			Index:         chunkIndex,
			Path:          chunkPath,
			UniqueCount:   int64(len(unique)),
			OriginalCount: chunkTarget,
			Hash:          chunkHash,
			SizeBytes:     info.Size(),
			CreatedAt:     time.Now(),
		}
		man.Chunks = append(man.Chunks, meta)
		if err := saveManifest(tempDir, man); err != nil {
			return Result{}, err
		}

		totalGenerated += chunkTarget
		totalUniqueSum += int64(len(unique))
		chunkIndex++

		if cfg.Verbose && totalGenerated%cfg.LogInterval == 0 {
			fmt.Printf("[%s] generated %d / %d IDs\n", scheme, totalGenerated, cfg.Scale)
		}
	}

	if err := verifyChunks(man); err != nil {
		return Result{}, err
	}

	unique, duplicates, err := mergeChunks(ctx, man, cfg.Verbose, cfg.LogInterval)
	if err != nil {
		return Result{}, err
	}
	if totalUniqueSum != unique+duplicates {
		return Result{}, fmt.Errorf("inconsistent counts: chunk unique sum=%d, merged unique=%d, duplicates=%d",
			totalUniqueSum, unique, duplicates)
	}

	return Result{
		Scheme:       scheme,
		Chunks:       len(man.Chunks),
		Generated:    totalGenerated,
		ChunkUnique:  totalUniqueSum,
		Unique:       unique,
		Duplicates:   duplicates,
		ManifestPath: filepath.Join(tempDir, "manifest.json"),
		OutputDir:    tempDir,
	}, nil
}

func generatorFor(name string) (func() string, error) {
	switch strings.ToLower(name) {
	case "nanoid16", "nanoid":
		return func() string { return tools.GetNanoIdBy(16) }, nil
	case "ulid":
		return tools.GenerateULID, nil
	case "ksuid":
		return tools.GenerateKSUID, nil
	case "customuid", "custom":
		return tools.GenerateCustomUID, nil
	default:
		return nil, fmt.Errorf("unknown scheme %q", name)
	}
}

func ensureMemory(cfg Config, chunkTarget int64) error {
	neededMB := float64(chunkTarget*cfg.ApproxBytesPerID) / 1024 / 1024
	if neededMB <= 0 && cfg.MemGuardMB <= 0 {
		return nil
	}
	vm, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Errorf("read memory info: %w", err)
	}
	availableMB := float64(vm.Available) / 1024 / 1024
	threshold := neededMB + cfg.MemGuardMB
	if threshold == 0 {
		threshold = neededMB
	}
	if availableMB < threshold {
		return fmt.Errorf("insufficient memory: need %.2f MB, available %.2f MB", threshold, availableMB)
	}
	return nil
}

func ensureDisk(path string, estimatedBytes int64, safety float64) error {
	usage, err := disk.Usage(path)
	if err != nil {
		return fmt.Errorf("read disk usage: %w", err)
	}
	required := float64(estimatedBytes) * safety
	if float64(usage.Free) < required {
		return fmt.Errorf("insufficient disk space %s: need ~%.2f MB, available %.2f MB",
			path, required/1024/1024, float64(usage.Free)/1024/1024)
	}
	return nil
}

func hashStrings(values []string) string {
	h := sha256.New()
	for _, v := range values {
		h.Write([]byte(v))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeChunkFile(path string, values []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	for _, v := range values {
		if _, err := writer.WriteString(v); err != nil {
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func saveManifest(dir string, man *manifest) error {
	data, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "manifest.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "manifest.json")); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	return os.WriteFile(filepath.Join(dir, "manifest.json.sha256"), []byte(hex.EncodeToString(sum[:])), 0o644)
}

func verifyChunks(man *manifest) error {
	for _, ch := range man.Chunks {
		hash, err := hashFile(ch.Path)
		if err != nil {
			return fmt.Errorf("hash chunk %s: %w", ch.Path, err)
		}
		if hash != ch.Hash {
			return fmt.Errorf("chunk %s hash mismatch, expected %s got %s", ch.Path, ch.Hash, hash)
		}
	}
	return nil
}

type chunkReader struct {
	meta   chunkMeta
	file   *os.File
	reader *bufio.Scanner
	value  string
	eof    bool
}

func newChunkReader(meta chunkMeta) (*chunkReader, error) {
	f, err := os.Open(meta.Path)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)
	cr := &chunkReader{
		meta:   meta,
		file:   f,
		reader: sc,
	}
	if err := cr.advance(); err != nil {
		f.Close()
		return nil, err
	}
	return cr, nil
}

func (c *chunkReader) advance() error {
	if c.eof {
		return nil
	}
	if !c.reader.Scan() {
		if err := c.reader.Err(); err != nil {
			return err
		}
		c.eof = true
		c.value = ""
		return nil
	}
	c.value = c.reader.Text()
	return nil
}

func (c *chunkReader) close() error {
	return c.file.Close()
}

type heapEntry struct {
	value  string
	reader *chunkReader
}

type mergeHeap []*heapEntry

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*heapEntry))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func mergeChunks(ctx context.Context, man *manifest, verbose bool, logInterval int64) (int64, int64, error) {
	if len(man.Chunks) == 0 {
		return 0, 0, errors.New("manifest contains no chunks")
	}

	readers := make([]*chunkReader, 0, len(man.Chunks))
	for _, meta := range man.Chunks {
		cr, err := newChunkReader(meta)
		if err != nil {
			for _, r := range readers {
				r.close()
			}
			return 0, 0, err
		}
		if cr.eof {
			cr.close()
			continue
		}
		readers = append(readers, cr)
	}
	if len(readers) == 0 {
		return 0, 0, errors.New("all chunks empty")
	}
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()

	h := mergeHeap{}
	for _, r := range readers {
		h = append(h, &heapEntry{value: r.value, reader: r})
	}
	heap.Init(&h)

	var (
		prevValue  string
		hasPrev    bool
		unique     int64
		duplicates int64
		processed  int64
	)

	for len(h) > 0 {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		default:
		}

		entry := heap.Pop(&h).(*heapEntry)

		if hasPrev && entry.value == prevValue {
			duplicates++
		} else {
			unique++
			prevValue = entry.value
			hasPrev = true
		}
		processed++

		if verbose && logInterval > 0 && processed%logInterval == 0 {
			fmt.Printf("[merge %s] processed %d IDs (unique=%d duplicates=%d)\n",
				man.Scheme, processed, unique, duplicates)
		}

		if err := entry.reader.advance(); err != nil {
			return 0, 0, err
		}
		if !entry.reader.eof {
			heap.Push(&h, &heapEntry{value: entry.reader.value, reader: entry.reader})
		}
	}

	return unique, duplicates, nil
}

func dedupeSorted(values []string) []string {
	if len(values) == 0 {
		return values[:0]
	}
	writeIdx := 1
	prev := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] != prev {
			values[writeIdx] = values[i]
			prev = values[i]
			writeIdx++
		}
	}
	return values[:writeIdx]
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
