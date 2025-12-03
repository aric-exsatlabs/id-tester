package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"id-tester/internal/tools/uidstress"
)

func main() {
	var (
		schemesFlag     = flag.String("schemes", "nanoid16,ulid,ksuid", "comma separated list of schemes (nanoid16, ulid, ksuid)")
		scaleFlag       = flag.Int64("scale", 50_000_000, "number of IDs to generate per scheme")
		chunkFlag       = flag.Int64("chunk", 1_000_000, "number of IDs per chunk")
		tempDirFlag     = flag.String("tempdir", "", "base directory for temporary chunk files")
		keepFlag        = flag.Bool("keep", false, "keep temporary data after completion")
		logIntervalFlag = flag.Int64("log-interval", 1_000_000, "progress log interval")
		memGuardFlag    = flag.Float64("mem-guard", 512, "minimum free memory (MB) to keep above estimated chunk usage")
		verboseFlag     = flag.Bool("verbose", false, "enable verbose logging")
		bytesPerIDFlag  = flag.Int64("bytes-per-id", 64, "approximate bytes per ID for resource estimation")
		diskFactorFlag  = flag.Float64("disk-factor", 1.25, "disk safety factor multiplier")
	)
	flag.Parse()

	cfg := uidstress.Config{
		Schemes:          parseSchemes(*schemesFlag),
		Scale:            *scaleFlag,
		ChunkSize:        *chunkFlag,
		TempDir:          *tempDirFlag,
		KeepTempData:     *keepFlag,
		LogInterval:      *logIntervalFlag,
		Verbose:          *verboseFlag,
		ApproxBytesPerID: *bytesPerIDFlag,
		MemGuardMB:       *memGuardFlag,
		DiskSafetyFactor: *diskFactorFlag,
	}

	ctx := context.Background()
	results, err := uidstress.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "uidstress failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("UID Stress Test Summary")
	fmt.Println(strings.Repeat("=", 72))
	for _, res := range results {
		fmt.Printf("Scheme:        %s\n", res.Scheme)
		fmt.Printf("Duration:      %s\n", res.Duration.Round(time.Millisecond))
		fmt.Printf("Chunks:        %d\n", res.Chunks)
		fmt.Printf("Generated:     %d\n", res.Generated)
		fmt.Printf("Chunk Unique:  %d\n", res.ChunkUnique)
		fmt.Printf("Unique:        %d\n", res.Unique)
		fmt.Printf("Duplicates:    %d\n", res.Duplicates)
		if cfg.KeepTempData {
			fmt.Printf("Manifest:      %s\n", res.ManifestPath)
			fmt.Printf("Temp Dir:      %s\n", res.OutputDir)
		}
		fmt.Println(strings.Repeat("-", 72))
	}
}

func parseSchemes(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
