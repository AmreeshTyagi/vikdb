package compactor

import (
	"os"
	"sort"

	"vikdb/internal/kv"
	"vikdb/internal/sstable"
)

// Compactor handles compaction of SSTables
type Compactor struct {
	sstableDir string
	maxFiles   int // Maximum number of SSTables before compaction
}

// NewCompactor creates a new Compactor
func NewCompactor(sstableDir string, maxFiles int) *Compactor {
	return &Compactor{
		sstableDir: sstableDir,
		maxFiles:   maxFiles,
	}
}

// ShouldCompact returns true if compaction should be triggered
func (c *Compactor) ShouldCompact(sstableCount int) bool {
	return sstableCount >= c.maxFiles
}

// Compact merges multiple SSTables into a single SSTable
// Removes duplicates (keeping newest value) and deletes old SSTables
func (c *Compactor) Compact(sstables []*sstable.SSTable, sequenceNum int64) (string, error) {
	if len(sstables) == 0 {
		return "", nil
	}

	allEntries := make(map[string]kv.KeyValue)

	for _, sst := range sstables {
		entries := sst.GetRange(nil, nil)
		for _, entry := range entries {
			keyStr := string(entry.Key)
			allEntries[keyStr] = entry
		}
	}

	mergedEntries := make([]kv.KeyValue, 0, len(allEntries))
	for _, entry := range allEntries {
		mergedEntries = append(mergedEntries, entry)
	}
	sort.Sort(sstable.KeyValueSlice(mergedEntries))

	mergedPath, err := sstable.WriteSSTable(mergedEntries, c.sstableDir, sequenceNum)
	if err != nil {
		return "", err
	}

	for _, sst := range sstables {
		_ = sst.Close()
		_ = os.Remove(sst.FilePath())
	}

	return mergedPath, nil
}
