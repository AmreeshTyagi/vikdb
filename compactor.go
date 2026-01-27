package vikdb

import (
	"os"
	"sort"
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
func (c *Compactor) Compact(sstables []*SSTable, sequenceNum int64) (string, error) {
	if len(sstables) == 0 {
		return "", nil
	}

	// Collect all entries from all SSTables
	allEntries := make(map[string]KeyValue) // key -> entry (newest wins)

	// Read from all SSTables (oldest to newest, so newer overwrites older)
	for _, sst := range sstables {
		// Get all entries using range query
		entries := sst.GetRange(nil, nil)
		for _, entry := range entries {
			keyStr := string(entry.Key)
			allEntries[keyStr] = entry
		}
	}

	// Convert to slice and sort
	mergedEntries := make([]KeyValue, 0, len(allEntries))
	for _, entry := range allEntries {
		mergedEntries = append(mergedEntries, entry)
	}
	sort.Sort(KeyValueSlice(mergedEntries))

	// Write merged SSTable
	mergedPath, err := WriteSSTable(mergedEntries, c.sstableDir, sequenceNum)
	if err != nil {
		return "", err
	}

	// Delete old SSTables
	for _, sst := range sstables {
		if err := sst.Close(); err != nil {
			// Log error but continue
		}
		if err := os.Remove(sst.FilePath()); err != nil {
			// Log error but continue
		}
	}

	return mergedPath, nil
}

