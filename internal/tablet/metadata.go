package tablet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const metadataFileName = "tablet.json"

type storedMetadata struct {
	MappingVersion int `json:"mappingVersion"`
}

func writeMetadata(dir string, id Identity) error {
	data, err := json.MarshalIndent(storedMetadata{
		MappingVersion: id.MappingVersion,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tablet metadata: %w", err)
	}

	path := filepath.Join(dir, metadataFileName)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write tablet metadata: %w", err)
	}

	return nil
}

func readMetadata(dir string) (storedMetadata, bool, error) {
	var meta storedMetadata

	path := filepath.Join(dir, metadataFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return meta, false, nil
		}
		return meta, false, fmt.Errorf("read tablet metadata: %w", err)
	}

	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, false, fmt.Errorf("decode tablet metadata: %w", err)
	}

	return meta, true, nil
}
