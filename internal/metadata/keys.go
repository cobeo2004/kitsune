package metadata

import "fmt"

const (
	rootPrefix       = "/kitsune/"
	indexPrefix      = rootPrefix + "indexes/"
	tabletPrefix     = rootPrefix + "tablets/"
	checkpointPrefix = rootPrefix + "checkpoints/"
	snapshotPrefix   = rootPrefix + "snapshots/"
)

func indexKey(name string) string {
	return indexPrefix + name + "/config"
}

func indexNamespacePrefix() string {
	return indexPrefix
}

func shardReplicaKey(indexName string, shardID int, replicaID string) string {
	return fmt.Sprintf("%s%s/shards/%d/replicas/%s", indexPrefix, indexName, shardID, replicaID)
}

func shardReplicaPrefix(indexName string) string {
	return fmt.Sprintf("%s%s/shards/", indexPrefix, indexName)
}

func tabletStatusKey(indexName string, shardID int, replicaID string) string {
	return fmt.Sprintf("%s%s/%d/%s/state", tabletPrefix, indexName, shardID, replicaID)
}

func checkpointKey(indexName string, shardID int, replicaID string) string {
	return fmt.Sprintf("%s%s/%d/%s", checkpointPrefix, indexName, shardID, replicaID)
}

func snapshotPointerKey(indexName string, shardID int, replicaID string) string {
	return fmt.Sprintf("%s%s/%d/%s/latest", snapshotPrefix, indexName, shardID, replicaID)
}
