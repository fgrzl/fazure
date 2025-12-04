package common

import (
	"fmt"
	"strings"
)

// Key layout helpers for consistent Pebble key structure

// BlobKey returns the key for a blob's data
func BlobKey(container, blobName string) []byte {
	return []byte(fmt.Sprintf("blobs/%s/%s", container, blobName))
}

// BlobMetaKey returns the key for a blob's metadata
func BlobMetaKey(container, blobName string) []byte {
	return []byte(fmt.Sprintf("blobs/%s/%s/meta", container, blobName))
}

// BlobContainerPrefix returns the prefix for all blobs in a container
func BlobContainerPrefix(container string) []byte {
	return []byte(fmt.Sprintf("blobs/%s/", container))
}

// QueueKey returns the key for queue metadata
func QueueKey(queue string) []byte {
	return []byte(fmt.Sprintf("queues/%s/meta", queue))
}

// QueueMessageKey returns the key for a specific message
func QueueMessageKey(queue, msgID string) []byte {
	return []byte(fmt.Sprintf("queues/%s/messages/%s", queue, msgID))
}

// QueueMessagesPrefix returns the prefix for all messages in a queue
func QueueMessagesPrefix(queue string) []byte {
	return []byte(fmt.Sprintf("queues/%s/messages/", queue))
}

// TableKey returns the key for a table entity
func TableKey(table, partitionKey, rowKey string) []byte {
	return []byte(fmt.Sprintf("tables/%s/%s/%s", table, partitionKey, rowKey))
}

// TablePrefix returns the prefix for all entities in a table
func TablePrefix(table string) []byte {
	return []byte(fmt.Sprintf("tables/%s/", table))
}

// TablePartitionPrefix returns the prefix for all entities in a partition
func TablePartitionPrefix(table, partitionKey string) []byte {
	return []byte(fmt.Sprintf("tables/%s/%s/", table, partitionKey))
}

// ParseTableKey extracts partition key and row key from a table key
func ParseTableKey(key []byte) (partitionKey, rowKey string, ok bool) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 4 || parts[0] != "tables" {
		return "", "", false
	}
	return parts[2], parts[3], true
}
