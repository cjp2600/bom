package bom

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

// Option bom option type
type Option func(*Bom) error

// SetMongoClient set go.mongodb.org/mongo-driver client
func SetMongoClient(client *mongo.Client) Option {
	return func(b *Bom) error {
		b.client = client
		return nil
	}
}

// SetDatabaseName set db name
func SetDatabaseName(dbName string) Option {
	return func(b *Bom) error {
		b.dbName = dbName
		return nil
	}
}

// SetSkipWhenUpdating set skip fields when update
func SetSkipWhenUpdating(fieldsMap map[string]bool) Option {
	return func(b *Bom) error {
		b.skipWhenUpdating = fieldsMap
		return nil
	}
}

// SetCollection set collection name
func SetCollection(collection string) Option {
	return func(b *Bom) error {
		b.dbCollection = collection
		return nil
	}
}

// SetQueryTimeout set query timeout
func SetQueryTimeout(time time.Duration) Option {
	return func(b *Bom) error {
		b.queryTimeout = time
		return nil
	}
}

// SetModel set work model
func SetModel(document interface{}) Option {
	return func(b *Bom) error {
		b.model = document
		return nil
	}
}

// Deprecated: SetContext method should not be used
func SetContext(ctx context.Context) Option {
	return func(b *Bom) error {
		return nil
	}
}
