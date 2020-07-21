package bom

import "go.mongodb.org/mongo-driver/bson/primitive"

// ElMatch create ElemMatch object
func ElMatch(key string, val interface{}) ElemMatch {
	return ElemMatch{Key: key, Val: val}
}

// ToObj convert string to ObjectID
func ToObj(val string) primitive.ObjectID {
	objectID, _ := primitive.ObjectIDFromHex(val)
	return objectID
}

// ToObjects convert slice strings to slice ObjectID
func ToObjects(values []string) []primitive.ObjectID {
	var objectIds []primitive.ObjectID
	for _, id := range values {
		objectId, _ := primitive.ObjectIDFromHex(id)
		objectIds = append(objectIds, objectId)
	}
	return objectIds
}
