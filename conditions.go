package bom

import "go.mongodb.org/mongo-driver/bson/primitive"

// Stager интерфейс
type Stager interface {
	// GetStage стадии
	GetStage() primitive.M
}

const (
	// EqualConditionOperator eq
	EqualConditionOperator = "$eq"
	// NotEqualConditionOperator ne
	NotEqualConditionOperator = "$ne"
	// GreaterConditionOperator gt
	GreaterConditionOperator = "$gt"
	// GreaterOrEqualConditionOperator gte
	GreaterOrEqualConditionOperator = "$gte"
	// LessConditionOperator lt
	LessConditionOperator = "$lt"
	// LessOrEqualConditionOperator lte
	LessOrEqualConditionOperator = "$lte"
	// InConditionOperator in
	InConditionOperator = "$in"
	// NotInConditionOperator nin
	NotInConditionOperator = "$nin"
	// AndConditionOperator and
	AndConditionOperator = "$and"
	// OrConditionOperator or
	OrConditionOperator = "$or"
	// NotConditionOperator not
	NotConditionOperator = "$not"
	// NorConditionOperator nor
	NorConditionOperator = "$nor"
	// ExistsConditionOperator exists
	ExistsConditionOperator = "$exists"
	// TypeConditionOperator type
	TypeConditionOperator = "$type"
	// LookupAggregateOperator $lookup
	LookupAggregateOperator = "$lookup"
	// FacetAggregateOperator $facet
	FacetAggregateOperator = "$facet"
	// MatchAggregateOperator $match
	MatchAggregateOperator = "$match"
	// ProjectAggregateOperator $project
	ProjectAggregateOperator = "$project"
	// LimitOperator $limit
	LimitOperator = "$limit"
	// SkipOperator $skip
	SkipOperator = "$skip"
	// SortOperator $sort
	SortOperator = "$sort"
)
