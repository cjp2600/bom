package bom

import "go.mongodb.org/mongo-driver/bson/primitive"

// SkipWhenUpdating data with skip update fields
var SkipWhenUpdating = map[string]bool{"id": true, "createdat": true, "updatedat": true}

// SortTypeMatcher value matcher
var SortTypeMatcher = map[string]int32{"asc": 1, "desc": -1}

// StageInterface interface for stage
type StageInterface interface {
	GetStage() primitive.M
}

const (
	// EqualConditionOperator mongo db operator
	EqualConditionOperator = "$eq"

	// NotEqualConditionOperator mongo db operator
	NotEqualConditionOperator = "$ne"

	// GreaterConditionOperator mongo db operator
	GreaterConditionOperator = "$gt"

	// GreaterOrEqualConditionOperator mongo db operator
	GreaterOrEqualConditionOperator = "$gte"

	// LessConditionOperator mongo db operator
	LessConditionOperator = "$lt"

	// LessOrEqualConditionOperator mongo db operator
	LessOrEqualConditionOperator = "$lte"

	// InConditionOperator mongo db operator
	InConditionOperator = "$in"

	// NotInConditionOperator mongo db operator
	NotInConditionOperator = "$nin"

	// AndConditionOperator mongo db operator
	AndConditionOperator = "$and"

	// OrConditionOperator mongo db operator
	OrConditionOperator = "$or"

	// NotConditionOperator mongo db operator
	NotConditionOperator = "$not"

	// NorConditionOperator mongo db operator
	NorConditionOperator = "$nor"

	// ExistsConditionOperator mongo db operator
	ExistsConditionOperator = "$exists"

	// TypeConditionOperator mongo db operator
	TypeConditionOperator = "$type"

	// LookupAggregateOperator mongo db operator
	LookupAggregateOperator = "$lookup"

	// FacetAggregateOperator mongo db operator
	FacetAggregateOperator = "$facet"

	// MatchAggregateOperator mongo db operator
	MatchAggregateOperator = "$match"

	// ProjectAggregateOperator mongo db operator
	ProjectAggregateOperator = "$project"

	// LimitOperator mongo db operator
	LimitOperator = "$limit"

	// SkipOperator mongo db operator
	SkipOperator = "$skip"

	// SortOperator mongo db operator
	SortOperator = "$sort"

	// ElMathConditionOperator mongo db operator
	ElMathConditionOperator = "$elemMatch"
)
