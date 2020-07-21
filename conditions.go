package bom

import "go.mongodb.org/mongo-driver/bson/primitive"

var SkipWhenUpdating = map[string]bool{"id": true, "createdat": true, "updatedat": true}

var SortTypeMatcher = map[string]int32{"asc": 1, "desc": -1}

type Stager interface {
	GetStage() primitive.M
}

const (
	EqualConditionOperator          = "$eq"
	NotEqualConditionOperator       = "$ne"
	GreaterConditionOperator        = "$gt"
	GreaterOrEqualConditionOperator = "$gte"
	LessConditionOperator           = "$lt"
	LessOrEqualConditionOperator    = "$lte"
	InConditionOperator             = "$in"
	NotInConditionOperator          = "$nin"

	AndConditionOperator = "$and"
	OrConditionOperator  = "$or"
	NotConditionOperator = "$not"
	NorConditionOperator = "$nor"

	ExistsConditionOperator = "$exists"
	TypeConditionOperator   = "$type"

	LookupAggregateOperator  = "$lookup"
	FacetAggregateOperator   = "$facet"
	MatchAggregateOperator   = "$match"
	ProjectAggregateOperator = "$project"

	LimitOperator = "$limit"
	SkipOperator  = "$skip"
	SortOperator  = "$sort"

	ElMathConditionOperator = "$elemMatch"
)
