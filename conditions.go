package bom

import "go.mongodb.org/mongo-driver/bson/primitive"

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
)
