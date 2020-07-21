package bom

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// AggregateStages Stage data
type AggregateStages []StageInterface

// LookupStage find stage
type LookupStage struct {
	from         string
	localField   string
	foreignField string
	as           string
}

// Aggregate method
func (as *AggregateStages) Aggregate() ([]primitive.M, error) {
	l := len(*as)
	result := make([]primitive.M, l)
	for i, s := range *as {
		result[i] = s.GetStage()
	}

	return result, nil
}

// GetStage get stage
func (l *LookupStage) GetStage() primitive.M {
	return primitive.M{
		LookupAggregateOperator: primitive.M{
			"from":         l.from,
			"localField":   l.localField,
			"foreignField": l.foreignField,
			"as":           l.as,
		},
	}
}

// NewLookupStage constructor
func NewLookupStage(from, localField, foreignField, as string) *LookupStage {
	return &LookupStage{
		from:         from,
		localField:   localField,
		foreignField: foreignField,
		as:           as,
	}
}

// MatchStage math stage cases
type MatchStage struct {
	cases primitive.M
}

// GetStage stage getter
func (m *MatchStage) GetStage() primitive.M {
	return primitive.M{
		MatchAggregateOperator: m.cases,
	}
}

// AddCondition add aggregation condition
func (m *MatchStage) AddCondition(key string, value interface{}) {
	if m.cases == nil {
		m.cases = primitive.M{}
	}

	cm := *m
	cm.cases[key] = value
	*m = cm
}

// NewMatchStage create stage
func NewMatchStage() *MatchStage {
	return new(MatchStage)
}

// FacetStage create facet stage
type FacetStage struct {
	conditions []primitive.M
}

// GetStage stage
func (fs *FacetStage) GetStage() primitive.M {
	return primitive.M{
		FacetAggregateOperator: primitive.M{
			"result": fs.conditions,
			"total": []primitive.M{
				{
					"$group": primitive.M{
						"_id": nil,
						"count": primitive.M{
							"$sum": 1,
						},
					},
				},
			},
		},
	}
}

// SetLimit limit stage
func (fs *FacetStage) SetLimit(limit int32) {
	fs.conditions = append(fs.conditions, primitive.M{LimitOperator: limit})
}

// SetSkip set skip
func (fs *FacetStage) SetSkip(skip int32) {
	fs.conditions = append(fs.conditions, primitive.M{SkipOperator: skip})
}

// SetSort set sort
func (fs *FacetStage) SetSort(sort primitive.M) {
	fs.conditions = append(fs.conditions, primitive.M{SortOperator: sort})
}

// NewFacetStage create facet stage
func NewFacetStage() *FacetStage {
	return &FacetStage{
		conditions: make([]primitive.M, 0),
	}
}

// ProjectStage projection stage
type ProjectStage struct {
	projects primitive.M
}

// GetStage get stage
func (p *ProjectStage) GetStage() primitive.M {
	return primitive.M{
		ProjectAggregateOperator: p.projects,
	}
}

// NewProjectStage create project stage
func NewProjectStage(project primitive.M) *ProjectStage {
	return &ProjectStage{projects: project}
}
