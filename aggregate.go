package bom

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type AggregateStages []Stager

func (as *AggregateStages) Aggregate() ([]primitive.M, error) {
	l := len(*as)
	result := make([]primitive.M, l)
	for i, s := range *as {
		result[i] = s.GetStage()
	}

	return result, nil
}

type LookupStage struct {
	from         string
	localField   string
	foreignField string
	as           string
}

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

func NewLookupStage(from, localField, foreignField, as string) *LookupStage {
	return &LookupStage{
		from:         from,
		localField:   localField,
		foreignField: foreignField,
		as:           as,
	}
}

type MatchStage struct {
	cases primitive.M
}

func (m *MatchStage) GetStage() primitive.M {
	return primitive.M{
		MatchAggregateOperator: m.cases,
	}
}

func (m *MatchStage) AddCondition(key string, value interface{}) {
	if m.cases == nil {
		m.cases = primitive.M{}
	}

	cm := *m
	cm.cases[key] = value
	*m = cm
}

func NewMatchStage() *MatchStage {
	return new(MatchStage)
}

type FacetStage struct {
	conditions []primitive.M
}

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

func (fs *FacetStage) SetLimit(limit int32) {
	fs.conditions = append(fs.conditions, primitive.M{LimitOperator: limit})
}

func (fs *FacetStage) SetSkip(skip int32) {
	fs.conditions = append(fs.conditions, primitive.M{SkipOperator: skip})
}

func (fs *FacetStage) SetSort(sort primitive.M) {
	fs.conditions = append(fs.conditions, primitive.M{SortOperator: sort})
}

func NewFacetStage() *FacetStage {
	return &FacetStage{
		conditions: make([]primitive.M, 0),
	}
}

type ProjectStage struct {
	projects primitive.M
}

func (p *ProjectStage) GetStage() primitive.M {
	return primitive.M{
		ProjectAggregateOperator: p.projects,
	}
}

func NewProjectStage(project primitive.M) *ProjectStage {
	return &ProjectStage{projects: project}
}
