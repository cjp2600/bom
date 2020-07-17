package bom

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// AggregateStages структура
type AggregateStages []Stager

// Aggregate агрегация
func (as *AggregateStages) Aggregate() ([]primitive.M, error) {
	l := len(*as)
	result := make([]primitive.M, l)
	for i, s := range *as {
		result[i] = s.GetStage()
	}

	return result, nil
}

// LookupStage структура
type LookupStage struct {
	from         string
	localField   string
	foreignField string
	as           string
}

// GetStage стадия
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

// NewLookupStage  конструктор
func NewLookupStage(from, localField, foreignField, as string) *LookupStage {
	return &LookupStage{
		from:         from,
		localField:   localField,
		foreignField: foreignField,
		as:           as,
	}
}

// MatchStage структура
type MatchStage struct {
	cases primitive.M
}

// GetStage стадия
func (m *MatchStage) GetStage() primitive.M {
	return primitive.M{
		MatchAggregateOperator: m.cases,
	}
}

// AddCondition добавление условий
func (m *MatchStage) AddCondition(key string, value interface{}) {
	if m.cases == nil {
		m.cases = primitive.M{}
	}

	cm := *m
	cm.cases[key] = value
	*m = cm
}

// NewMatchStage непонятно
func NewMatchStage() *MatchStage {
	return new(MatchStage)
}

// FacetStage структура
type FacetStage struct {
	conditions []primitive.M
}

// GetStage стадия
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

// SetLimit лимиты
func (fs *FacetStage) SetLimit(limit int32) {
	fs.conditions = append(fs.conditions, primitive.M{LimitOperator: limit})
}

// SetSkip что-то пропустить
func (fs *FacetStage) SetSkip(skip int32) {
	fs.conditions = append(fs.conditions, primitive.M{SkipOperator: skip})
}

// SetSort сортировка
func (fs *FacetStage) SetSort(sort primitive.M) {
	fs.conditions = append(fs.conditions, primitive.M{SortOperator: sort})
}

// NewFacetStage стадия
func NewFacetStage() *FacetStage {
	return &FacetStage{
		conditions: make([]primitive.M, 0),
	}
}

// ProjectStage структура
type ProjectStage struct {
	projects primitive.M
}

// GetStage стадия
func (p *ProjectStage) GetStage() primitive.M {
	return primitive.M{
		ProjectAggregateOperator: p.projects,
	}
}

// NewProjectStage стадия
func NewProjectStage(project primitive.M) *ProjectStage {
	return &ProjectStage{projects: project}
}
