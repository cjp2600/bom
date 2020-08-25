package bom

// BOM Mongodb Mongo builder of (go.mongodb.org/mongo-driver)

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Define common const
const (
	DefaultQueryTimeout = 5 * time.Second
	DefaultSize         = 20
)

// Size option bom size type
type Size int32

// Define common structures
type (

	// Bom main structure
	Bom struct {
		// default mongodb client (go.mongodb.org/mongo-driver)
		client *mongo.Client
		model  interface{}

		// configuration fields
		dbName       string
		dbCollection string
		queryTimeout time.Duration

		condition        interface{}
		skipWhenUpdating map[string]bool
		conditions       Conditions
		pipeline         AggregateStages

		// go.mongodb.org/mongo-driver options (with setters)
		options Options

		lastID         string
		useAggregation bool
		selectArg      []interface{}

		// query config
		limit *Limit
		sort  []*Sort
	}

	// Conditions mongodb conditions structure
	Conditions struct {
		whereConditions []map[string]interface{}
		orConditions    []map[string]interface{}
		inConditions    []map[string]interface{}
		notInConditions []map[string]interface{}
		notConditions   []map[string]interface{}
	}

	// Options client options
	Options struct {
		aggregateOptions        []*options.AggregateOptions
		updateOptions           []*options.UpdateOptions
		insertOptions           []*options.InsertOneOptions
		findOneOptions          []*options.FindOneOptions
		findOptions             []*options.FindOptions
		findOneAndUpdateOptions []*options.FindOneAndUpdateOptions
	}

	// Sort data
	Sort struct {
		Field string
		Type  string
	}

	// Limit data
	Limit struct {
		Page int32
		Size int32
	}

	// ElemMatch data for projection
	ElemMatch struct {
		Key string
		Val interface{}
	}
)

// Mongo source client (go.mongodb.org/mongo-driver)
func (b *Bom) Mongo() *mongo.Collection {
	return b.client.Database(b.dbName).Collection(b.dbCollection)
}

// New init bom object
func New(options ...Option) (*Bom, error) {
	// set common values
	b := &Bom{
		queryTimeout:     DefaultQueryTimeout,
		skipWhenUpdating: SkipWhenUpdating,
		limit:            &Limit{Page: 1, Size: DefaultSize},
	}

	// apply options
	for _, option := range options {
		if err := option(b); err != nil {
			return nil, err
		}
	}

	// check exist client
	if b.client == nil {
		return nil, ErrClientRequired
	}
	return b, nil
}

// WithDB enrich database name
func (b *Bom) WithDB(dbName string) *Bom {
	b.dbName = dbName
	return b
}

// WithColl enrich collection name
func (b *Bom) WithColl(collection string) *Bom {
	b.dbCollection = collection
	return b
}

// WithModel set work model
func (b *Bom) WithModel(document interface{}) *Bom {
	b.model = document
	return b
}

// WithTimeout enrich query with timeout
func (b *Bom) WithTimeout(time time.Duration) *Bom {
	b.queryTimeout = time
	return b
}

// Deprecated: WithContext method should not be used
func (b *Bom) WithContext(ctx context.Context) *Bom {
	return b
}

// WithCondition set default condition
func (b *Bom) WithCondition(condition interface{}) *Bom {
	b.condition = condition
	return b
}

// WithLimit set limit
func (b *Bom) WithLimit(limit *Limit) *Bom {
	if limit.Page > 0 {
		b.limit.Page = limit.Page
	}
	if limit.Size > 0 {
		b.limit.Size = limit.Size
	}
	return b
}

// WithSort set sort
func (b *Bom) WithSort(sort *Sort) *Bom {
	b.sort = append(b.sort, sort)
	return b
}

// WithLastID set custom lastID
func (b *Bom) WithLastID(lastID string) *Bom {
	b.lastID = lastID
	return b
}

// WithSize set size
func (b *Bom) WithSize(size int32) *Bom {
	if size > 0 {
		b.limit.Size = size
	}
	return b
}

// SetUpdateOptions set custom update options
func (b *Bom) SetUpdateOptions(opts ...*options.UpdateOptions) *Bom {
	b.options.updateOptions = append(b.options.updateOptions, opts...)
	return b
}

// SetAggregateOptions set custom aggregate options
func (b *Bom) SetAggregateOptions(opts ...*options.AggregateOptions) *Bom {
	b.options.aggregateOptions = append(b.options.aggregateOptions, opts...)
	return b
}

// SetFindOptions set custom find options
func (b *Bom) SetFindOptions(opts ...*options.FindOptions) *Bom {
	b.options.findOptions = append(b.options.findOptions, opts...)
	return b
}

// SetFindOnEndUpdateOptions set custom find one and update options
func (b *Bom) SetFindOnEndUpdateOptions(opts ...*options.FindOneAndUpdateOptions) *Bom {
	b.options.findOneAndUpdateOptions = append(b.options.findOneAndUpdateOptions, opts...)
	return b
}

// SetInsertOptions set custom insert options
func (b *Bom) SetInsertOptions(opts ...*options.InsertOneOptions) *Bom {
	b.options.insertOptions = append(b.options.insertOptions, opts...)
	return b
}

// SetFindOneOptions set custom find one options
func (b *Bom) SetFindOneOptions(opts ...*options.FindOneOptions) *Bom {
	b.options.findOneOptions = append(b.options.findOneOptions, opts...)
	return b
}

// Select analog classic orm method for field
func (b *Bom) Select(arg ...interface{}) *Bom {
	b.selectArg = arg
	return b
}

// AddSelect analog classic orm method for field
func (b *Bom) AddSelect(arg interface{}) *Bom {
	b.selectArg = append(b.selectArg, arg)
	return b
}

// FillPipeline fill aggregation pipelines
func (b *Bom) FillPipeline(p ...StageInterface) {
	if b.pipeline == nil {
		b.pipeline = make(AggregateStages, 0)
	}
	for _, stage := range p {
		if stage == nil {
			continue
		}
		b.pipeline = append(b.pipeline, stage)
	}
}

//Deprecated: should use WhereConditions or WhereEq
func (b *Bom) Where(field string, value interface{}) *Bom {
	return b.WhereEq(field, value)
}

// WhereEq where condition example: bom.WhereEq("_id", bom.ToObject(id))
func (b *Bom) WhereEq(field string, value interface{}) *Bom {
	b = b.whereConditions(field, EqualConditionOperator, value)
	return b
}

// WhereNotEq where condition example: bom.WhereNotEq("_id", bom.ToObject(id))
func (b *Bom) WhereNotEq(field string, value interface{}) *Bom {
	b = b.whereConditions(field, NotEqualConditionOperator, value)
	return b
}

// WhereGt Greater Condition example: bom.WhereGt("age", 30)
func (b *Bom) WhereGt(field string, value interface{}) *Bom {
	b = b.whereConditions(field, GreaterConditionOperator, value)
	return b
}

// WhereGte Greater Or Equal Condition example: bom.WhereGte("age", 30)
func (b *Bom) WhereGte(field string, value interface{}) *Bom {
	b = b.whereConditions(field, GreaterOrEqualConditionOperator, value)
	return b
}

// WhereLt Less Condition example: bom.WhereLt("age", 30)
func (b *Bom) WhereLt(field string, value interface{}) *Bom {
	b = b.whereConditions(field, LessConditionOperator, value)
	return b
}

// WhereLte Less Condition example: bom.WhereLt("age", 30)
func (b *Bom) WhereLte(field string, value interface{}) *Bom {
	b = b.whereConditions(field, LessOrEqualConditionOperator, value)
	return b
}

// Not Not Condition example: bom.Not("age", 30)
func (b *Bom) Not(field string, value interface{}) *Bom {
	b.conditions.notConditions = append(b.conditions.notConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

// WhereIn WhereIn Condition example: bom.WhereIn("age", 30)
func (b *Bom) WhereIn(field string, value interface{}) *Bom {
	b.conditions.inConditions = append(b.conditions.inConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

// NotWhereIn not where in condition example: bom.NotWhereIn("age", 30)
func (b *Bom) NotWhereIn(field string, value interface{}) *Bom {
	b.conditions.notInConditions = append(b.conditions.notInConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

// OrWhereEq or where in condition example: bom.OrWhereEq("age", 30)
func (b *Bom) OrWhereEq(field string, value interface{}) *Bom {
	b = b.orWhereConditions(field, EqualConditionOperator, value)
	return b
}

// OrWhereNotEq or not eq where in condition example: bom.OrWhereNotEq("age", 30)
func (b *Bom) OrWhereNotEq(field string, value interface{}) *Bom {
	b = b.orWhereConditions(field, NotEqualConditionOperator, value)
	return b
}

// OrWhereGt or where gt in condition example: bom.OrWhereGt("age", 30)
func (b *Bom) OrWhereGt(field string, value interface{}) *Bom {
	b = b.orWhereConditions(field, GreaterConditionOperator, value)
	return b
}

// OrWhereGte or where gte in condition example: bom.OrWhereGte("age", 30)
func (b *Bom) OrWhereGte(field string, value interface{}) *Bom {
	b = b.orWhereConditions(field, GreaterOrEqualConditionOperator, value)
	return b
}

// OrWhereLt or where lt in condition example: bom.OrWhereLt("age", 30)
func (b *Bom) OrWhereLt(field string, value interface{}) *Bom {
	b = b.orWhereConditions(field, LessConditionOperator, value)
	return b
}

// OrWhereLte or where lte in condition example: bom.OrWhereLte("age", 30)
func (b *Bom) OrWhereLte(field string, value interface{}) *Bom {
	b = b.orWhereConditions(field, LessOrEqualConditionOperator, value)
	return b
}

//OrWhere Deprecated: should use orWhereConditions or OrWhereEq
func (b *Bom) OrWhere(field string, value interface{}) *Bom {
	b.OrWhereEq(field, value)
	return b
}

// BuildProjection build projection
func (b *Bom) BuildProjection() primitive.M {
	var result primitive.M
	if len(b.selectArg) > 0 {
		result = make(primitive.M)
		for _, item := range b.selectArg {
			switch v := item.(type) {
			case string:
				result[v] = 1
			case ElemMatch:
				if vo, ok := v.Val.(ElemMatch); ok {
					var sub = make(primitive.M)
					sub[ElMathConditionOperator] = primitive.M{vo.Key: vo.Val}
					result[v.Key] = sub
				}
			}
		}
	}
	return result
}

// Update Deprecated: method works not correctly use bom generator (https://github.com/cjp2600/protoc-gen-bom)
func (b *Bom) Update(entity interface{}) (*mongo.UpdateResult, error) {
	mp, _ := b.structToMap(entity)
	var eRes []primitive.E
	if len(mp) > 0 {
		for key, val := range mp {
			if val != nil {
				eRes = append(eRes, primitive.E{Key: key, Value: val})
			}
		}
	}

	upResult := primitive.D{
		{"$set", eRes},
		{"$currentDate", primitive.D{{"updatedat", true}}},
	}

	return b.UpdateRaw(upResult)
}

// UpdateRaw - update one eq
func (b *Bom) UpdateRaw(update interface{}) (*mongo.UpdateResult, error) {
	err := callToBeforeUpdate(b.model)
	if err != nil {
		return nil, err
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	res, err := b.Mongo().UpdateOne(ctx, b.getCondition(), update, b.options.updateOptions...)

	err = callToAfterUpdate(b.model)
	if err != nil {
		return nil, err
	}

	return res, err
}

// InsertOne - insert one method
func (b *Bom) InsertOne(document interface{}) (*mongo.InsertOneResult, error) {
	err := callToBeforeInsert(b.model)
	if err != nil {
		return nil, err
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	insertOneResult, err := b.Mongo().InsertOne(ctx, document, b.options.insertOptions...)
	if err != nil {
		return nil, err
	}

	err = callToAfterInsert(b.model)
	if err != nil {
		return nil, err
	}

	return insertOneResult, nil
}

// InsertMany insert meany
func (b *Bom) InsertMany(documents []interface{}) (*mongo.InsertManyResult, error) {

	for _, document := range documents {
		err := callToBeforeInsert(document)
		if err != nil {
			return nil, err
		}
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	insertManyResult, err := b.Mongo().InsertMany(ctx, documents)
	if err != nil {
		return nil, err
	}

	for _, document := range documents {
		err = callToAfterInsert(document)
		if err != nil {
			return nil, err
		}
	}

	return insertManyResult, err
}

// FindOne find one item method.
func (b *Bom) FindOne(callback func(s *mongo.SingleResult) error) error {

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	s := b.Mongo().FindOne(ctx, b.getCondition(), b.options.findOneOptions...)
	return callback(s)
}

// FindOneAndUpdate find and update item method
func (b *Bom) FindOneAndUpdate(update interface{}) (*mongo.SingleResult, error) {

	err := callToBeforeUpdate(b.model)
	if err != nil {
		return nil, err
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	r := b.Mongo().FindOneAndUpdate(ctx, b.getCondition(), update, b.options.findOneAndUpdateOptions...)
	if r.Err() != nil {
		return nil, err
	}

	err = callToAfterUpdate(b.model)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// FindOneAndDelete find and delete item method
func (b *Bom) FindOneAndDelete() (*mongo.SingleResult, error) {
	err := callToBeforeDelete(b.model)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	r := b.Mongo().FindOneAndDelete(ctx, b.getCondition())
	if r.Err() != nil {
		return nil, err
	}

	err = callToAfterDelete(b.model)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// DeleteMany delete many
func (b *Bom) DeleteMany() (*mongo.DeleteResult, error) {

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	return b.Mongo().DeleteMany(ctx, b.getCondition())
}

// Delete delete item
func (b *Bom) Delete() (*mongo.DeleteResult, error) {
	err := callToBeforeDelete(b.model)
	if err != nil {
		return nil, err
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	r, err := b.Mongo().DeleteOne(ctx, b.getCondition())
	if err != nil {
		return nil, err
	}

	err = callToAfterDelete(b.model)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// AggregateWithPagination pagination aggr
func (b *Bom) AggregateWithPagination(callback func(c *mongo.Cursor) (int32, error)) (*Pagination, error) {
	aggregateOpts := options.Aggregate()
	aggregateOpts.SetAllowDiskUse(false)

	pagination := NewPagination(b.limit.Page, b.limit.Size)
	facet := NewFacetStage()
	limit, offset := pagination.CalculateOffset()
	facet.SetLimit(limit + offset)
	facet.SetSkip(offset)
	if sm := b.getSort(); sm != nil {
		facet.SetSort(sm)
	}

	b.FillPipeline(facet)
	b.options.aggregateOptions = append(b.options.aggregateOptions, aggregateOpts)

	pipeline, err := b.pipeline.Aggregate()
	if err != nil {
		return nil, err
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	cur, err := b.Mongo().Aggregate(ctx, pipeline, b.options.aggregateOptions...)
	if err != nil {
		return &Pagination{}, err
	}

	defer cur.Close(ctx)

	var count int32
	if count, err = callback(cur); err != nil {
		return nil, err
	}

	if err := cur.Err(); err != nil {
		return nil, err
	}

	return pagination.WithTotal(count), err
}

// ListWithPagination list of items with pagination
func (b *Bom) ListWithPagination(callback func(cursor *mongo.Cursor) error) (*Pagination, error) {
	pagination := NewPagination(b.limit.Page, b.limit.Size)
	limit, offset := pagination.CalculateOffset()

	var findOptions = options.Find()
	findOptions.SetLimit(int64(limit)).SetSkip(int64(offset))
	if sm := b.getSort(); sm != nil {
		findOptions.SetSort(sm)
	}
	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

	condition := b.getCondition()
	b.options.findOptions = append(b.options.findOptions, findOptions)

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	var count int64
	var err error
	if condition != nil {
		if bs, ok := condition.(primitive.M); ok {
			if len(bs) > 0 {
				count, err = b.Mongo().CountDocuments(ctx, condition)
			} else {
				count, err = b.Mongo().EstimatedDocumentCount(ctx)
			}
		}
	}
	if err != nil {
		return &Pagination{}, err
	}

	// set default context
	ctx, cancel = context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	cur, err := b.Mongo().Find(ctx, condition, b.options.findOptions...)
	if err != nil {
		return &Pagination{}, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		err = callback(cur)
	}
	if err := cur.Err(); err != nil {
		return &Pagination{}, err
	}
	return pagination.WithTotal(int32(count)), err
}

// ListWithLastID iteration method for deep pagination
func (b *Bom) ListWithLastID(callback func(cursor *mongo.Cursor) error) (lastID string, err error) {

	lastID = b.lastID
	findOptions := options.Find()
	findOptions.SetLimit(int64(b.limit.Size))

	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

	if lastID != "" {
		b.whereConditions("_id", GreaterConditionOperator, ToObj(lastID))
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	cur, err := b.Mongo().Find(ctx, b.getCondition(), findOptions)
	if err != nil {
		return "", err
	}

	defer func() {
		err := cur.Close(ctx)
		if err != nil {
			log.Println(err)
		}
	}()

	var lastElement primitive.ObjectID
	for cur.Next(ctx) {
		err = callback(cur)
		lastElement = cur.Current.Lookup("_id").ObjectID()
	}
	if err := cur.Err(); err != nil {
		return "", err
	}

	// set default context
	ctx, cancel = context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	var count int64
	if b.getCondition() != nil {
		if bs, ok := b.getCondition().(primitive.M); ok {
			if len(bs) > 0 {
				count, err = b.Mongo().CountDocuments(ctx, b.getCondition())
			} else {
				count, err = b.Mongo().EstimatedDocumentCount(ctx)
			}
		}
	} else {
		count, err = b.Mongo().EstimatedDocumentCount(ctx)
	}
	if err != nil {
		return "", err
	}

	if count > int64(b.limit.Size) {
		return lastElement.Hex(), err
	}

	return "", err
}

// List Common items list method
func (b *Bom) List(callback func(cursor *mongo.Cursor) error) error {
	findOptions := options.Find()
	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

	// set default context
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	cur, err := b.Mongo().Find(ctx, b.getCondition(), findOptions)
	if err != nil {
		return err
	}

	defer cur.Close(ctx)

	for cur.Next(ctx) {
		err = callback(cur)
	}

	if err := cur.Err(); err != nil {
		return err
	}

	return err
}

// conditionTransformer internal method for transform condition
func (b *Bom) conditionTransformer(field string, operator string, value interface{}) map[string]interface{} {
	var c = make(map[string]interface{})
	c["field"] = field
	c["value"] = value
	if operator != EqualConditionOperator {
		c["value"] = primitive.D{{Key: operator, Value: value}}
	}
	return c
}

// whereConditions internal method for build or condition
func (b *Bom) orWhereConditions(field string, conditions string, value interface{}) *Bom {
	b.conditions.orConditions = append(b.conditions.orConditions, b.conditionTransformer(field, conditions, value))
	return b
}

// whereConditions internal method for build condition
func (b *Bom) whereConditions(field string, conditions string, value interface{}) *Bom {
	b.conditions.whereConditions = append(b.conditions.whereConditions, b.conditionTransformer(field, conditions, value))
	return b
}

// buildCondition internal build condition method
func (b *Bom) buildCondition() interface{} {
	result := make(primitive.M)
	if len(b.conditions.whereConditions) > 0 {
		var query []primitive.M
		for _, cnd := range b.conditions.whereConditions {
			field := cnd["field"]
			value := cnd["value"]
			query = append(query, primitive.M{field.(string): value})
		}
		result[AndConditionOperator] = query
	}
	if len(b.conditions.orConditions) > 0 {
		var query []primitive.M
		for _, cnd := range b.conditions.orConditions {
			field := cnd["field"]
			value := cnd["value"]
			query = append(query, primitive.M{field.(string): value})
		}
		result[OrConditionOperator] = query
	}
	if len(b.conditions.inConditions) > 0 {
		for _, cnd := range b.conditions.inConditions {
			field := cnd["field"]
			value := cnd["value"]
			result[field.(string)] = primitive.M{InConditionOperator: value}
		}
	}
	if len(b.conditions.notInConditions) > 0 {
		for _, cnd := range b.conditions.notInConditions {
			field := cnd["field"]
			value := cnd["value"]
			result[field.(string)] = primitive.M{NotInConditionOperator: value}
		}
	}
	return result
}

// structToMap struct to map
func (b *Bom) structToMap(i interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	dataBytes, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(dataBytes, &result)
	if err != nil {
		return nil, err
	}
	correct := make(map[string]interface{})
	for key, value := range result {
		if _, ok := b.skipWhenUpdating[key]; !ok {
			correct[key] = value
		}
	}
	return correct, nil
}

// getSort get sort object
func (b *Bom) getSort() map[string]interface{} {
	var sortMap map[string]interface{}
	if len(b.sort) > 0 {
		sortMap = make(map[string]interface{})
		for _, sort := range b.sort {
			if len(sort.Field) > 0 {
				sortMap[strings.ToLower(sort.Field)] = 1
				if len(sort.Type) > 0 {
					if val, ok := SortTypeMatcher[strings.ToLower(sort.Type)]; ok {
						sortMap[strings.ToLower(sort.Field)] = val
					}
				}
			}
		}
	}
	return sortMap
}

// getCondition common condition builder method
func (b *Bom) getCondition() interface{} {
	if b.condition != nil {
		return b.condition
	}
	bc := b.buildCondition()
	if bc != nil {
		if val, ok := bc.(primitive.M); ok {
			return val
		}
	}
	return primitive.M{}
}
