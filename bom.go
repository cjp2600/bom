package bom

// BOM Mongodb Mongo builder of (go.mongodb.org/mongo-driver)

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	Bom struct {
		client                  *mongo.Client
		dbName                  string
		dbCollection            string
		queryTimeout            time.Duration
		condition               interface{}
		skipWhenUpdating        map[string]bool
		whereConditions         []map[string]interface{}
		orConditions            []map[string]interface{}
		inConditions            []map[string]interface{}
		notInConditions         []map[string]interface{}
		notConditions           []map[string]interface{}
		pipeline                AggregateStages
		aggregateOptions        []*options.AggregateOptions
		updateOptions           []*options.UpdateOptions
		insertOptions           []*options.InsertOneOptions
		findOneOptions          []*options.FindOneOptions
		findOneAndUpdateOptions []*options.FindOneAndUpdateOptions
		pagination              *Pagination
		limit                   *Limit
		sort                    []*Sort
		lastId                  string
		useAggrigate            bool
		selectArg               []interface{}
	}
	Pagination struct {
		TotalCount  int32
		TotalPages  int32
		CurrentPage int32
		Size        int32
	}
	Sort struct {
		Field string
		Type  string
	}
	Limit struct {
		Page int32
		Size int32
	}
	Size      int32
	Option    func(*Bom) error
	ElemMatch struct {
		Key string
		Val interface{}
	}
)

const (
	DefaultQueryTimeout = 5 * time.Second
	DefaultSize         = 20
)

var (
	mType            = map[string]int32{"asc": 1, "desc": -1}
	skipWhenUpdating = map[string]bool{"id": true, "createdat": true, "updatedat": true}
)

func New(options ...Option) (*Bom, error) {
	b := &Bom{
		queryTimeout: DefaultQueryTimeout,
		pagination: &Pagination{
			Size:        DefaultSize,
			CurrentPage: 1,
		},
		skipWhenUpdating: skipWhenUpdating,
		limit:            &Limit{Page: 1, Size: DefaultSize},
	}
	for _, option := range options {
		if err := option(b); err != nil {
			return nil, err
		}
	}
	if b.client == nil {
		return nil, fmt.Errorf("mondodb client is required")
	}
	return b, nil
}

func ElMatch(key string, val interface{}) ElemMatch {
	return ElemMatch{Key: key, Val: val}
}

func ToObj(id string) primitive.ObjectID {
	objectID, _ := primitive.ObjectIDFromHex(id)
	return objectID
}

func ToObjects(ids []string) []primitive.ObjectID {
	var objectIds []primitive.ObjectID
	for _, id := range ids {
		objectId, _ := primitive.ObjectIDFromHex(id)
		objectIds = append(objectIds, objectId)
	}
	return objectIds
}

func SetMongoClient(client *mongo.Client) Option {
	return func(b *Bom) error {
		b.client = client
		return nil
	}
}

func SetDatabaseName(dbName string) Option {
	return func(b *Bom) error {
		b.dbName = dbName
		return nil
	}
}

func SetSkipWhenUpdating(fieldsMap map[string]bool) Option {
	return func(b *Bom) error {
		b.skipWhenUpdating = fieldsMap
		return nil
	}
}

func SetCollection(collection string) Option {
	return func(b *Bom) error {
		b.dbCollection = collection
		return nil
	}
}

func SetQueryTimeout(time time.Duration) Option {
	return func(b *Bom) error {
		b.queryTimeout = time
		return nil
	}
}

func (b *Bom) WithDB(dbName string) *Bom {
	b.dbName = dbName
	return b
}

func (b *Bom) WithColl(collection string) *Bom {
	b.dbCollection = collection
	return b
}

func (b *Bom) WithTimeout(time time.Duration) *Bom {
	b.queryTimeout = time
	return b
}

func (b *Bom) WithCondition(condition interface{}) *Bom {
	b.condition = condition
	return b
}

func (b *Bom) WithLimit(limit *Limit) *Bom {
	if limit.Page > 0 {
		b.limit.Page = limit.Page
	}
	if limit.Size > 0 {
		b.limit.Size = limit.Size
	}
	return b
}

func (b *Bom) WithLastId(lastId string) *Bom {
	b.lastId = lastId
	return b
}

func (b *Bom) WithSort(sort *Sort) *Bom {
	b.sort = append(b.sort, sort)
	return b
}

func (b *Bom) WithSize(size int32) *Bom {
	if size > 0 {
		b.limit.Size = size
	}
	return b
}

func (b *Bom) FillPipeline(p ...Stager) {
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
	b = b.WhereConditions(field, "=", value)
	return b
}

func (b *Bom) WhereEq(field string, value interface{}) *Bom {
	b = b.WhereConditions(field, "=", value)
	return b
}

func (b *Bom) WhereNotEq(field string, value interface{}) *Bom {
	b = b.WhereConditions(field, "!=", value)
	return b
}

func (b *Bom) WhereGt(field string, value interface{}) *Bom {
	b = b.WhereConditions(field, ">", value)
	return b
}

func (b *Bom) WhereGte(field string, value interface{}) *Bom {
	b = b.WhereConditions(field, ">=", value)
	return b
}

func (b *Bom) WhereLt(field string, value interface{}) *Bom {
	b = b.WhereConditions(field, "<", value)
	return b
}

func (b *Bom) WhereLte(field string, value interface{}) *Bom {
	b = b.WhereConditions(field, "<=", value)
	return b
}
func (b *Bom) AddSelect(arg interface{}) *Bom {
	b.useAggrigate = true
	b.selectArg = append(b.selectArg, arg)
	return b
}

func (b *Bom) Select(arg ...interface{}) *Bom {
	b.useAggrigate = true
	b.selectArg = arg
	return b
}

func (b *Bom) WhereConditions(field string, conditions string, value interface{}) *Bom {
	switch conditions {
	case ">":
		b.whereConditions = append(b.whereConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$gt", Value: value}}})
	case ">=":
		b.whereConditions = append(b.whereConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$gte", Value: value}}})
	case "<":
		b.whereConditions = append(b.whereConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$lt", Value: value}}})
	case "<=":
		b.whereConditions = append(b.whereConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$lte", Value: value}}})
	case "!=":
		b.whereConditions = append(b.whereConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$ne", Value: value}}})
	default:
		b.whereConditions = append(b.whereConditions, map[string]interface{}{"field": field, "value": value})
	}
	return b
}

func (b *Bom) OrWhereConditions(field string, conditions string, value interface{}) *Bom {
	switch conditions {
	case ">":
		b.orConditions = append(b.orConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$gt", Value: value}}})
	case ">=":
		b.orConditions = append(b.orConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$gte", Value: value}}})
	case "<":
		b.orConditions = append(b.orConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$lt", Value: value}}})
	case "<=":
		b.orConditions = append(b.orConditions, map[string]interface{}{"field": field, "value": primitive.D{{Key: "$lte", Value: value}}})
	default:
		b.orConditions = append(b.orConditions, map[string]interface{}{"field": field, "value": value})
	}
	return b
}

func (b *Bom) SetUpdateOptions(opts ...*options.UpdateOptions) *Bom {
	for _, value := range opts {
		b.updateOptions = append(b.updateOptions, value)
	}
	return b
}

func (b *Bom) SetAggrigateOptions(opts ...*options.AggregateOptions) *Bom {
	b.aggregateOptions = opts
	return b
}

func (b *Bom) SetInsertOptions(opts ...*options.InsertOneOptions) *Bom {
	for _, value := range opts {
		b.insertOptions = append(b.insertOptions, value)
	}
	return b
}

func (b *Bom) SetFindOneOptions(opts ...*options.FindOneOptions) *Bom {
	for _, value := range opts {
		b.findOneOptions = append(b.findOneOptions, value)
	}
	return b
}

func (b *Bom) SetFindOnEndUpdateOptions(opts ...*options.FindOneAndUpdateOptions) *Bom {
	for _, value := range opts {
		b.findOneAndUpdateOptions = append(b.findOneAndUpdateOptions, value)
	}
	return b
}

func (b *Bom) OrWhereEq(field string, value interface{}) *Bom {
	b = b.OrWhereConditions(field, "=", value)
	return b
}

func (b *Bom) OrWhereGt(field string, value interface{}) *Bom {
	b = b.OrWhereConditions(field, ">", value)
	return b
}

func (b *Bom) OrWhereGte(field string, value interface{}) *Bom {
	b = b.OrWhereConditions(field, ">=", value)
	return b
}

func (b *Bom) OrWhereLt(field string, value interface{}) *Bom {
	b = b.OrWhereConditions(field, "<", value)
	return b
}

func (b *Bom) OrWhereLte(field string, value interface{}) *Bom {
	b = b.OrWhereConditions(field, "<=", value)
	return b
}

func (b *Bom) Not(field string, value interface{}) *Bom {
	b.notConditions = append(b.notConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

func (b *Bom) InWhere(field string, value interface{}) *Bom {
	b.inConditions = append(b.inConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

func (b *Bom) NotInWhere(field string, value interface{}) *Bom {
	b.notInConditions = append(b.notInConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

//Deprecated: should use OrWhereConditions or OrWhereEq
func (b *Bom) OrWhere(field string, value interface{}) *Bom {
	b.OrWhereEq(field, value)
	return b
}

func (b *Bom) AggregateWithPagination(callback func(c *mongo.Cursor) (int32, error)) (*Pagination, error) {
	p := &Pagination{}

	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	aggregateOpts := options.Aggregate()
	aggregateOpts.SetAllowDiskUse(false)

	facet := NewFacetStage()
	limit, offset := b.calculateOffset(b.limit.Page, b.limit.Size)
	facet.SetLimit(limit)
	facet.SetSkip(offset)
	if sm := b.getSort(); sm != nil {
		facet.SetSort(sm)
	}

	b.FillPipeline(facet)

	pipeline, err := b.pipeline.Aggregate()
	if err != nil {
		return p, err
	}

	cur, err := b.Mongo().Aggregate(ctx, pipeline, aggregateOpts)
	if err != nil {
		return &Pagination{}, err
	}

	defer cur.Close(ctx)

	count := int32(0)
	if count, err = callback(cur); err != nil {
		return p, err
	}

	if err := cur.Err(); err != nil {
		return p, err
	}

	return b.getPagination(count, b.limit.Page, b.limit.Size), err
}

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
					sub["$elemMatch"] = primitive.M{vo.Key: vo.Val}
					result[v.Key] = sub
				}
			}
		}
	}
	return result
}

func (b *Bom) buildCondition() interface{} {
	result := make(primitive.M)
	if len(b.whereConditions) > 0 {
		var query []primitive.M
		for _, cnd := range b.whereConditions {
			field := cnd["field"]
			value := cnd["value"]
			query = append(query, primitive.M{field.(string): value})
		}
		result["$and"] = query
	}
	if len(b.orConditions) > 0 {
		var query []primitive.M
		for _, cnd := range b.orConditions {
			field := cnd["field"]
			value := cnd["value"]
			query = append(query, primitive.M{field.(string): value})
		}
		result["$or"] = query
	}
	if len(b.inConditions) > 0 {
		for _, cnd := range b.inConditions {
			field := cnd["field"]
			value := cnd["value"]
			result[field.(string)] = primitive.M{"$in": value}
		}
	}
	if len(b.notInConditions) > 0 {
		for _, cnd := range b.notInConditions {
			field := cnd["field"]
			value := cnd["value"]
			result[field.(string)] = primitive.M{"$nin": value}
		}
	}
	return result
}

func (b *Bom) Mongo() *mongo.Collection {
	return b.client.Database(b.dbName).Collection(b.dbCollection)
}

func (b *Bom) getTotalPages() int32 {
	d := float64(b.pagination.TotalCount) / float64(b.pagination.Size)
	if d < 0 {
		d = 1
	}
	return int32(math.Ceil(d))
}

func (b *Bom) getPagination(total int32, page int32, size int32) *Pagination {
	b.pagination.TotalCount = total
	if page > 0 {
		b.pagination.CurrentPage = page
	}
	if size > 0 {
		b.pagination.Size = size
	}
	b.pagination.TotalPages = b.getTotalPages()
	return b.pagination
}

func (b *Bom) readFieldName(f reflect.StructField) string {
	val, ok := f.Tag.Lookup("json")
	if !ok {
		return strings.ToLower(f.Name)
	}
	opts := strings.Split(val, ",")
	return strings.ToLower(opts[0])
}

func (b *Bom) structToMap(i interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	v := reflect.ValueOf(i)
	t := v.Type()
	if t.Kind() != reflect.Struct {
		return result, fmt.Errorf("type %s is not supported", t.Kind())
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		if val, exist := f.Tag.Lookup("update"); exist {
			if strings.ToLower(val) != "true" {
				continue
			}
		} else {
			continue
		}

		fv := v.Field(i)
		key := b.readFieldName(f)
		tp := fv.Type().String()

		value := fv.Interface()
		switch tp {
		case "string":
			value = fv.String()
			if fv.String() == "" {
				continue
			}
		case "interface {}":
			value = fv.Interface()
		case "int", "int8", "int16", "int32", "int64":
			value = fv.Int()
		case "float64", "float32":
			value = fv.Float()
		}

		if _, ok := b.skipWhenUpdating[key]; !ok {
			result[key] = value
		}
	}
	return result, nil
}

func (b *Bom) calculateOffset(page, size int32) (limit int32, offset int32) {
	limit = b.limit.Size
	if page == 0 {
		page = 1
	}
	if size > 0 {
		limit = size
	}
	o := float64(page-1) * float64(limit)
	offset = int32(math.Ceil(o))
	return
}

func (b *Bom) getSort() map[string]interface{} {
	var sortMap map[string]interface{}
	if len(b.sort) > 0 {
		sortMap = make(map[string]interface{})
		for _, sort := range b.sort {
			if len(sort.Field) > 0 {
				sortMap[strings.ToLower(sort.Field)] = 1
				if len(sort.Type) > 0 {
					if val, ok := mType[strings.ToLower(sort.Type)]; ok {
						sortMap[strings.ToLower(sort.Field)] = val
					}
				}
			}
		}
	}
	return sortMap
}

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

//Deprecated: method works not correctly user bom generator (https://github.com/cjp2600/protoc-gen-bom)
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

func (b *Bom) UpdateRaw(update interface{}) (*mongo.UpdateResult, error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	res, err := b.Mongo().UpdateOne(ctx, b.getCondition(), update, b.updateOptions...)
	return res, err
}

func (b *Bom) InsertOne(document interface{}) (*mongo.InsertOneResult, error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	return b.Mongo().InsertOne(ctx, document, b.insertOptions...)
}

func (b *Bom) ConvertJsonToBson(document interface{}) (interface{}, error) {
	bytes, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	var bsonDocument interface{}
	err = bson.UnmarshalExtJSON(bytes, true, &bsonDocument)
	if err != nil {
		return nil, err
	}
	return bsonDocument, nil
}

func (b *Bom) InsertMany(documents []interface{}) (*mongo.InsertManyResult, error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	var bsonDocuments []interface{}
	for _, document := range documents {
		bsonDocuments = append(bsonDocuments, document)
	}
	return b.Mongo().InsertMany(ctx, documents)
}

func (b *Bom) FindOne(callback func(s *mongo.SingleResult) error) error {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	s := b.Mongo().FindOne(ctx, b.getCondition(), b.findOneOptions...)
	return callback(s)
}

func (b *Bom) FindOneAndUpdate(update interface{}) *mongo.SingleResult {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	return b.Mongo().FindOneAndUpdate(ctx, b.getCondition(), update, b.findOneAndUpdateOptions...)
}

func (b *Bom) FindOneAndDelete() *mongo.SingleResult {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	return b.Mongo().FindOneAndDelete(ctx, b.getCondition())
}

func (b *Bom) DeleteMany() (*mongo.DeleteResult, error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	return b.Mongo().DeleteMany(ctx, b.getCondition())
}

func (b *Bom) ListWithPagination(callback func(cursor *mongo.Cursor) error) (*Pagination, error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	findOptions := options.Find()
	limit, offset := b.calculateOffset(b.limit.Page, b.limit.Size)

	findOptions.SetLimit(int64(limit)).SetSkip(int64(offset))

	if sm := b.getSort(); sm != nil {
		findOptions.SetSort(sm)
	}

	condition := b.getCondition()

	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

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
	cur, err := b.Mongo().Find(ctx, condition, findOptions)
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
	pagination := b.getPagination(int32(count), b.limit.Page, b.limit.Size)
	return pagination, err
}

func (b *Bom) ListWithLastId(callback func(cursor *mongo.Cursor) error) (lastId string, err error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	lastId = b.lastId
	cur := &mongo.Cursor{}

	defer cur.Close(ctx)

	findOptions := options.Find()

	findOptions.SetLimit(int64(b.limit.Size))

	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

	if lastId != "" {
		b.WhereConditions("_id", ">", ToObj(lastId))
	}

	cur, err = b.Mongo().Find(ctx, b.getCondition(), findOptions)
	if err != nil {
		return "", err
	}

	var lastElement primitive.ObjectID

	for cur.Next(ctx) {
		err = callback(cur)
		lastElement = cur.Current.Lookup("_id").ObjectID()
	}

	if err := cur.Err(); err != nil {
		return "", err
	}

	count, err := b.Mongo().CountDocuments(ctx, b.getCondition())
	if err != nil {
		return "", err
	}

	if count > int64(b.limit.Size) {
		return lastElement.Hex(), err
	} else {
		return "", err
	}
}

func (b *Bom) List(callback func(cursor *mongo.Cursor) error) error {
	ctx, _ := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	findOptions := options.Find()
	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

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
