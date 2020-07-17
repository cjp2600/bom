package bom

// BOM Mongodb Mongo builder of (go.mongodb.org/mongo-driver)

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	// Bom структура
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
		lastID                  string
		useAggrigate            bool
		selectArg               []interface{}
	}
	// Pagination структура
	Pagination struct {
		TotalCount  int32
		TotalPages  int32
		CurrentPage int32
		Size        int32
	}
	// Sort структура
	Sort struct {
		Field string
		Type  string
	}
	// Limit структура
	Limit struct {
		Page int32
		Size int32
	}
	// Size структура
	Size int32
	// Option структура
	Option func(*Bom) error
	// ElemMatch структура
	ElemMatch struct {
		Key string
		Val interface{}
	}
)

const (
	// DefaultQueryTimeout время выполнение запрос по умолчанию
	DefaultQueryTimeout = 5 * time.Second
	// DefaultSize количетсво документов по умочанию
	DefaultSize = 20
)

var (
	mType            = map[string]int32{"asc": 1, "desc": -1}
	skipWhenUpdating = map[string]bool{"id": true, "createdat": true, "updatedat": true}
)

// New конструктор бома
func New(o ...Option) (*Bom, error) {
	b := &Bom{
		queryTimeout: DefaultQueryTimeout,
		pagination: &Pagination{
			Size:        DefaultSize,
			CurrentPage: 1,
		},
		skipWhenUpdating: skipWhenUpdating,
		limit:            &Limit{Page: 1, Size: DefaultSize},
	}
	for _, option := range o {
		if err := option(b); err != nil {
			return nil, err
		}
	}
	if b.client == nil {
		return nil, fmt.Errorf("mondodb client is required")
	}
	return b, nil
}

// ElMatch не используется
func ElMatch(key string, val interface{}) ElemMatch {
	return ElemMatch{Key: key, Val: val}
}

// ToObj преоброзование строки в ObjectId
func ToObj(id string) primitive.ObjectID {
	objectID, _ := primitive.ObjectIDFromHex(id)
	return objectID
}

// ToObjects преоброзование массива строк в  массив ObjectId
func ToObjects(ids []string) []primitive.ObjectID {
	objectIds := make([]primitive.ObjectID, 0, len(ids))
	for _, id := range ids {
		objectID, _ := primitive.ObjectIDFromHex(id)
		objectIds = append(objectIds, objectID)
	}
	return objectIds
}

// SetMongoClient подмена клиента монги
func SetMongoClient(client *mongo.Client) Option {
	return func(b *Bom) error {
		b.client = client
		return nil
	}
}

// SetDatabaseName установка название базы
// TODO: Deprecated ?
func SetDatabaseName(dbName string) Option {
	return func(b *Bom) error {
		b.dbName = dbName
		return nil
	}
}

// SetSkipWhenUpdating пропустить обновление
func SetSkipWhenUpdating(fieldsMap map[string]bool) Option {
	return func(b *Bom) error {
		b.skipWhenUpdating = fieldsMap
		return nil
	}
}

// SetCollection установка названия коллекции
// TODO: Deprecated ?
func SetCollection(collection string) Option {
	return func(b *Bom) error {
		b.dbCollection = collection
		return nil
	}
}

// SetQueryTimeout установка время запроса
// TODO: Deprecated ?
func SetQueryTimeout(t time.Duration) Option {
	return func(b *Bom) error {
		b.queryTimeout = t
		return nil
	}
}

// WithDB использовать базу
func (b *Bom) WithDB(dbName string) *Bom {
	b.dbName = dbName
	return b
}

// WithColl использовать коллекцию
func (b *Bom) WithColl(collection string) *Bom {
	b.dbCollection = collection
	return b
}

// WithTimeout использовать время выполнение запроса
func (b *Bom) WithTimeout(t time.Duration) *Bom {
	b.queryTimeout = t
	return b
}

// WithCondition условия запроса
func (b *Bom) WithCondition(condition interface{}) *Bom {
	b.condition = condition
	return b
}

// WithLimit лимиты
func (b *Bom) WithLimit(limit *Limit) *Bom {
	if limit.Page > 0 {
		b.limit.Page = limit.Page
	}
	if limit.Size > 0 {
		b.limit.Size = limit.Size
	}
	return b
}

// WithLastID последний элемент
func (b *Bom) WithLastID(lastID string) *Bom {
	b.lastID = lastID
	return b
}

// WithSort сортировка
func (b *Bom) WithSort(sort *Sort) *Bom {
	b.sort = append(b.sort, sort)
	return b
}

// WithSize размер страницы с документами
func (b *Bom) WithSize(size int32) *Bom {
	if size > 0 {
		b.limit.Size = size
	}
	return b
}

// FillPipeline очистить pipeline
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

// Where выборка
// Deprecated: should use whereConditions or WhereEq
func (b *Bom) Where(field string, value interface{}) *Bom {
	b = b.conditions("and", field, "=", value)
	return b
}

// WhereEq выртрать все элементы с фильтром
func (b *Bom) WhereEq(field string, value interface{}) *Bom {
	b = b.conditions("and", field, "=", value)
	return b
}

// WhereNotEq выртрать все элементы с фильтром
func (b *Bom) WhereNotEq(field string, value interface{}) *Bom {
	b = b.conditions("and", field, "!=", value)
	return b
}

// WhereGt выртрать все элементы с фильтром
func (b *Bom) WhereGt(field string, value interface{}) *Bom {
	b = b.conditions("and", field, ">", value)
	return b
}

// WhereGte выртрать все элементы с фильтром
func (b *Bom) WhereGte(field string, value interface{}) *Bom {
	b = b.conditions("and", field, ">=", value)
	return b
}

// WhereLt выртрать все элементы с фильтром
func (b *Bom) WhereLt(field string, value interface{}) *Bom {
	b = b.conditions("and", field, "<", value)
	return b
}

// WhereLte выртрать все элементы с фильтром
func (b *Bom) WhereLte(field string, value interface{}) *Bom {
	b = b.conditions("and", field, "<=", value)
	return b
}

// AddSelect добавить выборку значений
func (b *Bom) AddSelect(arg interface{}) *Bom {
	b.useAggrigate = true
	b.selectArg = append(b.selectArg, arg)
	return b
}

// Select добавить выборку значений
func (b *Bom) Select(arg ...interface{}) *Bom {
	b.useAggrigate = true
	b.selectArg = arg
	return b
}

func (b *Bom) conditions(t, field, conditions string, value interface{}) *Bom {
	var tp []map[string]interface{}

	switch conditions {
	case ">":
		tp = append(tp, map[string]interface{}{"field": field, "value": primitive.D{{Key: GreaterConditionOperator, Value: value}}})
	case ">=":
		tp = append(tp, map[string]interface{}{"field": field, "value": primitive.D{{Key: GreaterOrEqualConditionOperator, Value: value}}})
	case "<":
		tp = append(tp, map[string]interface{}{"field": field, "value": primitive.D{{Key: LessConditionOperator, Value: value}}})
	case "<=":
		tp = append(tp, map[string]interface{}{"field": field, "value": primitive.D{{Key: LessOrEqualConditionOperator, Value: value}}})
	case "!=":
		tp = append(tp, map[string]interface{}{"field": field, "value": primitive.D{{Key: NotEqualConditionOperator, Value: value}}})
	default:
		tp = append(tp, map[string]interface{}{"field": field, "value": value})
	}

	if t == "or" {
		b.orConditions = tp
		return b
	}
	b.whereConditions = tp
	return b
}

// SetUpdateOptions опиции для обновления
func (b *Bom) SetUpdateOptions(opts ...*options.UpdateOptions) *Bom {
	b.updateOptions = append(b.updateOptions, opts...)
	return b
}

// SetAggregateOptions опиции для агрегации
func (b *Bom) SetAggregateOptions(opts ...*options.AggregateOptions) *Bom {
	b.aggregateOptions = opts
	return b
}

// SetInsertOptions опиции для добавления
func (b *Bom) SetInsertOptions(opts ...*options.InsertOneOptions) *Bom {
	b.insertOptions = append(b.insertOptions, opts...)
	return b
}

// SetFindOneOptions опиции для поиска
func (b *Bom) SetFindOneOptions(opts ...*options.FindOneOptions) *Bom {
	b.findOneOptions = append(b.findOneOptions, opts...)
	return b
}

// SetFindOnEndUpdateOptions опиции
func (b *Bom) SetFindOnEndUpdateOptions(opts ...*options.FindOneAndUpdateOptions) *Bom {
	b.findOneAndUpdateOptions = append(b.findOneAndUpdateOptions, opts...)
	return b
}

// OrWhereEq условя выборки
func (b *Bom) OrWhereEq(field string, value interface{}) *Bom {
	b = b.conditions("or", field, "=", value)
	return b
}

// OrWhereNotEq условя выборки
func (b *Bom) OrWhereNotEq(field string, value interface{}) *Bom {
	b = b.conditions("or", field, "!=", value)
	return b
}

// OrWhereGt условя выборки
func (b *Bom) OrWhereGt(field string, value interface{}) *Bom {
	b = b.conditions("or", field, ">", value)
	return b
}

// OrWhereGte условя выборки
func (b *Bom) OrWhereGte(field string, value interface{}) *Bom {
	b = b.conditions("or", field, ">=", value)
	return b
}

// OrWhereLt условя выборки
func (b *Bom) OrWhereLt(field string, value interface{}) *Bom {
	b = b.conditions("or", field, "<", value)
	return b
}

// OrWhereLte условя выборки
func (b *Bom) OrWhereLte(field string, value interface{}) *Bom {
	b = b.conditions("or", field, "<=", value)
	return b
}

// Not условя выборки
func (b *Bom) Not(field string, value interface{}) *Bom {
	b.notConditions = append(b.notConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

// InWhere условя выборки
func (b *Bom) InWhere(field string, value interface{}) *Bom {
	b.inConditions = append(b.inConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

// NotInWhere условя выборки
func (b *Bom) NotInWhere(field string, value interface{}) *Bom {
	b.notInConditions = append(b.notInConditions, map[string]interface{}{"field": field, "value": value})
	return b
}

// OrWhere старый метод
// Deprecated: OrWhere should be of the form orWhereConditions or OrWhereEq
func (b *Bom) OrWhere(field string, value interface{}) *Bom {
	b.OrWhereEq(field, value)
	return b
}

// AggregateWithPagination условя выборки
func (b *Bom) AggregateWithPagination(callback func(c *mongo.Cursor) (int32, error)) (*Pagination, error) {
	p := &Pagination{}

	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	aggregateOpts := options.Aggregate()
	aggregateOpts.SetAllowDiskUse(false)

	facet := NewFacetStage()
	limit, offset := b.calculateOffset(b.limit.Page, b.limit.Size)
	facet.SetLimit(limit + offset)
	facet.SetSkip(offset)
	if sm := b.getSort(); sm != nil {
		facet.SetSort(sm)
	}

	b.FillPipeline(facet)

	pipeline, err := b.pipeline.Aggregate()
	if err != nil {
		return p, err
	}

	var cur *mongo.Cursor
	cur, err = b.Mongo().Aggregate(ctx, pipeline, aggregateOpts)
	if err != nil {
		return &Pagination{}, err
	}

	defer cur.Close(ctx)

	count := int32(0)
	if count, err = callback(cur); err != nil {
		return p, err
	}

	if cur.Err() != nil {
		return p, cur.Err()
	}

	return b.getPagination(count, b.limit.Page, b.limit.Size), err
}

// BuildProjection что-то непонтяное
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

// Mongo иницилизация монги
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

func (b *Bom) getPagination(total, page, size int32) *Pagination {
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

func (b *Bom) calculateOffset(page, size int32) (limit, offset int32) {
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

// UpdateRaw обновление
func (b *Bom) UpdateRaw(update interface{}) (*mongo.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	res, err := b.Mongo().UpdateOne(ctx, b.getCondition(), update, b.updateOptions...)
	return res, err
}

// InsertOne добавление
func (b *Bom) InsertOne(document interface{}) (*mongo.InsertOneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	return b.Mongo().InsertOne(ctx, document, b.insertOptions...)
}

// ConvertJSONToBson конвертор
func (b *Bom) ConvertJSONToBson(document interface{}) (interface{}, error) {
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

// InsertMany добавление
func (b *Bom) InsertMany(documents []interface{}) (*mongo.InsertManyResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	return b.Mongo().InsertMany(ctx, documents)
}

// FindOne поиск
func (b *Bom) FindOne(callback func(s *mongo.SingleResult) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	s := b.Mongo().FindOne(ctx, b.getCondition(), b.findOneOptions...)
	return callback(s)
}

// FindOneAndUpdate поиск и обновление
func (b *Bom) FindOneAndUpdate(update interface{}) *mongo.SingleResult {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	return b.Mongo().FindOneAndUpdate(ctx, b.getCondition(), update, b.findOneAndUpdateOptions...)
}

// FindOneAndDelete поиск и удаление
func (b *Bom) FindOneAndDelete() *mongo.SingleResult {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	return b.Mongo().FindOneAndDelete(ctx, b.getCondition())
}

// DeleteMany удаление
func (b *Bom) DeleteMany() (*mongo.DeleteResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	return b.Mongo().DeleteMany(ctx, b.getCondition())
}

// ListWithPagination пагинатор
func (b *Bom) ListWithPagination(callback func(cursor *mongo.Cursor) error) (*Pagination, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

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

	var (
		count int64
		err   error
		cur   *mongo.Cursor
	)
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
	cur, err = b.Mongo().Find(ctx, condition, findOptions)
	if err != nil {
		return &Pagination{}, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		err = callback(cur)
		if err != nil {
			return &Pagination{}, err
		}
	}
	if cur.Err() != nil {
		return &Pagination{}, cur.Err()
	}
	pagination := b.getPagination(int32(count), b.limit.Page, b.limit.Size)
	return pagination, err
}

// ListWithLastID вывод списка
func (b *Bom) ListWithLastID(callback func(cursor *mongo.Cursor) error) (lastID string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

	lastID = b.lastID
	cur := &mongo.Cursor{}

	defer cur.Close(ctx)

	findOptions := options.Find()
	findOptions.SetLimit(int64(b.limit.Size))

	if projection := b.BuildProjection(); projection != nil {
		findOptions.SetProjection(projection)
	}

	if lastID != "" {
		b.conditions("and", "_id", ">", ToObj(lastID))
	}

	cur, err = b.Mongo().Find(ctx, b.getCondition(), findOptions)
	if err != nil {
		return "", err
	}

	var (
		count       int64
		lastElement primitive.ObjectID
	)

	for cur.Next(ctx) {
		err = callback(cur)
		lastElement = cur.Current.Lookup("_id").ObjectID()
	}

	if cur.Err() != nil {
		return "", cur.Err()
	}

	count, err = b.Mongo().CountDocuments(ctx, b.getCondition())
	if err != nil {
		return "", err
	}

	if count > int64(b.limit.Size) {
		return lastElement.Hex(), err
	}
	return "", err
}

// List получение списка документов
func (b *Bom) List(callback func(cursor *mongo.Cursor) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.queryTimeout)
	defer cancel()

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
		if err != nil {
			return err
		}
	}

	if cur.Err() != nil {
		return cur.Err()
	}

	return err
}
