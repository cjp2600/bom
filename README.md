# BOM (builder objects of mongodb)
Mongodb query wrapper based on (go.mongodb.org/mongo-driver)

### Example
``` go
var users []*model.User

// create new instace
bm, err := bom.New(
	bom.SetMongoClient(repo.GetClient()), // go.mongodb.org/mongo-driver
	bom.SetDatabaseName(repo.config.DBName),
)

if err != nil {
    fmt.Error(err)
}

// Pagination List
bm.WithColl(MongoUser).WithLimit(&bom.Limit{Page: pg.Page, Size: pg.Size})

// set sorting
if sort != nil {
	bm.WithSort(&bom.Sort{Field: sort.Field, Type: sort.Type})
}

// set condition
bm.Where("_id", bom.ToObj(id))

// execute with 
pagination, err := bm.ListWithPagination(func(cur *mongo.Cursor) error {
	var result model.User
	err := cur.Decode(&result)
	users = append(users, &result)
	return err
})
	
```