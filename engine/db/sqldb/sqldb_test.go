package sqldb

import "testing"

func openDB(t *testing.T) *SqlDB {
	dbConfig := SqlDBConfig{
		Username:     "root",
		Password:     "666666",
		Host:         "localhost",
		Port:         3306,
		Database:     "test",
		MaxOpenConns: 100,
		MaxIdleConns: 10,
		CheckArgs:    false,
	}

	db, err := NewSqlDB("mysql", &dbConfig)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Connect()
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func TestSqlDB_Count(t *testing.T) {
	sqlDB := openDB(t)
	if sqlDB == nil {
		t.Fatal("openDB error")
	}

	deco := &Decorator{decorator: ""}
	deco.Where("id = ?", 1)

	count, err := sqlDB.Count("test", deco)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("record count : ", count)
}

func TestSqlDB_Delete(t *testing.T) {
	sqlDB := openDB(t)
	if sqlDB == nil {
		t.Fatal("openDB error")
	}

	deco := &Decorator{decorator: ""}
	deco.Where("id = ?", 1)

	_, err := sqlDB.Delete("test", deco)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLBuilder_Insert(t *testing.T) {
	sqlDB := openDB(t)

	values := []interface{}{1, "name"}

	sql := "INSERT INTO test (id, value) VALUES (?, ?)"
	_, err := sqlDB.Exec(sql, values...)
	if err != nil {
		t.Fatal(err)
	}

	defer sqlDB.Close()
}

func TestSQLBuilder_Select(t *testing.T) {
	sqlDB := openDB(t)

	columns := []string{"id", "value"}

	decorator := &Decorator{decorator: ""}
	decorator.Where("id =?", 1)

	type Test struct {
		Id    int    `json:"id"`
		Value string `json:"value"`
	}

	var test []Test
	err := sqlDB.Select(&test, "test", columns, decorator)

	if err != nil {
		t.Fatal(err)
	}

	for _, v := range test {
		t.Log(v)
	}

	defer sqlDB.Close()
}
