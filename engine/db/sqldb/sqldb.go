package sqldb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type (
	SqlDBConfig struct {
		Username     string `json:"username"`
		Password     string `json:"password"`
		Host         string `json:"host"`
		Port         int    `json:"port"`
		Database     string `json:"database"`
		MaxOpenConns int    `json:"maxopenconns"`
		MaxIdleConns int    `json:"maxidleconns"`
		CheckArgs    bool   `json:"checkargs"` //参数检查，开启后，执行SQL语句时，会检查参数是否符合要求，但是会影响性能，默认不开启
	}

	SqlDB struct {
		*sqlx.DB

		config        SqlDBConfig
		connectstring string //连接字符串
		dbtype        string //数据库类型，mysql，postgres，sqlite3等
	}
)

func NewSqlDB(dbtype string, conf *SqlDBConfig) (*SqlDB, error) {
	db := &SqlDB{
		config:        *conf,
		dbtype:        dbtype,
		connectstring: "",
	}

	switch dbtype {
	case "mysql":
		db.connectstring = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", conf.Username, conf.Password, conf.Host, conf.Port, conf.Database)
	case "postgres":
		db.connectstring = fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=disable", conf.Username, conf.Password, conf.Host, conf.Port, conf.Database)
	case "sqlite3":
		db.connectstring = fmt.Sprintf("%s", conf.Database)
	default:
		return nil, errors.New("dbtype error")
	}

	return db, nil
}

func (db *SqlDB) Connect() error {
	xdb, err := sqlx.Connect(db.dbtype, db.connectstring)
	if err != nil {
		return err
	}

	db.DB = xdb

	// 设置连接池参数
	db.SetMaxOpenConns(db.config.MaxOpenConns)
	db.SetMaxIdleConns(db.config.MaxIdleConns)

	return nil
}

func (db *SqlDB) Close() {
	if db != nil {
		db.DB.Close()
	}
}

// 执行SQL语句，返回结果
func (db *SqlDB) ExecSQL(strSql string, args ...interface{}) (sql.Result, error) {
	if db.config.CheckArgs {
		err := db.checkArgs(args...)
		if err != nil {
			return nil, err
		}
	}

	return db.DB.Exec(strSql, args...)
}

func (db *SqlDB) NewTransaction() (*Transaction, error) {
	tx, err := db.DB.Beginx()
	if err != nil {
		return nil, err
	}

	return &Transaction{Tx: tx}, nil
}

func (db *SqlDB) Select(dest interface{}, tableName string, columns []string, decorator *Decorator) error {
	// 参数类型验证
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest必须是指向切片的指针")
	}

	sql := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName
	if decorator != nil {
		sql += " " + decorator.decorator
	}

	rows, err := db.DB.Queryx(sql, decorator.params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	elemType := dv.Elem().Type().Elem()
	slice := dv.Elem()
	for rows.Next() {
		elem := reflect.New(elemType).Elem()
		err = rows.StructScan(elem.Addr().Interface())
		if err != nil {
			return err
		}

		slice = reflect.Append(slice, elem)
	}

	return nil
}

func (db *SqlDB) Insert(tableName string, columns []string, values []interface{}, decorator *Decorator) (sql.Result, error) {
	if len(columns) != len(values) {
		return nil, errors.New("columns and values length not equal")
	}

	sql := "INSERT INTO " + tableName + " (" + strings.Join(columns, ", ") + ") VALUES ("
	sql += strings.Repeat("?, ", len(columns)-1) + "?)"
	if decorator != nil {
		sql += " " + decorator.decorator
		values = append(values, decorator.params...)
	}

	return db.DB.Exec(sql, values...)
}

func (db *SqlDB) Update(tableName string, columns []string, values []interface{}, decorator *Decorator) (sql.Result, error) {
	if len(columns) != len(values) {
		return nil, errors.New("columns and values length not equal")
	}

	sql := "UPDATE " + tableName + " SET " + strings.Join(columns, "=?, ") + "=? "
	if decorator != nil {
		sql += " " + decorator.decorator
		values = append(values, decorator.params...)
	}

	return db.DB.Exec(sql, values...)
}

func (db *SqlDB) Delete(tableName string, decorator *Decorator) (sql.Result, error) {
	sql := "DELETE FROM " + tableName
	if decorator != nil {
		sql += " " + decorator.decorator
	}

	sql += ";"

	return db.DB.Exec(sql, decorator.params...)
}

func (db *SqlDB) Count(tableName string, decorator *Decorator) (int64, error) {
	sql := "SELECT COUNT(*) FROM " + tableName
	if decorator != nil {
		sql += " " + decorator.decorator
	}

	var count int64
	err := db.DB.Get(&count, sql, decorator.params...)
	return count, err
}

// SQL语句参数检查
func (db *SqlDB) checkArgs(args ...interface{}) error {
	for _, val := range args {
		if reflect.TypeOf(val).Kind() == reflect.String {
			retVal := val.(string)
			if strings.Contains(retVal, "-") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(retVal, "#") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(retVal, "&") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(retVal, "=") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(retVal, "%") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(retVal, "'") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(strings.ToLower(retVal), "delete ") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(strings.ToLower(retVal), "truncate ") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(strings.ToLower(retVal), " or ") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(strings.ToLower(retVal), "from ") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
			if strings.Contains(strings.ToLower(retVal), "set ") == true {
				return fmt.Errorf("error arg is %+v", retVal)
			}
		}
	}

	return nil
}
