package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beego/beego/v2/adapter/logs"
	_ "github.com/mattn/go-sqlite3"
	"reflect"
	"strconv"
)

var sillyGirl Bucket
var Zero Bucket

/*
func MakeBucket(name string) Bucket {
	if Zero == nil {
		logs.Error("找不到存储器，开发者自行实现接口。")
	}
	return Zero.Copy(name)
}
*/

type Bucket interface {
	Copy(string) Bucket
	Set(interface{}, interface{}) error
	Empty() (bool, error)
	Size() (int64, error)
	Delete() error
	Buckets() ([][]byte, error)
	GetString(...interface{}) string
	GetBytes(string) []byte
	GetInt(interface{}, ...int) int
	GetBool(interface{}, ...bool) bool
	Foreach(func([]byte, []byte) error)
	Create(interface{}) error
	First(interface{}) error
	String() string
}

const (
	dbDriverName = "sqlite3"
	dbName       = "./sillyGirl.db"
)

var db *sql.DB

type Sqlite3 string

type KeyValueMap struct {
	key   string
	value string
}

func init() {
	//初始化日志
	//core.InitMyLog()

	//logs.Info("初始化sqlite3数据库")
	var err error
	db, err = sql.Open(dbDriverName, dbName)
	if err != nil {
		logs.Info("打开数据库错误", err)
	}
	//defer db.Close()
	if db == nil {
		logs.Info("sqlite3数据库初始化错误")
	} else {
		logs.Info("sqlite3连接成功")
	}
	Zero = MakeBucket("sillyGirl")
}

func (s Sqlite3) String() string {
	return string(s)
}

func createTable(bucketName string) error {
	//logs.Info("创建表", bucketName)
	sql := `create table if not exists ` + bucketName + `(
		key text primary key,
		value text
	)`
	_, err := db.Exec(sql)
	return err
}

func checkTable(tableName string) (bool, error) {
	exist := false
	sql := `SELECT count(*) as cnt FROM sqlite_master WHERE type = 'table' AND name='` + tableName + `'`
	rows, err := db.Query(sql)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		exist = true
		break
	}
	//logs.Info("检查", tableName, "是否存在：", exist)
	return exist, nil
}

func insertRow(tableName, key, value string) error {
	//logs.Info("插入数据行", `table=`, tableName, `key=`, key, `value=`, value)
	//sql := `insert into ` + table + `(key,value)values('` + key + `','` + value + `')`
	sql := fmt.Sprintf(`INSERT INTO %s(key, value) VALUES ('%s', '%s') ON CONFLICT (key) DO UPDATE SET value='%s'`, tableName, key, value, value)
	_, err := db.Exec(sql)
	//logs.Info("插入数据行", rlt, err)
	return err
}

func deleteData(table, key string) (bool, error) {
	//logs.Info("删除数据库数据")
	sql := `delete from ` + table + ` where key='` + key + `'`
	res, err := db.Exec(sql)
	if err != nil {
		logs.Error(err)
		return false, err
	}
	_, err = res.RowsAffected()
	if err != nil {
		logs.Error(err)
		return false, err
	}
	return true, nil
}

func queryData(table, key string) (string, error) {
	//logs.Info("查询数据库数据", `table=`, table, `key=`, key)
	sql := `select * from ` + table + ` where key='` + key + `'`
	//logs.Info(sql)
	rows, err := db.Query(sql)
	//logs.Info("查询数据", rows, err)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var result = make([]KeyValueMap, 0)
	for rows.Next() {
		var key, value string
		rows.Scan(&key, &value)
		result = append(result, KeyValueMap{key, value})
		break
	}
	if len(result) <= 0 {
		return "", errors.New("没有" + key + "值")
	} else {
		return result[0].value, nil
	}
}

func queryDatas(table string) ([]KeyValueMap, error) {
	//logs.Info("查询数据库表", table, "的所有数据")
	sql := `select * from ` + table
	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result = make([]KeyValueMap, 0)
	for rows.Next() {
		var key, value string
		rows.Scan(&key, &value)
		result = append(result, KeyValueMap{key, value})
	}
	//logs.Info("查询数据库某表的所有数据", result, err)
	return result, nil
}

func MakeBucket(name string) Bucket {
	var store Bucket = Sqlite3(name)
	createTable(name)
	return store
}

//生成一个自动增加的整数
func (s Sqlite3) NextSequence() (int64, error) {
	//查询最后一条记录
	sql := `select * from ` + s.String() + ` order by key desc limit 1`
	row, err := db.Query(sql)
	if err != nil {
		return -1, err
	}
	defer row.Close()
	var key, value string
	for row.Next() {
		row.Scan(&key, &value)
	}
	if key != "" {
		if no, err := strconv.ParseInt(key, 10, 64); err != nil {
			return -1, err
		} else {
			return no + 1, nil
		}
	} else {
		return 0, nil
	}
}

func (s Sqlite3) Copy(bucket string) Bucket {
	return MakeBucket(bucket)
}
func (s Sqlite3) Set(key interface{}, value interface{}) error {
	if fmt.Sprint(value) == "" {
		//logs.Info("赋值为空")
		_, err := deleteData(s.String(), fmt.Sprint(key))
		return err
	}
	return insertRow(s.String(), fmt.Sprint(key), fmt.Sprint(value))
}

func (s Sqlite3) Empty() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (s Sqlite3) Size() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (s Sqlite3) Delete() error {
	//TODO implement me
	panic("implement me")
}

func (s Sqlite3) Buckets() ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (s Sqlite3) GetString(kv ...interface{}) string {
	//logs.Info("GetString")
	var key, value string
	for i := range kv {
		if i == 0 {
			key = fmt.Sprint(kv[0])
		} else {
			value = fmt.Sprint(kv[1])
		}
	}
	v, _ := queryData(s.String(), key)
	if v != "" {
		return v
	} else {
		return value
	}
}
func (s Sqlite3) GetBytes(key string) []byte {
	v, _ := queryData(s.String(), key)
	if v != "" {
		return []byte(v)
	} else {
		return []byte("")
	}
}
func (s Sqlite3) GetInt(key interface{}, vs ...int) int {
	var value int
	if len(vs) != 0 {
		value = vs[0]
	}
	v, _ := queryData(s.String(), fmt.Sprint(key))
	if v != "" {
		val, err := strconv.Atoi(v)
		if err != nil {
			return value
		} else {
			return val
		}
	} else {
		return value
	}
}
func (s Sqlite3) GetBool(key interface{}, vs ...bool) bool {
	var value bool
	if len(vs) != 0 {
		value = vs[0]
	}
	v, _ := queryData(s.String(), fmt.Sprint(key))
	if v != "" {
		val, err := strconv.ParseBool(v)
		if err != nil {
			return value
		} else {
			return val
		}
	} else {
		return value
	}
}
func (s Sqlite3) Foreach(f func(k, v []byte) error) {
	kvm, _ := queryDatas(s.String())
	for _, kv := range kvm {
		f([]byte(kv.key), []byte(kv.value))
	}

}

//将结构体更新或存储到持久化表中
func (s3 Sqlite3) Create(i interface{}) error {
	//logs.Error("进入数据库create函数")
	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("ID")
	sequence := s.FieldByName("Sequence")

	//如果表不存在，就创建
	b, _ := checkTable(s3.String())
	if !b { //id为int型
		err := createTable(s3.String())
		if err != nil {
			return err
		}
	}

	//如果id为int类型
	if _, ok := id.Interface().(int); ok {
		key := id.Int()
		sq, err := s3.NextSequence()
		if err != nil {
			return err
		}
		if key == 0 {
			key = int64(sq)
			id.SetInt(key)
		}
		if sequence != reflect.ValueOf(nil) {
			sequence.SetInt(int64(sq))
		}
		buf, err := json.Marshal(i)
		if err != nil {
			return err
		}
		return s3.Set(fmt.Sprintf("%d", key), string(buf))
	} else { //id为string类型
		key := id.String()
		sq, err := s3.NextSequence()
		//logs.Error(sq, err)
		if err != nil {
			return err
		}
		if key == "" {
			key = fmt.Sprint(sq)
			id.SetString(key)
		}
		if sequence != reflect.ValueOf(nil) {
			sequence.SetInt(int64(sq))
		}
		buf, err := json.Marshal(i)
		if err != nil {
			return err
		}
		return s3.Set(key, string(buf))
	}
}

//获取数据表中第一个元素并解析到i
func (s3 Sqlite3) First(i interface{}) error {

	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("ID")
	if v, ok := id.Interface().(int); ok {
		if bl, _ := checkTable(s3.String()); bl {
			err := errors.New("bucket not find")
			return err
		}
		data, err := queryData(s3.String(), fmt.Sprintf("%d", v))
		if err != nil {
			return err
		}
		if len(data) == 0 {
			err := errors.New("record not find")
			return err
		}
		return json.Unmarshal([]byte(data), i)

	} else {
		if v, ok := id.Interface().(string); !ok {
			err := errors.New("bucket not find")
			return err
		} else {
			data, err := queryData(s3.String(), v)
			if err != nil {
				return err
			}
			if len(data) == 0 {
				err := errors.New("record not find")
				return err
			}
			return json.Unmarshal([]byte(data), i)
		}

	}
}
