package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
    "reflect"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/recover"
	gocursor "github.com/sijms/go-ora/v2"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logs = logrus.New()
var db *sql.DB
var dbMutex sync.RWMutex
var debug *bool
var port *string
var dns *string
var multiSpaceRegex = regexp.MustCompile(`\s+`)

type Param struct {
	IsOutput bool
	Value    interface{} // 输入参数值或输出参数指针
	Type     string      // 输出参数类型
	Name     string      // 输出参数名称
}

func init() {
	debug = flag.Bool("d", true, "debug msg")
	port  = flag.String("p", "8004", "服务端口")
	dns   = flag.String("dns", "oracle://user:pwd@localhost:port/db", "oracle://user:pwd@localhost:port/db")
	flag.Parse()
}

// 完全去除所有空格
func productionClean(arr []string) []string {
	var result []string
	for _, s := range arr {
		processed := multiSpaceRegex.ReplaceAllString(strings.TrimSpace(s), " ")
		if processed != "" { // 过滤空字符串
			result = append(result, processed)
		}
	}
	return result
}

func initLog() {
	// 创建日志目录
	logDir := "log"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("创建日志目录失败: %v", err)
		// 可以选择继续执行，使用当前目录
	}

	// 日志
	logs.SetFormatter(&logrus.JSONFormatter{})
	logger := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "execproc.log"), // 日志文件路径
		MaxSize:    2,                                     // 日志文件大小，单位是 MB
		MaxBackups: 30,                                     // 最大过期日志保留个数
		MaxAge:     60,                                    // 保留过期文件最大时间，单位 天
		Compress:   true,                                  // 是否压缩日志，默认是不压缩。这里设置为true，压缩日志
	}
	logs.SetOutput(logger) // logrus 设置日志的输出方式
}

// 初始化数据
func initDb() error {
	// 连接配置
	conn, err := sql.Open("oracle", *dns)
	if err != nil {
		return fmt.Errorf("数据库连接失败: %v", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("数据库连接测试失败: %v", err)
	}

	// 配置连接池参数
	conn.SetMaxOpenConns(20)           // 最大活动连接数
	conn.SetMaxIdleConns(5)           // 最大空闲连接数
	conn.SetConnMaxLifetime(30 * time.Minute) // 连接最大生命周期
	conn.SetConnMaxIdleTime(5 * time.Minute)  // 空闲连接最大存活时间

	dbMutex.Lock()
	db = conn
	dbMutex.Unlock()

	return nil
}

// 获取数据库连接（带重连机制）
func getDB() (*sql.DB, error) {
	dbMutex.RLock()
	conn := db
	dbMutex.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("数据库连接未初始化")
	}

	// 检查连接是否有效
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		// 连接无效，尝试重连
		logs.Warnf("数据库连接失败，尝试重连: %v", err)
		if err := initDb(); err != nil {
			return nil, fmt.Errorf("数据库重连失败: %v", err)
		}
		
		dbMutex.RLock()
		conn = db
		dbMutex.RUnlock()
	}

	return conn, nil
}

type JsonRes struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// 主函数入口
func main() {
	// 初始日志
	initLog()

	// 初始化数据库
	if err := initDb(); err != nil {
		logs.Fatalf("数据库初始化失败: %v", err)
	}

	// 定期检查数据库连接
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if conn, err := getDB(); err != nil {
				logs.Errorf("数据库连接检查失败: %v", err)
			} else {
				// 执行简单的健康检查
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := conn.PingContext(ctx); err != nil {
					logs.Errorf("数据库健康检查失败: %v", err)
				}
				cancel()
			}
		}
	}()

	app := fiber.New(fiber.Config{
		BodyLimit: 10 * 1024 * 1024, // 10MB
		ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout: 60 * time.Second,
	})

	// 恢复 panic
	app.Use(recover.New())

	// 启用 Gzip/Brotli 压缩中间件
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
	}))

	// 执行存储过程
	app.Post("/execproc", ExecProc)

	// 启动端口
	logs.Infof("服务启动，监听端口: %s", *port)
	if err := app.Listen(fmt.Sprintf(":%v", *port)); err != nil {
		logs.Fatalf("服务启动失败: %v", err)
	}
}

// 执行存储过程
func ExecProc(c *fiber.Ctx) error {
	startTime := time.Now()
	
	c.Set("content-type", "application/json")

	// Field names should start with an uppercase letter
	type Data struct {
		ProcName string `json:"proc_name" xml:"proc_name" form:"proc_name"`
		Params   string `json:"params" xml:"params" form:"params"`
	}

	// 参数绑定
	newdata := new(Data)
	if err := c.BodyParser(newdata); err != nil {
		return c.JSON(JsonRes{Code: 4000, Msg: "请求参数解析失败"})
	}

	procName := strings.TrimSpace(newdata.ProcName)
	paramStr := strings.TrimSpace(newdata.Params)

	// 记录请求日志
	requestID := c.Get("X-Request-ID")
	if requestID == "" {
		requestID = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	logEntry := logs.WithFields(logrus.Fields{
		"request_id": requestID,
		"proc_name":  procName,
		"params":     paramStr,
	})

	// sql输出
	if *debug {
		logEntry.Info("请求")
		log.Println("procName:", procName, "params:", paramStr)
	}

	// 参数验证
	if procName == "" || paramStr == "" {
		logEntry.Warn("参数为空")
		return c.JSON(JsonRes{Code: 4001, Msg: "参数不能为空"})
	}

	// 获取数据库连接
	conn, err := getDB()
	if err != nil {
		logEntry.Errorf("获取数据库连接失败: %v", err)
		return c.JSON(JsonRes{Code: 5001, Msg: "数据库连接失败"})
	}

	// 解析参数
	params, err := parseParams(paramStr)
	if err != nil {
		logEntry.Errorf("参数解析失败: %v", err)
		return c.JSON(JsonRes{Code: 4002, Msg: fmt.Sprintf("参数解析失败: %v", err)})
	}

	// 构建存储过程调用
	cmdText := buildProcCommand(procName, len(params))

	// 构建执行参数
	execParams := buildExecParams(params)

	// 执行存储过程
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = conn.ExecContext(ctx, cmdText, execParams...)
	if err != nil {
		logEntry.Errorf("存储过程执行失败: %v", err)
		return c.JSON(JsonRes{Code: 5002, Msg: fmt.Sprintf("存储过程执行失败: %v", err)})
	}

	// 获取输出参数
	results, err := Outputs(conn, params)
	if err != nil {
		logEntry.Errorf("获取输出参数失败: %v", err)
		return c.JSON(JsonRes{Code: 5003, Msg: fmt.Sprintf("获取输出参数失败: %v", err)})
	}

	logEntry.Info(fmt.Sprintf("耗时: %v", time.Since(startTime)))
	return c.JSON(JsonRes{Code: 200, Msg: "成功", Data: results})
}

// 解析参数字符串
func parseParams(paramStr string) ([]Param, error) {
	parts := strings.Split(paramStr, "|")
	params := make([]Param, 0, len(parts))
	
	// 完全去除所有空格
	parts = productionClean(parts)
	
	for _, p := range parts {
		if strings.Contains(p, "#") { // 输出参数
			pair := strings.Split(p, "#")
			if len(pair) != 2 {
				return nil, fmt.Errorf("无效的输出参数格式: %s", p)
			}
			
			paramType := strings.ToLower(strings.TrimSpace(pair[0]))
			paramName := strings.TrimSpace(pair[1])
			
			param := Param{IsOutput: true, Type: paramType, Name: paramName}
			
			switch paramType {
			case "int":
				var v int
				param.Value = &v
			case "float", "number":
				var v float64
				param.Value = &v
			case "string":
				var v string
				param.Value = &v
			case "cursor":
				var v gocursor.RefCursor
				param.Value = &v
			case "bool":
				var v bool
				param.Value = &v
			default:
				return nil, fmt.Errorf("不支持的类型: %s", paramType)
			}
			params = append(params, param)
		} else { // 输入参数
			params = append(params, Param{
				IsOutput: false,
				Value:    strings.TrimSpace(p),
			})
		}
	}
	return params, nil
}

// 构建存储过程调用命令
func buildProcCommand(name string, paramCount int) string {
	placeholders := make([]string, paramCount)
	for i := 0; i < paramCount; i++ {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	return fmt.Sprintf(`
		BEGIN
			%s(%s);
		END;`, name, strings.Join(placeholders, ","))
}

// 构建执行参数列表
func buildExecParams(params []Param) []interface{} {
	execParams := make([]interface{}, 0, len(params))
	for _, p := range params {
		if p.IsOutput {
			execParams = append(execParams, sql.Out{Dest: p.Value})
		} else {
			execParams = append(execParams, p.Value)
		}
	}
	return execParams
}

// 获取输出参数
func Outputs(conn *sql.DB, params []Param) ([]interface{}, error) {
	var results []interface{}
	
	for _, p := range params {
		if !p.IsOutput {
			continue
		}
		
		record := make(map[string]interface{})
		switch p.Type {
		case "int":
			if ptr, ok := p.Value.(*int); ok && ptr != nil {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "float", "number":
			if ptr, ok := p.Value.(*float64); ok && ptr != nil {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "string":
			if ptr, ok := p.Value.(*string); ok && ptr != nil {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "bool":
			if ptr, ok := p.Value.(*bool); ok && ptr != nil {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "cursor":
			if cursorPtr, ok := p.Value.(*gocursor.RefCursor); ok && cursorPtr != nil {
				item, err := handleCursor(conn, cursorPtr)
				if err != nil {
					return nil, fmt.Errorf("处理游标失败: %v", err)
				}
				results = append(results, item)
				continue
			}
		default:
			record[strings.ToLower(p.Name)] = "(未知类型)"
		}
		
		if len(record) > 0 {
			results = append(results, record)
		}
	}
	return results, nil
}

// 游标结果集处理
func handleCursor(conn *sql.DB, cursor *gocursor.RefCursor) ([]interface{}, error) {
	if cursor == nil {
		return nil, fmt.Errorf("游标为空")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rows, err := gocursor.WrapRefCursor(ctx, conn, cursor)
	if err != nil {
		return nil, fmt.Errorf("打开游标失败: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}

	// 处理结果集
	var results []interface{}
	rowCount := 0

	// 预估结果集大小，避免过多的内存分配
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("获取列类型失败: %v", err)
	}

	for rows.Next() {
		// 使用具体的类型而不是 interface{} 来减少内存分配
		scanArgs := make([]interface{}, len(columns))
		for i, colType := range columnTypes {
			switch colType.DatabaseTypeName() {
			case "NUMBER", "INTEGER", "FLOAT", "DECIMAL":
				var v sql.NullFloat64
				scanArgs[i] = &v
			case "VARCHAR2", "CHAR", "CLOB", "NVARCHAR2":
				var v sql.NullString
				scanArgs[i] = &v
			case "DATE", "TIMESTAMP":
				var v sql.NullTime
				scanArgs[i] = &v
			case "RAW", "BLOB":
				var v []byte
				scanArgs[i] = &v
			default:
				var v interface{}
				scanArgs[i] = &v
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("扫描行数据失败: %v", err)
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			val := reflectValue(scanArgs[i])
			record[strings.ToLower(col)] = val
		}
		results = append(results, record)
		rowCount++

		// 限制结果集大小，防止内存溢出
		if rowCount > 10000 { // 可以根据实际情况调整
			logs.Warnf("游标结果集超过限制，已截断: %d 行", rowCount)
			break
		}
	}

	// 检查是否有迭代错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历行时出错: %v", err)
	}

	logs.Debugf("游标处理完成，共 %d 行", rowCount)
	return results, nil
}

// 反射值处理
func reflectValue(arg interface{}) interface{} {
	switch v := arg.(type) {
	case *sql.NullString:
		if v.Valid {
			return v.String
		}
		return nil
	case *sql.NullFloat64:
		if v.Valid {
			return v.Float64
		}
		return nil
	case *sql.NullInt64:
		if v.Valid {
			return v.Int64
		}
		return nil
	case *sql.NullBool:
		if v.Valid {
			return v.Bool
		}
		return nil
	case *sql.NullTime:
		if v.Valid {
			return v.Time
		}
		return nil
	case *[]byte:
		if v != nil {
			return string(*v)
		}
		return nil
	case *interface{}:
		return *v
	default:
		// 如果是双重指针，解引用
		val := reflect.ValueOf(arg)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				return nil
			}
			elem := val.Elem()
			if elem.Kind() == reflect.Ptr {
				if elem.IsNil() {
					return nil
				}
				return elem.Elem().Interface()
			}
			return elem.Interface()
		}
		return arg
	}
}