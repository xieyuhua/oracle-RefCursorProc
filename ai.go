package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"github.com/gofiber/adaptor/v2"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	gocursor "github.com/sijms/go-ora/v2"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

// 版本信息
const (
	Version   = "2.1.0"
	BuildTime = "2026-05-08"
)

var (
	logs        = logrus.New()
	db          *sql.DB
	dbMutex     sync.RWMutex
	debugMode   *bool
	port        *string
	dns         *string
	pprofPort   *string
	cpuprofile  *string
	memprofile  *string
	multiSpaceRegex = regexp.MustCompile(`\s+`)

	// 统计信息
	stats = &Stats{
		ReqCount:   atomic.Int64{},
		ReqSuccess: atomic.Int64{},
		ReqFailed:  atomic.Int64{},
	}
)

// Stats 统计信息 - 使用环形缓冲区防止内存泄露
type Stats struct {
	ReqCount   atomic.Int64
	ReqSuccess atomic.Int64
	ReqFailed  atomic.Int64
	ReqTimes   [1024]time.Duration // 环形缓冲区
	ReqTimesIdx atomic.Int64       // 当前索引
	ReqTimesMutex sync.Mutex       // 保护统计计算
}

// Param 参数定义
type Param struct {
	IsOutput bool
	Value    interface{}
	Type     string
	Name     string
}

// JsonRes 统一响应结构
type JsonRes struct {
	Code   int         `json:"code"`
	Msg    string      `json:"msg"`
	Data   interface{} `json:"data,omitempty"`
	CostMs int64       `json:"cost_ms,omitempty"`
}

// Config 配置结构
type Config struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	QueryTimeout    time.Duration
	MaxResultRows   int
	ReadBodyLimit   int
}

var cfg = &Config{
	MaxOpenConns:    20,
	MaxIdleConns:    5,
	ConnMaxLifetime: 30 * time.Minute,
	ConnMaxIdleTime: 5 * time.Minute,
	QueryTimeout:    30 * time.Second,
	MaxResultRows:   10000,
	ReadBodyLimit:   10 * 1024 * 1024,
}

func init() {
	debugMode = flag.Bool("d", true, "debug mode")
	port = flag.String("p", "8004", "service port")
	dns = flag.String("dns", "oracle://user:pwd@localhost:1521/orcl", "oracle connection string")
	pprofPort = flag.String("pprof", "8005", "pprof port")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile = flag.String("memprofile", "", "write memory profile to file")
	flag.Parse()
}

// 初始化日志
func initLog() {
	logDir := "log"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("创建日志目录失败: %v", err)
	}

	logs.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
	})
	logs.SetLevel(logrus.InfoLevel)
	if *debugMode {
		logs.SetLevel(logrus.DebugLevel)
	}

	logger := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "execproc.log"),
		MaxSize:    10,
		MaxBackups: 30,
		MaxAge:     60,
		Compress:   true,
	}
	logs.SetOutput(logger)
}

// 初始化数据库
func initDb() error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if db != nil {
		_ = db.Close()
		db = nil
	}

	conn, err := sql.Open("oracle", *dns)
	if err != nil {
		return fmt.Errorf("数据库连接失败: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("数据库连接测试失败: %w", err)
	}

	conn.SetMaxOpenConns(cfg.MaxOpenConns)
	conn.SetMaxIdleConns(cfg.MaxIdleConns)
	conn.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	conn.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	db = conn
	logs.Info("数据库连接初始化成功")
	return nil
}

// 获取数据库连接
func getDB() (*sql.DB, error) {
	dbMutex.RLock()
	conn := db
	dbMutex.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("数据库连接未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		dbMutex.Lock()
		if db != nil && db == conn {
			_ = db.Close()
		}
		dbMutex.Unlock()

		logs.Warnf("数据库连接失效，尝试重连: %v", err)
		if err := initDb(); err != nil {
			return nil, fmt.Errorf("数据库重连失败: %w", err)
		}

		dbMutex.RLock()
		conn = db
		dbMutex.RUnlock()
	}

	return conn, nil
}

// 清理资源
func cleanup() {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if db != nil {
		_ = db.Close()
		db = nil
		logs.Info("数据库连接已关闭")
	}
}

// 健康检查
func handleHealth(c *fiber.Ctx) error {
	conn, err := getDB()
	if err != nil {
		return c.JSON(JsonRes{Code: 5001, Msg: err.Error()})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		return c.JSON(JsonRes{Code: 5002, Msg: "数据库连接失败"})
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return c.JSON(JsonRes{
		Code: 200,
		Msg:  "ok",
		Data: fiber.Map{
			"version":       Version,
			"goroutines":    runtime.NumGoroutine(),
			"memory_alloc":  m.Alloc,
			"total_req":     stats.ReqCount.Load(),
			"success_req":   stats.ReqSuccess.Load(),
			"failed_req":    stats.ReqFailed.Load(),
		},
	})
}

func setupPprof(app *fiber.App) {
    // 创建专门用于 pprof 的 HTTP 多路复用器
    pprofMux := http.NewServeMux()
    pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
    pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
    pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
    pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
    
    pprofMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
    pprofMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
    pprofMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
    pprofMux.Handle("/debug/pprof/block", pprof.Handler("block"))
    pprofMux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
    pprofMux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
    
    // 将整个 pprofMux 适配到 Fiber
    app.All("/debug/pprof/*", adaptor.HTTPHandler(pprofMux))
}

// 主函数
func main() {
	initLog()
	logs.Infof("服务启动，版本: %s", Version)

	if err := initDb(); err != nil {
		logs.Fatalf("数据库初始化失败: %v", err)
	}

	app := fiber.New(fiber.Config{
		BodyLimit:         cfg.ReadBodyLimit,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		DisableKeepalive:  false,
		ReduceMemoryUsage: true,
		Prefork:           false,
	})
	
    // 设置 pprof
    setupPprof(app)

	app.Use(recover.New(recover.Config{
		EnableStackTrace: *debugMode,
		StackTraceHandler: func(c *fiber.Ctx, e interface{}) {
			logs.WithField("request_id", c.Get("X-Request-ID")).Errorf("Panic: %v", e)
		},
	}))

	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
	}))

	app.Use(cors.New())

	app.Use(logger.New(logger.Config{
		Format: `{"time":"${time}","status":${status},"latency":"${latency}","ip":"${ip}","method":"${method}","path":"${path}"}` + "\n",
	}))

	app.Use(limiter.New(limiter.Config{
		Max:        200,
		Expiration: 1 * time.Minute,
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()
		},
	}))

	app.Get("/health", handleHealth)
	app.Post("/execproc", handleExecProc)


	// 使用context控制goroutine生命周期
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if conn, err := getDB(); err != nil {
					logs.Errorf("数据库连接检查失败: %v", err)
				} else {
					tCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err := conn.PingContext(tCtx); err != nil {
						logs.Errorf("数据库健康检查失败: %v", err)
					}
					cancel()
				}

				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				logs.Debugf("GC Stats: runs=%d,pause=%dms,next=%dMB",
					m.NumGC, m.PauseTotalNs/1e6, m.NextGC/1e6)
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-quit
		logs.Info("收到关闭信号，正在清理资源...")
		stop()           // 停止所有goroutine
		cleanup()
		os.Exit(0)
	}()

	logs.Infof("服务启动，监听端口: %s", *port)
	if err := app.Listen(fmt.Sprintf(":%v", *port)); err != nil {
		logs.Fatalf("服务启动失败: %v", err)
	}
}

// 处理存储过程执行
func handleExecProc(c *fiber.Ctx) error {
	startTime := time.Now()
	stats.ReqCount.Add(1)

	c.Set("Content-Type", "application/json")

	type RequestData struct {
		ProcName string `json:"proc_name" form:"proc_name"`
		Params   string `json:"params" form:"params"`
	}

	var req RequestData
	contentType := string(c.Request().Header.ContentType())

	// 根据 Content-Type 选择解析方式
	var err error
	switch {
	case strings.HasPrefix(contentType, "application/json"):
		err = c.BodyParser(&req)
	case strings.HasPrefix(contentType, "application/x-www-form-urlencoded"),
		strings.HasPrefix(contentType, "multipart/form-data"):
		err = c.BodyParser(&req)
	default:
		// 尝试自动解析
		err = c.BodyParser(&req)
	}

	if err != nil {
		stats.ReqFailed.Add(1)
		return c.JSON(JsonRes{Code: 4000, Msg: "请求参数解析失败"})
	}

	req.ProcName = strings.TrimSpace(req.ProcName)
	req.Params = strings.TrimSpace(req.Params)

	requestID := c.Get("X-Request-ID")
	if requestID == "" {
		requestID = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	logEntry := logs.WithFields(logrus.Fields{
		"request_id": requestID,
		"proc_name":  req.ProcName,
		"params":     req.Params,
	})

	if req.ProcName == "" || req.Params == "" {
		logEntry.Warn("参数为空")
		stats.ReqFailed.Add(1)
		return c.JSON(JsonRes{Code: 4001, Msg: "参数不能为空"})
	}

	conn, err := getDB()
	if err != nil {
		logEntry.Errorf("获取数据库连接失败: %v", err)
		stats.ReqFailed.Add(1)
		return c.JSON(JsonRes{Code: 5001, Msg: "数据库连接失败"})
	}

	params, err := parseParams(req.Params)
	if err != nil {
		logEntry.Errorf("参数解析失败: %v", err)
		stats.ReqFailed.Add(1)
		return c.JSON(JsonRes{Code: 4002, Msg: fmt.Sprintf("参数解析失败: %v", err)})
	}

	cmdText := buildProcCommand(req.ProcName, len(params))
	execParams := buildExecParams(params)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
	defer cancel()

	_, err = conn.ExecContext(ctx, cmdText, execParams...)
	if err != nil {
		logEntry.Errorf("存储过程执行失败: %v", err)
		stats.ReqFailed.Add(1)
		return c.JSON(JsonRes{Code: 5002, Msg: fmt.Sprintf("存储过程执行失败: %v", err)})
	}

	results, err := extractOutputs(params)
	if err != nil {
		logEntry.Errorf("获取输出参数失败: %v", err)
		stats.ReqFailed.Add(1)
		return c.JSON(JsonRes{Code: 5003, Msg: fmt.Sprintf("获取输出参数失败: %v", err)})
	}

	stats.ReqSuccess.Add(1)

	// 使用环形缓冲区记录响应时间
	idx := stats.ReqTimesIdx.Add(1) % 1024
	stats.ReqTimes[idx] = time.Since(startTime)

	costMs := time.Since(startTime).Milliseconds()
	logEntry.Infof("执行成功，耗时: %dms", costMs)

	return c.JSON(JsonRes{
		Code:   200,
		Msg:    "成功",
		Data:   results,
		CostMs: costMs,
	})
}

// 解析参数字符串
func parseParams(paramStr string) ([]Param, error) {
	parts := strings.Split(paramStr, "|")
	for i := range parts {
		parts[i] = multiSpaceRegex.ReplaceAllString(strings.TrimSpace(parts[i]), " ")
	}

	params := make([]Param, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}

		if strings.Contains(p, "#") {
			pair := strings.SplitN(p, "#", 2)
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
		} else {
			params = append(params, Param{
				IsOutput: false,
				Value:    p,
			})
		}
	}
	return params, nil
}

// 构建存储过程调用命令
func buildProcCommand(name string, paramCount int) string {
	placeholders := make([]string, paramCount)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	return fmt.Sprintf("BEGIN %s(%s); END;", name, strings.Join(placeholders, ","))
}

// 构建执行参数列表
func buildExecParams(params []Param) []interface{} {
	execParams := make([]interface{}, len(params))
	for i, p := range params {
		if p.IsOutput {
			execParams[i] = sql.Out{Dest: p.Value}
		} else {
			execParams[i] = p.Value
		}
	}
	return execParams
}

// 提取输出参数
func extractOutputs(params []Param) ([]interface{}, error) {
	// 预分配结果切片，避免动态扩容
	results := make([]interface{}, 0, len(params))

	for _, p := range params {
		if !p.IsOutput {
			continue
		}

		if p.Type == "cursor" {
			cursorPtr, ok := p.Value.(*gocursor.RefCursor)
			if !ok || cursorPtr == nil {
				continue
			}
			items, err := processCursor(cursorPtr)
			if err != nil {
				return nil, fmt.Errorf("处理游标失败: %w", err)
			}
			results = append(results, items...)
			continue
		}

		record := make(map[string]interface{}, 1)
		switch p.Type {
		case "int":
			if ptr, ok := p.Value.(*int); ok {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "float", "number":
			if ptr, ok := p.Value.(*float64); ok {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "string":
			if ptr, ok := p.Value.(*string); ok {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "bool":
			if ptr, ok := p.Value.(*bool); ok {
				record[strings.ToLower(p.Name)] = *ptr
			}
		default:
			record[strings.ToLower(p.Name)] = "(未知类型)"
		}
		results = append(results, record)
	}
	return results, nil
}

// 处理游标
func processCursor(cursor *gocursor.RefCursor) ([]interface{}, error) {
	if cursor == nil {
		return nil, fmt.Errorf("游标为空")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
	defer cancel()

	conn, err := getDB()
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %w", err)
	}

	rows, err := gocursor.WrapRefCursor(ctx, conn, cursor)
	if err != nil {
		return nil, fmt.Errorf("打开游标失败: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("获取列类型失败: %w", err)
	}

	// 预分配结果切片，设置初始容量
	results := make([]interface{}, 0, min(1000, cfg.MaxResultRows))
	rowCount := 0

	// 预分配scanArgs切片，在循环外创建
	scanArgs := make([]interface{}, len(columns))
	// 预创建columnType缓存，避免重复获取
	colTypeNames := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		colTypeNames[i] = ct.DatabaseTypeName()
	}

	for rows.Next() {
		// 复用scanArgs
		for i := range scanArgs {
			scanArgs[i] = newScanValue(colTypeNames[i])
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("扫描行数据失败: %w", err)
		}

		record := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			record[strings.ToLower(col)] = convertValue(scanArgs[i])
		}
		results = append(results, record)
		rowCount++

		if rowCount >= cfg.MaxResultRows {
			logs.Warnf("游标结果集超过限制，已截断: %d 行", rowCount)
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历行时出错: %w", err)
	}

	return results, nil
}

// 创建扫描值
func newScanValue(dbType string) interface{} {
	switch strings.ToUpper(dbType) {
	case "NUMBER", "INTEGER", "FLOAT", "DECIMAL", "BINARY_FLOAT", "BINARY_DOUBLE":
		return &sql.NullFloat64{}
	case "VARCHAR2", "CHAR", "CLOB", "NVARCHAR2", "NCHAR":
		return &sql.NullString{}
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE":
		return &sql.NullTime{}
	case "RAW", "BLOB", "BFILE":
		var v []byte
		return &v
	default:
		var v interface{}
		return &v
	}
}

// 转换值
func convertValue(v interface{}) interface{} {
	switch ptr := v.(type) {
	case *sql.NullString:
		if ptr.Valid {
			return ptr.String
		}
	case *sql.NullFloat64:
		if ptr.Valid {
			return ptr.Float64
		}
	case *sql.NullInt64:
		if ptr.Valid {
			return ptr.Int64
		}
	case *sql.NullBool:
		if ptr.Valid {
			return ptr.Bool
		}
	case *sql.NullTime:
		if ptr.Valid {
			return ptr.Time
		}
	case *[]byte:
		if ptr != nil {
			return string(*ptr)
		}
	case *interface{}:
		if ptr != nil {
			return *ptr
		}
	}
	return nil
}
