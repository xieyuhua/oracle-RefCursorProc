package main

import (
	"log"
    "github.com/sirupsen/logrus"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"fmt"
	"time"
	"regexp"
	"strings"
	"context"
    "database/sql"
    gocursor "github.com/sijms/go-ora/v2"
	"flag"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logs = logrus.New()
var err error
var db *sql.DB
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
// 示例输入
// paramStr := "101,2025011514173976211572,cursor|val3"
// procName := "p_order_lock"

// paramStr := "101,2025011415334104634615,cursor|val3, cursor|val2, int|val1, string|val"
// procName := "p_order_acc"

// paramStr := "1,222,cursor|val3"
// procName := "p_b2c_order_cancel"

func init (){
	debug   = flag.Bool("d", true, "debug msg")
    port    = flag.String("p", "8004", "服务端口")
    dns     = flag.String("dns", "oracle://user:pwd@localhost:port/db", "oracle://user:pwd@localhost:port/db")
    flag.Parse()
}

//完全去除所有空格
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

func initLog (){
    //日志
    logs.SetFormatter(&logrus.JSONFormatter{})
	logger := &lumberjack.Logger{
		Filename:   "exec-proc.log",
		MaxSize:    50,  // 日志文件大小，单位是 MB
		MaxBackups: 3,    // 最大过期日志保留个数
		MaxAge:     30,   // 保留过期文件最大时间，单位 天
		Compress:   true, // 是否压缩日志，默认是不压缩。这里设置为true，压缩日志
	}
	logs.SetOutput(logger) // logrus 设置日志的输出方式
}

// 初始化数据
func initDb (){
    // 连接配置
    db, _ = sql.Open("oracle", *dns)
    // defer db.Close()
	// 配置连接池参数
	db.SetMaxOpenConns(20)          // 最大活动连接数:ml-citation{ref="3,6" data="citationList"}
	db.SetMaxIdleConns(5)           // 最大空闲连接数
	db.SetConnMaxLifetime(30*time.Minute)  // 连接最大生命周期
}

type JsonRes struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

//主函数入口
func main() {
	//初始化
    initDb()
	//初始日志
    initLog()
    //Ping  10
    go func(){
    	for range time.Tick(time.Duration(10) * time.Second) {
    	    _ = db.Ping()
	    }
    }()
    app := fiber.New()
    //启用 Gzip/Brotli 压缩中间件，减少传输体积（文本压缩率可达 60-80%
    app.Use(compress.New(compress.Config{
        Level: compress.LevelBestSpeed,
    }))
	//执行存储过程   取消
    app.Use("/execproc", ExecProc)
	//启动端口
    app.Listen(fmt.Sprintf(":%v", *port))
}


// 执行存储过程
func ExecProc(c *fiber.Ctx) error {
	c.Set("content-type", "text/json")
	// Field names should start with an uppercase letter
	type Data struct {
		ProcName string `json:"proc_name" xml:"proc_name" form:"proc_name"`
		Params string `json:"params" xml:"params" form:"params"`
	}
	//参数绑定
	newdata := new(Data)
    if err := c.BodyParser(newdata); err != nil {
         return c.JSON(JsonRes{Code: 4000, Msg: "  DATA NOT ALLOW !"})
    }
	procName := fmt.Sprintf("%s", newdata.ProcName)
	paramStr := fmt.Sprintf("%s", newdata.Params)

	// return c.JSON(JsonRes{Code: 200, Data: newdata})
	//sql输出
	if *debug {
	    log.Println("procName:", procName, "params:",paramStr)
	}
	// 订单必须存在
	if procName == "" || paramStr == ""  {
		return c.JSON(JsonRes{Code: 4001, Msg:  "DATA NOT ALLOW !"})
	}
	//捕获 panic
	defer func() {
		data_str := fmt.Sprintf("procName:%s, params:%s", procName, paramStr)
		if err := recover(); err != nil {
            logs.WithFields(logrus.Fields{
                "error": err,
            }).Info(data_str)
            c.JSON(JsonRes{Code: 4003, Msg: fmt.Sprintf(":%v", err)})
			log.Println("执行错误", err)
		}
	}()
    //当前连接是否需要重新连接
    errors  := db.Ping()
    if errors != nil {
		db, _ = sql.Open("oracle", *dns)
		// 配置连接池参数
		db.SetMaxOpenConns(20)          // 最大活动连接数:ml-citation{ref="3,6" data="citationList"}
		db.SetMaxIdleConns(5)           // 最大空闲连接数
		db.SetConnMaxLifetime(30*time.Minute)  // 连接最大生命周期 
    }

    // 解析参数
    params, err := parseParams(paramStr)
    if err != nil {
		return c.JSON(JsonRes{Code: 201, Msg: fmt.Sprintf("%v", err)})
    }

    // 构建存储过程调用
    cmdText := buildProcCommand(procName, len(params))

    // 构建执行参数
    execParams := buildExecParams(params)
	// log.Println(params, cmdText)

    // 执行存储过程
    _, err = db.Exec(cmdText, execParams...)
    if err != nil {
		return c.JSON(JsonRes{Code: 201, Msg: fmt.Sprintf("%v", err)})
    }

    // 打印输出参数
    results, err:= Outputs(params)
    if err != nil {
		return c.JSON(JsonRes{Code: 201, Msg: fmt.Sprintf("%v", err)})
    }

	return c.JSON(JsonRes{Code: 200, Data: results})
}

// 解析参数字符串
func parseParams(paramStr string) ([]Param, error) {
    parts := strings.Split(paramStr, ",")
    params := make([]Param, 0, len(parts))
	//完全去除所有空格
	parts = productionClean(parts)
    for _, p := range parts {
        if strings.Contains(p, "|") { // 输出参数
            pair := strings.Split(p, "|")
            if len(pair) != 2 {
                return nil, fmt.Errorf("无效的输出参数格式: %s", p)
            }
            param := Param{IsOutput: true, Type: pair[0], Name: pair[1]}
            switch strings.ToLower(pair[0]) {
            case "int":
                var v float64
                param.Value = &v
            case "string":
                var v string
                param.Value = &v
			case "cursor":
                var v gocursor.RefCursor
                param.Value = &v
            default:
                return nil, fmt.Errorf("不支持的类型: %s", pair[0])
            }
            params = append(params, param)
        } else { // 输入参数
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

// 打印输出参数
func Outputs(params []Param) ([]interface{},error) {
	var results []interface{}
    for _, p := range params {
        if !p.IsOutput {
            continue
        }
		record := make(map[string]interface{})
        switch p.Type  {
        case "int":
			record[strings.ToLower(p.Name)] = *p.Value.(*float64)
			results = append(results, record)
        case "string":
			record[strings.ToLower(p.Name)] = *p.Value.(*string)
			results = append(results, record)
		case "cursor":
			item, err := handleCursor(*p.Value.(*gocursor.RefCursor), db)
			if err != nil {
				return nil, fmt.Errorf("%v", err)
			}
			results = append(results, item)
        default:
			record[strings.ToLower(p.Name)] = "(未知类型)"
			results = append(results, record)
        }
    }
	return results,nil
}

// 游标结果集处理
func handleCursor(outVal gocursor.RefCursor, db *sql.DB) ([]interface{}, error) {
	rows, err := gocursor.WrapRefCursor(context.Background(), db, &outVal)
	if err != nil {
		return nil, fmt.Errorf("WrapRefCursor %v", err)
	}
	// 处理结果集
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("Columns %v", err)
	}
	//格式化输出数据
	var results []interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("Scan %v", err)
		}
		record := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				record[strings.ToLower(col)] = string(b)
			} else {
				record[strings.ToLower(col)] = val
			}
		}
		results = append(results, record)
	}
	return results,nil
}
