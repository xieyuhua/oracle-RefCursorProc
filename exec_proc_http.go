package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
    "runtime"
	gocursor "github.com/sijms/go-ora/v2"
)

var err error
var db *sql.DB
var dns *string
var multiSpaceRegex = regexp.MustCompile(`\s+`)
var paramStr *string
var procName *string

type Param struct {
	IsOutput bool
	Value    interface{} // 输入参数值或输出参数指针
	Type     string      // 输出参数类型
	Name     string      // 输出参数名称
}

// 主函数入口
func main() {
	// 初始化
	procName = flag.String("pname", "", "存储过程名字")
	paramStr = flag.String("pstr", "", "101,cursor|val1,int|val2,string|val3")
	dns = flag.String("dns", "", "oracle://user:pwd@localhost:port/db")
	flag.Parse()

	hosts := os.Getenv("DBDNS")
	if hosts != "" {
		*dns = hosts
	}
	if *dns == "" {
		log.Println("oracle://user:pwd@localhost:port/db")
		return
	}
	
	// 参数检查
	if *procName == "" || *paramStr == "" {
		log.Println("DATA NOT ALLOW !")
		return
	}
	
	// 连接配置
	db, err = sql.Open("oracle", *dns)
	if err != nil {
		log.Println(fmt.Sprintf("%s %v", *dns, err))
		return
	}
	defer db.Close()
	
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("数据库连接失败: %v", err)
		return
	}
	
	// 配置连接池参数（根据实际情况调整）
	db.SetMaxOpenConns(10)           // 根据并发量调整
	db.SetMaxIdleConns(5)            // 空闲连接数
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(10 * time.Minute) // 空闲连接最大存活时间

	// 解析参数
	params, err := parseParams(*paramStr)
	if err != nil {
		log.Println("paramStr NOT ALLOW !")
		return
	}

	// 构建存储过程调用
	cmdText := buildProcCommand(*procName, len(params))

	// 构建执行参数
	execParams := buildExecParams(params)

	// 执行存储过程
	_, err = db.Exec(cmdText, execParams...)
	if err != nil {
		log.Println(fmt.Sprintf("执行存储过程失败: %v", err))
		return
	}

	// 获取输出参数
	results, err := Outputs(params)
	if err != nil {
		log.Println(fmt.Sprintf("获取输出参数失败: %v", err))
		return
	}

	log.Printf("执行成功，结果数量: %d", len(results))
	
	// 强制垃圾回收（可选，主要用于调试）
	runtime.GC()
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
			param := Param{IsOutput: true, Type: pair[0], Name: pair[1]}
			switch strings.ToLower(pair[0]) {
			case "int":
				var v int
				param.Value = &v
			case "float":
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

// 获取输出参数
func Outputs(params []Param) ([]interface{}, error) {
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
		case "float":
			if ptr, ok := p.Value.(*float64); ok && ptr != nil {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "string":
			if ptr, ok := p.Value.(*string); ok && ptr != nil {
				record[strings.ToLower(p.Name)] = *ptr
			}
		case "cursor":
			if cursor, ok := p.Value.(*gocursor.RefCursor); ok && cursor != nil {
				item, err := handleCursor(*cursor, db)
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
func handleCursor(outVal gocursor.RefCursor, db *sql.DB) ([]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	rows, err := gocursor.WrapRefCursor(ctx, db, &outVal)
	if err != nil {
		return nil, fmt.Errorf("WrapRefCursor: %v", err)
	}
	defer rows.Close() // 重要：确保关闭结果集
	
	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}
	
	// 处理结果集
	var results []interface{}
// 	rowCount := 0
	
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("扫描行数据失败: %v", err)
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
// 		rowCount++
	}
	
	// 检查是否有迭代错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历行时出错: %v", err)
	}
	
// 	log.Printf("处理游标完成，返回 %d 行数据", rowCount)
	return results, nil
}