package main

import (
	"fmt"
	"log"
	"time"
    "database/sql"
    cur "github.com/sijms/go-ora/v2"
)

func main() {
    // 连接配置
    dsn := "oracle://root:hoft@192.165.7.38:1521/ord"
    db, _ := sql.Open("oracle", dsn)
    defer db.Close()
	// 配置连接池参数
	db.SetMaxOpenConns(20)          // 最大活动连接数:ml-citation{ref="3,6" data="citationList"}
	db.SetMaxIdleConns(5)           // 最大空闲连接数
	db.SetConnMaxLifetime(30*time.Minute)  // 连接最大生命周期

    // 调用存储过程
    var outVal cur.RefCursor

	cmdText := `
	DECLARE
		v_cursor SYS_REFCURSOR;  -- 显式声明游标变量
	BEGIN
		P_SB2C_GOODSFORSALE(:1, :2, v_cursor);
		:3 := v_cursor;          -- 结果集绑定到输出参数
	END;`

	// cmdText = `BEGIN
	// 	OPEN :cursorss FOR 
	// 	SELECT  * from t_memcard_log
	// 	where memcardno ='136962';
	// END;`;

    _, err := db.Exec(cmdText, "2020", "98656,86614,28738", sql.Out{Dest: &outVal} )
	if err != nil {
		panic(fmt.Sprintf("执行错误: %v", err))
	}
	rows, err := outVal.Query()
	if err != nil {
		log.Fatal(err)
	}

	// 处理结果集
	fmt.Println(rows.Columns())
    // 错误处理
    if err != nil {
        panic(err.Error())
    }
    fmt.Println("输出参数:", outVal)
}
