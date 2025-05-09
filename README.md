# oracle-RefCursorProc


```

// 调用存储过程
var outVal cur.RefCursor
cmdText := `
BEGIN
	P_SNSJ_B2C_GOODSFORSALE(:1, :2, :3);
END;`
_, err := db.Exec(cmdText, "2020", "98656,2", sql.Out{Dest: &outVal} )


/*
	CREATE OR REPLACE 
	procedure js_p_dict_q(
		i_groupId varchar2,
		cresult in out SYS_REFCURSOR
	)
	is
	BEGIN
			open cresult for
		SELECT groupid as groupId,itemid as itemId,itemname as itemName,notes as notes FROM JS_DICT
		where i_groupId=''or groupid=i_groupId;
	END js_p_dict_q;
*/

// cmdText = `
// BEGIN
// 	js_p_dict_q(:1, :2);
// END;`
//执行参数
// var vals = []any{
// 	"201",
// 	sql.Out{Dest: &outVal},
// }


// cmdText = `BEGIN
// 	OPEN :cursorss FOR 
// 	SELECT  * from t_memcard_reg
// 	where memcardno ='13340369862';
// END;`;
// var vals = []any{
// 	sql.Named("cursorss", sql.Out{Dest: &outVal}),
// }


// _, err := db.Exec(cmdText, vals... )
if err != nil {
	panic(fmt.Sprintf("执行错误: %v", err))
}
fmt.Println(outVal)

err = db.Ping()
if err != nil {
	panic(fmt.Sprintf("Can't ping connection %v", err))
}
// rows, err := outVal.Query()
// function WrapRefCursor in last commit that convert *RefCursor into *sql.Rows
rows, err := cur.WrapRefCursor(context.Background(), db, &outVal)
if err != nil {
	panic(fmt.Sprintf("Query: %v", err))
}

// 处理结果集
fmt.Println(rows.Columns())
// 错误处理
if err != nil {
	panic(err.Error())
}
fmt.Println("输出参数:", outVal)
```
