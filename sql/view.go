package sql

import (
	"bitcask/utils"
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"
)

// View 函数：以表格形式显示指定表的数据，包含表名
func (db *RDBMS) View(tableName string) error {
	// 检查表是否存在
	table, exists := db.Tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// 准备表头
	headers := make([]string, len(table.Fields))
	for i, field := range table.Fields {
		headers[i] = field.Name
	}

	// 使用 bytes.Buffer 构造表格输出
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 0, 2, ' ', tabwriter.Debug)

	// 写入表名和表头
	fmt.Fprintf(writer, "Table: %s\n", tableName)
	fmt.Fprintln(writer, strings.Join(headers, "\t"))

	// 遍历表中的所有记录
	err := db.Store.Fold(func(key, value []byte) bool {
		// 过滤仅属于当前表的记录
		if !strings.HasPrefix(string(key), tableName+":") {
			return true // 跳过不匹配的记录
		}

		// 反序列化记录
		rowData, err := utils.DeserializeRow(value)
		if err != nil {
			fmt.Printf("error deserializing row: %v\n", err)
			return false // 中断遍历
		}

		// 提取行数据按字段顺序显示
		row := make([]string, len(headers))
		for i, field := range headers {
			row[i] = string(rowData[field]) // 转为字符串以便打印
		}

		// 写入表格行
		fmt.Fprintln(writer, strings.Join(row, "\t"))
		return true // 继续遍历
	})
	if err != nil {
		return fmt.Errorf("error during Fold: %w", err)
	}

	// 输出表格
	writer.Flush()
	fmt.Println(buffer.String())
	return nil
}
