package main

import (
	"fmt"
	"io"
	"os"
)

func A(fileName string, inputString string) {
	//创建文件
	os.Create("./" + fileName)

	//打开文件
	file, err1 := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err1 != nil {
		fmt.Println("文件访问异常")
	} else {
		fmt.Println("文件打开成功", file.Name())

		//写入文件
		offset, err2 := file.WriteString(inputString)
		if err2 != nil {
			fmt.Println(offset, err2)
		}
		fmt.Println(file.Close())
	}
}

func B(fileName string) {
	//打开文件
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件访问异常")
	} else {
		fmt.Println("文件打开成功", file.Name())

		//读取文件
		bytes, err1 := io.ReadAll(file)
		if err1 != nil {
			fmt.Println(err1)
		} else {
			fmt.Println(string(bytes))
		}
		file.Close()
	}
}

func main() {
	fileName := "test2file.txt"
	inputString := "This is Chelsea's test2 /n 小徐文件内容"
	A(fileName, inputString)
	B(fileName)

}
