package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)
import "6.824/mr"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	//if len(os.Args) < 3 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
	//	os.Exit(1)
	//}
	// 不从启动命令中读取 pg*.txt 直接指定
	fileNameList := []string{
		"pg-being_ernest.txt",
		"pg-dorian_gray.txt",
		"pg-frankenstein.txt",
		"pg-grimm.txt",
		"pg-huckleberry_finn.txt",
		"pg-metamorphosis.txt",
		"pg-sherlock_holmes.txt",
		"pg-tom_sawyer.txt",
	} //*指定要处理的文件列表
	//mapf, reducef := loadPlugin(os.Args[1])
	// loadPlugin 无法在 Windows 中运行，这里手动 load 我们的 Map 和 Reduce 函数, 在 mrapps 文件夹的 wc.go 中
	mapf := func(filename string, contents string) []mr.KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []mr.KeyValue{}
		for _, w := range words {
			kv := mr.KeyValue{w, "1"} // *将每个单词转换为键值对，值固定为 "1"
			kva = append(kva, kv)     // *将每个单词放入kva中
		}
		return kva
	}

	// 手动指定 reducef 函数
	reducef := func(key string, values []string) string {
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	}

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}         //*存取中间数据的数组
	for _, filename := range fileNameList { //*读取所有文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file) //*读取文件内容
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content)) //*对每个文件执行 Map 函数，将生成的键值对添加到中间数据数组
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate)) //*对中间数据按键进行排序

	oname := "mr-out-0" //*输出文件
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0 //*执行reduce
	for i < len(intermediate) {
		j := i + 1 //*寻找相同的键，直到找到不同的键或达到数组末尾
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{} //*收集相同键的所有值
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values) //*对每个不同的键执行 Reduce 函数，将输出写入输出文件
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j //*移动到下一个不同的键
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot load plugin %v", filename)
//	}
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("cannot find Map in %v", filename)
//	}
//	mapf := xmapf.(func(string, string) []mr.KeyValue)
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("cannot find Reduce in %v", filename)
//	}
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}
