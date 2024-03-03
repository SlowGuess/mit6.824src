package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (job *Job) DoMapJob(mapf func(string, string) []KeyValue) error {
	// 读取文件内容
	content, err := job.ReadFile(job.FileName)
	if err != nil {
		return err
	}
	// 调用mapf函数，将文件内容转换为键值对
	kva := mapf(job.FileName, content)

	// 根据键的哈希值确定对应的分区号
	partition := make([][]KeyValue, job.ReduceNumber)
	for _, kv := range kva {
		partition_index := ihash(kv.Key) % job.ReduceNumber
		// 将生成的键值对按照 Reduce 任务数量分组
		// 将 kv 存放到对应的分组中
		partition[partition_index] = append(partition[partition_index], kv)
	}

	for i := 0; i < job.ReduceNumber; i++ {
		//使分区内部有序
		sort.Sort(ByKey(partition[i]))

		//将map工作完成的内容放在本地，供reduce工作使用
		map_filename := fmt.Sprintf("worker-file%d-map-%03d-out.txt", job.ListIndex, i)
		file, err := os.OpenFile(map_filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		defer file.Close()
		// 写入文件
		for _, kv := range partition[i] {
			fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value)
		}
	}
	return nil
}

func (job *Job) DoReduceJob(reducef func(string, []string) string) error {

}

func (job *Job) ReadFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("cannot open file: %v", err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("cannot read file: %v", err)
	}
	return string(content), nil
}

// 多路归并，找最小值
func findMinIndex(keyValueList2 [][]*KeyValue, indexList []int) int {

}

// 返回当前时间戳， 打印日志时候用
func logTime() string {
	return fmt.Sprint((time.Now().UnixNano()/1e6)%10000, " ")
}

func CallFetchJob() JobFetchResp {
	req := JobFetchReq{}
	resp := JobFetchResp{}
	call("Coordinator.JobFetch", &req, &resp)
	//fmt.Printf(WorkerLogPrefix+"CallFetchJob job resp %+v\n", resp)
	return resp
}

func CallCommitJob(job *JobDoneReq) {
	//fmt.Printf(WorkerLogPrefix+"CallCommitJob job req %+v\n", *job)
	resp := JobDoneResp{}
	call("Coordinator.JobDone", job, &resp)
}

// DoJob 开始工作
func (job *Job) DoJob(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var err error
	switch job.JobType {
	case MapJob:
		if err = job.DoMapJob(mapf); err != nil {
			fmt.Println("DoMapJob_error ", err)
		}
	case ReduceJob:
		if err = job.DoReduceJob(reducef); err != nil {
			fmt.Println("DoReduceJob_error ", err)
		}
	}
	// 成功做完修改状态
	if err == nil {
		job.JobFinished = true
	}
	fmt.Printf(WorkerLogPrefix+"DoMapJob_finished %v\n ", toJsonString(job))
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	startTime := int64(0)
	for true {
		startTime = time.Now().UnixNano()
		// 索要任务，得到的可能是 Map 或者 Reduce 的任务
		job := CallFetchJob()
		// 需要等待流转到 reduce
		if job.NeedWait {
			time.Sleep(5 * BackgroundInterval)
			continue
		}
		// 任务都做完了，停止循环
		if job.FetchCount == 0 {
			fmt.Println(logTime() + WorkerLogPrefix + "任务都做完了，worker退出")
			break
		}
		// 做任务
		job.DoJob(mapf, reducef)
		// 做完了，提交
		CallCommitJob(&JobDoneReq{job.Job})
		//fmt.Println(WorkerLogPrefix+"一次worker循环耗时[毫秒]:", (time.Now().UnixNano()-startTime)/1e6)
		startTime++
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
