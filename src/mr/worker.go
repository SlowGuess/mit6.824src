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
			_, err := fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value)
			if err != nil {
				fmt.Println("写入分区%v文件有误", i)
				return err
			}
		}
	}
	return nil
}

func (job *Job) DoReduceJob(reducef func(string, []string) string) error {
	//存储每个文件的第job.ReduceID号分区的单词对
	Reduce_partition := make([][]KeyValue, job.ReduceNumber)

	// 按任务的ReduceID读取Map阶段输出的对应分区的文件
	for i := 0; i < job.MapNumber; i++ {
		MapFilename := fmt.Sprintf("worker-file%d-map-%03d-out.txt", i, job.ReduceID)
		content, err := job.ReadFile(MapFilename)
		if err != nil {
			return err
		}

		//按行划分出每对单词和value
		lines := strings.Split(content, "\n")
		for _, line := range lines {
			if line == "" {
				//行为空，跳过即可
				continue
			}

			//将单词和value划分开来
			parts := strings.Fields(line)

			if len(parts) != 2 {
				//读取的格式有误，不是 word value 的形式
				return fmt.Errorf("invalid format in map output file %s", MapFilename)
			}

			//将读取出的kv对放入对应分区号的二维KV对中,以便进行k路归并，为merge工作作准备
			Reduce_partition[i] = append(Reduce_partition[i], KeyValue{Key: parts[0], Value: parts[1]})
		}
	}
	//将所有该分区的单词对进行归并
	mergeOut := MergeSort(Reduce_partition)

	oname := fmt.Sprintf("mr-out-%d.txt", job.ReduceID) //*输出文件

	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Println("创建输出文件mr-out失败")
		return err
	}

	// defer 语句确保锁会被释放
	defer ofile.Close()

	// 执行 reduce 操作统计单词出现次数
	i := 0 //*执行reduce
	for i < len(mergeOut) {
		j := i + 1 //*寻找相同的键，直到找到不同的键或达到数组末尾
		for j < len(mergeOut) && mergeOut[j].Key == mergeOut[i].Key {
			j++
		}
		values := []string{} //*收集相同键的所有值
		for k := i; k < j; k++ {
			values = append(values, mergeOut[k].Value)
		}
		output := reducef(mergeOut[i].Key, values) //*对每个不同的键执行 Reduce 函数，将输出写入输出文件
		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", mergeOut[i].Key, output)
		if err != nil {
			fmt.Println("写入mr-out失败")
			return err
		}

		i = j //*移动到下一个不同的键
	}

	return nil

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

// MergeSortTwo 将两个有序的KeyValue切片合并成一个有序的切片
func MergeSortTwo(left, right []KeyValue) []KeyValue {
	var result []KeyValue
	i, j := 0, 0

	for i < len(left) && j < len(right) {
		if left[i].Key < right[j].Key {
			result = append(result, left[i])
			i++
		} else {
			result = append(result, right[j])
			j++
		}
	}

	result = append(result, left[i:]...)
	result = append(result, right[j:]...)

	return result
}

// MergeSort 对 k 个 KeyValue 切片进行归并排序
func MergeSort(slices [][]KeyValue) []KeyValue {
	if len(slices) <= 1 {
		return slices[0]
	}

	mid := len(slices) / 2
	left := MergeSort(slices[:mid])
	right := MergeSort(slices[mid:])

	return MergeSortTwo(left, right)
}

// 返回当前时间戳， 打印日志时候用
func logTime() string {
	return fmt.Sprint((time.Now().UnixNano()/1e6)%10000, " ")
}

func CallFetchJob() JobFetchResp {
	req := JobFetchReq{os.Getpid()}
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
		} else if err == nil {
			fmt.Printf("worker:%v DoMapJob_succeess ", os.Getpid())
		}

	case ReduceJob:
		if err = job.DoReduceJob(reducef); err != nil {
			fmt.Println("DoReduceJob_error ", err)
		} else if err == nil {
			fmt.Printf("worker:%v DoReduceJob_succeess ", os.Getpid())
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
			fmt.Printf(logTime()+WorkerLogPrefix+"任务都做完了，worker:%v退出", os.Getpid())
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
