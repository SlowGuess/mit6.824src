package main

import (
	"fmt"
)

// 冒泡排序
func BubbleSort(arr []int) []int {
	//长度为1，直接返回
	if len(arr) == 1 {
		return arr
	}

	//长度大于1，进行交换排序
	for i := 0; i < len(arr); i++ {
		for j := i + 1; j < len(arr); j++ {
			if arr[i] > arr[j] {
				arr[i], arr[j] = arr[j], arr[i]
			}
		}
	}
	return arr
}

// 快速排序(考研背的模板
func Partition(arr []int, L int, R int) int {
	mid := arr[L]
	for L < R {
		for arr[R] >= mid && L < R {
			R--
		}
		arr[L] = arr[R]

		for arr[L] <= mid && L < R {
			L++
		}
		arr[R] = arr[L]

	}
	arr[L] = mid
	return L
}

func QuickSort(arr []int, L int, R int) []int {
	//递归终止
	if L >= R {
		return arr
	}

	M := Partition(arr, L, R)

	//左半部分快排
	QuickSort(arr, L, M-1)

	//右半部分快排
	QuickSort(arr, M+1, R)

	return arr
}

func main() {
	arr1 := []int{41, 24, 76, 11, 45, 64, 21, 69, 19, 36}
	arr2 := []int{41, 24, 76, 11, 45, 64, 21, 69, 19, 36}
	fmt.Println("原数组：", arr1)

	fmt.Println("冒泡排序后：：", BubbleSort(arr1))
	fmt.Println("快速排序后：", QuickSort(arr2, 0, len(arr2)-1))
}
