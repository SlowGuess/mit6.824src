package main

import "fmt"

type ListNode struct {
	Val  int
	Next *ListNode
}

func MergeKLists(lists []*ListNode) *ListNode {
	return MergeSort(lists)
}

func MergeSort(lists []*ListNode) *ListNode {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}

	mid := len(lists) / 2
	L, R := MergeSort(lists[:mid]), MergeSort(lists[mid:])
	head := &ListNode{}
	cur := head
	for L != nil && R != nil {
		if L.Val < R.Val {
			cur.Next = &ListNode{Val: L.Val}
			L = L.Next
		} else {
			cur.Next = &ListNode{Val: R.Val}
			R = R.Next
		}
		cur = cur.Next
	}
	for L != nil {
		cur.Next = &ListNode{Val: L.Val}
		L = L.Next
		cur = cur.Next
	}
	for R != nil {
		cur.Next = &ListNode{Val: R.Val}
		R = R.Next
		cur = cur.Next
	}
	return head.Next
}

func main() {
	// 创建测试用的链表
	list1 := &ListNode{Val: 1, Next: &ListNode{Val: 4, Next: &ListNode{Val: 5}}}
	list2 := &ListNode{Val: 1, Next: &ListNode{Val: 3, Next: &ListNode{Val: 4}}}
	list3 := &ListNode{Val: 2, Next: &ListNode{Val: 6}}

	// 将链表放入数组中
	lists := []*ListNode{list1, list2, list3}

	result := MergeKLists(lists)
	for result != nil {
		fmt.Print(result.Val, " ")
		result = result.Next
	}

}
