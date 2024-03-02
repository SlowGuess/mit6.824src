package main

import "fmt"

func RomanNumtoInt(s string) int {
	RomanNum := map[string]int{
		"I": 1,
		"V": 5,
		"X": 10,
		"L": 50,
		"C": 100,
		"D": 500,
		"M": 1000,
	}

	l_num, sum := 0, 0
	for i := len(s) - 1; i >= 0; i-- {
		num := RomanNum[string(s[i])]
		if l_num <= num {
			sum += num
			l_num = num
		} else {
			left := l_num - num
			if left == 4 || left == 9 || left == 40 || left == 90 || left == 400 || left == 900 {
				sum -= num
			} else {
				fmt.Println("罗马数字不合规范")
				return -1
			}

		}
	}
	return sum
}

func main() {
	s := "IM"
	fmt.Println(RomanNumtoInt(s))
	//s1 :="IL"
	//fmt.Println(RomanNumtoInt(s1))
}
