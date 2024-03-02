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

	//控制序列,保证输入罗马数字规范
	//sequence :=map[string]int{
	//	"I": 1,
	//	"V": 2,
	//	"X": 3,
	//	"L": 4,
	//	"C": 5,
	//	"D": 6,
	//	"M": 7,
	//}

	max, sum := 0, 0
	for i := len(s) - 1; i >= 0; i-- {
		num := RomanNum[string(s[i])]
		if max <= num {
			sum += num
			max = num
		} else {

			sum -= num
		}
	}
	return sum
}

func main() {
	s := "MCMXCIV"
	fmt.Println(RomanNumtoInt(s))
	//s1 :="IL"
	//fmt.Println(RomanNumtoInt(s1))
}
