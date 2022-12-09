package paxos_simple

import (
	"fmt"
	"net/rpc"
	"strconv"
)

// 生成提案编号(format: 自增 + 议员id)
func generateNumber(id int, number float32) float32 {
	var strNum string
	if number == 0 {
		strNum = fmt.Sprintf("1.%d", id)
		n, err := strconv.ParseFloat(strNum, 32)
		if err != nil {
			panic("数字转换失败：" + err.Error())
		}
		return float32(n)
	}

	// 自增流水号
	i := int(number) + 1
	strNum = fmt.Sprintf("%d.%d", i, id)
	n, err := strconv.ParseFloat(strNum, 32)
	if err != nil {
		panic("数字转换失败：" + err.Error())
	}
	return float32(n)
}

func logPrint(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

func callRpc(addr, roleService, method string, arg interface{}, reply interface{}) error {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Close()
	}()

	err = c.Call(roleService+"."+method, arg, reply)
	if err != nil {
		return err
	}
	return nil
}
