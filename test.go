package main

import (
	"github.com/go-pg/pg"
	//"log"
	//"time"
	"fmt"
	"time"
)

type Device struct {
	Id int
	Name string
	UserId int
}

var DB *pg.DB

func init() {

	DB = pg.Connect(&pg.Options{
		User: "postgres",
		Password: "gfhjkm",
		Database: "dm",
		Addr:"localhost:5432",
	})

}


func getAllDevices() chan Device {

	out := make(chan Device)
	for i:=1; i<10000; i++ {
		go func(i int) {
			var newD Device
			_, err := DB.QueryOne(pg.Scan(&newD.Id, &newD.Name, &newD.UserId), "SELECT * from devices where id = ?", i)
			if err != nil {
				//panic(err)
			}
			//log.Println(newD)
			out <- newD
		}(i)
	}

	return out
}

func main() {

	select {
	case t := <- getAllDevices():
			fmt.Println(t)
	default:
		time.Sleep(time.Second*5)
	}



}