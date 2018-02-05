package main

import (
	"time"
	"database/sql"
	"math/rand"
	"fmt"
	//"log"
	"strconv"
	_ "github.com/lib/pq"
)

//devices
type DevicesStruct struct {
	Id     int
	Name   string
	Userid int
}

//metrics
type DevicesMetricStruct struct {
	Id         int
	Deviceid   int
	Metric     [5]int
	LocalTime  time.Time
	ServerTime time.Time
}

//alerts
type DeviceAlertStruct struct {
	Id       int
	Deviceid int
	Message  string
}

var DB *sql.DB
var dbUser = "postgres"
var dbPass = "postgres"
var dbName = "postgres"

func init() {

	var err error
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", dbUser, dbPass, dbName)
	DB, err = sql.Open("postgres", dbinfo)
	if err != nil {
		panic(err)
	}
	//defer DB.Close() 

}


func GetAllDevicesFromDB() chan DevicesStruct {

	out := make(chan DevicesStruct)
	rows, err := DB.Query("SELECT * FROM devices")
	if err != nil {
		//panic(err)
	}
	defer rows.Close()

	go func() {
		for rows.Next() {
			var newDevice DevicesStruct
			err := rows.Scan(&newDevice.Id, &newDevice.Name, &newDevice.Userid)
			if err != nil {
				//panic(err)
			}
			out <- newDevice
		}
		close(out)
	}()

	return out
}

func CreateMetric(in chan DevicesStruct) chan DevicesMetricStruct {

	time.Sleep(5 * time.Second)
	out := make(chan DevicesMetricStruct)

	go func() {
		var newMetric DevicesMetricStruct
		for v := range in {
			
			newMetric.Id = TableIDs("device_metrics")
			newMetric.Deviceid = v.Id
			
			for i := 0; i < len(newMetric.Metric); i++ {
				newMetric.Metric[i] = rand.Intn(50)
			}
			newMetric.LocalTime = time.Now().AddDate(0, 0, -1)
			newMetric.ServerTime = time.Now()
			//log.Println(newMetric)

			
			var stringQ = "INSERT INTO device_metrics (Id, device_Id, metric_1, metric_2, metric_3, metric_4, metric_5, local_time, server_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
			_, err := DB.Exec(stringQ,
				newMetric.Id,
				newMetric.Deviceid,
				newMetric.Metric[0],
				newMetric.Metric[1],
				newMetric.Metric[2],
				newMetric.Metric[3],
				newMetric.Metric[4],
				newMetric.LocalTime,
				newMetric.ServerTime)
			if err != nil {
				fmt.Println(err.Error())
				//return
			}
			out <- newMetric
		}
		close(out)
	}()
	return out
}


func checkMetrics(in chan DevicesMetricStruct) {

	go func() {
		var newAlert DeviceAlertStruct
		for v := range in {
			for i := 0; i < len(v.Metric); i++ {
				
				if v.Metric[i] == 43 {
					
					newAlert.Id = TableIDs("device_alerts")
					newAlert.Deviceid = v.Deviceid
					newAlert.Message = "Bad metric param on device " + strconv.Itoa(v.Deviceid)

					
					_, err := DB.Exec("INSERT INTO device_alerts (id, device_id, message) VALUES ($1, $2, $3)", newAlert.Id, newAlert.Deviceid, newAlert.Message)
					if err != nil {
						fmt.Println(err.Error())
						//return
					}
				}
			}
		}
	}()
}

func TableIDs(nameT string) (lastID int) {
	stringQ := "SELECT COUNT(ID) FROM " + nameT + ";"
	rows, err := DB.Query(stringQ)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&lastID)
		if err != nil {
			fmt.Println(err.Error())
			//return
		}
	}
	if err = rows.Err(); err != nil {
		fmt.Println(err.Error())
	}

	lastID++
	return lastID
}


func main() {


	allDevices := GetAllDevicesFromDB()
	for {
		allMetrics := CreateMetric(allDevices)
		checkMetrics(allMetrics)
	}

}
