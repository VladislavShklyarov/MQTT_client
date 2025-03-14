package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

var done = make(chan struct{})

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Ошибка загрузки .env файла")
	}

	var broker = "srv2.clusterfly.ru"
	var port = 9991
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID("VladislavLeonidovichShklyarov")
	uname := os.Getenv("MQTT_USERNAME")
	fmt.Print(uname)
	opts.SetUsername(os.Getenv("MQTT_USERNAME"))
	opts.SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.OnConnect = func(client mqtt.Client) {
		fmt.Println("MQTT connected successfully")
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	file, err := os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error while open file:", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Print("Error while closing file:", err)
		}
	}(file)

	go sub(client, "user_0d181660/Student6/Value1", file)
	go sub(client, "user_0d181660/Student6/Value2", file)
	go sub(client, "user_0d181660/Student6/Value3", file)

	<-done
	fmt.Println("Message limit is reached. Finising...")
	client.Disconnect(250)
	os.Exit(0)
}

func sub(client mqtt.Client, topic string, file *os.File) {
	fmt.Printf("Subscribed to the topic: %s\n", topic)

	count := 0
	token := client.Subscribe(topic, 1, func(client mqtt.Client, message mqtt.Message) {
		count++
		timestamp := time.Now().Format("2006-01-02 15:04:05")
		logEntry := fmt.Sprintf("Message %d: %s | Time: %s | Topic: %s\n", count, message.Payload(), timestamp, message.Topic())

		fmt.Print(logEntry) // Вывод в консоль

		if _, err := file.WriteString(logEntry); err != nil {
			fmt.Println("Error while writing a file:", err)
		}

		if count >= 10 {
			done <- struct{}{}
		}
	})
	token.Wait()
}
