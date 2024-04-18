package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	PROTOCOL = "tcp"
)

type Record struct {
	Key   [10]byte
	Value [90]byte
}

var numServers int
var clientConnections []net.Conn
var nodeChannels []chan Record
var receivedRecords []Record
var waitGroup sync.WaitGroup

type ServerConfig struct {
	Servers []struct {
		ServerID int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfig(configPath string) ServerConfig {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}
	var config ServerConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("error parsing config file: %v", err)
	}
	return config
}

func checkErrorAndExit(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s\n", err)
	}
}

func checkErrorWithoutExit(err error) {
	if err != nil {
		log.Printf("Error: %s\n", err)
	}
}

func getAddress(host string, port string) string {
	return host + ":" + port
}

func connectToSocket(address string) (net.Conn, error) {
	waitTime := time.Duration(250)
	for {
		conn, err := net.Dial(PROTOCOL, address)
		checkErrorWithoutExit(err)
		if err == nil {
			return conn, nil
		}
		time.Sleep(waitTime * time.Millisecond)
	}
}

func acceptConnections(listener net.Listener) {
	for {
		if len(clientConnections) == numServers-1 {
			go collectReceivedData()
			break
		}
		conn, err := listener.Accept()
		checkErrorAndExit(err)
		clientConnections = append(clientConnections, conn)
		if err == nil {
			ch := make(chan Record)
			nodeChannels = append(nodeChannels, ch)
			go receiveData(conn, ch)
		}
	}
}

func receiveData(conn net.Conn, dataChannel chan<- Record) {
	defer close(dataChannel)
	for {
		var key [10]byte
		var value [90]byte
		var bytesToRead int
		var buf []byte
		var data []byte
		data = make([]byte, 0)
		for {
			buf = make([]byte, 101-bytesToRead)
			n, err := conn.Read(buf)
			checkErrorAndExit(err)
			bytesToRead += n
			data = append(data, buf[:n]...)
			if bytesToRead >= 101 {
				break
			}
		}
		streamComplete := (data[0] == 1)
		if !streamComplete {
			copy(key[:], data[1:11])
			copy(value[:], data[11:101])
			dataChannel <- Record{key, value}
		} else {
			break
		}
	}
}

func collectReceivedData() {
	for i := 0; i < len(nodeChannels); i++ {
		for rec := range nodeChannels[i] {
			receivedRecords = append(receivedRecords, rec)
		}
	}
	waitGroup.Done()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	serverID, err := strconv.Atoi(os.Args[1])
	checkErrorAndExit(err)

	serverConfig := readServerConfig(os.Args[4])
	sort.Slice(serverConfig.Servers, func(i, j int) bool {
		return serverConfig.Servers[i].ServerID <= serverConfig.Servers[j].ServerID
	})
	numServers = len(serverConfig.Servers)

	serverPort := serverConfig.Servers[serverID].Port
	serverAddr := serverConfig.Servers[serverID].Host
	addr := getAddress(serverAddr, serverPort)

	listener, err := net.Listen(PROTOCOL, addr)
	checkErrorAndExit(err)
	defer listener.Close()

	waitGroup.Add(1)
	go acceptConnections(listener)
	time.Sleep(1000 * time.Millisecond)

	inputFileName := os.Args[2]
	inputFile, err := os.Open(inputFileName)
	checkErrorAndExit(err)

	msb := int(math.Log2(float64(numServers)))
	connections := make(map[int]net.Conn)

	for i := 0; i < numServers; i++ {
		if i == serverID {
			continue
		}
		peerPort := serverConfig.Servers[i].Port
		peerAddr := serverConfig.Servers[i].Host
		addr := getAddress(peerAddr, peerPort)
		conn, _ := connectToSocket(addr)
		connections[i] = conn
		defer conn.Close()
	}

	ownRecords := []Record{}
	for {
		var key [10]byte
		var value [90]byte
		_, err := inputFile.Read(key[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErrorAndExit(err)
		}
		_, err = inputFile.Read(value[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErrorAndExit(err)
		}
		firstByte := key[0]
		keyToServer := int(firstByte) >> (8 - msb)
		if keyToServer == serverID {
			ownRecords = append(ownRecords, Record{key, value})
			continue
		}
		conn, exists := connections[keyToServer]
		if !exists && keyToServer != serverID {
			peerPort := serverConfig.Servers[keyToServer].Port
			peerAddr := serverConfig.Servers[keyToServer].Host
			addr := getAddress(peerAddr, peerPort)
			conn, _ = connectToSocket(addr)
			connections[keyToServer] = conn
		}
		streamCompleteByte := byte(0)
		_, err = conn.Write([]byte{streamCompleteByte})
		checkErrorAndExit(err)
		_, err = conn.Write([]byte(key[:]))
		checkErrorAndExit(err)
		_, err = conn.Write([]byte(value[:]))
		checkErrorAndExit(err)
	}

	for _, conn := range connections {
		streamCompleteByte := byte(1)
		_, err = conn.Write([]byte{streamCompleteByte})
		checkErrorAndExit(err)
		var key [10]byte
		var value [90]byte
		_, err = conn.Write([]byte(key[:]))
		checkErrorAndExit(err)
		_, err = conn.Write([]byte(value[:]))
		checkErrorAndExit(err)
	}

	inputFile.Close()

	records := []Record{}
	records = append(records, ownRecords...)
	waitGroup.Wait()
	records = append(records, receivedRecords...)

	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key[:], records[j].Key[:]) <= 0
	})

	outputFileName := os.Args[3]
	outputFile, err := os.Create(outputFileName)
	checkErrorAndExit(err)
	for _, rec := range records {
		_, err = outputFile.Write(rec.Key[:])
		checkErrorAndExit(err)
		_, err = outputFile.Write(rec.Value[:])
		checkErrorAndExit(err)
	}
	outputFile.Close()
}
