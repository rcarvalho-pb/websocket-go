package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/websocket"
)

type Client struct {
	Name  string
	Rooms map[string]*Room
	WS    *websocket.Conn
}

type Room struct {
	Clients map[*Client]bool
	Mutex   sync.Mutex
	Name    string
}

var (
	rooms       = make(map[string]*Room)
	globalMutex sync.Mutex
)

func main() {
	http.Handle("/", http.FileServer(http.Dir("static")))
	http.Handle("/ws", websocket.Handler(handleWS))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getOrCreateRoom(name string) *Room {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	if room, exist := rooms[name]; exist {
		return room
	}
	room := &Room{
		Name:    name,
		Clients: make(map[*Client]bool),
	}
	rooms[name] = room
	return room
}

func (r *Room) broadcast(msg string) {
	for client := range r.Clients {
		if err := websocket.Message.Send(client.WS, msg); err != nil {
			log.Printf("Error sending msg to '%s'\n", client.Name)
		}
	}
}

func handleWS(ws *websocket.Conn) {
	var initMsg string
	if err := websocket.Message.Receive(ws, &initMsg); err != nil {
		log.Println("Failed to receive init message:", err)
		return
	}

	parts := strings.SplitN(initMsg, ":", 2)
	if len(parts) != 2 {
		log.Println("Invalid init message format. Expected 'room:username'")
		return
	}

	roomName := parts[0]
	username := parts[1]
	room := getOrCreateRoom(roomName)
	client := &Client{
		Name:  username,
		Rooms: make(map[string]*Room),
		WS:    ws,
	}
	client.Rooms[roomName] = room
	room.Mutex.Lock()
	room.Clients[client] = true
	room.Mutex.Unlock()
	msg := fmt.Sprintf("🟢 %s joined", client.Name)
	room.broadcast(msg)
	for {
		var msg string
		if err := websocket.Message.Receive(client.WS, &msg); err != nil {
			log.Printf("'%s' disconnected from server", username)
			break
		}
		room.broadcast(fmt.Sprintf("%s: %s", client.Name, msg))
	}
	room.Mutex.Lock()
	delete(client.Rooms, roomName)
	delete(room.Clients, client)
	room.Mutex.Unlock()
	room.broadcast(fmt.Sprintf("🔴 %s left", client.Name))
}
