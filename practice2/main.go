package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

// Message - сообщение между пользователями
type Message struct {
	SendUserID    int    `json:"send_user_id"`
	ReceiveUserID int    `json:"receive_user_id"`
	Message       string `json:"message"`
}

// State - общее состояние
type State struct {
	BannedWords  map[string]bool `json:"banned_words"`
	BlockedUsers map[int][]int   `json:"blocked_users"`
}

// JsonCodec общий кодек для JSON
type JsonCodec[T any] struct{}

func (jc JsonCodec[T]) Encode(value interface{}) ([]byte, error) {
	if v, ok := value.(T); ok {
		return json.Marshal(v)
	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (jc JsonCodec[T]) Decode(data []byte) (interface{}, error) {
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return t, nil
}

var (
	brokers = []string{"kafka-0:9092", "kafka-1:9092", "kafka-2:9092"}

	topicMessages         goka.Stream = "messages"
	topicFilteredMessages goka.Stream = "filtered-messages"
	topicBlockedUsers     goka.Stream = "blocked-users"
	topicBannedWords      goka.Stream = "banned-words"

	groupMain goka.Group = "main-processor-group"
)

const stateKey = "global-state"

func main() {
	go messageEmitter()
	go mainProcessor()

	select {}
}

// messageEmitter — генерирует тестовые данные
func messageEmitter() {
	msgEmitter, err := goka.NewEmitter(brokers, topicMessages, new(JsonCodec[Message]))
	if err != nil {
		log.Fatal(err)
	}
	defer msgEmitter.Finish()

	bannedEmitter, err := goka.NewEmitter(brokers, topicBannedWords, new(codec.String))
	if err != nil {
		log.Fatal(err)
	}
	defer bannedEmitter.Finish()

	blockEmitter, err := goka.NewEmitter(brokers, topicBlockedUsers, new(codec.String))
	if err != nil {
		log.Fatal(err)
	}
	defer blockEmitter.Finish()

	bannedTicker := time.NewTicker(15 * time.Second)
	blockTicker := time.NewTicker(20 * time.Second)
	defer bannedTicker.Stop()
	defer blockTicker.Stop()

	words := []string{"coffee", "car", "book", "music", "city", "work", "movie", "apple", "orange", "pineapple", "bar", "fruit", "human"}

	for {
		select {
		default:
			time.Sleep(1 * time.Second)

			sender := rand.IntN(50)
			receiver := rand.IntN(50)
			for receiver == sender {
				receiver = rand.IntN(50)
			}

			wordCount := rand.IntN(2) + 3
			textParts := make([]string, wordCount)
			for i := 0; i < wordCount; i++ {
				textParts[i] = words[rand.IntN(len(words))]
			}
			text := strings.Join(textParts, " ")

			m := Message{
				SendUserID:    sender,
				ReceiveUserID: receiver,
				Message:       text,
			}

			// Ключ = stateKey, чтобы всё шло в одну партицию
			if err := msgEmitter.EmitSync(stateKey, m); err != nil {
				log.Printf("Ошибка отправки сообщения: %v", err)
			} else {
				log.Printf("Сообщение от %d к %d: %s", sender, receiver, text)
			}

		case <-bannedTicker.C:
			newWord := words[rand.IntN(len(words))]
			if err := bannedEmitter.EmitSync(stateKey, newWord); err != nil {
				log.Printf("Ошибка добавления слова: %v", err)
			} else {
				log.Printf("Добавлено запрещённое слово: %s", newWord)
			}

		case <-blockTicker.C:
			user := rand.IntN(50)
			blocked := rand.IntN(50)
			for blocked == user {
				blocked = rand.IntN(50)
			}
			value := fmt.Sprintf("%d:%d", user, blocked)
			if err := blockEmitter.EmitSync(stateKey, value); err != nil {
				log.Printf("Ошибка блокировки: %v", err)
			} else {
				log.Printf("Пользователь %d заблокировал %d", user, blocked)
			}
		}
	}
}

// mainProcessor - единственный процессор, обрабатывает все события
func mainProcessor() {
	emitter, err := goka.NewEmitter(brokers, topicFilteredMessages, new(JsonCodec[Message]))
	if err != nil {
		log.Fatal(err)
	}
	defer emitter.Finish()

	// Обработка сообщений
	processMessage := func(ctx goka.Context, msg interface{}) {
		m := msg.(Message)
		state := getState(ctx)

		// Проверяем блокировку
		if blockedList, ok := state.BlockedUsers[m.ReceiveUserID]; ok {
			for _, blockedID := range blockedList {
				if blockedID == m.SendUserID {
					log.Printf("Сообщение от %d к %d заблокировано", m.SendUserID, m.ReceiveUserID)
					return
				}
			}
		}

		// Фильтруем запрещённые слова
		words := strings.Fields(m.Message)
		for i, w := range words {
			if state.BannedWords[strings.ToLower(w)] {
				words[i] = "****"
			}
		}
		m.Message = strings.Join(words, " ")

		emitter.EmitSync(strconv.Itoa(m.ReceiveUserID), m)
		log.Printf("Сообщение от %d к %d прошло фильтр: %s", m.SendUserID, m.ReceiveUserID, m.Message)
	}

	// Обработка banned words
	processBannedWord := func(ctx goka.Context, msg interface{}) {
		word := strings.ToLower(msg.(string))
		state := getState(ctx)
		state.BannedWords[word] = true
		ctx.SetValue(state)
		log.Printf("Запрещённое слово сохранено: %s", word)
	}

	// Обработка blocked users
	processBlockedUser := func(ctx goka.Context, msg interface{}) {
		parts := strings.Split(msg.(string), ":")
		if len(parts) != 2 {
			return
		}
		receiverID, _ := strconv.Atoi(parts[0])
		blockedID, _ := strconv.Atoi(parts[1])

		state := getState(ctx)
		state.BlockedUsers[receiverID] = append(state.BlockedUsers[receiverID], blockedID)
		ctx.SetValue(state)
		log.Printf("Пользователь %d заблокировал отправителя %d", receiverID, blockedID)
	}

	g := goka.DefineGroup(groupMain,
		goka.Input(topicMessages, new(JsonCodec[Message]), processMessage),
		goka.Input(topicBannedWords, new(codec.String), processBannedWord),
		goka.Input(topicBlockedUsers, new(codec.String), processBlockedUser),
		goka.Persist(new(JsonCodec[State])),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// getState возвращает текущее состояние или создаёт новое
func getState(ctx goka.Context) State {
	if ctx.Value() == nil {
		return State{
			BannedWords:  make(map[string]bool),
			BlockedUsers: make(map[int][]int),
		}
	}
	return ctx.Value().(State)
}
