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

// Message
type Message struct {
	SendUserID    int    `json:"send_user_id"`
	ReceiveUserID int    `json:"receive_user_id"`
	Message       string `json:"message"`
}

type BlockedUser struct {
	Users []int `json:"blocked_users"`
}

// JsonCodec общий кодек, который предназначен для сериализации и десериализации в json
type JsonCodec[T any] struct{}

func (jc JsonCodec[T]) Encode(value interface{}) ([]byte, error) {
	if user, ok := value.(T); ok {
		return json.Marshal(user)
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

type BoolCodec struct{}

func (b *BoolCodec) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("BoolCodec: expected bool, got %T", value)
	}
	if v {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (b *BoolCodec) Decode(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}
	switch data[0] {
	case 1:
		return true, nil
	case 0:
		return false, nil
	default:
		return nil, fmt.Errorf("BoolCodec: invalid byte %v", data)
	}
}

var (
	brokers = []string{"kafka-0:9092", "kafka-1:9093", "kafka-2:9094"}

	topicMessages         goka.Stream = "messages"
	topicFilteredMessages goka.Stream = "filtered-messages"
	topicBlockedUsers     goka.Stream = "blocked-users"
	topicBannedWords      goka.Stream = "banned-words"

	groupBlockedUsers     goka.Group = "blocked-users-group"
	groupFilteredMessages goka.Group = "filtered-messages-group"
	groupBannedWords      goka.Group = "group-banned-words"
)

func main() {
	go messageEmitter()
	go blockProcessor()
	go filterProcessor()
	go banProcessor()

	select {} // Блокируем main, чтобы горутины работали
}

// messageEmitter — эмиттер, который генерирует данные в топик messages
func messageEmitter() {
	// emitters для всех топиков
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

	// Таймеры
	bannedTicker := time.NewTicker(15 * time.Second)
	blockTicker := time.NewTicker(20 * time.Second)
	defer bannedTicker.Stop()
	defer blockTicker.Stop()

	words := []string{"coffee", "car", "book", "music", "city", "work"}

	for {
		select {
		default:
			// Каждую секунду отправляем обычное сообщение
			time.Sleep(1 * time.Second)

			sender := rand.IntN(100)   // от 0 до 9
			receiver := rand.IntN(100) // от 0 до 9
			for receiver == sender {
				receiver = rand.IntN(100) // не самому себе
			}

			// Случайная генерация текста
			wordCount := rand.IntN(5) + 3
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

			key := strconv.Itoa(sender)
			if err := msgEmitter.EmitSync(key, m); err != nil {
				log.Printf("Ошибка при отправке сообщения: %v", err)
			} else {
				log.Printf("Сообщение от %d к %d: %s", sender, receiver, text)
			}

		case <-bannedTicker.C:
			// Каждые 15 сек добавляем новое запрещённое слово
			newWord := "badword_" + strconv.Itoa(rand.IntN(10000))
			if err := bannedEmitter.EmitSync(newWord, newWord); err != nil {
				log.Printf("Ошибка при добавлении запрещённого слова: %v", err)
			} else {
				log.Printf("Добавлено запрещённое слово: %s", newWord)
			}

		case <-blockTicker.C:
			// Каждые 20 сек пользователь блокирует другого
			user := rand.IntN(100)
			blocked := rand.IntN(100)
			for blocked == user {
				blocked = rand.IntN(100)
			}
			key := strconv.Itoa(user)
			value := strconv.Itoa(blocked)

			if err := blockEmitter.EmitSync(key, value); err != nil {
				log.Printf("Ошибка при блокировке: %v", err)
			} else {
				log.Printf("Пользователь %s заблокировал %s", key, value)
			}
		}
	}
}

// blockProcessor - процессор для обновления таблицы заблокированных пользователей
func blockProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		blockedID, _ := strconv.Atoi(msg.(string))
		userID := ctx.Key()
		var blocked BlockedUser
		if ctx.Value() != nil {
			blocked = ctx.Value().(BlockedUser)
		}
		blocked.Users = append(blocked.Users, blockedID)
		ctx.SetValue(blocked)
		log.Printf("Пользователь %s заблокировал %d", userID, blockedID)
	}

	g := goka.DefineGroup(groupBlockedUsers,
		goka.Input(topicBlockedUsers, new(codec.String), processFunc),
		goka.Persist(new(JsonCodec[BlockedUser])),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// banProcessor - процессор для обновления таблицы запрещённых слов
func banProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		word := msg.(string)
		ctx.SetValue(true)
		log.Printf("Добавлено запрещённое слово: %s", word)
	}

	g := goka.DefineGroup(groupBannedWords,
		goka.Input(topicBannedWords, new(codec.String), processFunc),
		goka.Persist(new(BoolCodec)),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// filterProcessor - процессор фильтрации сообщений
func filterProcessor() {
	// Создаём Views на таблицы
	viewBlocked, _ := goka.NewView(brokers, goka.Table(topicBlockedUsers), new(JsonCodec[BlockedUser]))
	viewBanned, _ := goka.NewView(brokers, goka.Table(topicBannedWords), new(BoolCodec))

	go viewBlocked.Run(context.Background())
	go viewBanned.Run(context.Background())

	emitter, _ := goka.NewEmitter(brokers, topicFilteredMessages, new(JsonCodec[Message]))

	processFunc := func(ctx goka.Context, msg interface{}) {
		m := msg.(Message)

		// Проверка блокировки
		blockedUsers, _ := viewBlocked.Get(strconv.Itoa(m.ReceiveUserID))
		if blockedUsers != nil {
			for _, u := range blockedUsers.(BlockedUser).Users {
				if u == m.SendUserID {
					log.Printf("Сообщение от %d к %d заблокировано", m.SendUserID, m.ReceiveUserID)
					return
				}
			}
		}

		// Цензура запрещённых слов
		words := strings.Fields(m.Message)
		for i, w := range words {
			if val, _ := viewBanned.Get(w); val != nil {
				words[i] = "****"
			}
		}
		m.Message = strings.Join(words, " ")

		// Отправка в filtered-messages
		emitter.EmitSync(strconv.Itoa(m.ReceiveUserID), m)
		log.Printf("Сообщение от %d к %d прошло фильтр: %s", m.SendUserID, m.ReceiveUserID, m.Message)
	}

	g := goka.DefineGroup(groupFilteredMessages,
		goka.Input(topicMessages, new(JsonCodec[Message]), processFunc),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
