package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// User представляет структуру для маппинга строк базы данных
type User struct {
	ID    int
	Name  string
	Email string
	Score int
}

const dbName = "user_data.sqlite"

func main() {
	// 1. ОТКРЫТИЕ И ИНИЦИАЛИЗАЦИЯ
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal("Ошибка открытия БД:", err)
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		log.Fatal("Ошибка инициализации БД:", err)
	}

	log.Println("--- 1. Вставка (Exec) ---")
	// 2. ВСТАВКА (INSERT) И ОБНОВЛЕНИЕ (UPDATE)
	idAlice, _ := insertUser(db, "Alice", "alice@example.com", 100)
	idBob, _ := insertUser(db, "Bob", "bob@example.com", 150)
	log.Printf("Вставлены Alice (ID: %d) и Bob (ID: %d)", idAlice, idBob)

	log.Println("--- 2. Обновление (Exec) ---")
	updateScore(db, idAlice, 120)

	log.Println("--- 3. Чтение одной строки (QueryRow) ---")
	// 3. ЧТЕНИЕ ОДНОЙ СТРОКИ (QueryRow)
	alice, err := getUserByID(db, idAlice)
	if err == nil {
		log.Printf("Прочитан пользователь: %+v", alice)
	} else {
		log.Println("Ошибка чтения пользователя:", err)
	}

	log.Println("--- 4. Чтение множества строк (Query) ---")
	// 4. ЧТЕНИЕ МНОЖЕСТВА СТРОК (Query)
	allUsers, _ := getAllUsers(db)
	log.Printf("Все пользователи (%d):", len(allUsers))
	for _, u := range allUsers {
		log.Printf("  ID: %d, Name: %s, Score: %d", u.ID, u.Name, u.Score)
	}

	log.Println("--- 5. Транзакция (Begin/Commit) ---")
	// 5. ТРАНЗАКЦИИ
	transferScore(db, idBob, idAlice, 20) // Перевод 20 очков от Bob к Alice

	// 6. ПРИМЕРЫ С CONTEXT
	log.Println("--- 6. Работа с Context (Таймаут) ---")
	if err := queryWithTimeout(db); err != nil {
		log.Println("Результат таймаута:", err)
	}
}

// initDB создает таблицу
func initDB(db *sql.DB) error {
	const createTableSQL = `
	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		score INTEGER
	);`
	_, err := db.Exec(createTableSQL)
	return err
}

// insertUser вставляет новую запись и возвращает ее ID
func insertUser(db *sql.DB, name, email string, score int) (int64, error) {
	result, err := db.Exec(
		"INSERT INTO users (name, email, score) VALUES (?, ?, ?)",
		name, email, score,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// updateScore обновляет счет пользователя по ID
func updateScore(db *sql.DB, id int64, newScore int) {
	_, err := db.Exec("UPDATE users SET score = ? WHERE id = ?", newScore, id)
	if err != nil {
		log.Printf("Ошибка обновления счета: %v", err)
	} else {
		log.Printf("Обновлен счет пользователя %d до %d", id, newScore)
	}
}

// getUserByID читает одну строку по ID
func getUserByID(db *sql.DB, id int64) (User, error) {
	var user User
	// QueryRow готовит, выполняет запрос и возвращает одну строку
	row := db.QueryRow("SELECT id, name, email, score FROM users WHERE id = ?", id)

	// Scan сканирует результат в переменные.
	// Важно: он должен вызываться даже для проверки sql.ErrNoRows
	err := row.Scan(&user.ID, &user.Name, &user.Email, &user.Score)
	if err == sql.ErrNoRows {
		return user, fmt.Errorf("пользователь с ID %d не найден", id)
	}
	return user, err
}

// getAllUsers читает все строки
func getAllUsers(db *sql.DB) ([]User, error) {
	// Query выполняет запрос, возвращая *sql.Rows (набор строк)
	rows, err := db.Query("SELECT id, name, email, score FROM users ORDER BY score DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close() // Обязательно закрываем rows!

	var users []User
	// Цикл по всем строкам в наборе результатов
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.Score); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	// Проверяем ошибки, возникшие во время итерации (цикла)
	return users, rows.Err()
}

// transferScore демонстрирует использование транзакций для атомарных операций
func transferScore(db *sql.DB, fromID, toID int64, amount int) {
	// Начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Ошибка начала транзакции: %v", err)
		return
	}
	defer tx.Rollback() // Откат в случае паники или возврата (если Commit не был вызван)

	// 1. Уменьшаем счет отправителя
	_, err = tx.Exec("UPDATE users SET score = score - ? WHERE id = ?", amount, fromID)
	if err != nil {
		log.Printf("Ошибка уменьшения счета: %v", err)
		return
	}

	// 2. Увеличиваем счет получателя
	_, err = tx.Exec("UPDATE users SET score = score + ? WHERE id = ?", amount, toID)
	if err != nil {
		log.Printf("Ошибка увеличения счета: %v", err)
		return
	}

	// 3. Фиксируем изменения, если все прошло успешно
	err = tx.Commit()
	if err != nil {
		log.Printf("Ошибка Commit: %v", err)
		return
	}
	log.Printf("Успешный перевод %d очков от %d к %d.", amount, fromID, toID)
}

// ====================================================================
// ПРИМЕРЫ С CONTEXT
// ====================================================================

// queryWithTimeout демонстрирует, как использовать context.WithTimeout
func queryWithTimeout(db *sql.DB) error {
	// Создаем контекст с таймаутом 1 миллисекунда
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel() // Освобождаем ресурсы контекста по завершении

	log.Println("  Попытка выполнить медленный запрос...")

	// Используем QueryContext вместо обычного Query
	rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE 1=1 AND ? = 1", 1)

	// Примечание: В SQLite сложно имитировать долгие запросы,
	// поэтому этот пример просто показывает синтаксис.
	// Если бы запрос занял больше 1 мс, он бы вернул ошибку контекста.

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("превышен таймаут: %w", err)
		}
		return err
	}
	defer rows.Close()

	log.Println("  Запрос выполнен успешно (контекст не истек)")
	return nil
}
