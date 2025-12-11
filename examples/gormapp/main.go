/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func main() {
	// Set up GORM logger
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	// Set up connection to Stoolap
	// Using memory:// for in-memory database (easier for demo purposes)
	dsn := "memory://"
	db, err := gorm.Open(mysql.New(mysql.Config{
		DriverName: "stoolap",
		DSN:        dsn,
	}), &gorm.Config{
		Logger: newLogger,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "",
			SingularTable: true, // Use singular table name
		},
		DisableForeignKeyConstraintWhenMigrating: true, // Disable foreign key constraints
		CreateBatchSize:                          1,    // Create one record at a time
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	fmt.Println("Successfully connected to Stoolap with GORM")

	// Skip auto-migration for now since Stoolap doesn't support some SQL syntax
	// Instead, create tables manually
	err = createTables(db)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}
	fmt.Println("Tables created manually")

	// Perform CRUD operations
	performCRUDOperations(db)
}

func createTables(db *gorm.DB) error {
	// Create users table with proper syntax
	userStmt := "CREATE TABLE user (id INTEGER, name TEXT NOT NULL, email TEXT, age INTEGER, active INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP)"
	err := db.Exec(userStmt).Error
	if err != nil {
		return fmt.Errorf("failed to create user table: %w", err)
	}

	// Create unique index on email
	err = db.Exec("CREATE UNIQUE INDEX idx_user_email ON user (email)").Error
	if err != nil {
		return fmt.Errorf("failed to create index on user.email: %w", err)
	}

	// Create products table with proper syntax
	productStmt := "CREATE TABLE product (id INTEGER, name TEXT NOT NULL, description TEXT, price FLOAT, stock INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP)"
	err = db.Exec(productStmt).Error
	if err != nil {
		return fmt.Errorf("failed to create product table: %w", err)
	}

	return nil
}

func performCRUDOperations(db *gorm.DB) {
	// Create operations
	now := time.Now()
	users := []User{
		{ID: 1, Name: "John Doe", Email: "john@example.com", Age: 30, Active: true, CreatedAt: now, UpdatedAt: now},
		{ID: 2, Name: "Jane Smith", Email: "jane@example.com", Age: 25, Active: true, CreatedAt: now, UpdatedAt: now},
	}

	// Create users one by one to avoid batch issues
	for _, user := range users {
		result := db.Create(&user)
		if result.Error != nil {
			log.Printf("Failed to create user %s: %v", user.Name, result.Error)
		} else {
			fmt.Printf("Created user: %s\n", user.Name)
		}
	}

	products := []Product{
		{ID: 1, Name: "Laptop", Description: "High-performance laptop", Price: 999.99, Stock: 10, CreatedAt: now, UpdatedAt: now},
		{ID: 2, Name: "Smartphone", Description: "Latest model", Price: 699.99, Stock: 25, CreatedAt: now, UpdatedAt: now},
	}

	// Create products one by one
	for _, product := range products {
		result := db.Create(&product)
		if result.Error != nil {
			log.Printf("Failed to create product %s: %v", product.Name, result.Error)
		} else {
			fmt.Printf("Created product: %s\n", product.Name)
		}
	}

	// Read operations
	var userCount int64
	db.Model(&User{}).Count(&userCount)
	fmt.Printf("Total users: %d\n", userCount)

	var firstUser User
	db.First(&firstUser)
	fmt.Printf("First user: %s (Email: %s)\n", firstUser.Name, firstUser.Email)

	var activeUsers []User
	db.Where("active = ?", true).Find(&activeUsers)
	fmt.Printf("Found %d active users\n", len(activeUsers))

	// Update operations
	// Use UpdateColumn to only update specific column (avoid including other fields)
	db.Model(&User{}).Where("id = ?", 1).UpdateColumn("age", 31)
	fmt.Println("Updated John's age to 31")

	var laptopProduct Product
	db.Where("name = ?", "Laptop").First(&laptopProduct)

	// Update one field at a time to avoid complex update statements
	// Use UpdateColumn to only update specific column (avoid including other fields)
	db.Model(&Product{}).Where("id = ?", laptopProduct.ID).UpdateColumn("price", 899.99)
	db.Model(&Product{}).Where("id = ?", laptopProduct.ID).UpdateColumn("stock", 8)
	fmt.Println("Updated laptop price to $899.99 and stock to 8")

	// Delete operations
	db.Where("email = ?", "john@example.com").Delete(&User{})
	fmt.Println("Deleted user: John Doe")

	// Verify deletion
	var remainingUsers []User
	db.Find(&remainingUsers)
	fmt.Printf("Remaining users: %d\n", len(remainingUsers))
}
