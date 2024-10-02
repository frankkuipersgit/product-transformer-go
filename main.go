package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/schollz/progressbar/v3"
)

const (
	dbUser     = "username"  // Update with your MySQL username
	dbPassword = "password"  // Update with your MySQL password
	dbName     = "productdb" // The database name you created
)

type Product struct {
	ID          int
	Name        string
	Category    string
	Price       float64
	Description string
	Stock       int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Expected 'clear', 'generate', 'transform', or 'all' subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "clear":
		clearCmd := flag.NewFlagSet("clear", flag.ExitOnError)
		clearCmd.Parse(os.Args[2:])
		clearDatabase()
	case "generate":
		generateCmd := flag.NewFlagSet("generate", flag.ExitOnError)
		totalProducts := generateCmd.Int("n", 1000000, "Total number of products to generate")
		batchSize := generateCmd.Int("batchSize", 1000, "Number of products per batch operation")
		generateCmd.Parse(os.Args[2:])
		generateAndInsertProducts(*totalProducts, *batchSize)
	case "transform":
		transformCmd := flag.NewFlagSet("transform", flag.ExitOnError)
		batchSize := transformCmd.Int("batchSize", 1000, "Number of products per batch operation")
		transformCmd.Parse(os.Args[2:])
		transformExistingProducts(*batchSize)
	case "all":
		allCmd := flag.NewFlagSet("all", flag.ExitOnError)
		totalProducts := allCmd.Int("n", 1000000, "Total number of products to generate")
		batchSize := allCmd.Int("batchSize", 1000, "Number of products per batch operation")
		allCmd.Parse(os.Args[2:])
		clearDatabase()
		generateAndInsertProducts(*totalProducts, *batchSize)
		transformExistingProducts(*batchSize)
	default:
		fmt.Println("Expected 'clear', 'generate', 'transform', or 'all' subcommands")
		os.Exit(1)
	}
}

func connectDatabase() *sql.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s?parseTime=true", dbUser, dbPassword, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	return db
}

func clearDatabase() {
	db := connectDatabase()
	defer db.Close()

	fmt.Println("Clearing the products table...")
	if _, err := db.Exec("TRUNCATE TABLE products"); err != nil {
		log.Fatal("Failed to truncate products table:", err)
	}
	fmt.Println("Database cleared.")
}

func generateAndInsertProducts(totalProducts int, batchSize int) {
	db := connectDatabase()
	defer db.Close()

	fmt.Println("Generating products in memory...")
	products := generateProducts(totalProducts)

	fmt.Println("Inserting products into the database...")
	insertProducts(db, products, batchSize)
}

func transformExistingProducts(batchSize int) {
	db := connectDatabase()
	defer db.Close()

	// Count total products
	var totalProducts int
	err := db.QueryRow("SELECT COUNT(*) FROM products").Scan(&totalProducts)
	if err != nil {
		log.Fatal("Failed to count products:", err)
	}

	if totalProducts == 0 {
		fmt.Println("No products found in the database.")
		return
	}

	fmt.Printf("Transforming %d products...\n", totalProducts)

	// start time
	start := time.Now()

	// Use worker pool to process batches concurrently
	workerCount := 8                       // Number of worker goroutines
	batchCh := make(chan int, workerCount) // Channel for batch indices

	var wg sync.WaitGroup

	// Progress bar setup
	bar := progressbar.NewOptions(
		totalProducts,
		progressbar.OptionSetDescription("Transforming Products"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)

	var mu sync.Mutex // Mutex to protect the progress bar

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				transformedCount := processBatch(db, batch, batchSize)

				// Safely update the progress bar
				mu.Lock()
				bar.Add(transformedCount)
				mu.Unlock()
			}
		}()
	}

	totalBatches := (totalProducts + batchSize - 1) / batchSize

	// Enqueue batches
	for batch := 0; batch < totalBatches; batch++ {
		batchCh <- batch
	}

	close(batchCh) // Close the channel to signal workers to stop
	wg.Wait()      // Wait for all workers to finish

	bar.Finish()
	// print total time taken

	fmt.Printf("Total time taken: %v\n", time.Since(start))
	fmt.Println("Transformation of existing products completed.")
}

func processBatch(db *sql.DB, batch int, batchSize int) int {
	offset := batch * batchSize
	limit := batchSize

	// Read a batch of products
	rows, err := db.Query("SELECT id, name, category, price, description, stock FROM products LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		log.Fatalf("Failed to query products: %v", err)
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.Description, &p.Stock); err != nil {
			log.Fatalf("Failed to scan product: %v", err)
		}
		// Apply transformation rules
		transformed := applyTransformations(&p)
		if transformed {
			products = append(products, p)
		}
	}

	// Return the number of transformed products for progress tracking
	transformedCount := len(products)

	if transformedCount > 0 {
		// Validate products before updating
		if err := validateProducts(products); err != nil {
			log.Fatalf("Data validation error: %v", err)
		}
		// Update transformed products
		updateProducts(db, products)
	}

	return transformedCount
}

func generateProducts(totalProducts int) []Product {
	products := make([]Product, totalProducts)

	var wg sync.WaitGroup
	numWorkers := 8 // Adjust based on your CPU cores
	jobs := make(chan int, totalProducts)

	// Progress bar for generation with mutex
	bar := progressbar.NewOptions(
		totalProducts,
		progressbar.OptionSetDescription("Generating Products"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)

	var mu sync.Mutex // Mutex to protect progress bar updates

	// Seed the random number generator once
	rand.Seed(time.Now().UnixNano())

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Create a local random source for each goroutine
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			localCount := 0 // Keep track of progress locally
			for index := range jobs {
				p := generateRandomProduct(index+1, r)
				products[index] = p
				localCount++

				// Update progress bar every 100 items
				if localCount%100 == 0 {
					mu.Lock()
					bar.Add(100)
					mu.Unlock()
					localCount = 0
				}
			}
			// Update any remaining progress
			if localCount > 0 {
				mu.Lock()
				bar.Add(localCount)
				mu.Unlock()
			}
		}(i)
	}

	// Send jobs to workers
	for i := 0; i < totalProducts; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	bar.Finish()

	return products
}

func generateRandomProduct(id int, r *rand.Rand) Product {
	categories := []string{"Electronics", "Books", "Clothing", "Home", "Sports", "Toys"}
	names := []string{"Gadget", "Widget", "Device", "Item", "Product", "Thing"}
	descriptions := []string{
		"High quality", "Durable", "Limited edition", "Best seller", "New arrival", "On sale",
	}

	category := categories[r.Intn(len(categories))]
	name := fmt.Sprintf("%s %s %d", descriptions[r.Intn(len(descriptions))], names[r.Intn(len(names))], id)
	price := r.Float64()*100 + 1 // Random price between $1 and $100
	description := fmt.Sprintf("%s %s", descriptions[r.Intn(len(descriptions))], category)
	stock := r.Intn(1000) + 1 // Random stock between 1 and 1000

	return Product{
		ID:          id,
		Name:        name,
		Category:    category,
		Price:       price,
		Description: description,
		Stock:       stock,
	}
}

func insertProducts(db *sql.DB, products []Product, batchSize int) {
	totalProducts := len(products)
	totalBatches := (totalProducts + batchSize - 1) / batchSize
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8) // Limit concurrent database operations

	// Progress bar for database insertion with mutex
	bar := progressbar.NewOptions(
		totalProducts,
		progressbar.OptionSetDescription("Inserting into Database"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)

	var mu sync.Mutex // Mutex to protect progress bar updates

	for batch := 0; batch < totalBatches; batch++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(batch int) {
			defer wg.Done()
			defer func() { <-sem }()

			start := batch * batchSize
			end := start + batchSize
			if end > totalProducts {
				end = totalProducts
			}
			batchProducts := products[start:end]

			// Build the bulk insert query
			valueStrings := make([]string, 0, len(batchProducts))
			valueArgs := make([]interface{}, 0, len(batchProducts)*6)
			for _, p := range batchProducts {
				valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
				valueArgs = append(valueArgs, p.ID, p.Name, p.Category, p.Price, p.Description, p.Stock)
			}

			stmt := fmt.Sprintf("INSERT INTO products (id, name, category, price, description, stock) VALUES %s", strings.Join(valueStrings, ","))
			if _, err := db.Exec(stmt, valueArgs...); err != nil {
				log.Fatalf("Failed to execute batch insert for batch %d: %v", batch, err)
			}

			// Update progress bar
			mu.Lock()
			bar.Add(len(batchProducts))
			mu.Unlock()
		}(batch)
	}

	wg.Wait()
	bar.Finish()
}
func updateProducts(db *sql.DB, products []Product) {
	// Use a transaction to batch updates for efficiency
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Prepare the update statement
	stmt, err := tx.Prepare("UPDATE products SET name = ?, category = ?, price = ?, description = ?, stock = ? WHERE id = ?")
	if err != nil {
		log.Fatalf("Failed to prepare update statement: %v", err)
	}
	defer stmt.Close()

	// Execute update for each product
	for _, p := range products {
		_, err := stmt.Exec(p.Name, p.Category, p.Price, p.Description, p.Stock, p.ID)
		if err != nil {
			// Rollback in case of failure
			tx.Rollback()
			log.Fatalf("Failed to update product ID %d: %v", p.ID, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
}

// Helper function to create placeholders for the IN clause
func placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func applyTransformations(p *Product) bool {
	transformed := false

	// Transformation Rule 1: Increase price by 10% for Electronics
	if p.Category == "Electronics" {
		p.Price *= 1.10
		transformed = true
	}

	// Transformation Rule 2: Decrease stock by 20% for items with stock over 500
	if p.Stock > 500 {
		newStock := int(float64(p.Stock) * 0.80)
		if newStock <= 0 {
			newStock = 1 // Ensure stock does not become zero or negative
		}
		p.Stock = newStock
		transformed = true
	}

	// Transformation Rule 3: Append " (Sale)" to the name of items under $20
	if p.Price < 20 {
		p.Name += " (Sale)"
		transformed = true
	}

	// Ensure price is not zero or negative
	if p.Price <= 0 {
		p.Price = 1.00
	}

	// Ensure stock is not zero or negative
	if p.Stock <= 0 {
		p.Stock = 1
	}

	return transformed
}

func validateProducts(products []Product) error {
	for _, p := range products {
		if p.Stock <= 0 {
			return fmt.Errorf("Product ID %d has invalid stock value: %d", p.ID, p.Stock)
		}
		if p.Price <= 0 {
			return fmt.Errorf("Product ID %d has invalid price value: %.2f", p.ID, p.Price)
		}
		if len(p.Name) == 0 {
			return fmt.Errorf("Product ID %d has empty name", p.ID)
		}
		// Add more validations as needed
	}
	return nil
}
