module github.com/stoolap/stoolap/examples/gormapp

go 1.23.0

toolchain go1.24.3

require (
	github.com/stoolap/stoolap v0.1.1
	gorm.io/driver/mysql v1.5.4
	gorm.io/gorm v1.25.7
)

replace github.com/stoolap/stoolap => ../../

require (
	github.com/go-sql-driver/mysql v1.7.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
)
