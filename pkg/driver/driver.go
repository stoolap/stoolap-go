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
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/stoolap/stoolap-go/internal/cs"
)

func init() {
	sql.Register("stoolap", &Driver{})
}

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

// Open implements driver.Driver.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	// connector.Connect clones the base DB handle; the connector itself is
	// ephemeral here, so close it to free the base C handle regardless of
	// whether Connect succeeded.
	conn, connErr := connector.Connect(context.Background())
	connector.(*Connector).Close() //nolint:errcheck // best-effort; connErr takes precedence
	return conn, connErr
}

// OpenConnector implements driver.DriverContext.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	db, err := cs.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &Connector{db: db, dsn: dsn}, nil
}

// Connector implements driver.Connector.
type Connector struct {
	db  *cs.DB
	dsn string
	mu  sync.Mutex
}

// Connect implements driver.Connector.
// Each connection gets its own cloned DB handle for thread safety.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.db == nil {
		return nil, errors.New("stoolap: connector is closed")
	}
	clone, err := c.db.Clone()
	if err != nil {
		return nil, err
	}
	return &Conn{db: clone}, nil
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

// Close closes the connector's underlying database handle.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}
