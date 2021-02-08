package mycanal

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/replication"
)

// Config is used for fulldump and incrdump.
type Config struct {
	// Host of MySQL server.
	Host string `json:"host"`

	// Port of MySQL server.
	Port uint16 `json:"port"`

	// User for connection.
	User string `json:"user"`

	// Password for connection.
	Password string `json:"password"`

	// Charset for connecting.
	Charset string `json:"charset"`

	// ServerId is used by incrdump only (as a replication node).
	ServerId uint32 `json:"serverId"`
}

// ToDriverCfg converts cfg to mysql driver config.
func (cfg *Config) ToDriverCfg() *mysql.Config {
	ret := mysql.NewConfig()
	ret.Net = "tcp"
	ret.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	ret.User = cfg.User
	ret.Passwd = cfg.Password
	ret.ParseTime = true
	ret.InterpolateParams = true
	if ret.Params == nil {
		ret.Params = map[string]string{}
	}
	ret.Params["charset"] = cfg.getCharset()
	return ret
}

// ToBinlogSyncerCfg converts cfg to binlog syncer config. Needs ServerId.
func (cfg *Config) ToBinlogSyncerCfg() replication.BinlogSyncerConfig {
	if cfg.ServerId == 0 {
		panic(fmt.Errorf("ToBinlogSyncerCfg: no ServerId"))
	}
	return replication.BinlogSyncerConfig{
		ServerID:   cfg.ServerId,
		Host:       cfg.Host,
		Port:       cfg.Port,
		User:       cfg.User,
		Password:   cfg.Password,
		Charset:    cfg.getCharset(),
		ParseTime:  true,
		UseDecimal: true,
	}
}

// Client opens mysql db.
func (cfg *Config) Client() (*sql.DB, error) {
	return sql.Open("mysql", cfg.ToDriverCfg().FormatDSN())
}

func (cfg *Config) getCharset() string {
	if cfg.Charset != "" {
		return cfg.Charset
	}
	return "utf8mb4"
}
