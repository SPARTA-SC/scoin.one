package puller

import (
	"database/sql"
	"flag"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"github.com/pressly/goose"
	"github.com/zyjblockchain/sandy_log/log"
	log2 "github.com/zyjblockchain/sandy_log/log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"tezos_index/common"
	"tezos_index/puller/index"
	_ "tezos_index/puller/migration"
	"tezos_index/puller/models"
	"tezos_index/rpc"
)

type Configuration struct {
	Mysql         string
	Redis         string
	ApiUrl        string
	ProxyUrl      string
	Start         uint
	End           uint
	Fix           bool
	Verbose       int
	Listen        string
	Chain         string
	Network       string
	Sentry        string
	Kafka         string
	OnlyBlock     bool
	GasStationUrl string
}

type Environment struct {
	Conf        Configuration
	Engine      *gorm.DB
	Client      *rpc.Client
	RedisClient *redis.Client
}

func NewEnvironment() *Environment {
	flag.String("mysql", common.DefaultString, "tezos mysql uri like 'mysql://tcp(ip:[port]/database'")
	flag.String("chain", common.DefaultString, "tezos json-rpc like 'http://faq:faq@localhost:18332/0'")
	flag.String("proxy", common.DefaultString, "tezos json-rpc like 'http://faq:faq@localhost:18332/0'")
	flag.String("api-url", common.DefaultString, "api url like 'http://:111")
	flag.String("redis", common.DefaultString, "redis url like ")
	flag.Int("start", common.DefaultInt, "tezos start with special block number")
	flag.String("network", common.DefaultString, "tezos network mainnet or kovan")
	flag.String("listen", common.DefaultString, "tezos biz api listen ")
	flag.Int("verbose", common.DefaultInt, "tezos print verbose message")
	flag.Bool("fix", false, "tezos fix blocks")
	flag.Int("end", common.DefaultInt, "tezos fix end special blocks")
	flag.String("sentry", common.DefaultString, "sentry url")
	flag.String("kafka", common.DefaultString, "kafka broker")
	flag.String("gas-station-url", common.DefaultString, "gas station url")
	flag.Bool("only-block", false, "only sync blocks")
	flag.String("node-type", common.DefaultString, "node-type")

	viperConfig := common.NewViperConfig()

	domain := "tezos"

	conf := Configuration{}

	conf.Verbose = viperConfig.GetInt("", "verbose")

	conf.Redis = viperConfig.GetString(domain, "redis")
	if conf.Redis == "" {
		log2.Crit("please set redis connection info")
		panic("system fail")
	}
	spl := strings.Split(strings.TrimPrefix(conf.Redis, "redis://"), "/")
	db, _ := strconv.Atoi(spl[1])
	redisClient := redis.NewClient(&redis.Options{
		Addr:     spl[0],
		Password: "",
		DB:       db,
	})

	conf.Mysql = viperConfig.GetString(domain, "mysql")
	if conf.Mysql == "" {
		log2.Crit("please set mysql connection info")
		panic("system fail")
	}
	engine := index.InitDB(conf.Mysql)

	conf.Chain = viperConfig.GetString(domain, "chain")

	httpClient := http.DefaultClient
	pUrl := viperConfig.GetString(domain, "proxy")
	if pUrl != "" {
		proxyUrl, err := url.Parse(pUrl)
		if err != nil {
			log.Errorf("url parse error: %v", err)
			panic(err)
		}
		tr := &http.Transport{Proxy: http.ProxyURL(proxyUrl)}
		httpClient = &http.Client{Transport: tr}
	}
	// conf.Chain = "https://mainnet-tezos.giganode.io" // todo 节点还在同步数据，暂时使用第三方node
	client, err := rpc.NewClient(httpClient, conf.Chain)
	if err != nil {
		log.Errorf("connect tezos node client error: %v", err)
		panic("connet tezos node error")
	}
	conf.Start = uint(viperConfig.GetInt(domain, "start"))
	conf.End = uint(viperConfig.GetInt(domain, "end"))
	conf.Fix = viperConfig.GetBool(domain, "fix")
	conf.Network = viperConfig.GetString(domain, "network")
	conf.Listen = viperConfig.GetString("", "listen")
	conf.Sentry = viperConfig.GetString("", "sentry")
	conf.Kafka = viperConfig.GetString(domain, "kafka")
	conf.GasStationUrl = viperConfig.GetString(domain, "gas-station-url")
	conf.OnlyBlock = viperConfig.GetBool(domain, "only-block")

	return &Environment{Conf: conf, Engine: engine, Client: client, RedisClient: redisClient}
}

func (e *Environment) NewPuller() *Crawler {
	indexer := NewIndexer(IndexerConfig{
		StateDB: e.Engine,
		CacheDB: e.RedisClient,
		Indexes: []models.BlockIndexer{ // **** 此处顺序不能变 ****
			index.NewAccountIndex(e.Engine),
			index.NewContractIndex(e.Engine),
			index.NewBlockIndex(e.Engine),
			index.NewOpIndex(e.Engine),
			index.NewFlowIndex(e.Engine),
			index.NewChainIndex(e.Engine),
			index.NewSupplyIndex(e.Engine),
			index.NewRightsIndex(e.Engine),
			index.NewSnapshotIndex(e.Engine), // 需要脏读 account
			index.NewIncomeIndex(e.Engine),
			index.NewGovIndex(e.Engine),
			index.NewBigMapIndex(e.Engine), // 需要脏读contract
		},
	})

	cf := CrawlerConfig{
		DB:            e.Engine,
		Indexer:       indexer,
		Client:        e.Client,
		Queue:         4,
		StopBlock:     0,
		EnableMonitor: true, // 等同步到最新区块之后则开启 monitor
	}
	return NewCrawler(cf)
}

// UpgradeSchema auto migrate
func (e *Environment) UpgradeSchema() {
	if err := upgrade(e.Conf.Mysql); err != nil {
		log.Crit("upgrade database", "uri", e.Conf.Mysql, "err", err)
		panic("system fail")
	}
}

// RollbackSchema when need to rollback database
func (e *Environment) RollbackSchema(version string) {
	if err := rollback(e.Conf.Mysql, version); err != nil {
		log.Crit("rollback database", "uri", e.Conf.Mysql, "err", err)
		panic("system fail")
	}
}

func upgrade(dsn string) error {
	var err error
	var db *sql.DB

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	err = goose.SetDialect("mysql")
	if err != nil {
		return err
	}
	err = goose.Run("up", db, ".")
	if err != nil {
		return err
	}

	return nil
}

func rollback(dsn string, version string) error {
	var err error
	var db *sql.DB

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	err = goose.SetDialect("mysql")
	if err != nil {
		return err
	}
	if version == "" {
		err = goose.Run("down", db, ".")
	} else {
		err = goose.Run("down-to", db, ".", version)
	}
	return err
}
