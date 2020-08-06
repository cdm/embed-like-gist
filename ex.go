package converters

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	types "code.vegaprotocol.io/vega/proto"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"

	"github.com/vegaprotocol/vdb/util"

	log "github.com/sirupsen/logrus"
)

func TradesToCSV(
	wg *sync.WaitGroup,
	dbPath string,
	config *Config,
	markets map[string]types.Market,
) {
	log.Info("Processing trades STARTED")

	// __create_wallet:

	opts := badger.DefaultOptions(dbPath)
	opts.ReadOnly = true
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	file, err := os.Create(getTradesCsvFile(config.chainId))
	defer file.Close()

	// :create_wallet__

	w := csv.NewWriter(file)
	defer w.Flush()

	// Write out header to csv file
	err = w.Write(tradeHeaderToCsvRow())
	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("M:")
		pos := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			pos++
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var trade types.Trade
				if err := proto.Unmarshal(v, &trade); err != nil {
					return err
				}

				config.stats.TotalTrades.Inc()

				// Do we need to exclude this trade? [update: only for bot<>bot]
				isBot := false
				if _, foundBuyer := config.whitelist[trade.Buyer]; !foundBuyer {
					if _, foundSeller := config.whitelist[trade.Seller]; !foundSeller {
						if config.exclude {
							config.stats.ExcludedTrades.Inc()
							return nil
						}
						isBot = true
					}
				}

				dp := 0
				if m, ok := markets[trade.MarketID]; ok {
					if m.DecimalPlaces > 0 {
						dp = int(m.DecimalPlaces)
					}
				} else {
					log.Warnf("No market found for trade.MarketID %s - using default DP [0]",
							trade.MarketID,
						)
				}

				// Write out line to csv writer
				err = w.Write(tradeToCsvRow(trade, dp, isBot, config.chainId))
				if err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				return err
			}
			if pos == config.batchSize {
				log.Infof("Processing trades SLEEPING: %s", config.wait)
				time.Sleep(config.wait)
				pos = 0
			}
		}
		return nil
	})
	if err != nil {
		log.Error(err)
	}

	wg.Done()
	log.Info("Processing trades COMPLETED")
}

func tradeHeaderToCsvRow() []string {
	var res []string
	res = append(res, "ChainID")
	res = append(res, "ID")
	res = append(res, "MarketID")
	res = append(res, "Seller")
	res = append(res, "Buyer")
	res = append(res, "Size")
	res = append(res, "Price")     // DP corrected price
	res = append(res, "TickPrice") // 'Price' in trade msg
	res = append(res, "Aggressor")
	res = append(res, "BuyOrder")
	res = append(res, "SellOrder")
	res = append(res, "Timestamp")
	res = append(res, "IsBot")
	return res
}


func tradeToCsvRow(trade types.Trade, decimalPlaces int, isBot bool, chainId string) []string {
	var res []string
	res = append(res, chainId)
	res = append(res, trade.Id)
	res = append(res, trade.MarketID)
	res = append(res, trade.Seller)
	res = append(res, trade.Buyer)
	res = append(res, strconv.FormatUint(trade.Size, 10))
	res = append(res, util.GetFormattedStringByDP(trade.Price, decimalPlaces))
	res = append(res, strconv.FormatUint(trade.Price, 10))
	res = append(res, trade.Aggressor.String())
	res = append(res, trade.BuyOrder)
	res = append(res, trade.SellOrder)
	res = append(res, formatCsvDate(trade.Timestamp))
	res = append(res, util.BtoI(isBot))
	return res
}

func getTradesCsvFile(chainId string) string {
	return fmt.Sprintf("trades-%s.csv", chainId)
}

