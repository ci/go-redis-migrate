package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v3"
	"github.com/obukhov/go-redis-migrate/src/pusher"
	"github.com/obukhov/go-redis-migrate/src/reporter"
	"github.com/obukhov/go-redis-migrate/src/scanner"
	"github.com/spf13/cobra"
)

var pattern string
var scanCount, report, exportRoutines, pushRoutines int
var sourceTLS, sourceTLSInsecure bool
var destTLS, destTLSInsecure bool

func createRedisClient(addr string, useTLS, insecureSkipVerify bool) (radix.Client, error) {
	if useTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: insecureSkipVerify,
		}
		connFunc := func(network, addr string) (radix.Conn, error) {
			return radix.Dial(network, addr, radix.DialUseTLS(tlsConfig))
		}
		return radix.NewPool("tcp", addr, 4, radix.PoolConnFunc(connFunc))
	}
	return radix.DefaultClientFunc("tcp", addr)
}

var copyCmd = &cobra.Command{
	Use:   "copy <source> <destination>",
	Short: "Copy keys from source redis instance to destination by given pattern",
	Long:  "Copy keys from source redis instance to destination by given pattern <source> and <destination> can be provided as just `<host>:<port>` or in Redis URL format: `redis://[:<password>@]<host>:<port>[/<dbIndex>]",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Start copying")

		clientSource, err := createRedisClient(args[0], sourceTLS, sourceTLSInsecure)
		if err != nil {
			log.Fatal(err)
		}

		clientTarget, err := createRedisClient(args[1], destTLS, destTLSInsecure)
		if err != nil {
			log.Fatal(err)
		}

		statusReporter := reporter.NewReporter()

		redisScanner := scanner.NewScanner(
			clientSource,
			scanner.RedisScannerOpts{
				Pattern:          pattern,
				ScanCount:        scanCount,
				PullRoutineCount: exportRoutines,
			},
			statusReporter,
		)

		redisPusher := pusher.NewRedisPusher(clientTarget, redisScanner.GetDumpChannel(), statusReporter)

		waitingGroup := new(sync.WaitGroup)

		statusReporter.Start(time.Second * time.Duration(report))
		redisPusher.Start(waitingGroup, pushRoutines)
		redisScanner.Start()

		waitingGroup.Wait()
		statusReporter.Stop()
		statusReporter.Report()

		fmt.Println("Finish copying")
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	copyCmd.Flags().StringVar(&pattern, "pattern", "*", "Match pattern for keys")
	copyCmd.Flags().IntVar(&scanCount, "scanCount", 100, "COUNT parameter for redis SCAN command")
	copyCmd.Flags().IntVar(&report, "report", 1, "Report current status every N seconds")
	copyCmd.Flags().IntVar(&exportRoutines, "exportRoutines", 30, "Number of parallel export goroutines")
	copyCmd.Flags().IntVar(&pushRoutines, "pushRoutines", 30, "Number of parallel push goroutines")
	copyCmd.Flags().BoolVar(&sourceTLS, "source-tls", false, "Enable TLS for source Redis connection")
	copyCmd.Flags().BoolVar(&sourceTLSInsecure, "source-tls-insecure", false, "Skip TLS certificate verification for source")
	copyCmd.Flags().BoolVar(&destTLS, "dest-tls", false, "Enable TLS for destination Redis connection")
	copyCmd.Flags().BoolVar(&destTLSInsecure, "dest-tls-insecure", false, "Skip TLS certificate verification for destination")
}
