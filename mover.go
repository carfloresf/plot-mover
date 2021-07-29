package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/ricochet2200/go-disk-usage/du"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"
)

var KB = uint64(1024)
var minUsage = uint64(105)

type client struct {
	id        string
	sourceDir string
	conn      *ssh.Client
	client    *sftp.Client
}

func initLog() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
}

func main() {
	initLog()

	config, err := getConfig(".")
	if err != nil {
		log.Fatal("error getting config: ", err)
	}

	go getUsage(config.Destinations)

	clients := []client{}

	dirs := make(map[string]string)

	for ip, dir := range config.Sources {
		conn, sftp := createClient(ip, config.User, config.Password)
		cl := client{
			id:        ip,
			conn:      conn,
			client:    sftp,
			sourceDir: dir,
		}

		clients = append(clients, cl)
	}

	var wg sync.WaitGroup

	for _, c := range clients {
		c := c

		wg.Add(1)

		go func(client *client) {
			defer wg.Done()
			log.Printf("starting routine for %s", c.id)

			for {
				now := time.Now()

				destinationDir := getDestination(config.Destinations, dirs)
				if destinationDir == "" {
					log.Printf("no drive selected for %s on %s", c.id, c.sourceDir)
					return
				}

				dirs[c.id] = destinationDir

				plotFile, err := listFiles(c.client, c.sourceDir)
				if err != nil {
					dirs[c.id] = ""

					log.Fatal(err)
				}

				if plotFile == "" {
					log.Printf("no candidate file for %s on %s", c.id, c.sourceDir)

					dirs[c.id] = ""

					time.Sleep(5 * time.Minute)

					continue
				}

				log.Printf("file to transfer %s for %s to %s", plotFile, c.id, destinationDir)

				err = downloadFile(c.client, c.sourceDir+plotFile, destinationDir+plotFile)
				if err != nil {
					log.Error(err)
				} else {
					log.Printf("delete from: %s, file: %s, on: %s ", c.sourceDir, plotFile, c.sourceDir)

					err := c.client.Remove(strings.TrimSpace(c.sourceDir + plotFile))
					if err != nil {
						log.Error(err)
					}
				}

				dirs[c.id] = ""

				timeTrack(now, fmt.Sprintf("copied in %s", c.id))
			}
		}(&c)
	}

	wg.Wait()
}

func createClient(ipAddress string, user string, pass string) (*ssh.Client, *sftp.Client) {
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(pass),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// connect client 1
	conn, err := ssh.Dial("tcp", ipAddress, config)
	if err != nil {
		panic("Failed to dial: " + err.Error())
	}

	log.Printf("Successfully connected to ssh server %s.", ipAddress)

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Fatal(err)
	}

	return conn, client
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

// Download file from sftp server
func downloadFile(sc *sftp.Client, remoteFile, localFile string) (err error) {
	log.Printf("Downloading [%s] to [%s] ... \n", remoteFile, localFile)

	srcFile, err := sc.OpenFile(remoteFile, (os.O_RDONLY))
	if err != nil {
		log.Errorf("Unable to open remote file: %v\n", err)
		return
	}
	defer srcFile.Close()

	dstFile, err := os.Create(localFile)
	if err != nil {
		log.Errorf("Unable to open local file: %v\n", err)
		return
	}
	defer dstFile.Close()

	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		log.Errorf("Unable to download remote file: %v\n", err)
		return err
	}

	log.Printf("%d bytes copied to remoteFile %s \n", bytes, remoteFile)

	return
}

func listFiles(sc *sftp.Client, remoteDir string) (plotFile string, err error) {
	files, err := sc.ReadDir(remoteDir)
	if err != nil {
		log.Errorf("Unable to list remote dir: %v\n", err)
		return
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".plot") && plotFile == "" {
			plotFile = f.Name()
		}
	}

	return
}

func getUsage(dests map[string]bool) {
	for {
		for k := range dests {
			usage := du.NewDiskUsage(k).Usage()

			log.Printf("usage for %s: %f", k, usage*100)
		}

		time.Sleep(60 * time.Minute)
	}
}

func getDestination(dests map[string]bool, dirs map[string]string) string {
	for k, v := range dests {
		var inUse bool

		available := du.NewDiskUsage(k).Available() / (KB * KB * KB)

		for _, kdir := range dirs {
			if kdir == k {
				inUse = true
			}
		}

		if available >= minUsage && !inUse && v {
			return k
		}
	}

	return ""
}

type MoverConfig struct {
	User         string            `mapstructure:"user"`
	Password     string            `mapstructure:"password"`
	Sources      map[string]string `mapstructure:"sources"`
	Destinations map[string]bool   `mapstructure:"destinations"`
}

func getConfig(configPath string) (MoverConfig, error) {
	var conf MoverConfig

	viper.SetConfigName("mover")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(configPath)

	err := viper.ReadInConfig()
	if err != nil {
		return conf, err
	}

	err = viper.Unmarshal(&conf)

	return conf, err
}
