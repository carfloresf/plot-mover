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
	"golang.org/x/crypto/ssh"
)

var KB = uint64(1024)
var minUsage = uint64(105)

const user = "carlos"
const pass = "xxxx"

type client struct {
	id        string
	sourceDir string
	conn      *ssh.Client
	client    *sftp.Client
}

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true

	destinations := map[string]bool{
		"/media/hd1/":  true,
		"/media/hd5/":  true,
		"/media/hd6/":  true,
		"/media/hd8/":  true,
		"/media/hda1/": true,
		"/media/hda2/": true,
		"/media/hda3/": true,
		"/media/hda4/": true,
		"/media/hd9/":  true,
		"/media/hd10/": true,
		"/media/hd11/": true,
		"/media/hd12/": true,
		"/media/hd13/": true,
		"/media/hd14/": true,
		"/media/hd15/": true,
		"/media/hd16/": true,
		"/media/hd17/": true,
		"/media/hdf1/": true,
		"/media/hdf2/": true,
		"/media/hd18/": true,
		"/media/hd19/": true,
		"/media/hd20/": true,
		"/media/hd22/": true,
		"/media/hd23/": true,
		"/media/hd24/": true,
	}

	go getUsage(destinations)

	ipAddrs := []string{"192.168.2.12:22", "192.168.2.124:22"}
	sourceDirs := []string{"/media/hd2/", "/media/ssd1/"}

	clients := []client{}

	dirs := make(map[string]string)

	for i, ip := range ipAddrs {
		conn, sftp := createClient(ip)
		cl := client{
			id:        ip,
			conn:      conn,
			client:    sftp,
			sourceDir: sourceDirs[i],
		}

		clients = append(clients, cl)
	}

	var wg sync.WaitGroup

	for _, c := range clients {
		c := c
		wg.Add(1)
		time.Sleep(5 * time.Second)

		go func(client *client) {
			defer wg.Done()
			log.Printf("starting routine for %s", c.id)
			for {
				now := time.Now()

				destinationDir := getDestination(destinations, dirs)
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

func createClient(ipAddress string) (*ssh.Client, *sftp.Client) {
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
		for k, _ := range dests {
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

		if available >= minUsage && !inUse && v == true {
			return k
		}
	}

	return ""
}
