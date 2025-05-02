package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

func main() {
	// Open a log file for writing
	logFile, err := os.OpenFile("harness_logs.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}
	defer logFile.Close()

	// Set log output to the file
	log.SetOutput(logFile)

	// Build a standalone client binary (faster launches)
	if err := exec.Command("go", "build", "-o", "client_bin", "client.go").Run(); err != nil {
		log.Fatalln("build client:", err)
	}

	// Start server in background
	srv := exec.Command("go", "run", "server.go")
	srvStd, _ := srv.StdoutPipe()
	srv.Stderr = srv.Stdout
	if err := srv.Start(); err != nil {
		log.Fatalln(err)
	}
	go pipe("SERVER", srvStd, logFile)

	time.Sleep(time.Second) // Give server a moment to bind

	// Launch eight clients
	for i := 1; i <= 8; i++ {
		// fmt.Printf("Starting client %d\n", i)
		// randomly wait between 0 and 400 ms before starting each client
		time.Sleep(time.Duration(rand.Intn(400)) * time.Millisecond)
		cmd := exec.Command("./client_bin", fmt.Sprint(i))
		out, _ := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			log.Println(err)
			continue
		}
		go pipe(fmt.Sprintf("C%d", i), out, logFile)
		time.Sleep(10 * time.Millisecond) // Stagger starts slightly
	}

	select {} // Block forever – Ctrl‑C to stop
}

func pipe(tag string, r io.Reader, logFile *os.File) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := fmt.Sprintf("[%s] %s\n", tag, s.Text())
		fmt.Print(line)           // Print to console
		logFile.WriteString(line) // Write to log file
	}
}
