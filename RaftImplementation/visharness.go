// ---------- harness.go ----------
// Console-visualiser harness: builds client, launches broker + 8 clients,
// parses stdout, and paints a live ANSI dashboard refreshed at 10 Hz.
//
//	╔════════════════════════════════════════════╗
//	║ Node 1  term: 1   █ (Leader – green)       ║
//	║ Node 2  term: 1   ░ (Follower – blue)      ║
//	║ …                                          ║
//	╚════════════════════════════════════════════╝
//
// Compile + run:
//
//	go run harness.go
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var (
	colour = map[string]string{"Follower": "34", "Candidate": "33", "Leader": "32"} // ANSI
	glyph  = map[string]string{"Follower": "░", "Candidate": "▒", "Leader": "█"}

	// now includes Follower, Candidate, and Leader
	reState = regexp.MustCompile(
		`Node (\d+) → Follower .*term (\d+)|` +
			`Node (\d+) → Candidate .*term (\d+)|` +
			`Node (\d+) → Leader .*term (\d+)`,
	)

	mu   sync.Mutex
	term = [9]int{}    // 1–8
	role = [9]string{} // "Follower","Candidate","Leader"
)

func main() {
	must(exec.Command("go", "build", "-o", "client_bin", "client.go").Run())

	srv := spawn("SERVER", "go", "run", "server.go")
	defer srv.Process.Kill()
	time.Sleep(800 * time.Millisecond)

	for i := 1; i <= 8; i++ {
		// randomly wait between 0 and 400 ms before starting each client
		time.Sleep(time.Duration(rand.Intn(400)) * time.Millisecond)
		role[i] = "Follower"
		spawn(fmt.Sprintf("C%d", i), "./client_bin", fmt.Sprint(i))
	}

	go dashboard()
	select {}
}

func spawn(tag string, cmd ...string) *exec.Cmd {
	c := exec.Command(cmd[0], cmd[1:]...)
	pipe, _ := c.StdoutPipe()
	c.Stderr = c.Stdout
	must(c.Start())
	go tap(tag, pipe)
	return c
}

func tap(tag string, r io.Reader) {
	scan := bufio.NewScanner(r)
	for scan.Scan() {
		line := scan.Bytes()
		parse(line)
		fmt.Printf("[%s] %s\n", tag, line)
	}
}

func parse(b []byte) {
	mu.Lock()
	defer mu.Unlock()

	if m := reState.FindSubmatch(b); m != nil {
		switch {
		// Follower: m[1]=id, m[2]=term
		case len(m[1]) > 0:
			id := atoi(m[1])
			role[id] = "Follower"
			term[id] = atoi(m[2])

		// Candidate: m[3]=id, m[4]=term
		case len(m[3]) > 0:
			id := atoi(m[3])
			role[id] = "Candidate"
			term[id] = atoi(m[4])

		// Leader:    m[5]=id, m[6]=term
		case len(m[5]) > 0:
			id := atoi(m[5])
			role[id] = "Leader"
			term[id] = atoi(m[6])
		}
	}
}

func dashboard() {
	for {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		var buf bytes.Buffer
		buf.WriteString("\033[2J\033[HRaft cluster live view (10 Hz)\n")
		for i := 1; i <= 8; i++ {
			clr := colour[role[i]]
			buf.WriteString(fmt.Sprintf(
				"Node %d  term:%2d  \033[1;%sm%s\033[0m\n",
				i, term[i], clr, glyph[role[i]],
			))
		}
		os.Stdout.Write(buf.Bytes())
		mu.Unlock()
	}
}

func atoi(b []byte) int { n, _ := strconv.Atoi(string(b)); return n }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
