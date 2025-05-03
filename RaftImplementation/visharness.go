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
	// ANSI colour codes
	color = map[string]string{
		"Follower":  "34",
		"Candidate": "33",
		"Leader":    "32",
		"Dead":      "31",
	}
	// Glyphs
	glyph = map[string]string{
		"Follower":  "F",
		"Candidate": "C",
		"Leader":    "L",
		"Dead":      "X",
	}

	reState = regexp.MustCompile(
		`Node (\d+) Follower .*term (\d+)|` +
			`Node (\d+) Candidate .*term (\d+)|` +
			`Node (\d+) Leader .*term (\d+)`,
	)

	mu       sync.Mutex
	term     = [9]int{}            // 1–8
	role     = [9]string{}         // "Follower","Candidate","Leader,Daead"
	procs    = map[int]*exec.Cmd{} // 1–8
	killedID int
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
		cmd := spawn(fmt.Sprintf("C%d", i), "./client_bin", fmt.Sprint(i))
		procs[i] = cmd
	}
	const killInterval = 15 * time.Second
	go func() {
		for {
			time.Sleep(killInterval)

			mu.Lock()
			leaderID := 0
			for i := 1; i <= 8; i++ {
				if role[i] == "Leader" {
					leaderID = i
					break
				}
			}
			// Mark dead and kill process
			if leaderID != 0 {
				role[leaderID] = "Dead"
				fmt.Printf("\nKilling leader C%d at t=%v\n", leaderID, time.Now().Format("15:04:05"))
				if cmd, ok := procs[leaderID]; ok {
					cmd.Process.Kill()
					delete(procs, leaderID)
				}
				mu.Unlock()
			} else {
				mu.Unlock()
				fmt.Println("\nNo leader found — at least half of nodes dead, or failed to re-elect. Exiting.")
				os.Exit(0)
			}
		}
	}()

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
		// Clear & move cursor to top
		buf.WriteString("\033[2J\033[HRaft cluster live view\n")
		for i := 1; i <= 8; i++ {
			clr := color[role[i]]
			g := glyph[role[i]]
			buf.WriteString(fmt.Sprintf(
				"Node %d  term:%2d  \033[1;%sm%s\033[0m\n",
				i, term[i], clr, g,
			))
		}
		if killedID != 0 {
			buf.WriteString(fmt.Sprintf("Killed node: %d (marked X)\n", killedID))
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
