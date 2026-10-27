//go:build native_step_integration

package redis_state

// Private RESP2 relay: real sockets and real Valkey replies. No synthesized
// successful script result, process Go effect port or host-network mutation.
import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type nativeFrame struct {
	raw, value []byte
	items      []nativeFrame
}

func readNativeFrame(r *bufio.Reader) (nativeFrame, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nativeFrame{}, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nativeFrame{}, fmt.Errorf("invalid RESP header")
	}
	f := nativeFrame{raw: line, value: line[1 : len(line)-2]}
	switch line[0] {
	case '$':
		n, e := strconv.Atoi(string(f.value))
		if e != nil || n < -1 || n > 16<<20 {
			return f, fmt.Errorf("invalid RESP bulk length")
		}
		if n == -1 {
			return f, nil
		}
		b := make([]byte, n+2)
		if _, err = io.ReadFull(r, b); err != nil {
			return f, err
		}
		if !bytes.Equal(b[n:], []byte("\r\n")) {
			return f, fmt.Errorf("invalid RESP bulk terminator")
		}
		f.raw = append(f.raw, b...)
		f.value = b[:n]
	case '*':
		n, e := strconv.Atoi(string(f.value))
		if e != nil || n < 0 || n > 64 {
			return f, fmt.Errorf("invalid RESP array length")
		}
		for range n {
			item, e := readNativeFrame(r)
			if e != nil {
				return f, e
			}
			f.items = append(f.items, item)
			f.raw = append(f.raw, item.raw...)
		}
	case '+', '-', ':':
	default:
		return f, fmt.Errorf("unexpected RESP type")
	}
	return f, nil
}

type nativeWireCommand struct {
	name            string
	args            [][]byte
	result          []int64
	forwarded       bool
	serverErrorCode string
	at              time.Time
}
type nativeRelay struct {
	listener    net.Listener
	upstream    string
	mode        string
	mu          sync.Mutex
	connections []net.Conn
	commands    []nativeWireCommand
	faults      int
	commits     chan struct{}
	release     chan struct{}
	stop        chan struct{}
	wg          sync.WaitGroup
	errs        []error
}

func startNativeRelay(t *testing.T, upstream, mode string) *nativeRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &nativeRelay{listener: listener, upstream: upstream, mode: mode, commits: make(chan struct{}, 1), release: make(chan struct{}), stop: make(chan struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			p.mu.Lock()
			p.connections = append(p.connections, c)
			p.mu.Unlock()
			p.wg.Add(1)
			go p.serve(c)
		}
	}()
	t.Cleanup(func() {
		close(p.stop)
		_ = listener.Close()
		p.mu.Lock()
		for _, c := range p.connections {
			_ = c.Close()
		}
		p.mu.Unlock()
		done := make(chan struct{})
		go func() { p.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("native relay cleanup exceeded deadline")
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Empty(t, p.errs)
	})
	return p
}
func (p *nativeRelay) serve(c net.Conn) {
	defer p.wg.Done()
	defer c.Close()
	u, err := net.DialTimeout("tcp", p.upstream, time.Second)
	if err != nil {
		p.fail(err)
		return
	}
	defer u.Close()
	p.mu.Lock()
	p.connections = append(p.connections, u)
	p.mu.Unlock()
	_ = u.SetDeadline(time.Now().Add(10 * time.Second))
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	r, back := bufio.NewReader(c), bufio.NewReader(u)
	for {
		f, e := readNativeFrame(r)
		if e != nil {
			if e != io.EOF { // Partial writes are expected only in the explicit write-stall case.
				if p.mode != "write-stall" {
					if ne, ok := e.(net.Error); !ok || !ne.Timeout() {
						p.fail(e)
					}
				}
			}
			return
		}
		if len(f.items) == 0 {
			p.fail(fmt.Errorf("empty native command"))
			return
		}
		name := string(f.items[0].value)
		script := name == "EVAL" || name == "EVALSHA"
		// The combined response operation uses the same socket for both scripts.
		// Target queue faults by its actual key count, after SaveStep has replied.
		faultMode := strings.TrimPrefix(p.mode, "queue-")
		target := !strings.HasPrefix(p.mode, "queue-") || (script && len(f.items) > 2 && string(f.items[2].value) == "22")
		p.mu.Lock()
		index := len(p.commands)
		command := nativeWireCommand{name: name, at: time.Now()}
		if script || name == "HGET" {
			for _, v := range f.items[1:] {
				command.args = append(command.args, bytes.Clone(v.value))
			}
		}
		p.commands = append(p.commands, command)
		fault := script && target && (p.faults == 0 || p.mode == "before-always") && p.mode != "" && p.mode != "write-stall"
		if fault {
			p.faults++
		}
		p.mu.Unlock()
		if fault && (faultMode == "before" || faultMode == "before-always" || faultMode == "read-stall") {
			if p.mode == "read-stall" {
				<-p.stop
			}
			return
		}
		if p.mode == "handshake-stall" {
			<-p.stop
			return
		}
		if _, e = u.Write(f.raw); e != nil {
			p.fail(e)
			return
		}
		reply, e := readNativeFrame(back)
		if e != nil {
			p.fail(e)
			return
		}
		p.mu.Lock()
		p.commands[index].forwarded = true
		if script && reply.raw[0] == '-' {
			p.commands[index].serverErrorCode = strings.SplitN(string(reply.value), " ", 2)[0]
		}
		if script && len(reply.items) > 0 {
			for _, v := range reply.items {
				n, e := strconv.ParseInt(string(v.value), 10, 64)
				if e != nil {
					p.errs = append(p.errs, e)
				}
				p.commands[index].result = append(p.commands[index].result, n)
			}
		}
		if script && reply.raw[0] == ':' {
			n, e := strconv.ParseInt(string(reply.value), 10, 64)
			if e != nil {
				p.errs = append(p.errs, e)
			}
			p.commands[index].result = []int64{n}
		}
		p.mu.Unlock()
		if fault && faultMode == "after" {
			arrayCommit := len(reply.items) > 0 && (string(reply.items[0].value) == "0" || string(reply.items[0].value) == "1")
			scalarCommit := reply.raw[0] == ':' && string(reply.value) == "0"
			if !arrayCommit && !scalarCommit {
				p.fail(fmt.Errorf("fault must follow an actual fresh commit"))
				return
			}
			p.commits <- struct{}{}
			select {
			case <-p.release:
			case <-p.stop:
			}
			return // Drop a genuine reply while the native caller is still live.
		}
		if _, e = c.Write(reply.raw); e != nil {
			return
		}
		if p.mode == "write-stall" && name == "AUTH" {
			<-p.stop
			return
		}
	}
}
func (p *nativeRelay) fail(e error) { p.mu.Lock(); defer p.mu.Unlock(); p.errs = append(p.errs, e) }
func (p *nativeRelay) inventory() ([]nativeWireCommand, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]nativeWireCommand(nil), p.commands...), p.faults
}
func nativeByteArray(b []byte) []uint16 {
	a := make([]uint16, len(b))
	for i, v := range b {
		a[i] = uint16(v)
	}
	return a
}

// One fixed test artifact request, no worker framework or effect protocol.
func invokeNative(t *testing.T, input map[string]any, relay *nativeRelay, afterCommit func()) map[string]json.RawMessage {
	t.Helper()
	binary := os.Getenv("NATIVE_STEP_TEST_BINARY")
	require.NotEmpty(t, binary, "required artifact: cd rust && cargo build --locked -p native-step-state --features test-helper --bin native-step-test-helper; set NATIVE_STEP_TEST_BINARY to its absolute path")
	require.True(t, filepath.IsAbs(binary), "NATIVE_STEP_TEST_BINARY must be an absolute artifact path")
	require.FileExists(t, binary)
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	timeout := 15 * time.Second
	if budget, ok := input["budgetMs"].(int); ok && budget > 10000 {
		timeout = time.Duration(budget)*time.Millisecond + 3*time.Second
	}
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Stdin = bytes.NewReader(raw)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	if afterCommit != nil {
		select {
		case <-relay.commits:
			select {
			case err := <-done:
				t.Fatalf("caller exited before independent postcommit read: %v", err)
			default:
			}
			afterCommit()
			close(relay.release)
		case err := <-done:
			t.Fatalf("native exited without expected real commit: %v stderr=%s", err, stderr.String())
		case <-ctx.Done():
			t.Fatal("native commit observation deadline")
		}
	}
	require.NoError(t, <-done, "native helper stderr=%s", stderr.String())
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &out))
	return out
}
