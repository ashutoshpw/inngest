//go:build linux && native_step_integration

package redis_state

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Fill only this private listener's one-entry accept queue. Subsequent SYNs
// cannot complete, so this exercises an actual TCP connect timeout rather than
// a refused connection or stalled AUTH. No host routes/firewall are changed.
func TestNativeRustConnectTimeout(t *testing.T) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })
	require.NoError(t, unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	require.NoError(t, unix.Listen(fd, 0))
	bound, err := unix.Getsockname(fd)
	require.NoError(t, err)
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(bound.(*unix.SockaddrInet4).Port))
	blocker, err := net.DialTimeout("tcp", address, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = blocker.Close() })
	probe, err := net.DialTimeout("tcp", address, 60*time.Millisecond)
	if probe != nil {
		_ = probe.Close()
	}
	require.Error(t, err, "the private queue must actually be full")
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	require.True(t, netErr.Timeout())
	input := nativeInput(address, "unused", StateDefaultKey, false, nativeSaveOperation("opaque", 0))
	input["disabledRetries"] = true
	input["store"].(map[string]any)["connectTimeoutMs"] = 60
	start := time.Now()
	out := invokeNative(t, input, nil, nil)
	elapsed := time.Since(start)
	require.JSONEq(t, `"ErrMaxAttemptReached"`, string(out["error"]))
	require.JSONEq(t, `"NotSent"`, string(out["writeStatus"]))
	require.JSONEq(t, `"connect-or-auth"`, string(out["lastFailure"]))
	require.Equal(t, "1", string(out["attempts"]))
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	require.Less(t, elapsed, 3*time.Second)
}
