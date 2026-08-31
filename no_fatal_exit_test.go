package cdc

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestLibraryCodeNeverCallsLogFatalOsExitOrPanic guards against B1 (a library
// calling log.Fatal/os.Exit terminates the ENTIRE host process, and no
// recover() can ever intercept it) and its T3.3 sibling: an unrecovered
// panic in a goroutine also kills the whole process, and a goroutine spawned
// by this library (e.g. the replication stream's sink loop) cannot be
// recover()-ed by a caller's own goroutine, since recover() only catches a
// panic on the same goroutine that panicked. connector.go used to call
// log.Fatal on a snapshot invalidation error, and pq/replication used to
// panic("corrupted connection") on an unrecoverable stream failure, both
// instead of returning the error; this scans the library's own source
// (excluding examples, benchmarks, integration tests, and the .dev/ scratch
// harnesses, which are standalone programs where os.Exit/panic is the
// normal, correct way to report a failure) so a future reintroduction fails
// the build instead of shipping silently.
func TestLibraryCodeNeverCallsLogFatalOsExitOrPanic(t *testing.T) {
	forbidden := regexp.MustCompile(`\blog\.Fatal(f|ln)?\(|(^|[^.\w])os\.Exit\(|(^|[^.\w])panic\(`)

	excludedDirs := map[string]bool{
		".dev": true, "example": true, "benchmark": true,
		"integration_test": true, ".git": true,
	}

	root := "."
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if excludedDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if forbidden.Match(content) {
			t.Errorf("%s calls log.Fatal/os.Exit/panic: a library must return errors, never terminate the host process directly", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to walk repo for log.Fatal/os.Exit/panic check: %v", err)
	}
}
