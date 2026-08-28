package cdc

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestLibraryCodeNeverCallsLogFatalOrOsExit guards against B1: a library
// calling log.Fatal/os.Exit terminates the ENTIRE host process, and no
// recover() can ever intercept it -- unlike a panic, which at least a
// supervising goroutine could catch. connector.go used to call log.Fatal on
// a snapshot invalidation error instead of returning it; this scans the
// library's own source (excluding examples, benchmarks, integration tests,
// and the .dev/ scratch harnesses, which are standalone programs where
// os.Exit is the normal, correct way to report a failure) so a future
// reintroduction fails the build instead of shipping silently.
func TestLibraryCodeNeverCallsLogFatalOrOsExit(t *testing.T) {
	forbidden := regexp.MustCompile(`\blog\.Fatal(f|ln)?\(|(^|[^.\w])os\.Exit\(`)

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
			t.Errorf("%s calls log.Fatal/os.Exit: a library must return errors, never terminate the host process directly", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to walk repo for log.Fatal/os.Exit check: %v", err)
	}
}
