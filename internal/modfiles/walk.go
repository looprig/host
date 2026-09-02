// Package modfiles enumerates Go source files owned by this module while excluding
// structural directories and nested repository or module boundaries.
package modfiles

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// SymlinkError reports a module-owned Go source path that is a symbolic link.
// Discovery fails closed rather than following a link that may escape the module.
type SymlinkError struct {
	Path string
}

func (e *SymlinkError) Error() string {
	return "modfiles: module-owned Go file is a symbolic link: " + e.Path
}

// Files returns absolute paths to every module-owned Go source file below root.
// Build constraints are deliberately ignored so tagged production files remain
// visible to dependency and formatting checks.
func Files(root string) ([]string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	var files []string
	err = filepath.WalkDir(absoluteRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path != absoluteRoot && entry.IsDir() {
			if IsIgnoredDirectoryName(entry.Name()) {
				return filepath.SkipDir
			}
			nested, err := IsBoundaryDirectory(path)
			if err != nil {
				return err
			}
			if nested {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() || IsIgnoredFileName(entry.Name()) || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return &SymlinkError{Path: path}
		}
		if info.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	return files, nil
}

// WriteNull writes paths separated and terminated by NUL bytes for safe piping to
// tools such as xargs -0, including when a filename contains whitespace or newlines.
func WriteNull(w io.Writer, paths []string) error {
	for _, path := range paths {
		if _, err := io.WriteString(w, path); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\x00"); err != nil {
			return err
		}
	}
	return nil
}

// IsIgnoredDirectoryName reports whether a directory with this name is excluded
// from enumeration: vendor and testdata, which the Go tool does not build as
// module content, and any name the Go tool itself ignores.
//
// It is EXPORTED because a second walk over the same tree must exclude exactly
// what this one excludes. It was not, and the consequence was a copy: the
// dependency guard's nested-module walk retyped these literals and carried a
// comment claiming the two could not disagree. They could — widening this
// function alone silently un-guarded a directory in both walks at once, and two
// mutants survived the whole suite proving it. Callers share the predicate now;
// the SET it returns is pinned separately, because sharing makes the two walks
// agree without stopping the shared answer from being widened.
//
// This package has THREE structural predicates, not two, and the seam is only
// as good as the count: IsIgnoredFileName is the third, and it was left
// unshared and unpinned for a round while a comment here claimed the exclusions
// were exactly mirrored. Any predicate that decides what a walk does not see
// belongs in this exported set and needs an oracle in the consumer.
func IsIgnoredDirectoryName(name string) bool {
	return name == "vendor" || name == "testdata" || goIgnoredName(name)
}

// IsIgnoredFileName reports whether a file with this name is excluded from
// enumeration. It is exported for the same reason the two directory predicates
// are, and it was the one left behind when they were shared: widening it hides
// a file from the dependency guard AND from `make fmt-check`, which pipes this
// same enumerator into gofmt. A mutant adding a "_generated.go" suffix here
// survived the whole suite and the whole check — the escaped file was
// unformatted, unchecked and unguarded at once.
func IsIgnoredFileName(name string) bool {
	return goIgnoredName(name)
}

func goIgnoredName(name string) bool {
	return name != "" && (name[0] == '.' || name[0] == '_')
}

// IsBoundaryDirectory reports whether dir is owned by a different module or
// repository, and is therefore not this module's content. It is exported for
// the same reason IsIgnoredDirectoryName is: it is the definition of a
// boundary, and there must be exactly one.
func IsBoundaryDirectory(dir string) (bool, error) {
	for _, marker := range []string{"go.mod", ".git"} {
		_, err := os.Lstat(filepath.Join(dir, marker))
		switch {
		case err == nil:
			return true, nil
		case os.IsNotExist(err):
			continue
		default:
			return false, err
		}
	}
	return false, nil
}
