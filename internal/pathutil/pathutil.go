// Package pathutil provides path joining that is safe for both local filesystem
// paths and URI-scheme paths (gs://, s3://, etc.).
//
// The standard path.Join collapses double slashes, turning "gs://bucket/foo"
// into "gs:/bucket/foo" which breaks GCS and S3 clients.
package pathutil

import (
	"path"
	"strings"
)

// Join joins path elements onto base, preserving any URI scheme double-slash.
func Join(base string, elems ...string) string {
	if isURI(base) {
		result := strings.TrimRight(base, "/")
		for _, e := range elems {
			if e != "" {
				result += "/" + strings.Trim(e, "/")
			}
		}
		return result
	}
	all := make([]string, 0, len(elems)+1)
	all = append(all, base)
	all = append(all, elems...)
	return path.Join(all...)
}

// Base returns the last element of a path, handling URI schemes.
func Base(p string) string {
	if isURI(p) {
		p = strings.TrimRight(p, "/")
		i := strings.LastIndexByte(p, '/')
		if i < 0 {
			return p
		}
		return p[i+1:]
	}
	return path.Base(p)
}

func isURI(p string) bool {
	return strings.Contains(p, "://")
}
