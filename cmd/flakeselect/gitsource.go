package main

import (
	"path"
	"strings"
)

// gitSource reads the post-image from HEAD, not the working tree.
type gitSource struct{}

func (gitSource) Read(file string) ([]byte, error) {
	out, err := git("show", "HEAD:"+file)
	return []byte(out), err
}

func (gitSource) TestFiles(dir string) ([]string, error) {
	out, err := git("ls-tree", "--name-only", "HEAD", dir+"/")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, f := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.HasSuffix(f, "_test.go") && path.Dir(f) == dir {
			files = append(files, f)
		}
	}
	return files, nil
}
