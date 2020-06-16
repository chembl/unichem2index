// +build mage

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const (
	packageName = "github.com/chembl/unichem2index"
)

var ldflags = "-X $PACKAGE.commitHash=$COMMIT_HASH -X $PACKAGE.buildDate=$BUILD_DATE"

// Runs go install for gnorm.  This generates the embedded docs and the version
// info into the binary.
func Build() error {

	// ldf, err := flags()
	// if err != nil {
	// 	return err
	// }

	log.Print("running go build")
	// use -tags make so we can have different behavior for when we know we've built with mage.
	// return sh.Run("go", "build", "-tags", "make", "--ldflags="+ldf, "gnorm.org/gnorm")
	return sh.RunWith(flagEnv(), "go", "build", "-o", "build/unichem2index", "-ldflags", ldflags, packageName)
}

func flagEnv() map[string]string {
	hash, _ := sh.Output("git", "rev-parse", "--short", "HEAD")
	return map[string]string{
		"PACKAGE":     packageName,
		"COMMIT_HASH": hash,
		"BUILD_DATE":  time.Now().Format("2006-01-02T15:04:05Z0700"),
	}
}

func flags() (string, error) {
	timestamp := time.Now().Format(time.RFC3339)
	hash, err := output("git", "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	version, err := gitTag()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`-X "github.com/chembl/unichem2index.timestamp=%s" -X "github.com/chembl/unichem2index.commitHash=%s" -X "github.com/chembl/unichem2index.version=%s"`, timestamp, hash, version), nil
}

func gitTag() (string, error) {
	s, err := output("git", "describe", "--tags")
	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(s, "\n"), nil
}

func output(cmd string, args ...string) (string, error) {
	c := exec.Command(cmd, args...)
	c.Env = os.Environ()
	c.Stderr = os.Stderr
	b, err := c.Output()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Default target to run when none is specified
// If not set, running mage will list available targets
// var Default = Build

// A build step that requires additional params, or platform specific steps for example
// func Build() error {
// 	mg.Deps(InstallDeps)
// 	fmt.Println("Building...")
// 	cmd := exec.Command("go", "build", "-o", "MyApp", ".")
// 	return cmd.Run()
// }

// A custom install step if you need your bin someplace other than go/bin
func Install() error {
	mg.Deps(Build)
	fmt.Println("Installing...")
	return os.Rename("./MyApp", "/usr/bin/MyApp")
}

// Manage your deps, or running package managers.
func InstallDeps() error {
	fmt.Println("Installing Deps...")
	cmd := exec.Command("go", "get", "github.com/stretchr/piglatin")
	return cmd.Run()
}

// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("MyApp")
}
