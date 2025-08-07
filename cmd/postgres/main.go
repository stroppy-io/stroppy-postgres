package main

import (
	"github.com/stroppy-io/stroppy-core/pkg/plugins/driver"
)

func main() {
	driver.ServePlugin(NewDriver())
}
