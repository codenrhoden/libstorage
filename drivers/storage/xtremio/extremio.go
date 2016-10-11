package xtremio

import (
	"github.com/akutz/gofig"
)

const (
	// Name is the provider's name.
	Name = "xtremio"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofig.NewRegistration("XtremIO")
	r.Key(gofig.String, "", "", "", "xtremio.endpoint")
	r.Key(gofig.Bool, "", false, "", "xtremio.insecure")
	r.Key(gofig.String, "", "", "", "xtremio.userName")
	r.Key(gofig.String, "", "", "", "xtremio.password")
	r.Key(gofig.Bool, "", false, "", "xtremio.deviceMapper")
	r.Key(gofig.Bool, "", false, "", "xtremio.multipath")
	//r.Key(gofig.Bool, "", false, "", "xtremio.remoteManagement")
	gofig.Register(r)
}
