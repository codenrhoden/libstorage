package rbd

import (
	gofigCore "github.com/akutz/gofig"
	gofig "github.com/akutz/gofig/types"
)

const (
	// Name is the name of the storage driver
	Name = "rbd"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofigCore.NewRegistration("RBD")
	//r.Key(gofig.String, "", "ceph", "", "rbd.cluster")
	r.Key(gofig.String, "", "rbd", "", "rbd.defaultPool")
	//r.Key(gofig.String, "", "", "", "rbd.monitors")
	//r.Key(gofig.Bool, "", true, "", "rbd.cephx")
	//r.Key(gofig.String, "", "admin", "", "rbd.cephxUser")
	//r.Key(gofig.String, "", "", "", "rbd.cephxKey")
	gofigCore.Register(r)
}
