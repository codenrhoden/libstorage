package utils

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/akutz/goof"
)

type rbdMappedEntry struct {
	Device string `json:"device"`
	Name   string `json:"name"`
	Pool   string `json:"pool"`
	Snap   string `json:"snap"`
}

//GetVolumeID returns an RBD Volume formatted as <pool>.<imageName>
func GetVolumeID(pool, image *string) *string {

	volumeID := fmt.Sprintf("%s.%s", *pool, *image)
	return &volumeID
}

//GetMappedRBDs returns a map of RBDs currently mapped to the *local* host
func GetMappedRBDs() (map[string]string, error) {

	out, err := exec.Command(
		"rbd", "showmapped", "--format", "json").Output()
	if err != nil {
		return nil, goof.WithError("Unable to get RBD map", err)
	}

	devMap := map[string]string{}
	rbdMap := map[string]*rbdMappedEntry{}

	err = json.Unmarshal(out, &rbdMap)
	if err != nil {
		return nil, goof.WithError(
			"Unable to parse rbd showmapped", err)
	}

	for _, mapped := range rbdMap {
		volumeID := GetVolumeID(&mapped.Pool, &mapped.Name)
		devMap[*volumeID] = mapped.Device
	}

	return devMap, nil
}

//DoRBDMap attaches the given RBD image to the *local* host
func DoRBDMap(pool, image *string) (string, error) {

	out, err := exec.Command("rbd", "map", "--pool", *pool, *image).Output()
	if err != nil {
		return "", goof.WithError("Unable to map RBD", err)
	}

	return strings.TrimSpace(string(out)), nil
}

//DoRBDUnmap detaches the given RBD device from the *local* host
func DoRBDUnmap(device *string) error {

	err := exec.Command("rbd", "unmap", *device).Run()
	if err != nil {
		return goof.WithError("Unable to unmap RBD", err)
	}

	return nil
}

//GetRBDStatus returns a map of RBD status info
func GetRBDStatus(pool, image *string) (map[string]interface{}, error) {

	out, err := exec.Command(
		"rbd", "status", "--pool", *pool, *image, "--format", "json",
	).Output()
	if err != nil {
		return nil, goof.WithError("Unable to get RBD map", err)
	}

	watcherMap := map[string]interface{}{}

	err = json.Unmarshal(out, &watcherMap)
	if err != nil {
		return nil, goof.WithError(
			"Unable to parse rbd status", err)
	}

	return watcherMap, nil
}

//RBDHasWatchers returns true if RBD image has watchers
func RBDHasWatchers(pool *string, image *string) (bool, error) {

	m, err := GetRBDStatus(pool, image)
	if err != nil {
		return false, err
	}

	return len(m["watchers"].(map[string]interface{})) > 0, nil
}
