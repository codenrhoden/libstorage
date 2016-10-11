package executor

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/akutz/gofig"
	"github.com/akutz/goof"

	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/xtremio"
)

const (
	initiatorNameFile = "/etc/iscsi/initiatorname.iscsi"
	diskByIDPath      = "/dev/disk/by-id"
)

type driver struct {
	config gofig.Config
	iqn    string
}

func init() {
	registry.RegisterStorageExecutor(xtremio.Name, newdriver)
}

func newdriver() types.StorageExecutor {
	return &driver{}
}

func (d *driver) Init(context types.Context, config gofig.Config) error {
	d.config = config

	var err error
	d.iqn, err = getIQN()
	if err != nil {
		return goof.WithError("Error getting IQN", err)
	}
	return nil
}

func (d *driver) Name() string {
	return xtremio.Name
}

// NextDevice returns the next available device.
func (d *driver) NextDevice(
	ctx types.Context,
	opts types.Store) (string, error) {
	return "", types.ErrNotImplemented
}

// LocalDevices returns a map of the system's local devices.
func (d *driver) LocalDevices(
	ctx types.Context,
	opts *types.LocalDevicesOpts) (*types.LocalDevices, error) {

	ld := &types.LocalDevices{Driver: d.Name()}
	devMap := map[string]string{}

	files, err := ioutil.ReadDir(diskByIDPath)
	if err != nil {
		return nil, goof.WithError("error reading"+diskByIDPath, err)
	}

	var match1 *regexp.Regexp
	var match2 string

	if d.deviceMapper() || d.multipath() {
		match1, _ = regexp.Compile(`^dm-name-\w*$`)
		match2 = `^dm-name-\d+`
	} else {
		match1, _ = regexp.Compile(`^wwn-0x\w*$`)
		match2 = `^wwn-0x`
	}

	for _, f := range files {
		if match1.MatchString(f.Name()) {
			naaName := strings.Replace(f.Name(), match2, "", 1)
			naaName = naaName[len(naaName)-16:]
			devPath, _ := filepath.EvalSymlinks(fmt.Sprintf("%s/%s", diskByIDPath, f.Name()))
			devMap[naaName] = devPath
		}
	}

	if len(devMap) > 0 {
		ld.DeviceMap = devMap
	}

	return ld, nil
}

// InstanceID returns the local system's InstanceID.
func (d *driver) InstanceID(
	ctx types.Context,
	opts types.Store) (*types.InstanceID, error) {

	return GetInstanceID()
}

// GetInstanceID returns the instance ID object
func GetInstanceID() (*types.InstanceID, error) {
	iqn, err := getIQN()
	if err != nil {
		return nil, goof.WithError("Unable to get IQN", err)
	}
	return &types.InstanceID{
		Driver: xtremio.Name,
		ID:     iqn,
	}, nil
}

func getIQN() (string, error) {
	data, err := ioutil.ReadFile(initiatorNameFile)
	if err != nil {
		return "", goof.WithFieldE(
			"file",
			initiatorNameFile,
			"Unable to read initiator name from file",
			err,
		)
	}

	result := string(data)
	lines := strings.Split(result, "\n")

	for _, line := range lines {
		split := strings.Split(line, "=")
		if split[0] == "InitiatorName" {
			return split[1], nil
		}
	}
	return "", goof.New("IQN not found")
}

func (d *driver) deviceMapper() bool {
	return d.config.GetBool("xtremio.deviceMapper")
}

func (d *driver) multipath() bool {
	return d.config.GetBool("xtremio.multipath")
}
