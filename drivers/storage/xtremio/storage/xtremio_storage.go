package storage

import (
	"crypto/md5"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/akutz/goof"

	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/xtremio"

	xtio "github.com/emccode/goxtremio"
)

const (
	cacheKeyC = "cacheKey"
)

var (
	sessions         = map[string]*xtio.Client{}
	sessionsL        = &sync.Mutex{}
	errNoVolReturned = goof.New("no volume returned")
)

type driver struct {
	config gofig.Config
}

func init() {
	registry.RegisterStorageDriver(xtremio.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

func (d *driver) Name() string {
	return xtremio.Name
}

// Init initializes the driver.
func (d *driver) Init(context types.Context, config gofig.Config) error {
	d.config = config
	log.Info("storage driver initialized")
	return nil
}

func writeHkey(h hash.Hash, ps *string) {
	if ps == nil {
		return
	}
	h.Write([]byte(*ps))
}

func (d *driver) Login(ctx types.Context) (interface{}, error) {
	sessionsL.Lock()
	defer sessionsL.Unlock()

	var (
		endpoint = d.endpoint()
		insecure = d.insecure()
		user     = d.userName()

		ckey string
		hkey = md5.New()
	)

	if endpoint == "" {
		return nil, goof.New("No endpoint specified for XtremIO cluster")
	}

	if user == "" {
		return nil, goof.New("No userName specified for xtremio cluster")
	}

	strSecure := strconv.FormatBool(insecure)
	writeHkey(hkey, &endpoint)
	writeHkey(hkey, &strSecure)
	writeHkey(hkey, &user)
	ckey = fmt.Sprintf("%x", hkey.Sum(nil))

	// if the API client is cached then return it
	if svc, ok := sessions[ckey]; ok {
		log.WithField(cacheKeyC, ckey).Debug("using cached xtremio service")
		return svc, nil
	}

	fields := map[string]interface{}{
		"endpoint": endpoint,
		"insecure": insecure,
		"userName": user,
		cacheKeyC:  ckey,
	}

	if d.password() == "" {
		fields["password"] = ""
	} else {
		fields["password"] = "******"
	}

	log.WithFields(fields).Debug("beginning xtremio connection attempt")

	apiClient, err := xtio.NewClientWithArgs(
		endpoint,
		insecure,
		user,
		d.password())
	if err != nil {
		return nil, goof.WithFieldsE(fields,
			"error creating xtremio client", err)
	}

	sessions[ckey] = apiClient
	log.WithFields(fields).Info("xtremio connection created")

	return apiClient, nil
}

func mustSession(ctx types.Context) *xtio.Client {
	return context.MustSession(ctx).(*xtio.Client)
}

func (d *driver) Type(ctx types.Context) (types.StorageType, error) {
	return types.Block, nil
}

func (d *driver) NextDeviceInfo(
	ctx types.Context) (*types.NextDeviceInfo, error) {
	return nil, nil
}

func (d *driver) InstanceInspect(
	ctx types.Context,
	opts types.Store) (*types.Instance, error) {

	iid := context.MustInstanceID(ctx)
	return &types.Instance{
		Name:         iid.ID,
		InstanceID:   iid,
		ProviderName: iid.Driver,
	}, nil
}

func (d *driver) Volumes(
	ctx types.Context,
	opts *types.VolumesOpts) ([]*types.Volume, error) {

	xtioVolumes, err := d.getXtioVolumes(ctx)
	if err != nil {
		goof.WithError("Unable to retrieve list of volumes", err)
	}

	if len(xtioVolumes) == 0 {
		return nil, errNoVolReturned
	}
	// Convert retrieved volumes to libStorage types.Volume
	vols, convErr := d.toTypesVolume(ctx, &xtioVolumes, opts.Attachments)
	if convErr != nil {
		return nil, goof.WithError("error converting to types.Volume", convErr)
	}
	return vols, nil
}

func (d *driver) VolumeInspect(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeInspectOpts) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) VolumeCreate(ctx types.Context, volumeName string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) VolumeCreateFromSnapshot(
	ctx types.Context,
	snapshotID, volumeName string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) VolumeCopy(
	ctx types.Context,
	volumeID, volumeName string,
	opts types.Store) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) VolumeSnapshot(
	ctx types.Context,
	volumeID, snapshotName string,
	opts types.Store) (*types.Snapshot, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) VolumeRemove(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {
	return types.ErrNotImplemented
}

func (d *driver) VolumeAttach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeAttachOpts) (*types.Volume, string, error) {
	return nil, "", types.ErrNotImplemented
}

func (d *driver) VolumeDetach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeDetachOpts) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) VolumeDetachAll(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {
	return types.ErrNotImplemented
}

func (d *driver) Snapshots(
	ctx types.Context,
	opts types.Store) ([]*types.Snapshot, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) SnapshotInspect(
	ctx types.Context,
	snapshotID string,
	opts types.Store) (*types.Snapshot, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) SnapshotCopy(
	ctx types.Context,
	snapshotID, snapshotName, destinationID string,
	opts types.Store) (*types.Snapshot, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) SnapshotRemove(
	ctx types.Context,
	snapshotID string,
	opts types.Store) error {
	return types.ErrNotImplemented
}

func (d *driver) getXtioVolumes(
	ctx types.Context) ([]xtio.Volume, error) {

	refs, err := mustSession(ctx).GetVolumes()
	if err != nil {
		goof.WithError("Unable to retrieve list of volumes", err)
	}

	var volumes []xtio.Volume
	for _, volume := range refs {
		hrefFields := strings.Split(volume.Href, "/")
		index, _ := strconv.Atoi(hrefFields[len(hrefFields)-1])
		volumes = append(volumes,
			xtio.VolumeCtorNameIndex(volume.Name, index))
	}

	return volumes, nil
}

func (d *driver) toTypesVolume(
	ctx types.Context,
	xtioVols *[]xtio.Volume,
	attachments bool) ([]*types.Volume, error) {

	var volumes []*types.Volume

	for _, volume := range *xtioVols {
		lsVolume := &types.Volume{
			Name: volume.Name,
			ID:   strconv.Itoa(volume.Index),
		}
		volumes = append(volumes, lsVolume)
	}
	return volumes, nil
}

func (d *driver) endpoint() string {
	return d.config.GetString("xtremio.endpoint")
}

func (d *driver) insecure() bool {
	return d.config.GetBool("xtremio.insecure")
}

func (d *driver) userName() string {
	return d.config.GetString("xtremio.userName")
}

func (d *driver) password() string {
	return d.config.GetString("xtremio.password")
}

func (d *driver) deviceMapper() bool {
	return d.config.GetBool("xtremio.deviceMapper")
}

func (d *driver) multipath() bool {
	return d.config.GetBool("xtremio.multipath")
}

/*
func (d *driver) remoteManagement() bool {
	return d.r.Config.GetBool("xtremio.remoteManagement")
}
*/
