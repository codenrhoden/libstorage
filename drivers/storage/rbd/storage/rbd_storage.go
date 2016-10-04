// +build linux

package storage

import (
	"regexp"

	log "github.com/Sirupsen/logrus"

	gofig "github.com/akutz/gofig/types"
	"github.com/akutz/goof"

	"github.com/codedellemc/libstorage/api/context"
	"github.com/codedellemc/libstorage/api/registry"
	"github.com/codedellemc/libstorage/api/types"
	"github.com/codedellemc/libstorage/drivers/storage/rbd"
	"github.com/codedellemc/libstorage/drivers/storage/rbd/utils"

	"github.com/ceph/go-ceph/rados"
	gorbd "github.com/ceph/go-ceph/rbd"
)

const (
	RBDDefaultOrder = 22
	BytesPerGiB     = uint64(1024 * 1024 * 1024)
)

type driver struct {
	config gofig.Config
}

func init() {
	registry.RegisterStorageDriver(rbd.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

func (d *driver) Name() string {
	return rbd.Name
}

// Init initializes the driver.
func (d *driver) Init(context types.Context, config gofig.Config) error {
	d.config = config
	log.Info("storage driver initialized")
	return nil
}

func (d *driver) Login(ctx types.Context) (interface{}, error) {

	/*
		var (
			cluster  = d.cluster()
			monitors = d.monitors()
			user = d.cephxUser()
			key      = d.cephxKey()
		)

		if monitors == "" {
			return nil, goof.New("No monitors specified for Ceph cluster")
		}

		fields := map[string]interface{}{
			//"cluster":  cluster,
			//"monitors": monitors,
			"user": user,
		}

		if d.cephx() {
			if key == "" {
				fields["cephxKey"] = ""
			} else {
				fields["cephxKey"] = "******"
			}
		}
	*/
	fields := map[string]interface{}{}

	log.WithFields(fields).Debug("beginning RADOS connection attempt")

	conn, err := rados.NewConn()
	if err != nil {
		return nil, goof.WithFieldsE(
			fields, "failed to get RADOS conn", err)
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		return nil, goof.WithFieldsE(
			fields, "Could not read Ceph config", err)
	}

	/*
		if err = conn.SetConfigOption("mon_host", monitors); err != nil {
			return nil, goof.WithFieldsE(fields, "failed to set config option 'mon_host'", err)
		}
		if d.cephx() {
			if err = conn.SetConfigOption("id", user); err != nil {
				return nil, goof.WithFieldsE(fields, "failed to set config option 'id'", err)
			}
				if err = conn.SetConfigOption("key", key); err != nil {
					return nil, goof.WithFieldsE(fields, "failed to set config option 'key'", err)
				}
		}
	*/

	if err = conn.Connect(); err != nil {
		return nil, goof.WithFieldsE(
			fields, "Unable to create RADOS connection", err)
	}

	log.WithFields(fields).Info("RADOS connection created")

	return conn, nil
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
		InstanceID: iid,
	}, nil
}

func (d *driver) Volumes(
	ctx types.Context,
	opts *types.VolumesOpts) ([]*types.Volume, error) {

	// Get all Volumes in all pools
	pools, err := d.getRadosPools(ctx)
	if err != nil {
		return nil, err
	}

	volumes := make([]*types.Volume, 0)

	for _, pool := range pools {
		ioCtx, err := d.getIOContext(ctx, pool)
		if err != nil {
			return nil, err
		}
		defer ioCtx.Destroy()

		imageNames, err := d.getImageNames(ioCtx)
		if err != nil {
			return nil, err
		}

		// TODO: parallelize this for performance
		// TODO: probably best done in go-ceph itself
		imageInfos := make([]*RBDImageAndInfo, len(imageNames))
		for i, name := range imageNames {
			info, err := d.getImageInfo(ioCtx, name)
			if err != nil {
				return nil, err
			}

			imageInfo := &RBDImageAndInfo{
				info: info,
				pool: pool,
				name: name,
			}
			imageInfos[i] = imageInfo
		}

		lsVols, err := d.toTypeVolumes(
			ctx, imageInfos, opts.Attachments)
		if err != nil {
			/* Should we try to continue instead? */
			return nil, err
		}
		volumes = append(volumes, lsVols...)
	}

	return volumes, nil
}

func (d *driver) VolumeInspect(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeInspectOpts) (*types.Volume, error) {

	pool, image, err := d.parseVolumeID(&volumeID)
	if err != nil {
		return nil, err
	}

	ioCtx, err := d.getIOContext(ctx, pool)
	if err != nil {
		return nil, err
	}
	defer ioCtx.Destroy()

	info, err := d.getImageInfo(ioCtx, image)
	if err != nil {
		return nil, err
	}

	imageInfos := []*RBDImageAndInfo{
		&RBDImageAndInfo{
			info: info,
			pool: pool,
			name: image,
		},
	}

	vols, err := d.toTypeVolumes(ctx, imageInfos, opts.Attachments)
	if err != nil {
		return nil, err
	}

	return vols[0], nil
}

func (d *driver) VolumeCreate(ctx types.Context, volumeName string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {

	fields := map[string]interface{}{
		"driverName": d.Name(),
		"volumeName": volumeName,
		"opts":       opts,
	}

	log.WithFields(fields).Debug("creating volume")

	pool, imageName, err := d.parseVolumeID(&volumeName)
	if err != nil {
		return nil, err
	}

	//TODO: make sure volume does *not* exist already

	ioCtx, err := d.getIOContext(ctx, pool)
	if err != nil {
		return nil, err
	}
	defer ioCtx.Destroy()

	//TODO: config options for order and features?

	_, err = gorbd.Create(
		ioCtx,
		*imageName,
		uint64(*opts.Size)*BytesPerGiB,
		RBDDefaultOrder,
		RBDFeatureLayering,
	)
	if err != nil {
		return nil, goof.WithError("Failed to create new volume", err)
	}

	volumeID := utils.GetVolumeID(pool, imageName)
	return d.VolumeInspect(ctx, *volumeID,
		&types.VolumeInspectOpts{
			Attachments: types.VolAttNone,
		},
	)
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

	fields := map[string]interface{}{
		"driverName": d.Name(),
		"volumeID":   volumeID,
	}

	log.WithFields(fields).Debug("deleting volume")

	pool, imageName, err := d.parseVolumeID(&volumeID)
	if err != nil {
		return goof.WithError("Unable to set image name", err)
	}

	ioCtx, err := d.getIOContext(ctx, pool)
	if err != nil {
		return goof.WithError("Unable to open RADOS IO Context", err)
	}
	defer ioCtx.Destroy()

	image := gorbd.GetImage(ioCtx, *imageName)

	err = image.Remove()
	if err != nil {
		return goof.WithError("Error while deleting RBD image", err)
	}
	log.WithFields(fields).Debug("removed volume")

	return nil
}

func (d *driver) VolumeAttach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeAttachOpts) (*types.Volume, string, error) {

	fields := map[string]interface{}{
		"driverName": d.Name(),
		"volumeID":   volumeID,
	}

	log.WithFields(fields).Debug("attaching volume")

	pool, imageName, err := d.parseVolumeID(&volumeID)
	if err != nil {
		return nil, "", goof.WithError("Unable to set image name", err)
	}

	_, err = utils.DoRBDMap(pool, imageName)
	if err != nil {
		return nil, "", err
	}

	vol, err := d.VolumeInspect(ctx, volumeID,
		&types.VolumeInspectOpts{
			Attachments: types.VolAttReqTrue,
		},
	)
	if err != nil {
		return nil, "", err
	}

	return vol, volumeID, nil
}

func (d *driver) VolumeDetach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeDetachOpts) (*types.Volume, error) {

	fields := map[string]interface{}{
		"driverName": d.Name(),
		"volumeID":   volumeID,
	}

	log.WithFields(fields).Debug("detaching volume")

	// Can't rely on local devices header, so get local attachments
	localAttachMap, err := utils.GetMappedRBDs()
	if err != nil {
		return nil, err
	}

	dev, found := localAttachMap[volumeID]
	if !found {
		return nil, goof.New("Volume not attached")
	}

	err = utils.DoRBDUnmap(&dev)
	if err != nil {
		return nil, goof.WithError("Unable to detach volume", err)
	}

	return d.VolumeInspect(
		ctx, volumeID, &types.VolumeInspectOpts{
			Attachments: types.VolAttReqTrue,
		},
	)
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

func mustSession(ctx types.Context) *rados.Conn {
	return context.MustSession(ctx).(*rados.Conn)
}

/*
func (d *driver) cluster() string {
	return d.config.GetString("rbd.cluster")
}*/

func (d *driver) defaultPool() string {
	return d.config.GetString("rbd.defaultPool")
}

/*
func (d *driver) monitors() string {
	return d.config.GetString("rbd.monitors")
}

func (d *driver) cephx() bool {
	return d.config.GetBool("rbd.cephx")
}

func (d *driver) cephxUser() string {
	return d.config.GetString("rbd.cephxUser")
}

func (d *driver) cephxKey() string {
	return d.config.GetString("rbd.cephxKey")
}
*/

func convStrArrayToPtr(strArr []string) []*string {
	ptrArr := make([]*string, len(strArr))
	for i, _ := range strArr {
		ptrArr[i] = &strArr[i]
	}
	return ptrArr
}

// the resulting ioctx should be Destroy()'d by the caller
func (d *driver) getIOContext(
	ctx types.Context,
	pool *string) (*rados.IOContext, error) {

	ioctx, err := mustSession(ctx).OpenIOContext(*pool)
	if err != nil {
		return nil, err
	}

	return ioctx, nil
}

func (d *driver) getRadosPools(ctx types.Context) ([]*string, error) {
	poolsStr, err := mustSession(ctx).ListPools()
	if err != nil {
		return nil, goof.WithError("Unable to get Pools", err)
	}
	pools := convStrArrayToPtr(poolsStr)
	return pools, nil
}

func (d *driver) getImageNames(ioCtx *rados.IOContext) ([]*string, error) {

	imageNamesStr, err := gorbd.GetImageNames(ioCtx)
	if err != nil {
		return nil, err
	}
	names := convStrArrayToPtr(imageNamesStr)
	return names, nil
}

func (d *driver) getImageInfo(
	ioCtx *rados.IOContext,
	imageName *string) (*gorbd.ImageInfo, error) {

	image := gorbd.GetImage(ioCtx, *imageName)
	err := image.Open(true)
	if err != nil {
		return nil, goof.WithError("unable to open RBD image", err)
	}
	defer image.Close()

	imageInfo, err := image.Stat()
	if err != nil {
		return nil, goof.WithError("unable to stat RBD image", err)
	}

	return imageInfo, nil
}

func (d *driver) toTypeVolumes(
	ctx types.Context,
	imageInfos []*RBDImageAndInfo,
	getAttachments types.VolumeAttachmentsTypes) ([]*types.Volume, error) {

	lsVolumes := make([]*types.Volume, len(imageInfos))

	var localAttachMap map[string]string

	// Even though this will be the same as LocalDevices header, we can't
	// rely on that being present unless getAttachments.Devices is set
	if getAttachments.Requested() {
		var err error
		localAttachMap, err = utils.GetMappedRBDs()
		if err != nil {
			return nil, err
		}
	}

	for i, imageInfo := range imageInfos {
		rbdId := utils.GetVolumeID(imageInfo.pool, imageInfo.name)
		lsVolume := &types.Volume{
			Name: *imageInfo.name,
			ID:   *rbdId,
			Type: *imageInfo.pool,
			Size: int64(imageInfo.info.Size / BytesPerGiB),
		}

		if getAttachments.Requested() && localAttachMap != nil {
			// Set volumeAttachmentState to Unknown, because this
			// driver (currently) has no way of knowing if an image
			// is attached anywhere else but to the caller
			lsVolume.AttachmentState = types.VolumeAttachmentStateUnknown
			var attachments []*types.VolumeAttachment
			if _, found := localAttachMap[*rbdId]; found {
				lsVolume.AttachmentState = types.VolumeAttached
				attachment := &types.VolumeAttachment{
					VolumeID:   *rbdId,
					InstanceID: context.MustInstanceID(ctx),
				}
				if getAttachments.Devices() {
					ld, ok := context.LocalDevices(ctx)
					if ok {
						attachment.DeviceName = ld.DeviceMap[*rbdId]
					} else {
						log.Warnf("Unable to get local device map for volume %s", *rbdId)
					}
				}
				attachments = append(attachments, attachment)
				lsVolume.Attachments = attachments
			} else {
				//Check if RBD has watchers to infer attachment
				//to a different host
				b, err := utils.RBDHasWatchers(imageInfo.pool,
					imageInfo.name)
				if err == nil {
					if b {
						lsVolume.AttachmentState = types.VolumeUnavailable
					} else {
						lsVolume.AttachmentState = types.VolumeAvailable
					}
				}
			}
		}
		lsVolumes[i] = lsVolume
	}

	return lsVolumes, nil
}

func (d *driver) parseVolumeID(name *string) (*string, *string, error) {

	// Look for <pool>.<name>
	re, _ := regexp.Compile(`^(\w+)\.(\w+)$`)
	res := re.FindStringSubmatch(*name)
	if len(res) == 3 {
		// Name includes pool already
		return &res[1], &res[2], nil
	}

	// make sure <name> is valid
	re, _ = regexp.Compile(`^\w+$`)
	if !re.MatchString(*name) {
		return nil, nil, goof.New("Invalid VolumeID")
	}

	pool := d.defaultPool()
	return &pool, name, nil
}

// TODO: move this go-ceph
const (
	RBDFeatureLayering uint64 = 1 << iota
	RBDFeatureStripingV2
	RBDFeatureExclusiveLock
	RBDFeatureObjectMap
	RBDFeatureFaseDiff
	RBDFeatureDeepFlatten
	RBDFeatureJournaling
	RBDFeatureDataPool
)

type RBDImageAndInfo struct {
	info *gorbd.ImageInfo
	pool *string
	name *string
}
