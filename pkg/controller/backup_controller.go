/*
Copyright 2017 the Heptio Ark contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/cache"

	api "github.com/heptio/ark/pkg/apis/ark/v1"
	pkgbackup "github.com/heptio/ark/pkg/backup"
	arkv1client "github.com/heptio/ark/pkg/generated/clientset/versioned/typed/ark/v1"
	informers "github.com/heptio/ark/pkg/generated/informers/externalversions/ark/v1"
	listers "github.com/heptio/ark/pkg/generated/listers/ark/v1"
	"github.com/heptio/ark/pkg/metrics"
	"github.com/heptio/ark/pkg/persistence"
	"github.com/heptio/ark/pkg/plugin"
	"github.com/heptio/ark/pkg/util/collections"
	"github.com/heptio/ark/pkg/util/encode"
	kubeutil "github.com/heptio/ark/pkg/util/kube"
	"github.com/heptio/ark/pkg/util/logging"
)

const backupVersion = 1

type backupController struct {
	*genericController

	backupper                pkgbackup.Backupper
	lister                   listers.BackupLister
	client                   arkv1client.BackupsGetter
	clock                    clock.Clock
	backupLogLevel           logrus.Level
	newPluginManager         func(logrus.FieldLogger) plugin.Manager
	backupTracker            BackupTracker
	backupLocationLister     listers.BackupStorageLocationLister
	defaultBackupLocation    string
	snapshotLocationLister   listers.VolumeSnapshotLocationLister
	defaultSnapshotLocations map[string]*api.VolumeSnapshotLocation
	metrics                  *metrics.ServerMetrics
	newBackupStore           func(*api.BackupStorageLocation, persistence.ObjectStoreGetter, logrus.FieldLogger) (persistence.BackupStore, error)
}

func NewBackupController(
	backupInformer informers.BackupInformer,
	client arkv1client.BackupsGetter,
	backupper pkgbackup.Backupper,
	logger logrus.FieldLogger,
	backupLogLevel logrus.Level,
	newPluginManager func(logrus.FieldLogger) plugin.Manager,
	backupTracker BackupTracker,
	backupLocationInformer informers.BackupStorageLocationInformer,
	defaultBackupLocation string,
	volumeSnapshotLocationInformer informers.VolumeSnapshotLocationInformer,
	defaultSnapshotLocations map[string]*api.VolumeSnapshotLocation,
	metrics *metrics.ServerMetrics,
) Interface {
	c := &backupController{
		genericController:        newGenericController("backup", logger),
		backupper:                backupper,
		lister:                   backupInformer.Lister(),
		client:                   client,
		clock:                    &clock.RealClock{},
		backupLogLevel:           backupLogLevel,
		newPluginManager:         newPluginManager,
		backupTracker:            backupTracker,
		backupLocationLister:     backupLocationInformer.Lister(),
		defaultBackupLocation:    defaultBackupLocation,
		snapshotLocationLister:   volumeSnapshotLocationInformer.Lister(),
		defaultSnapshotLocations: defaultSnapshotLocations,
		metrics:                  metrics,

		newBackupStore: persistence.NewObjectBackupStore,
	}

	c.syncHandler = c.processBackup
	c.cacheSyncWaiters = append(c.cacheSyncWaiters,
		backupInformer.Informer().HasSynced,
		backupLocationInformer.Informer().HasSynced,
		volumeSnapshotLocationInformer.Informer().HasSynced,
	)

	backupInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				backup := obj.(*api.Backup)

				switch backup.Status.Phase {
				case "", api.BackupPhaseNew:
					// only process new backups
				default:
					c.logger.WithFields(logrus.Fields{
						"backup": kubeutil.NamespaceAndName(backup),
						"phase":  backup.Status.Phase,
					}).Debug("Backup is not new, skipping")
					return
				}

				key, err := cache.MetaNamespaceKeyFunc(backup)
				if err != nil {
					c.logger.WithError(err).WithField("backup", backup).Error("Error creating queue key, item not added to queue")
					return
				}
				c.queue.Add(key)
			},
		},
	)

	return c
}

func (c *backupController) processBackup(key string) error {
	log := c.logger.WithField("key", key)

	log.Debug("Running processBackup")
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return errors.Wrap(err, "error splitting queue key")
	}

	log.Debug("Getting backup")
	original, err := c.lister.Backups(ns).Get(name)
	if err != nil {
		return errors.Wrap(err, "error getting backup")
	}

	// Double-check we have the correct phase. In the unlikely event that multiple controller
	// instances are running, it's possible for controller A to succeed in changing the phase to
	// InProgress, while controller B's attempt to patch the phase fails. When controller B
	// reprocesses the same backup, it will either show up as New (informer hasn't seen the update
	// yet) or as InProgress. In the former case, the patch attempt will fail again, until the
	// informer sees the update. In the latter case, after the informer has seen the update to
	// InProgress, we still need this check so we can return nil to indicate we've finished processing
	// this key (even though it was a no-op).
	switch original.Status.Phase {
	case "", api.BackupPhaseNew:
		// only process new backups
	default:
		return nil
	}

	log.Debug("Cloning backup")
	request := &pkgbackup.Request{
		Backup: original.DeepCopy(), // don't modify items in the cache
	}

	// set backup version
	request.Status.Version = backupVersion

	// calculate expiration
	if request.Spec.TTL.Duration > 0 {
		request.Status.Expiration = metav1.NewTime(c.clock.Now().Add(request.Spec.TTL.Duration))
	}

	request.Status.ValidationErrors = append(request.Status.ValidationErrors, c.getLocationAndValidate(request, c.defaultBackupLocation)...)
	request.Status.ValidationErrors = append(request.Status.ValidationErrors, c.defaultAndValidateSnapshotLocations(request, c.defaultSnapshotLocations)...)

	if len(request.Status.ValidationErrors) > 0 {
		request.Status.Phase = api.BackupPhaseFailedValidation
	} else {
		request.Status.Phase = api.BackupPhaseInProgress
	}

	// update status
	updatedBackup, err := patchBackup(original, request.Backup, c.client)
	if err != nil {
		return errors.Wrapf(err, "error updating Backup status to %s", request.Status.Phase)
	}
	// store ref to just-updated item for creating patch
	original = updatedBackup
	request.Backup = updatedBackup.DeepCopy()

	if request.Status.Phase == api.BackupPhaseFailedValidation {
		return nil
	}

	c.backupTracker.Add(request.Namespace, request.Name)
	defer c.backupTracker.Delete(request.Namespace, request.Name)

	log.Debug("Running backup")
	// execution & upload of backup
	backupScheduleName := request.GetLabels()["ark-schedule"]
	c.metrics.RegisterBackupAttempt(backupScheduleName)

	if err := c.runBackup(request); err != nil {
		log.WithError(err).Error("backup failed")
		request.Status.Phase = api.BackupPhaseFailed
		c.metrics.RegisterBackupFailed(backupScheduleName)
	} else {
		c.metrics.RegisterBackupSuccess(backupScheduleName)
	}

	log.Debug("Updating backup's final status")
	if _, err := patchBackup(original, request.Backup, c.client); err != nil {
		log.WithError(err).Error("error updating backup's final status")
	}

	return nil
}

func patchBackup(original, updated *api.Backup, client arkv1client.BackupsGetter) (*api.Backup, error) {
	origBytes, err := json.Marshal(original)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original backup")
	}

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated backup")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for backup")
	}

	res, err := client.Backups(original.Namespace).Patch(original.Name, types.MergePatchType, patchBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return res, nil
}

func (c *backupController) getLocationAndValidate(itm *pkgbackup.Request, defaultBackupLocation string) []string {
	var validationErrors []string

	for _, err := range collections.ValidateIncludesExcludes(itm.Spec.IncludedResources, itm.Spec.ExcludedResources) {
		validationErrors = append(validationErrors, fmt.Sprintf("Invalid included/excluded resource lists: %v", err))
	}

	for _, err := range collections.ValidateIncludesExcludes(itm.Spec.IncludedNamespaces, itm.Spec.ExcludedNamespaces) {
		validationErrors = append(validationErrors, fmt.Sprintf("Invalid included/excluded namespace lists: %v", err))
	}

	if itm.Spec.StorageLocation == "" {
		itm.Spec.StorageLocation = defaultBackupLocation
	}

	// add the storage location as a label for easy filtering later.
	if itm.Labels == nil {
		itm.Labels = make(map[string]string)
	}
	itm.Labels[api.StorageLocationLabel] = itm.Spec.StorageLocation

	if storageLocation, err := c.backupLocationLister.BackupStorageLocations(itm.Namespace).Get(itm.Spec.StorageLocation); err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Error getting backup storage location: %v", err))
	} else {
		itm.StorageLocation = storageLocation
	}

	return validationErrors
}

// defaultAndValidateSnapshotLocations ensures:
// - each location name in Spec VolumeSnapshotLocation exists as a location
// - exactly 1 location per existing or default provider
// - a given default provider's location name is added to the Spec VolumeSnapshotLocation if it does not exist as a VSL
func (c *backupController) defaultAndValidateSnapshotLocations(itm *pkgbackup.Request, defaultLocations map[string]*api.VolumeSnapshotLocation) []string {
	errors := []string{}
	providerLocations := make(map[string]*api.VolumeSnapshotLocation)

	for _, locationName := range itm.Spec.VolumeSnapshotLocations {
		// validate each locationName exists as a VolumeSnapshotLocation
		location, err := c.snapshotLocationLister.VolumeSnapshotLocations(itm.Namespace).Get(locationName)
		if err != nil {
			errors = append(errors, fmt.Sprintf("error getting volume snapshot location named %s: %v", locationName, err))
			continue
		}

		// ensure we end up with exactly 1 location *per provider*
		if providerLocation, ok := providerLocations[location.Spec.Provider]; ok {
			// if > 1 location name per provider as in ["aws-us-east-1" | "aws-us-west-1"] (same provider, multiple names)
			if providerLocation.Name != locationName {
				errors = append(errors, fmt.Sprintf("more than one VolumeSnapshotLocation name specified for provider %s: %s; unexpected name was %s", location.Spec.Provider, locationName, providerLocation.Name))
				continue
			}
		} else {
			// keep track of all valid existing locations, per provider
			providerLocations[location.Spec.Provider] = location
		}
	}

	if len(errors) > 0 {
		return errors
	}

	for provider, defaultLocation := range defaultLocations {
		// if a location name for a given provider does not already exist, add the provider's default
		if _, ok := providerLocations[provider]; !ok {
			providerLocations[provider] = defaultLocation
		}
	}

	itm.Spec.VolumeSnapshotLocations = []string{}
	for _, loc := range providerLocations {
		itm.Spec.VolumeSnapshotLocations = append(itm.Spec.VolumeSnapshotLocations, loc.Name)
		itm.SnapshotLocations = append(itm.SnapshotLocations, loc)
	}

	return nil
}

func (c *backupController) runBackup(backup *pkgbackup.Request) error {
	log := c.logger.WithField("backup", kubeutil.NamespaceAndName(backup))
	log.Info("Starting backup")
	backup.Status.StartTimestamp.Time = c.clock.Now()

	logFile, err := ioutil.TempFile("", "")
	if err != nil {
		return errors.Wrap(err, "error creating temp file for backup log")
	}
	gzippedLogFile := gzip.NewWriter(logFile)
	// Assuming we successfully uploaded the log file, this will have already been closed below. It is safe to call
	// close multiple times. If we get an error closing this, there's not really anything we can do about it.
	defer gzippedLogFile.Close()
	defer closeAndRemoveFile(logFile, c.logger)

	// Log the backup to both a backup log file and to stdout. This will help see what happened if the upload of the
	// backup log failed for whatever reason.
	logger := logging.DefaultLogger(c.backupLogLevel)
	logger.Out = io.MultiWriter(os.Stdout, gzippedLogFile)
	log = logger.WithField("backup", kubeutil.NamespaceAndName(backup))

	log.Info("Starting backup")

	backupFile, err := ioutil.TempFile("", "")
	if err != nil {
		return errors.Wrap(err, "error creating temp file for backup")
	}
	defer closeAndRemoveFile(backupFile, log)

	pluginManager := c.newPluginManager(log)
	defer pluginManager.CleanupClients()

	actions, err := pluginManager.GetBackupItemActions()
	if err != nil {
		return err
	}

	backupStore, err := c.newBackupStore(backup.StorageLocation, pluginManager, log)
	if err != nil {
		return err
	}

	var errs []error

	// Do the actual backup
	if err := c.backupper.Backup(log, backup, backupFile, actions, pluginManager); err != nil {
		errs = append(errs, err)

		backup.Status.Phase = api.BackupPhaseFailed
	} else {
		backup.Status.Phase = api.BackupPhaseCompleted
	}

	// Mark completion timestamp before serializing and uploading.
	// Otherwise, the JSON file in object storage has a CompletionTimestamp of 'null'.
	backup.Status.CompletionTimestamp.Time = c.clock.Now()

	var backupJSONToUpload, backupFileToUpload io.Reader
	backupJSON := new(bytes.Buffer)
	if err := encode.EncodeTo(backup, "json", backupJSON); err != nil {
		errs = append(errs, errors.Wrap(err, "error encoding backup"))
	} else {
		// Only upload the json and backup tarball if encoding to json succeeded.
		backupJSONToUpload = backupJSON
		backupFileToUpload = backupFile
	}

	var backupSizeBytes int64
	if backupFileStat, err := backupFile.Stat(); err != nil {
		errs = append(errs, errors.Wrap(err, "error getting file info"))
	} else {
		backupSizeBytes = backupFileStat.Size()
	}

	if err := gzippedLogFile.Close(); err != nil {
		c.logger.WithError(err).Error("error closing gzippedLogFile")
	}

	if err := backupStore.PutBackup(backup.Name, backupJSONToUpload, backupFileToUpload, logFile); err != nil {
		errs = append(errs, err)
	}

	backupScheduleName := backup.GetLabels()["ark-schedule"]
	c.metrics.SetBackupTarballSizeBytesGauge(backupScheduleName, backupSizeBytes)

	backupDuration := backup.Status.CompletionTimestamp.Time.Sub(backup.Status.StartTimestamp.Time)
	backupDurationSeconds := float64(backupDuration / time.Second)
	c.metrics.RegisterBackupDuration(backupScheduleName, backupDurationSeconds)

	log.Info("Backup completed")

	return kerrors.NewAggregate(errs)
}

func closeAndRemoveFile(file *os.File, log logrus.FieldLogger) {
	if err := file.Close(); err != nil {
		log.WithError(err).WithField("file", file.Name()).Error("error closing file")
	}
	if err := os.Remove(file.Name()); err != nil {
		log.WithError(err).WithField("file", file.Name()).Error("error removing file")
	}
}
