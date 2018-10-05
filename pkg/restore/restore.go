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

package restore

import (
	go_context "context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubeerrs "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/archive"
	"github.com/heptio/ark/pkg/client"
	"github.com/heptio/ark/pkg/cloudprovider"
	"github.com/heptio/ark/pkg/discovery"
	arkv1client "github.com/heptio/ark/pkg/generated/clientset/versioned/typed/ark/v1"
	"github.com/heptio/ark/pkg/kuberesource"
	"github.com/heptio/ark/pkg/restic"
	"github.com/heptio/ark/pkg/util/boolptr"
	"github.com/heptio/ark/pkg/util/collections"
	"github.com/heptio/ark/pkg/util/filesystem"
	"github.com/heptio/ark/pkg/util/kube"
	arksync "github.com/heptio/ark/pkg/util/sync"
)

// Restorer knows how to restore a backup.
type Restorer interface {
	// Restore restores the backup data from backupReader, returning warnings and errors.
	Restore(log logrus.FieldLogger, restore *api.Restore, backup *api.Backup, backupReader io.Reader, actions []ItemAction) (api.RestoreResult, api.RestoreResult)
}

type gvString string
type kindString string

// kubernetesRestorer implements Restorer for restoring into a Kubernetes cluster.
type kubernetesRestorer struct {
	discoveryHelper       discovery.Helper
	dynamicFactory        client.DynamicFactory
	blockStore            cloudprovider.BlockStore
	backupClient          arkv1client.BackupsGetter
	namespaceClient       corev1.NamespaceInterface
	resticRestorerFactory restic.RestorerFactory
	resticTimeout         time.Duration
	resourcePriorities    []string
	fileSystem            filesystem.Interface
	logger                logrus.FieldLogger
}

// prioritizeResources returns an ordered, fully-resolved list of resources to restore based on
// the provided discovery helper, resource priorities, and included/excluded resources.
func prioritizeResources(helper discovery.Helper, priorities []string, includedResources *collections.IncludesExcludes, logger logrus.FieldLogger) ([]schema.GroupResource, error) {
	var ret []schema.GroupResource

	// set keeps track of resolved GroupResource names
	set := sets.NewString()

	// start by resolving priorities into GroupResources and adding them to ret
	for _, r := range priorities {
		gvr, _, err := helper.ResourceFor(schema.ParseGroupResource(r).WithVersion(""))
		if err != nil {
			return nil, err
		}
		gr := gvr.GroupResource()

		if !includedResources.ShouldInclude(gr.String()) {
			logger.WithField("groupResource", gr).Info("Not including resource")
			continue
		}
		ret = append(ret, gr)
		set.Insert(gr.String())
	}

	// go through everything we got from discovery and add anything not in "set" to byName
	var byName []schema.GroupResource
	for _, resourceGroup := range helper.Resources() {
		// will be something like storage.k8s.io/v1
		groupVersion, err := schema.ParseGroupVersion(resourceGroup.GroupVersion)
		if err != nil {
			return nil, err
		}

		for _, resource := range resourceGroup.APIResources {
			gr := groupVersion.WithResource(resource.Name).GroupResource()

			if !includedResources.ShouldInclude(gr.String()) {
				logger.WithField("groupResource", gr.String()).Info("Not including resource")
				continue
			}

			if !set.Has(gr.String()) {
				byName = append(byName, gr)
			}
		}
	}

	// sort byName by name
	sort.Slice(byName, func(i, j int) bool {
		return byName[i].String() < byName[j].String()
	})

	// combine prioritized with by-name
	ret = append(ret, byName...)

	return ret, nil
}

// NewKubernetesRestorer creates a new kubernetesRestorer.
func NewKubernetesRestorer(
	discoveryHelper discovery.Helper,
	dynamicFactory client.DynamicFactory,
	blockStore cloudprovider.BlockStore,
	resourcePriorities []string,
	backupClient arkv1client.BackupsGetter,
	namespaceClient corev1.NamespaceInterface,
	resticRestorerFactory restic.RestorerFactory,
	resticTimeout time.Duration,
	logger logrus.FieldLogger,
) (Restorer, error) {
	return &kubernetesRestorer{
		discoveryHelper:       discoveryHelper,
		dynamicFactory:        dynamicFactory,
		blockStore:            blockStore,
		backupClient:          backupClient,
		namespaceClient:       namespaceClient,
		resticRestorerFactory: resticRestorerFactory,
		resticTimeout:         resticTimeout,
		resourcePriorities:    resourcePriorities,
		logger:                logger,

		fileSystem: filesystem.NewFileSystem(),
	}, nil
}

// Restore executes a restore into the target Kubernetes cluster according to the restore spec
// and using data from the provided backup/backup reader. Returns a warnings and errors RestoreResult,
// respectively, summarizing info about the restore.
func (kr *kubernetesRestorer) Restore(log logrus.FieldLogger, restore *api.Restore, backup *api.Backup, backupReader io.Reader, actions []ItemAction) (api.RestoreResult, api.RestoreResult) {
	// metav1.LabelSelectorAsSelector converts a nil LabelSelector to a
	// Nothing Selector, i.e. a selector that matches nothing. We want
	// a selector that matches everything. This can be accomplished by
	// passing a non-nil empty LabelSelector.
	ls := restore.Spec.LabelSelector
	if ls == nil {
		ls = &metav1.LabelSelector{}
	}

	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		return api.RestoreResult{}, api.RestoreResult{Ark: []string{err.Error()}}
	}

	// get resource includes-excludes
	resourceIncludesExcludes := getResourceIncludesExcludes(kr.discoveryHelper, restore.Spec.IncludedResources, restore.Spec.ExcludedResources)
	prioritizedResources, err := prioritizeResources(kr.discoveryHelper, kr.resourcePriorities, resourceIncludesExcludes, log)
	if err != nil {
		return api.RestoreResult{}, api.RestoreResult{Ark: []string{err.Error()}}
	}

	resolvedActions, err := resolveActions(actions, kr.discoveryHelper)
	if err != nil {
		return api.RestoreResult{}, api.RestoreResult{Ark: []string{err.Error()}}
	}

	podVolumeTimeout := kr.resticTimeout
	if val := restore.Annotations[api.PodVolumeOperationTimeoutAnnotation]; val != "" {
		parsed, err := time.ParseDuration(val)
		if err != nil {
			log.WithError(errors.WithStack(err)).Errorf("Unable to parse pod volume timeout annotation %s, using server value.", val)
		} else {
			podVolumeTimeout = parsed
		}
	}

	ctx, cancelFunc := go_context.WithTimeout(go_context.Background(), podVolumeTimeout)
	defer cancelFunc()

	var resticRestorer restic.Restorer
	if kr.resticRestorerFactory != nil {
		resticRestorer, err = kr.resticRestorerFactory.NewRestorer(ctx, restore)
		if err != nil {
			return api.RestoreResult{}, api.RestoreResult{Ark: []string{err.Error()}}
		}
	}

	pvRestorer := &pvRestorer{
		logger:          log,
		snapshotVolumes: backup.Spec.SnapshotVolumes,
		restorePVs:      restore.Spec.RestorePVs,
		volumeBackups:   backup.Status.VolumeBackups,
		blockStore:      kr.blockStore,
	}

	restoreCtx := &context{
		backup:               backup,
		backupReader:         backupReader,
		restore:              restore,
		prioritizedResources: prioritizedResources,
		selector:             selector,
		log:                  log,
		dynamicFactory:       kr.dynamicFactory,
		fileSystem:           kr.fileSystem,
		namespaceClient:      kr.namespaceClient,
		actions:              resolvedActions,
		blockStore:           kr.blockStore,
		resticRestorer:       resticRestorer,
		pvsToProvision:       sets.NewString(),
		pvRestorer:           pvRestorer,
	}

	return restoreCtx.execute()
}

// getResourceIncludesExcludes takes the lists of resources to include and exclude, uses the
// discovery helper to resolve them to fully-qualified group-resource names, and returns an
// IncludesExcludes list.
func getResourceIncludesExcludes(helper discovery.Helper, includes, excludes []string) *collections.IncludesExcludes {
	resources := collections.GenerateIncludesExcludes(
		includes,
		excludes,
		func(item string) string {
			gvr, _, err := helper.ResourceFor(schema.ParseGroupResource(item).WithVersion(""))
			if err != nil {
				return ""
			}

			gr := gvr.GroupResource()
			return gr.String()
		},
	)

	return resources
}

type resolvedAction struct {
	ItemAction

	resourceIncludesExcludes  *collections.IncludesExcludes
	namespaceIncludesExcludes *collections.IncludesExcludes
	selector                  labels.Selector
}

func resolveActions(actions []ItemAction, helper discovery.Helper) ([]resolvedAction, error) {
	var resolved []resolvedAction

	for _, action := range actions {
		resourceSelector, err := action.AppliesTo()
		if err != nil {
			return nil, err
		}

		resources := getResourceIncludesExcludes(helper, resourceSelector.IncludedResources, resourceSelector.ExcludedResources)
		namespaces := collections.NewIncludesExcludes().Includes(resourceSelector.IncludedNamespaces...).Excludes(resourceSelector.ExcludedNamespaces...)

		selector := labels.Everything()
		if resourceSelector.LabelSelector != "" {
			if selector, err = labels.Parse(resourceSelector.LabelSelector); err != nil {
				return nil, err
			}
		}

		res := resolvedAction{
			ItemAction:                action,
			resourceIncludesExcludes:  resources,
			namespaceIncludesExcludes: namespaces,
			selector:                  selector,
		}

		resolved = append(resolved, res)
	}

	return resolved, nil
}

type context struct {
	backup               *api.Backup
	backupReader         io.Reader
	restore              *api.Restore
	prioritizedResources []schema.GroupResource
	selector             labels.Selector
	log                  logrus.FieldLogger
	dynamicFactory       client.DynamicFactory
	fileSystem           filesystem.Interface
	namespaceClient      corev1.NamespaceInterface
	actions              []resolvedAction
	blockStore           cloudprovider.BlockStore
	resticRestorer       restic.Restorer
	globalWaitGroup      arksync.ErrorGroup
	resourceWaitGroup    sync.WaitGroup
	resourceWatches      []watch.Interface
	pvsToProvision       sets.String
	pvRestorer           PVRestorer
}

func (ctx *context) execute() (api.RestoreResult, api.RestoreResult) {
	ctx.log.Infof("Starting restore of backup %s", kube.NamespaceAndName(ctx.backup))

	archiveReader := archive.NewGzipTarReader(ctx.backupReader)
	if err := archiveReader.Extract(); err != nil {
		ctx.log.Infof("error unzipping and extracting: %v", err)
		return api.RestoreResult{}, api.RestoreResult{Ark: []string{err.Error()}}
	}
	defer archiveReader.Close()

	return ctx.restoreFromArchive(archiveReader)
}

// restoreFromDir executes a restore based on backup data contained within a local
// directory.
func (ctx *context) restoreFromArchive(archiveReader archive.Reader) (api.RestoreResult, api.RestoreResult) {
	warnings, errs := api.RestoreResult{}, api.RestoreResult{}

	namespaceFilter := collections.NewIncludesExcludes().
		Includes(ctx.restore.Spec.IncludedNamespaces...).
		Excludes(ctx.restore.Spec.ExcludedNamespaces...)

	existingNamespaces := sets.NewString()

	// TODO this is not optimal since it'll keep watches open for all resources/namespaces
	// until the very end of the restore. This should be done per resource type. Deferring
	// refactoring for now since this may be able to be removed entirely if we eliminate
	// waiting for PV snapshot restores.
	defer func() {
		for _, watch := range ctx.resourceWatches {
			watch.Stop()
		}
	}()

	for _, resource := range ctx.prioritizedResources {
		// we don't want to explicitly restore namespace API objs because we'll handle
		// them as a special case prior to restoring anything into them
		if resource == kuberesource.Namespaces {
			continue
		}

		scope, exists, err := archiveReader.GetResourceScope(resource.String())
		if err != nil {
			addArkError(&errs, err)
			return warnings, errs
		}
		if !exists {
			continue
		}

		if scope == archive.ResourceScopeCluster {
			w, e := ctx.restoreResource(archiveReader, resource.String(), "")
			merge(&warnings, &w)
			merge(&errs, &e)
			continue
		}

		namespaces, err := archiveReader.ListNamespaces(resource.String())
		if err != nil {
			addArkError(&errs, err)
			return warnings, errs
		}

		for _, nsName := range namespaces {
			if !namespaceFilter.ShouldInclude(nsName) {
				ctx.log.Infof("Skipping namespace %s", nsName)
				continue
			}

			// fetch mapped NS name
			mappedNsName := nsName
			if target, ok := ctx.restore.Spec.NamespaceMapping[nsName]; ok {
				mappedNsName = target
			}

			// if we don't know whether this namespace exists yet, attempt to create
			// it in order to ensure it exists. Try to get it from the backup tarball
			// (in order to get any backed-up metadata), but if we don't find it there,
			// create a blank one.
			if !existingNamespaces.Has(mappedNsName) {
				logger := ctx.log.WithField("namespace", nsName)
				ns := getNamespace(logger, archiveReader, nsName, mappedNsName)
				if _, err := kube.EnsureNamespaceExists(ns, ctx.namespaceClient); err != nil {
					addArkError(&errs, err)
					continue
				}

				// keep track of namespaces that we know exist so we don't
				// have to try to create them multiple times
				existingNamespaces.Insert(mappedNsName)
			}

			w, e := ctx.restoreResource(archiveReader, resource.String(), mappedNsName)
			merge(&warnings, &w)
			merge(&errs, &e)
		}

		// TODO timeout?
		ctx.log.Debugf("Waiting on resource wait group for resource=%s", resource.String())
		ctx.resourceWaitGroup.Wait()
		ctx.log.Debugf("Done waiting on resource wait group for resource=%s", resource.String())
	}

	// TODO timeout?
	ctx.log.Debug("Waiting on global wait group")
	waitErrs := ctx.globalWaitGroup.Wait()
	ctx.log.Debug("Done waiting on global wait group")

	for _, err := range waitErrs {
		// TODO not ideal to be adding these to Ark-level errors
		// rather than a specific namespace, but don't have a way
		// to track the namespace right now.
		errs.Ark = append(errs.Ark, err.Error())
	}

	return warnings, errs
}

// getNamespace returns a namespace API object that we should attempt to
// create before restoring anything into it. It will come from the backup
// tarball if it exists, else will be a new one. If from the tarball, it
// will retain its labels, annotations, and spec.
func getNamespace(logger logrus.FieldLogger, archiveReader archive.Reader, nsName, remappedName string) *v1.Namespace {
	var nsBytes []byte
	var err error

	if nsBytes, err = archiveReader.Get("namespaces", "", nsName+".json"); err != nil {
		return &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: remappedName,
			},
		}
	}

	var backupNS v1.Namespace
	if err := json.Unmarshal(nsBytes, &backupNS); err != nil {
		logger.Warnf("Error unmarshalling namespace from backup, creating new one.")
		return &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: remappedName,
			},
		}
	}

	return &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        remappedName,
			Labels:      backupNS.Labels,
			Annotations: backupNS.Annotations,
		},
		Spec: backupNS.Spec,
	}
}

// merge combines two RestoreResult objects into one
// by appending the corresponding lists to one another.
func merge(a, b *api.RestoreResult) {
	a.Cluster = append(a.Cluster, b.Cluster...)
	a.Ark = append(a.Ark, b.Ark...)
	for k, v := range b.Namespaces {
		if a.Namespaces == nil {
			a.Namespaces = make(map[string][]string)
		}
		a.Namespaces[k] = append(a.Namespaces[k], v...)
	}
}

// addArkError appends an error to the provided RestoreResult's Ark list.
func addArkError(r *api.RestoreResult, err error) {
	r.Ark = append(r.Ark, err.Error())
}

// addToResult appends an error to the provided RestoreResult, either within
// the cluster-scoped list (if ns == "") or within the provided namespace's
// entry.
func addToResult(r *api.RestoreResult, ns string, e error) {
	if ns == "" {
		r.Cluster = append(r.Cluster, e.Error())
	} else {
		if r.Namespaces == nil {
			r.Namespaces = make(map[string][]string)
		}
		r.Namespaces[ns] = append(r.Namespaces[ns], e.Error())
	}
}

// restoreResource restores the specified cluster or namespace scoped resource. If namespace is
// empty we are restoring a cluster level resource, otherwise into the specified namespace.
func (ctx *context) restoreResource(archiveReader archive.Reader, resource, namespace string) (api.RestoreResult, api.RestoreResult) {
	warnings, errs := api.RestoreResult{}, api.RestoreResult{}

	if ctx.restore.Spec.IncludeClusterResources != nil && !*ctx.restore.Spec.IncludeClusterResources && namespace == "" {
		ctx.log.Infof("Skipping resource %s because it's cluster-scoped", resource)
		return warnings, errs
	}

	if namespace != "" {
		ctx.log.Infof("Restoring resource %q into namespace %q", resource, namespace)
	} else {
		ctx.log.Infof("Restoring cluster level resource %q", resource)
	}

	files, err := archiveReader.ListContents(resource, namespace)
	if err != nil {
		addToResult(&errs, namespace, fmt.Errorf("error reading resource directory %q: %v", resource, err))
		return warnings, errs
	}
	if len(files) == 0 {
		return warnings, errs
	}

	var (
		resourceClient    client.Dynamic
		groupResource     = schema.ParseGroupResource(resource)
		applicableActions []resolvedAction
		resourceWatch     watch.Interface
	)

	// pre-filter the actions based on namespace & resource includes/excludes since
	// these will be the same for all items being restored below
	for _, action := range ctx.actions {
		if !action.resourceIncludesExcludes.ShouldInclude(groupResource.String()) {
			continue
		}

		if namespace != "" && !action.namespaceIncludesExcludes.ShouldInclude(namespace) {
			continue
		}

		applicableActions = append(applicableActions, action)
	}

	for _, file := range files {
		bytes, err := archiveReader.Get(resource, namespace, file)
		if err != nil {
			addToResult(&errs, namespace, errors.Wrapf(err, "error getting item %q for resource %q in namespace %q", file, resource, namespace))
			continue
		}

		obj := new(unstructured.Unstructured)
		err = json.Unmarshal(bytes, obj)
		if err != nil {
			addToResult(&errs, namespace, errors.Wrapf(err, "error decoding item %q for resource %q in namespace %q", file, resource, namespace))
			continue
		}

		if !ctx.selector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}

		complete, err := isCompleted(obj, groupResource)
		if err != nil {
			addToResult(&errs, namespace, errors.Wrapf(err, "error checking completion for item %q for resource %q in namespace %q", file, resource, namespace))
			continue
		}
		if complete {
			ctx.log.Infof("%s is complete - skipping", kube.NamespaceAndName(obj))
			continue
		}

		if resourceClient == nil {
			// initialize client for this Resource. we need
			// metadata from an object to do this.
			ctx.log.Infof("Getting client for %v", obj.GroupVersionKind())

			resource := metav1.APIResource{
				Namespaced: len(namespace) > 0,
				Name:       groupResource.Resource,
			}

			var err error
			resourceClient, err = ctx.dynamicFactory.ClientForGroupVersionResource(obj.GroupVersionKind().GroupVersion(), resource, namespace)
			if err != nil {
				addArkError(&errs, fmt.Errorf("error getting resource client for namespace %q, resource %q: %v", namespace, &groupResource, err))
				return warnings, errs
			}
		}

		name := obj.GetName()

		// TODO: move to restore item action if/when we add a ShouldRestore() method to the interface
		if groupResource == kuberesource.Pods && obj.GetAnnotations()[v1.MirrorPodAnnotationKey] != "" {
			ctx.log.Infof("Not restoring pod because it's a mirror pod")
			continue
		}

		if groupResource == kuberesource.PersistentVolumes {
			_, found := ctx.backup.Status.VolumeBackups[name]
			reclaimPolicy, err := collections.GetString(obj.Object, "spec.persistentVolumeReclaimPolicy")
			if err == nil && !found && reclaimPolicy == "Delete" {
				ctx.log.Infof("Not restoring PV because it doesn't have a snapshot and its reclaim policy is Delete.")

				ctx.pvsToProvision.Insert(name)

				continue
			}

			// restore the PV from snapshot (if applicable)
			updatedObj, err := ctx.pvRestorer.executePVAction(obj)
			if err != nil {
				addToResult(&errs, namespace, errors.Wrapf(err, "error executing PVAction for item %q for resource %q in namespace %q", file, resource, namespace))
				continue
			}
			obj = updatedObj

			if resourceWatch == nil {
				resourceWatch, err = resourceClient.Watch(metav1.ListOptions{})
				if err != nil {
					addToResult(&errs, namespace, fmt.Errorf("error watching for namespace %q, resource %q: %v", namespace, &groupResource, err))
					return warnings, errs
				}
				ctx.resourceWatches = append(ctx.resourceWatches, resourceWatch)
				ctx.resourceWaitGroup.Add(1)
				go func() {
					defer ctx.resourceWaitGroup.Done()

					if _, err := waitForReady(resourceWatch.ResultChan(), name, isPVReady, time.Minute, ctx.log); err != nil {
						ctx.log.Warnf("Timeout reached waiting for persistent volume %s to become ready", name)
						addArkError(&warnings, fmt.Errorf("timeout reached waiting for persistent volume %s to become ready", name))
					}
				}()
			}
		}

		if groupResource == kuberesource.PersistentVolumeClaims {
			spec, err := collections.GetMap(obj.UnstructuredContent(), "spec")
			if err != nil {
				addToResult(&errs, namespace, err)
				continue
			}

			if volumeName, exists := spec["volumeName"]; exists && ctx.pvsToProvision.Has(volumeName.(string)) {
				ctx.log.Infof("Resetting PersistentVolumeClaim %s/%s for dynamic provisioning because its PV %v has a reclaim policy of Delete", namespace, name, volumeName)

				delete(spec, "volumeName")

				annotations := obj.GetAnnotations()
				delete(annotations, "pv.kubernetes.io/bind-completed")
				delete(annotations, "pv.kubernetes.io/bound-by-controller")
				obj.SetAnnotations(annotations)
			}
		}

		for _, action := range applicableActions {
			if !action.selector.Matches(labels.Set(obj.GetLabels())) {
				continue
			}

			ctx.log.Infof("Executing item action for %v", &groupResource)

			updatedObj, warning, err := action.Execute(obj, ctx.restore)
			if warning != nil {
				addToResult(&warnings, namespace, errors.Wrapf(err, "warning preparing item %q for resource %q in namespace %q", file, resource, namespace))
			}
			if err != nil {
				addToResult(&errs, namespace, errors.Wrapf(err, "error preparing item %q for resource %q in namespace %q", file, resource, namespace))
				continue
			}

			unstructuredObj, ok := updatedObj.(*unstructured.Unstructured)
			if !ok {
				addToResult(&errs, namespace, errors.Errorf("unexpected type %T for item %q for resource %q in namespace %q", updatedObj, file, resource, namespace))
				continue
			}

			obj = unstructuredObj
		}

		// clear out non-core metadata fields & status
		if obj, err = resetMetadataAndStatus(obj); err != nil {
			addToResult(&errs, namespace, err)
			continue
		}

		// necessary because we may have remapped the namespace
		// if the namespace is blank, don't create the key
		if namespace != "" {
			obj.SetNamespace(namespace)
		}

		// label the resource with the restore's name and the restored backup's name
		// for easy identification of all cluster resources created by this restore
		// and which backup they came from
		addRestoreLabels(obj, ctx.restore.Name, ctx.restore.Spec.BackupName)

		ctx.log.Infof("Restoring %s: %v", obj.GroupVersionKind().Kind, name)
		createdObj, restoreErr := resourceClient.Create(obj)
		if apierrors.IsAlreadyExists(restoreErr) {
			fromCluster, err := resourceClient.Get(name, metav1.GetOptions{})
			if err != nil {
				ctx.log.Infof("Error retrieving cluster version of %s: %v", kube.NamespaceAndName(obj), err)
				addToResult(&warnings, namespace, err)
				continue
			}
			// Remove insubstantial metadata
			fromCluster, err = resetMetadataAndStatus(fromCluster)
			if err != nil {
				ctx.log.Infof("Error trying to reset metadata for %s: %v", kube.NamespaceAndName(obj), err)
				addToResult(&warnings, namespace, err)
				continue
			}

			// We know the object from the cluster won't have the backup/restore name labels, so
			// copy them from the object we attempted to restore.
			labels := obj.GetLabels()
			addRestoreLabels(fromCluster, labels[api.RestoreNameLabel], labels[api.BackupNameLabel])

			if !equality.Semantic.DeepEqual(fromCluster, obj) {
				switch groupResource {
				case kuberesource.ServiceAccounts:
					desired, err := mergeServiceAccounts(fromCluster, obj)
					if err != nil {
						ctx.log.Infof("error merging secrets for ServiceAccount %s: %v", kube.NamespaceAndName(obj), err)
						addToResult(&warnings, namespace, err)
						continue
					}

					patchBytes, err := generatePatch(fromCluster, desired)
					if err != nil {
						ctx.log.Infof("error generating patch for ServiceAccount %s: %v", kube.NamespaceAndName(obj), err)
						addToResult(&warnings, namespace, err)
						continue
					}

					if patchBytes == nil {
						// In-cluster and desired state are the same, so move on to the next item
						continue
					}

					_, err = resourceClient.Patch(name, patchBytes)
					if err != nil {
						addToResult(&warnings, namespace, err)
					} else {
						ctx.log.Infof("ServiceAccount %s successfully updated", kube.NamespaceAndName(obj))
					}
				default:
					e := errors.Errorf("not restored: %s and is different from backed up version.", restoreErr)
					addToResult(&warnings, namespace, e)
				}
			}
			continue
		}
		// Error was something other than an AlreadyExists
		if restoreErr != nil {
			ctx.log.Infof("error restoring %s: %v", name, err)
			addToResult(&errs, namespace, errors.Wrapf(restoreErr, "error restoring item %q for resource %q in namespace %q", file, resource, namespace))
			continue
		}

		if groupResource == kuberesource.Pods && len(restic.GetPodSnapshotAnnotations(obj)) > 0 {
			if ctx.resticRestorer == nil {
				ctx.log.Warn("No restic restorer, not restoring pod's volumes")
			} else {
				ctx.globalWaitGroup.GoErrorSlice(func() []error {
					pod := new(v1.Pod)
					if err := runtime.DefaultUnstructuredConverter.FromUnstructured(createdObj.UnstructuredContent(), &pod); err != nil {
						ctx.log.WithError(err).Error("error converting unstructured pod")
						return []error{err}
					}

					if errs := ctx.resticRestorer.RestorePodVolumes(ctx.restore, pod, ctx.log); errs != nil {
						ctx.log.WithError(kubeerrs.NewAggregate(errs)).Error("unable to successfully complete restic restores of pod's volumes")
						return errs
					}

					return nil
				})
			}
		}
	}

	return warnings, errs
}

func waitForReady(
	watchChan <-chan watch.Event,
	name string,
	ready func(runtime.Unstructured) bool,
	timeout time.Duration,
	log logrus.FieldLogger,
) (*unstructured.Unstructured, error) {
	var timeoutChan <-chan time.Time
	if timeout != 0 {
		timeoutChan = time.After(timeout)
	} else {
		timeoutChan = make(chan time.Time)
	}

	for {
		select {
		case event := <-watchChan:
			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}

			obj, ok := event.Object.(*unstructured.Unstructured)
			switch {
			case !ok:
				log.Errorf("Unexpected type %T", event.Object)
				continue
			case obj.GetName() != name:
				continue
			case !ready(obj):
				log.Debugf("Item %s is not ready yet", name)
				continue
			default:
				return obj, nil
			}
		case <-timeoutChan:
			return nil, errors.New("failed to observe item becoming ready within the timeout")
		}
	}
}

type PVRestorer interface {
	executePVAction(obj *unstructured.Unstructured) (*unstructured.Unstructured, error)
}

type pvRestorer struct {
	logger          logrus.FieldLogger
	snapshotVolumes *bool
	restorePVs      *bool
	volumeBackups   map[string]*api.VolumeBackupInfo
	blockStore      cloudprovider.BlockStore
}

func (r *pvRestorer) executePVAction(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	pvName := obj.GetName()
	if pvName == "" {
		return nil, errors.New("PersistentVolume is missing its name")
	}

	spec, err := collections.GetMap(obj.UnstructuredContent(), "spec")
	if err != nil {
		return nil, err
	}

	delete(spec, "claimRef")
	delete(spec, "storageClassName")

	if boolptr.IsSetToFalse(r.snapshotVolumes) {
		// The backup had snapshots disabled, so we can return early
		return obj, nil
	}

	if boolptr.IsSetToFalse(r.restorePVs) {
		// The restore has pv restores disabled, so we can return early
		return obj, nil
	}

	// If we can't find a snapshot record for this particular PV, it most likely wasn't a PV that Ark
	// could snapshot, so return early instead of trying to restore from a snapshot.
	backupInfo, found := r.volumeBackups[pvName]
	if !found {
		return obj, nil
	}

	// Past this point, we expect to be doing a restore

	if r.blockStore == nil {
		return nil, errors.New("you must configure a persistentVolumeProvider to restore PersistentVolumes from snapshots")
	}

	log := r.logger.WithFields(
		logrus.Fields{
			"persistentVolume": pvName,
			"snapshot":         backupInfo.SnapshotID,
		},
	)

	log.Info("restoring persistent volume from snapshot")
	volumeID, err := r.blockStore.CreateVolumeFromSnapshot(backupInfo.SnapshotID, backupInfo.Type, backupInfo.AvailabilityZone, backupInfo.Iops)
	if err != nil {
		return nil, err
	}
	log.Info("successfully restored persistent volume from snapshot")

	updated1, err := r.blockStore.SetVolumeID(obj, volumeID)
	if err != nil {
		return nil, err
	}

	updated2, ok := updated1.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.Errorf("unexpected type %T", updated1)
	}

	return updated2, nil
}

func isPVReady(obj runtime.Unstructured) bool {
	phase, err := collections.GetString(obj.UnstructuredContent(), "status.phase")
	if err != nil {
		return false
	}

	return phase == string(v1.VolumeAvailable)
}

func resetMetadataAndStatus(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	metadata, err := collections.GetMap(obj.UnstructuredContent(), "metadata")
	if err != nil {
		return nil, err
	}

	for k := range metadata {
		switch k {
		case "name", "namespace", "labels", "annotations":
		default:
			delete(metadata, k)
		}
	}

	// Never restore status
	delete(obj.UnstructuredContent(), "status")

	return obj, nil
}

// addRestoreLabels labels the provided object with the restore name and
// the restored backup's name.
func addRestoreLabels(obj metav1.Object, restoreName, backupName string) {
	labels := obj.GetLabels()

	if labels == nil {
		labels = make(map[string]string)
	}

	labels[api.BackupNameLabel] = backupName
	labels[api.RestoreNameLabel] = restoreName

	// TODO(1.0): remove the below line, and remove the `RestoreLabelKey`
	// constant from the API pkg, since it's been replaced with the
	// namespaced label above.
	labels[api.RestoreLabelKey] = restoreName

	obj.SetLabels(labels)
}

// hasControllerOwner returns whether or not an object has a controller
// owner ref. Used to identify whether or not an object should be explicitly
// recreated during a restore.
func hasControllerOwner(refs []metav1.OwnerReference) bool {
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

// isCompleted returns whether or not an object is considered completed.
// Used to identify whether or not an object should be restored. Only Jobs or Pods are considered
func isCompleted(obj *unstructured.Unstructured, groupResource schema.GroupResource) (bool, error) {
	switch groupResource {
	case kuberesource.Pods:
		phase, _, err := unstructured.NestedString(obj.UnstructuredContent(), "status", "phase")
		if err != nil {
			return false, errors.WithStack(err)
		}
		if phase == string(v1.PodFailed) || phase == string(v1.PodSucceeded) {
			return true, nil
		}

	case kuberesource.Jobs:
		ct, found, err := unstructured.NestedString(obj.UnstructuredContent(), "status", "completionTime")
		if err != nil {
			return false, errors.WithStack(err)
		}
		if found && ct != "" {
			return true, nil
		}
	}
	// Assume any other resource isn't complete and can be restored
	return false, nil
}
