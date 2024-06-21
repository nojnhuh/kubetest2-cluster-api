// Package deployer implements the kubetest2 Cluster API deployer
package deployer

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/octago/sflags/gen/gpflag"
	"github.com/spf13/pflag"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/kubetest2/pkg/types"
)

var helmEnv = cli.New()

// Name is the name of the deployer
const Name = "cluster-api"

// New implements deployer.New for kind
func New(opts types.Options) (types.Deployer, *pflag.FlagSet) {
	d := &deployer{
		commonOptions: opts,
		helmCfg:       new(action.Configuration),
	}
	return d, bindFlags(d)
}

type deployer struct {
	commonOptions types.Options

	CAPIOperatorValues string `flag:"capi-operator-values" desc:"path to a file containing values to pass to the Cluster API Operator Helm chart"`
	ProviderManifest   string `flag:"provider-manifest" desc:"path to a YAML manifest containing Cluster API provider definitions"`
	ClusterManifest    string `flag:"cluster-manifest" desc:"path to a YAML manifest containing a Cluster API cluster definition"`

	helmCfg *action.Configuration
	dyn     dynamic.Interface
	typed   kubernetes.Interface
	mapper  meta.RESTMapper
}

var _ types.DeployerWithInit = &deployer{}

func (d *deployer) Init() error {
	restConfig, err := helmEnv.RESTClientGetter().ToRESTConfig()
	if err != nil {
		return fmt.Errorf("error getting rest config: %w", err)
	}
	d.typed, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("error getting typed client: %w", err)
	}
	d.dyn, err = dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("error getting dynamic client: %w", err)
	}
	d.mapper, err = helmEnv.RESTClientGetter().ToRESTMapper()
	if err != nil {
		return fmt.Errorf("error getting REST mapper: %w", err)
	}

	err = d.helmCfg.Init(helmEnv.RESTClientGetter(), helmEnv.Namespace(), "secret", func(format string, v ...interface{}) {
		klog.V(4).InfofDepth(2, format, v...)
	})
	if err != nil {
		return fmt.Errorf("error initializing helm: %w", err)
	}

	if !d.commonOptions.ShouldUp() {
		return nil
	}

	ctx := context.Background()

	klog.Info("installing cert-manager")
	certManager := action.NewInstall(d.helmCfg)
	certManager.ChartPathOptions = action.ChartPathOptions{
		RepoURL: "https://charts.jetstack.io",
	}
	certManager.ReleaseName = "cert-manager"
	certManager.Namespace = "cert-manager"
	certManager.CreateNamespace = true
	certManager.Wait = true
	certManager.Timeout = 5 * time.Minute
	certManagerValues := map[string]interface{}{
		"installCRDs": true,
	}

	certMangerPath, err := certManager.LocateChart("cert-manager", helmEnv)
	if err != nil {
		return fmt.Errorf("error locating cert-manager chart: %w", err)
	}
	certManagerChart, err := loader.Load(certMangerPath)
	if err != nil {
		return fmt.Errorf("error loading cert-manager chart: %w", err)
	}
	_, err = certManager.RunWithContext(ctx, certManagerChart, certManagerValues)
	if err != nil {
		return fmt.Errorf("error installing cert-manager chart: %w", err)
	}
	klog.Info("cert-manager installed")

	klog.Info("installing cluster-api-operator")
	capiOperator := action.NewInstall(d.helmCfg)
	capiOperator.ChartPathOptions = action.ChartPathOptions{
		RepoURL: "https://kubernetes-sigs.github.io/cluster-api-operator",
	}
	capiOperator.ReleaseName = "capi-operator"
	capiOperator.Namespace = "capi-operator-system"
	capiOperator.CreateNamespace = true
	capiOperator.Wait = true
	capiOperator.Timeout = 5 * time.Minute
	capiOperatorValues := map[string]interface{}{}
	if d.CAPIOperatorValues != "" {
		capiOperatorValuesFile, err := os.Open(d.CAPIOperatorValues)
		if err != nil {
			return fmt.Errorf("error opening %s: %w", d.CAPIOperatorValues, err)
		}
		capiOperatorValuesBytes, err := io.ReadAll(capiOperatorValuesFile)
		if err != nil {
			return fmt.Errorf("error reading %s: %w", d.CAPIOperatorValues, err)
		}
		err = yaml.Unmarshal(capiOperatorValuesBytes, &capiOperatorValues)
		if err != nil {
			return fmt.Errorf("error unmarshaling %s: %w", d.CAPIOperatorValues, err)
		}
	}

	capiOperatorPath, err := capiOperator.LocateChart("cluster-api-operator", helmEnv)
	if err != nil {
		return fmt.Errorf("error locating cluster-api-operator chart: %w", err)
	}
	capiOperatorChart, err := loader.Load(capiOperatorPath)
	if err != nil {
		return fmt.Errorf("error loading cluster-api-operator chart: %w", err)
	}
	_, err = capiOperator.RunWithContext(ctx, capiOperatorChart, capiOperatorValues)
	if err != nil {
		return fmt.Errorf("error installing cluster-api-operator chart: %w", err)
	}
	klog.Info("cluster-api-operator installed")

	klog.Info("creating cluster-api-operator providers")
	if err := d.createFromFile(ctx, d.ProviderManifest); err != nil {
		return err
	}
	operatorGV := schema.GroupVersion{Group: "operator.cluster.x-k8s.io", Version: "v1alpha2"}
	waitResources := []schema.GroupVersionResource{
		operatorGV.WithResource("addonproviders"),
		operatorGV.WithResource("bootstrapproviders"),
		operatorGV.WithResource("controlplaneproviders"),
		operatorGV.WithResource("coreproviders"),
		operatorGV.WithResource("infrastructureproviders"),
		operatorGV.WithResource("ipamproviders"),
		operatorGV.WithResource("runtimeextensionproviders"),
	}
	providerWaitTimeout := 5 * time.Minute
	klog.Infof("Waiting %v for Cluster API Providers to become ready", providerWaitTimeout)
	providerWaitCtx, cancel := context.WithTimeout(ctx, providerWaitTimeout)
	defer cancel()
	if err := waitForResourcesReady(providerWaitCtx, d.dyn, waitResources); err != nil {
		return err
	}
	klog.Info("all creating cluster-api-operator providers are Ready")

	return nil
}

var _ types.DeployerWithFinish = &deployer{}

func (d *deployer) Finish() error {
	if !d.commonOptions.ShouldDown() {
		return nil
	}

	ctx := context.Background()

	providerWaitTimeout := 5 * time.Minute
	klog.Infof("Waiting %v for Cluster API Providers to become ready", providerWaitTimeout)
	providerWaitCtx, cancel := context.WithTimeout(ctx, providerWaitTimeout)
	defer cancel()

	klog.Info("deleting cluster-api-operator providers")
	if err := d.deleteFromFileAndWait(providerWaitCtx, d.ProviderManifest); err != nil {
		return err
	}
	klog.Info("deleted cluster-api-operator providers")

	waitForNamespaceDelete := func(namespace string) error {
		return wait.PollUntilContextTimeout(ctx, 10*time.Second, 5*time.Minute, true, func(context.Context) (bool, error) {
			_, err := d.typed.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				klog.V(2).Info("namespace is gone")
				return true, nil
			}
			if err == nil {
				klog.V(4).Info("namespace still exists")
			}
			return false, err
		})
	}

	klog.Info("deleting cluster-api-operator")
	capiOperator := action.NewUninstall(d.helmCfg)
	capiOperator.IgnoreNotFound = true
	capiOperator.Wait = true
	capiOperator.Timeout = 5 * time.Minute
	if _, err := capiOperator.Run("capi-operator"); err != nil {
		return fmt.Errorf("error deleting cluster-api-operator: %w", err)
	}
	if err := d.typed.CoreV1().Namespaces().Delete(ctx, "capi-operator-system", metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("error deleting capi-operator-system namespace: %w", err)
	}
	if err := waitForNamespaceDelete("capi-operator-system"); err != nil {
		return err
	}
	klog.Info("deleted cluster-api-operator")

	klog.Info("deleting cert-manager")
	certManager := action.NewUninstall(d.helmCfg)
	certManager.IgnoreNotFound = true
	certManager.Wait = true
	certManager.Timeout = 5 * time.Minute
	if _, err := certManager.Run("cert-manager"); err != nil {
		return fmt.Errorf("error deleting cert-manager: %w", err)
	}
	if err := d.typed.CoreV1().Namespaces().Delete(ctx, "cert-manager", metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("error deleting cert-manager namespace: %w", err)
	}
	if err := waitForNamespaceDelete("cert-manager"); err != nil {
		return err
	}
	klog.Info("deleted cert-manager")

	return nil
}

func (d *deployer) Up() error {
	ctx := context.Background()
	klog.Infof("creating cluster from %s", d.ClusterManifest)

	if err := d.createFromFile(ctx, d.ClusterManifest); err != nil {
		return err
	}

	capiGV := schema.GroupVersion{Group: "cluster.x-k8s.io", Version: "v1beta1"}
	waitResources := []schema.GroupVersionResource{
		capiGV.WithResource("clusters"),
		capiGV.WithResource("machinedeployments"),
		capiGV.WithResource("machinepools"),
	}
	clusterWaitTimeout := 30 * time.Minute
	klog.Infof("Waiting %v for Cluster to become ready", clusterWaitTimeout)
	providerWaitCtx, cancel := context.WithTimeout(ctx, clusterWaitTimeout)
	defer cancel()
	return waitForResourcesReady(providerWaitCtx, d.dyn, waitResources)
}

func (d *deployer) Down() error {
	ctx := context.Background()
	klog.Infof("deleting cluster from %s", d.ClusterManifest)

	var clusterName, clusterNamespace string
	clusterGVK := schema.GroupVersionKind{
		Group:   "cluster.x-k8s.io",
		Version: "v1beta1",
		Kind:    "Cluster",
	}

	resources, err := os.Open(d.ClusterManifest)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", d.ClusterManifest, err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(resources, 4096)
	for {
		u := &unstructured.Unstructured{}
		err := dec.Decode(u)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading cluster manifest: %w", err)
		}
		if u.GroupVersionKind().Group != clusterGVK.Group ||
			u.GroupVersionKind().Kind != clusterGVK.Kind {
			continue
		}
		namespace := u.GetNamespace()
		if namespace == "" {
			namespace = helmEnv.Namespace()
		}
		gvr := u.GroupVersionKind().GroupVersion().WithResource("clusters")
		klog.V(2).Infof("Deleting %s %s/%s", gvr, namespace, u.GetName())
		err = d.dyn.Resource(gvr).Namespace(namespace).Delete(ctx, u.GetName(), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("error deleting resource %s %s/%s: %w", u.GroupVersionKind(), namespace, u.GetName(), err)
		}
		clusterName = u.GetName()
		clusterNamespace = namespace
		break
	}

	mapping, err := d.mapper.RESTMapping(clusterGVK.GroupKind(), clusterGVK.Version)
	if err != nil {
		return fmt.Errorf("error getting REST mapping for %s %s/%s: %w", clusterGVK, clusterNamespace, clusterName, err)
	}
	clusterWaitTimeout := 30 * time.Minute
	klog.Infof("Waiting %v for Cluster to be deleted", clusterWaitTimeout)
	clusterWaitCtx, cancel := context.WithTimeout(ctx, clusterWaitTimeout)
	defer cancel()
	return waitForResourceDeleted(clusterWaitCtx, d.dyn, mapping, clusterName, clusterNamespace)
}

func (d *deployer) IsUp() (up bool, err error) {
	klog.Info(" IsUp IS NOT IMPLEMENTED")
	return false, nil
}

func (d *deployer) DumpClusterLogs() error {
	return nil
}

func (d *deployer) Build() error {
	// TODO: build should probably still exist with common options
	return nil
}

var _ types.DeployerWithKubeconfig = &deployer{}

func (d *deployer) Kubeconfig() (k string, reterr error) {
	defer func() {
		// kubetest2 doesn't log this itself
		if reterr != nil {
			klog.Errorf("failed to get kubeconfig: %v", reterr)
		}
	}()

	ctx := context.Background()
	var clusterName, clusterNamespace string

	resources, err := os.Open(d.ClusterManifest)
	if err != nil {
		return "", fmt.Errorf("error opening %s: %w", d.ClusterManifest, err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(resources, 4096)
	for {
		u := &unstructured.Unstructured{}
		err := dec.Decode(u)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("error reading cluster manifest: %w", err)
		}
		if u.GroupVersionKind().Group != "cluster.x-k8s.io" ||
			u.GroupVersionKind().Kind != "Cluster" {
			continue
		}
		clusterNamespace = u.GetNamespace()
		if clusterNamespace == "" {
			clusterNamespace = helmEnv.Namespace()
		}
		clusterName = u.GetName()
		break
	}
	secretName := clusterName + "-kubeconfig"

	kubeconfig, err := d.typed.CoreV1().Secrets(clusterNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("error getting kubeconfig Secret %s/%s: %w", clusterNamespace, secretName, err)
	}
	kubeconfigFile, err := os.Create(filepath.Join(d.commonOptions.RunDir(), "kubeconfig"))
	if err != nil {
		return "", fmt.Errorf("failed to create kubeconfig: %w", err)
	}
	_, err = io.Copy(kubeconfigFile, bytes.NewReader(kubeconfig.Data["value"]))
	if err != nil {
		return "", fmt.Errorf("error writing kubeconfig: %w", err)
	}
	klog.Infof("Wrote kubeconfig to %s", kubeconfigFile.Name())

	return kubeconfigFile.Name(), nil
}

// helper used to create & bind a flagset to the deployer
func bindFlags(d *deployer) *pflag.FlagSet {
	flags, err := gpflag.Parse(d)
	if err != nil {
		klog.Fatalf("unable to generate flags from deployer")
		return nil
	}

	// initing the klog flags adds them to goflag.CommandLine
	// they can then be added to the built pflag set
	klog.InitFlags(nil)
	flags.AddGoFlagSet(flag.CommandLine)

	return flags
}

func (d *deployer) createFromFile(ctx context.Context, path string) error {
	resources, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", path, err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(resources, 4096)
	for {
		u := &unstructured.Unstructured{}
		err := dec.Decode(u)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading manifest: %w", err)
		}
		mapping, err := d.mapper.RESTMapping(u.GroupVersionKind().GroupKind(), u.GroupVersionKind().Version)
		if err != nil {
			return fmt.Errorf("error getting REST mapping for %s %s/%s: %w", u.GroupVersionKind(), u.GetNamespace(), u.GetName(), err)
		}
		namespace := u.GetNamespace()
		var res dynamic.ResourceInterface
		namespaceRes := d.dyn.Resource(mapping.Resource)
		res = namespaceRes
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			if namespace == "" {
				namespace = helmEnv.Namespace()
			}
			res = namespaceRes.Namespace(namespace)
		}
		klog.V(2).Infof("Creating %s %s/%s", mapping.Resource, namespace, u.GetName())
		_, err = res.Create(ctx, u, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("error creating resource %s %s/%s: %w", u.GroupVersionKind(), namespace, u.GetName(), err)
		}
	}
	return nil
}

func (d *deployer) deleteFromFileAndWait(ctx context.Context, path string) error {
	type waitResource struct {
		mapping   *meta.RESTMapping
		name      string
		namespace string
	}
	var waitResources []waitResource

	resources, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", path, err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(resources, 4096)
	for {
		u := &unstructured.Unstructured{}
		err := dec.Decode(u)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading manifest: %w", err)
		}
		mapping, err := d.mapper.RESTMapping(u.GroupVersionKind().GroupKind(), u.GroupVersionKind().Version)
		if err != nil {
			return fmt.Errorf("error getting REST mapping for %s %s/%s: %w", u.GroupVersionKind(), u.GetNamespace(), u.GetName(), err)
		}
		namespace := u.GetNamespace()
		var res dynamic.ResourceInterface
		namespaceRes := d.dyn.Resource(mapping.Resource)
		res = namespaceRes
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			if namespace == "" {
				namespace = helmEnv.Namespace()
			}
			res = namespaceRes.Namespace(namespace)
		}
		klog.V(2).Infof("Deleting %s %s/%s", mapping.Resource, namespace, u.GetName())
		err = res.Delete(ctx, u.GetName(), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("error creating resource %s %s/%s: %w", u.GroupVersionKind(), namespace, u.GetName(), err)
		}

		waitResources = append(waitResources, waitResource{
			mapping:   mapping,
			name:      u.GetName(),
			namespace: namespace,
		})
	}

	for _, resource := range waitResources {
		err := waitForResourceDeleted(ctx, d.dyn, resource.mapping, resource.name, resource.namespace)
		if err != nil {
			return err
		}
	}

	return nil
}

func waitForResourcesReady(ctx context.Context, dyn dynamic.Interface, waitResources []schema.GroupVersionResource) error {
	for _, resource := range waitResources {
		err := wait.PollUntilContextCancel(ctx, 10*time.Second, true, func(context.Context) (done bool, err error) {
			list, err := dyn.Resource(resource).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			if len(list.Items) == 0 {
				klog.V(4).Infof("No %s exist to wait for", resource)
				return true, nil
			}
			for _, provider := range list.Items {
				conditions, _, err := unstructured.NestedSlice(provider.UnstructuredContent(), "status", "conditions")
				if err != nil {
					return true, err
				}
				ready := false
				for _, cond := range conditions {
					condition, ok := cond.(map[string]any)
					if !ok {
						return true, fmt.Errorf("failed to get conditions")
					}
					typ, _, err := unstructured.NestedString(condition, "type")
					if err != nil {
						return true, err
					}
					if typ != "Ready" {
						continue
					}
					status, _, err := unstructured.NestedString(condition, "status")
					if err != nil {
						return true, err
					}
					ready = status == "True"
				}
				if !ready {
					klog.V(4).Infof("%s %s/%s is not Ready", provider.GetKind(), provider.GetNamespace(), provider.GetName())
					return false, nil
				}
				klog.V(4).Infof("%s %s/%s is Ready", provider.GetKind(), provider.GetNamespace(), provider.GetName())
			}
			klog.V(2).Infof("All %s are Ready", resource)
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func waitForResourceDeleted(ctx context.Context, dyn dynamic.Interface, mapping *meta.RESTMapping, name, namespace string) error {
	var res dynamic.ResourceInterface
	namespaceRes := dyn.Resource(mapping.Resource)
	res = namespaceRes
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		if namespace == "" {
			namespace = helmEnv.Namespace()
		}
		res = namespaceRes.Namespace(namespace)
	}

	return wait.PollUntilContextCancel(ctx, 10*time.Second, true, func(context.Context) (bool, error) {
		_, err := res.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			klog.V(2).Info("resource is gone")
			return true, nil
		}
		if err == nil {
			klog.V(4).Info("resource still exists")
		}
		return false, err
	})
}
