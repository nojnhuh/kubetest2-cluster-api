package main

import (
	"sigs.k8s.io/kubetest2/pkg/app"

	"github.com/nojnhuh/kubetest2-cluster-api/deployer"
)

func main() {
	app.Main(deployer.Name, deployer.New)
}
