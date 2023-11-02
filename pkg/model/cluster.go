package model

type Cluster struct {
}

func (c Cluster) Owner(key QualifiedDomainKey) (Instance, bool) {
	return Instance{}, false
}

func NewCluster() Cluster {
	return Cluster{}
}
