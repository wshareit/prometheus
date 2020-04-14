package huawei_elb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/auth/aksk"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/networking/v2/extensions/lbaas_v2/pools"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"

	"github.com/prometheus/prometheus/discovery/targetgroup"
)

const (
	elbLabel    = model.MetaLabelPrefix + "huawei_elb_"
	elbLabelTag = elbLabel + "tag_"
)

var (
	patFileSDName = regexp.MustCompile(`^[^*]*(\*[^/]*)?\.(json|yml|yaml|JSON|YML|YAML)$`)

	elbSDRefreshFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_sd__huawei_elb_refresh_failures_total",
			Help: "The number of ELB-SD scrape failures.",
		})
	// DefaultSDConfig is the default ELB SD configuration.
	DefaultSDConfig = SDConfig{
		RefreshInterval: model.Duration(60 * time.Second),
	}
)

// elb pool
type ElbPool struct {
	Name   string            `yaml:"name"`
	Port   int               `yaml:"port"`
	Labels map[string]string `yaml: "labels"`
}

// SDConfig is the configuration for ECS based service discovery.
type SDConfig struct {
	File             string         `yaml:"file"`
	IdentityEndpoint string         `yaml:"identity_endpoint"`
	DomainID         string         `yaml:"domain_id,omitempty"`
	Domain           string         `yaml:"domain,omitempty"`
	ProjectID        string         `yaml:"project_id,omitempty"`
	Region           string         `yaml:"region,omitempty"`
	AccessKey        string         `yaml:"access_key,omitempty"`
	SecretKey        string         `yaml:"secret_key,omitempty"`
	RefreshInterval  model.Duration `yaml:"refresh_interval,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}

	if !patFileSDName.MatchString(c.File) {
		return fmt.Errorf("path name %q is not valid for elb file discovery", c.File)
	}

	return nil
}

func init() {
	prometheus.MustRegister(elbSDRefreshFailuresCount)
}

// Discovery periodically performs LB-SD requests. It implements
// the Discoverer interface.
type Discovery struct {
	file     string
	interval time.Duration
	port     int
	logger   log.Logger
	opts     aksk.AKSKOptions
	lock     sync.RWMutex
}

// NewDiscovery returns a new ALBDiscovery which periodically refreshes its targets.
func NewDiscovery(conf *SDConfig, logger log.Logger) *Discovery {

	opts := aksk.AKSKOptions{
		IdentityEndpoint: conf.IdentityEndpoint,
		ProjectID:        conf.ProjectID,
		AccessKey:        conf.AccessKey,
		SecretKey:        conf.SecretKey,
		Domain:           conf.Domain,
		DomainID:         conf.DomainID,
		Region:           conf.Region,
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Discovery{
		file:     conf.File,
		opts:     opts,
		interval: time.Duration(conf.RefreshInterval),
		logger:   logger,
	}
}

// Run implements the Discoverer interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	// Get an initial set right away.
	d.refreshAll(ctx, ch)

	for {
		select {
		case <-ticker.C:
			d.refreshAll(ctx, ch)
		case <-ctx.Done():
			return
		}
	}
}

func (d *Discovery) refreshAll(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// get albTargetGroups
	tgs, err := d.readFile(d.file)
	if err != nil {
		level.Error(d.logger).Log("msg", "Error get  alb targets from file", "err", err)
		return
	}

	// get target
	var wg sync.WaitGroup
	wg.Add(len(tgs))
	for _, tg := range tgs {
		go func(n *ElbPool) {
			if err := d.refresh(ctx, n, ch); err != nil {
				elbSDRefreshFailuresCount.Inc()
				level.Error(d.logger).Log("msg", "Error refreshing elb pools", "err", err)
			}
			wg.Done()
		}(tg)
	}

	wg.Wait()
}

func (d *Discovery) refresh(ctx context.Context, atg *ElbPool, ch chan<- []*targetgroup.Group) error {

	provider, err_auth := openstack.AuthenticatedClient(d.opts)
	if err_auth != nil {
		return err_auth
	}

	sc, err := openstack.NewNetworkV2(provider, gophercloud.EndpointOpts{})
	if err != nil {
		return err
	}

	allPages, err := pools.ListMembers(sc, atg.Name, pools.ListMembersOpts{}).AllPages()
	if err != nil {
		return err
	}

	members, err := pools.ExtractMembers(allPages)
	if err != nil {
		return err
	}

	tg := &targetgroup.Group{
		Source: atg.Name,
	}

	if len(members) > 0 {
		for _, member := range members {
			port := atg.Port
			if port == 0 {
				port = member.ProtocolPort
			}
			addr := net.JoinHostPort(member.Address, fmt.Sprintf("%d", port))

			labels := model.LabelSet{
				model.AddressLabel: model.LabelValue(addr),
			}

			for name, v := range atg.Labels {
				if name == "" || v == "" {
					continue
				}

				labels[elbLabelTag+model.LabelName(name)] = model.LabelValue(v)
			}
			tg.Targets = append(tg.Targets, labels)
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- []*targetgroup.Group{tg}:
	}

	return nil
}

// readFile reads a JSON or YAML list of targets groups from the file, depending on its
// file extension. It returns full configuration target groups.
func (d *Discovery) readFile(filename string) ([]*ElbPool, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	content, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}

	_, err = fd.Stat()
	if err != nil {
		return nil, err
	}

	var targetGroups []*ElbPool

	switch ext := filepath.Ext(filename); strings.ToLower(ext) {
	case ".json":
		if err := json.Unmarshal(content, &targetGroups); err != nil {
			return nil, err
		}
	case ".yml", ".yaml":
		if err := yaml.UnmarshalStrict(content, &targetGroups); err != nil {
			return nil, err
		}
	default:
		panic(fmt.Errorf("discovery.File.readFile: unhandled file extension %q", ext))
	}

	for _, tg := range targetGroups {
		if tg == nil {
			err = errors.New("nil target group item found")
			return nil, err
		}
	}

	return targetGroups, nil
}
