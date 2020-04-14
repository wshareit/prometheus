package autoscala

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

	"gopkg.in/yaml.v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/discovery/targetgroup"
)

const (
	autoscalaLabel    = model.MetaLabelPrefix + "autoscala_"
	autoscalaLabelTag = autoscalaLabel + "tag_"
)

var (
	patFileSDName = regexp.MustCompile(`^[^*]*(\*[^/]*)?\.(json|yml|yaml|JSON|YML|YAML)$`)

	autoscalaSDRefreshFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_sd_autoscala_refresh_failures_total",
			Help: "The number of AUTOSCALA-SD scrape failures.",
		})
	// DefaultSDConfig is the default Autoscala SD configuration.
	DefaultSDConfig = SDConfig{
		RefreshInterval: model.Duration(60 * time.Second),
	}

	autoscalaInstanceHealthStatus = "Healthy"
)

// Autoscala autoscala
type Autoscala struct {
	Name   string            `yaml:"name"`
	Port   int               `yaml:"port"`
	Labels map[string]string `yaml: "labels"`
}

// SDConfig is the configuration for EC2 based service discovery.
type SDConfig struct {
	File            string             `yaml:"file"`
	Endpoint        string             `yaml:"endpoint"`
	Region          string             `yaml:"region"`
	AccessKey       string             `yaml:"access_key,omitempty"`
	SecretKey       config_util.Secret `yaml:"secret_key,omitempty"`
	Profile         string             `yaml:"profile,omitempty"`
	RoleARN         string             `yaml:"role_arn,omitempty"`
	RefreshInterval model.Duration     `yaml:"refresh_interval,omitempty"`
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
		return fmt.Errorf("path name %q is not valid for autoscaling file discovery", c.File)
	}

	if c.Region == "" {
		sess, err := session.NewSession()
		if err != nil {
			return err
		}
		metadata := ec2metadata.New(sess)
		region, err := metadata.Region()
		if err != nil {
			return fmt.Errorf("EC2 SD configuration requires a region")
		}
		c.Region = region
	}

	return nil
}

func init() {
	prometheus.MustRegister(autoscalaSDRefreshFailuresCount)
}

// Discovery periodically performs ALB-SD requests. It implements
// the Discoverer interface.
type Discovery struct {
	file     string
	aws      *aws.Config
	interval time.Duration
	profile  string
	roleARN  string
	port     int
	logger   log.Logger

	lock sync.RWMutex
}

// NewDiscovery returns a new ALBDiscovery which periodically refreshes its targets.
func NewDiscovery(conf *SDConfig, logger log.Logger) *Discovery {
	creds := credentials.NewStaticCredentials(conf.AccessKey, string(conf.SecretKey), "")
	if conf.AccessKey == "" && conf.SecretKey == "" {
		creds = nil
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Discovery{
		file: conf.File,
		aws: &aws.Config{
			Endpoint:    &conf.Endpoint,
			Region:      &conf.Region,
			Credentials: creds,
		},
		profile:  conf.Profile,
		roleARN:  conf.RoleARN,
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
	// get autoscalas
	tgs, err := d.readFile(d.file)
	if err != nil {
		level.Error(d.logger).Log("msg", "Error get  autoscaling targets from file", "err", err)
		return
	}

	// get target
	var wg sync.WaitGroup
	wg.Add(len(tgs))
	for _, tg := range tgs {
		go func(n *Autoscala) {
			if err := d.refresh(ctx, n, ch); err != nil {
				autoscalaSDRefreshFailuresCount.Inc()
				level.Error(d.logger).Log("msg", "Error refreshing autoscala targets", "err", err)
			}
			wg.Done()
		}(tg)
	}

	wg.Wait()
}

func (d *Discovery) refresh(ctx context.Context, auto *Autoscala, ch chan<- []*targetgroup.Group) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  *d.aws,
		Profile: d.profile,
	})
	if err != nil {
		return fmt.Errorf("could not create aws session: %s", err)
	}

	var autos *autoscaling.AutoScaling
	if d.roleARN != "" {
		creds := stscreds.NewCredentials(sess, d.roleARN)
		autos = autoscaling.New(sess, &aws.Config{Credentials: creds})
	} else {
		autos = autoscaling.New(sess)
	}

	var ec2s *ec2.EC2
	if d.roleARN != "" {
		creds := stscreds.NewCredentials(sess, d.roleARN)
		ec2s = ec2.New(sess, &aws.Config{Credentials: creds})
	} else {
		ec2s = ec2.New(sess)
	}

	tg := &targetgroup.Group{
		Source: auto.Name,
	}

	describeAutoScalingGroupsOutput, err := autos.DescribeAutoScalingGroups(&autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{
			aws.String(auto.Name),
		},
	})

	if err != nil {
		return fmt.Errorf("could not describe autoscala: %s", err)
	}

	if len(describeAutoScalingGroupsOutput.AutoScalingGroups) > 0 {
		var ids []*string

		for _, group := range describeAutoScalingGroupsOutput.AutoScalingGroups {
			for _, instance := range group.Instances {
				if *instance.HealthStatus == autoscalaInstanceHealthStatus {
					ids = append(ids, instance.InstanceId)
				}
			}
		}

		if len(ids) > 0 {
			describeInstancesOutput, err := ec2s.DescribeInstances(&ec2.DescribeInstancesInput{
				InstanceIds: ids,
			})
			if err != nil {
				return fmt.Errorf("could not describe ec2 instances: %s", err)
			}

			for _, r := range describeInstancesOutput.Reservations {
				for _, inst := range r.Instances {
					if inst.PrivateIpAddress == nil {
						continue
					}
					addr := net.JoinHostPort(*inst.PrivateIpAddress, fmt.Sprintf("%d", auto.Port))

					labels := model.LabelSet{
						model.AddressLabel: model.LabelValue(addr),
					}

					for name, v := range auto.Labels {
						if name == "" || v == "" {
							continue

						}

						labels[autoscalaLabelTag+model.LabelName(name)] = model.LabelValue(v)
					}
					tg.Targets = append(tg.Targets, labels)
				}
			}
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
// file extension. It returns full configuration autoscalas.
func (d *Discovery) readFile(filename string) ([]*Autoscala, error) {
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

	var autoscalas []*Autoscala

	switch ext := filepath.Ext(filename); strings.ToLower(ext) {
	case ".json":
		if err := json.Unmarshal(content, &autoscalas); err != nil {
			return nil, err
		}
	case ".yml", ".yaml":
		if err := yaml.UnmarshalStrict(content, &autoscalas); err != nil {
			return nil, err
		}
	default:
		panic(fmt.Errorf("discovery.File.readFile: unhandled file extension %q", ext))
	}

	for _, auto := range autoscalas {
		if auto == nil {
			err = errors.New("nil autoscala item found")
			return nil, err
		}
	}

	return autoscalas, nil
}
