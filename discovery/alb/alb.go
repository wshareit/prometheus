package alb

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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/elbv2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"

	"github.com/prometheus/prometheus/discovery/targetgroup"
)

const (
	albLabel    = model.MetaLabelPrefix + "alb_"
	albLabelTag = albLabel + "tag_"
)

var (
	patFileSDName = regexp.MustCompile(`^[^*]*(\*[^/]*)?\.(json|yml|yaml|JSON|YML|YAML)$`)

	albSDRefreshFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_sd_alb_refresh_failures_total",
			Help: "The number of ALB-SD scrape failures.",
		})
	// DefaultSDConfig is the default ALB SD configuration.
	DefaultSDConfig = SDConfig{
		RefreshInterval: model.Duration(60 * time.Second),
	}
)

// alb target group
type AlbTargetGroup struct {
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
		return fmt.Errorf("path name %q is not valid for alb file discovery", c.File)
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
	prometheus.MustRegister(albSDRefreshFailuresCount)
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
		go func(n *AlbTargetGroup) {
			if err := d.refresh(ctx, n, ch); err != nil {
				albSDRefreshFailuresCount.Inc()
				level.Error(d.logger).Log("msg", "Error refreshing alb targets", "err", err)
			}
			wg.Done()
		}(tg)
	}

	wg.Wait()
}

func (d *Discovery) refresh(ctx context.Context, atg *AlbTargetGroup, ch chan<- []*targetgroup.Group) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  *d.aws,
		Profile: d.profile,
	})
	if err != nil {
		return fmt.Errorf("could not create aws session: %s", err)
	}

	var elb2s *elbv2.ELBV2
	if d.roleARN != "" {
		creds := stscreds.NewCredentials(sess, d.roleARN)
		elb2s = elbv2.New(sess, &aws.Config{Credentials: creds})
	} else {
		elb2s = elbv2.New(sess)
	}

	var ec2s *ec2.EC2
	if d.roleARN != "" {
		creds := stscreds.NewCredentials(sess, d.roleARN)
		ec2s = ec2.New(sess, &aws.Config{Credentials: creds})
	} else {
		ec2s = ec2.New(sess)
	}

	tg := &targetgroup.Group{
		Source: atg.Name + "-" + fmt.Sprintf("%d", atg.Port),
	}

	describeTargetHealthOutput, err := elb2s.DescribeTargetHealth(&elbv2.DescribeTargetHealthInput{
		TargetGroupArn: &atg.Name,
	})
	if err != nil {
		return fmt.Errorf("could not describe target health: %s", err)
	}

	if len(describeTargetHealthOutput.TargetHealthDescriptions) > 0 {
		var ids []*string
		port := atg.Port
		if port == 0 {
			port = int(*describeTargetHealthOutput.TargetHealthDescriptions[0].Target.Port)
		}

		for _, targetHealthDescription := range describeTargetHealthOutput.TargetHealthDescriptions {
			if *targetHealthDescription.TargetHealth.State == elbv2.TargetHealthStateEnumHealthy {
				ids = append(ids, targetHealthDescription.Target.Id)
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
					addr := net.JoinHostPort(*inst.PrivateIpAddress, fmt.Sprintf("%d", port))

					labels := model.LabelSet{
						model.AddressLabel: model.LabelValue(addr),
					}

					for name, v := range atg.Labels {
						if name == "" || v == "" {
							continue
						}

						labels[albLabelTag+model.LabelName(name)] = model.LabelValue(v)
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
// file extension. It returns full configuration target groups.
func (d *Discovery) readFile(filename string) ([]*AlbTargetGroup, error) {
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

	var targetGroups []*AlbTargetGroup

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
