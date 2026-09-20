package deploy_test

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v2"
)

type exampleObject = map[interface{}]interface{}

func decodeExample(t *testing.T) []exampleObject {
	t.Helper()
	data, err := os.ReadFile("pooled-cloud.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var docs []exampleObject
	for _, part := range strings.Split(string(data), "\n---\n") {
		var doc exampleObject
		if err := yaml.UnmarshalStrict([]byte(part), &doc); err != nil {
			t.Fatal(err)
		}
		docs = append(docs, doc)
	}
	return docs
}

func exampleAt(m exampleObject, path ...string) exampleObject {
	for _, key := range path {
		next, _ := m[key].(exampleObject)
		m = next
		if m == nil {
			return nil
		}
	}
	return m
}

func exampleList(m exampleObject, key string) []interface{} {
	v, _ := m[key].([]interface{})
	return v
}

func exampleNamed(items []interface{}, name string) exampleObject {
	for _, item := range items {
		m, _ := item.(exampleObject)
		if m["name"] == name {
			return m
		}
	}
	return nil
}

func document(docs []exampleObject, kind string) exampleObject {
	for _, doc := range docs {
		if doc["kind"] == kind {
			return doc
		}
	}
	return nil
}

// checkExample keeps the deployment example's operational safety properties
// structural: equivalent YAML spelling must not bypass a text search.
func checkExample(docs []exampleObject) string {
	if len(docs) != 3 {
		return "expected Deployment, Service, HPA"
	}
	for _, doc := range docs {
		if doc["kind"] != "Deployment" && doc["kind"] != "Service" && doc["kind"] != "HorizontalPodAutoscaler" {
			return "unapproved or secret-bearing resource"
		}
	}
	deploy, service, hpa := document(docs, "Deployment"), document(docs, "Service"), document(docs, "HorizontalPodAutoscaler")
	if deploy == nil || service == nil || hpa == nil {
		return "missing deployment resource"
	}
	pod := exampleAt(deploy, "spec", "template", "spec")
	containers := exampleList(pod, "containers")
	if len(containers) != 1 {
		return "expected one Host container"
	}
	c, _ := containers[0].(exampleObject)
	if c == nil {
		return "invalid Host container"
	}
	if serviceSpec := exampleAt(service, "spec"); serviceSpec["clusterIP"] != "None" || serviceSpec["type"] == "LoadBalancer" || serviceSpec["type"] == "NodePort" || serviceSpec["externalIPs"] != nil {
		return "public or load-balanced HostLink"
	}
	if exampleAt(c, "readinessProbe", "httpGet")["path"] != "/readyz" || exampleAt(c, "livenessProbe", "httpGet")["path"] != "/healthz" {
		return "readiness and liveness probes must be distinct"
	}
	env := exampleList(c, "env")
	values := make(map[string]string)
	for _, item := range env {
		e, _ := item.(exampleObject)
		name, _ := e["name"].(string)
		if _, duplicate := values[name]; duplicate {
			return "duplicate environment variable"
		}
		values[name], _ = e["value"].(string)
		lower := strings.ToLower(name)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "credential") || strings.Contains(lower, "token") {
			if exampleAt(e, "valueFrom", "secretKeyRef")["name"] == nil || e["value"] != nil {
				return "credential committed in YAML"
			}
		}
	}
	if exampleAt(exampleNamed(env, "POD_IP"), "valueFrom", "fieldRef")["fieldPath"] != "status.podIP" || exampleAt(exampleNamed(env, "HOST_ID"), "valueFrom", "fieldRef")["fieldPath"] != "metadata.name" || values["HOST_INTERNAL_ENDPOINT"] != "ws://$(POD_IP):7100" {
		return "HostLink base must identify this Pod"
	}
	for _, key := range []string{"HOST_CAPACITY", "HOST_COMMAND_QUEUE_SIZE", "HOST_RECONCILE_BATCH", "HOST_MAX_BINDINGS", "HOST_MAX_BINDINGS_PER_LINK", "HOST_MAX_TENANT_LINKS"} {
		n, err := strconv.Atoi(values[key])
		if err != nil || n < 1 || n > 64 {
			return "missing or unbounded " + key
		}
	}
	resources := exampleAt(c, "resources")
	for _, key := range []string{"requests", "limits"} {
		if exampleAt(resources, key)["cpu"] == nil || exampleAt(resources, key)["memory"] == nil {
			return "missing resource requests or limits"
		}
	}
	grace, err := time.ParseDuration(values["HOST_DRAIN_GRACE"])
	if err != nil {
		return "missing drain grace"
	}
	seconds, ok := pod["terminationGracePeriodSeconds"].(int)
	if !ok || time.Duration(seconds)*time.Second < grace+15*time.Second {
		return "unsafe termination timing"
	}
	if exampleAt(hpa, "spec")["maxReplicas"] == nil {
		return "unbounded autoscaling"
	}
	return ""
}

func TestPooledCloudExample(t *testing.T) {
	if problem := checkExample(decodeExample(t)); problem != "" {
		t.Fatal(problem)
	}
}

func TestPooledCloudExampleRejectsUnsafeChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]exampleObject)
	}{
		{"committed credential", func(d []exampleObject) {
			exampleNamed(exampleList(hostContainer(d), "env"), "PRODUCT_HOSTLINK_CREDENTIAL")["value"] = "real-credential"
		}},
		{"public HostLink", func(d []exampleObject) { exampleAt(document(d, "Service"), "spec")["type"] = "LoadBalancer" }},
		{"shared HostLink base", func(d []exampleObject) {
			exampleNamed(exampleList(hostContainer(d), "env"), "HOST_INTERNAL_ENDPOINT")["value"] = "ws://pooled-host:7100"
		}},
		{"unbounded queue", func(d []exampleObject) {
			exampleNamed(exampleList(hostContainer(d), "env"), "HOST_COMMAND_QUEUE_SIZE")["value"] = "0"
		}},
		{"missing limits", func(d []exampleObject) { delete(exampleAt(hostContainer(d), "resources", "limits"), "memory") }},
		{"unsafe timing", func(d []exampleObject) {
			exampleAt(document(d, "Deployment"), "spec", "template", "spec")["terminationGracePeriodSeconds"] = 60
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docs := decodeExample(t)
			test.mutate(docs)
			if problem := checkExample(docs); problem == "" {
				t.Fatal("unsafe mutation accepted")
			}
		})
	}
}

func hostContainer(docs []exampleObject) exampleObject {
	items := exampleList(exampleAt(document(docs, "Deployment"), "spec", "template", "spec"), "containers")
	c, _ := items[0].(exampleObject)
	return c
}
