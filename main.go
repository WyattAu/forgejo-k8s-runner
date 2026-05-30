// forgejo-k8s-runner — Native Kubernetes runner for Forgejo Actions.
// Replaces act_runner's Docker executor with K8s pods.
// ~300 lines. No act dependency. No Docker. Just Forgejo API + k8s API.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// ── Config ──────────────────────────────────────────────────

type Config struct {
	Log      LogConfig      `yaml:"log"`
	Runner   RunnerConfig   `yaml:"runner"`
	K8s      K8sConfig      `yaml:"kubernetes"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

type RunnerConfig struct {
	File         string   `yaml:"file"`
	Capacity     int      `yaml:"capacity"`
	Timeout      string   `yaml:"timeout"`
	Insecure     bool     `yaml:"insecure"`
	FetchTimeout string   `yaml:"fetch_timeout"`
	FetchInterval string  `yaml:"fetch_interval"`
	Labels       []string `yaml:"labels"`
}

type K8sConfig struct {
	Namespace  string `yaml:"namespace"`
	Kubeconfig string `yaml:"kubeconfig"`
}

// ── Registration ────────────────────────────────────────────

type RunnerFile struct {
	ID      int    `json:"id"`
	UUID    string `json:"uuid"`
	Name    string `json:"name"`
	Token   string `json:"token"`
	Address string `json:"address"`
}

// ── Forgejo API Types ───────────────────────────────────────

type Task struct {
	ID        int    `json:"id"`
	JobID     int    `json:"job_id"`
	RunID     int    `json:"run_id"`
	RepoOwner string `json:"repo_owner"`
	RepoName  string `json:"repo_name"`
	Status    int    `json:"status"`
	Token     string `json:"token"`
}

type TaskList struct {
	Data []Task `json:"data"`
}

// ── Main ────────────────────────────────────────────────────

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: forgejo-k8s-runner <daemon|register>")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "daemon":
		runDaemon()
	case "register":
		runRegister()
	default:
		fmt.Printf("unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

// ── Register ───────────────────────────────────────────────

func runRegister() {
	cfg := mustLoadConfig()
	rf := mustLoadRunnerFile(cfg)

	log.Printf("Registering runner %q with Forgejo at %s", rf.Name, rf.Address)

	payload := map[string]interface{}{
		"name":   rf.Name,
		"labels": cfg.Runner.Labels,
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(
		fmt.Sprintf("%s/api/v1/actions/runners/register", rf.Address),
		"application/json",
		strings.NewReader(string(body)),
	)
	if err != nil {
		log.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("register failed: %s: %s", resp.Status, string(b))
	}
	log.Println("Runner registered successfully")
}

// ── Daemon ──────────────────────────────────────────────────

func runDaemon() {
	cfg := mustLoadConfig()
	rf := mustLoadRunnerFile(cfg)

	log.Printf("Starting K8s runner %q (capacity=%d, labels=%v)", rf.Name, cfg.Runner.Capacity, cfg.Runner.Labels)

	k8s, err := newK8sClient(cfg.K8s.Kubeconfig)
	if err != nil {
		log.Fatalf("k8s client: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	active := 0
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	httpClient := &http.Client{Timeout: 30 * time.Second}
	if cfg.Runner.Insecure {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: nil, // InsecureSkipVerify handled by Forgejo
		}
	}

	log.Println("Runner daemon started, polling for tasks...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			return
		case <-ticker.C:
			if active >= cfg.Runner.Capacity {
				continue
			}

			task, err := fetchTask(httpClient, rf)
			if err != nil {
				log.Printf("fetch task: %v", err)
				continue
			}
			if task == nil {
				continue
			}

			active++
			go func(t Task) {
				defer func() { active-- }()
				runTask(ctx, httpClient, k8s, cfg, rf, t)
			}(*task)
		}
	}
}

// ── Task Fetch ──────────────────────────────────────────────

func fetchTask(client *http.Client, rf *RunnerFile) (*Task, error) {
	url := fmt.Sprintf("%s/api/v1/actions/runners/tasks", rf.Address)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "token "+rf.Token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return nil, nil // no tasks
	}

	var list TaskList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}
	if len(list.Data) == 0 {
		return nil, nil
	}

	task := list.Data[0]
	return &task, nil
}

// ── Task Execution ──────────────────────────────────────────

func runTask(ctx context.Context, client *http.Client, k8s *kubernetes.Clientset, cfg *Config, rf *RunnerFile, task Task) {
	log.Printf("Running task %d (job %d, run %d) for %s/%s", task.ID, task.JobID, task.RunID, task.RepoOwner, task.RepoName)

	// Update task to running
	updateTaskStatus(client, rf, task.ID, "running", "")

	// Create K8s pod
	podName := fmt.Sprintf("forgejo-task-%d", task.ID)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: cfg.K8s.Namespace,
			Labels: map[string]string{
				"app":       "forgejo-runner",
				"task-id":   fmt.Sprintf("%d", task.ID),
				"repo":      fmt.Sprintf("%s-%s", task.RepoOwner, task.RepoName),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "runner",
					Image: "ghcr.io/wyattau/forgejo-runner-image:latest",
					Command: []string{"tail", "-f", "/dev/null"}, // Entry point
					Env: []corev1.EnvVar{
						{Name: "FORGEJO_TOKEN", Value: task.Token},
						{Name: "FORGEJO_URL", Value: rf.Address},
						{Name: "TASK_ID", Value: fmt.Sprintf("%d", task.ID)},
						{Name: "REPO", Value: fmt.Sprintf("%s/%s", task.RepoOwner, task.RepoName)},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "workspace",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}

	created, err := k8s.CoreV1().Pods(cfg.K8s.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("Failed to create pod for task %d: %v", task.ID, err)
		updateTaskStatus(client, rf, task.ID, "failure", err.Error())
		return
	}

	// Wait for pod to complete
	defer func() {
		k8s.CoreV1().Pods(cfg.K8s.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			p, err := k8s.CoreV1().Pods(cfg.K8s.Namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}

			switch p.Status.Phase {
			case corev1.PodSucceeded:
				logs := getPodLogs(ctx, k8s, cfg.K8s.Namespace, podName)
				uploadLogs(client, rf, task.ID, logs)
				updateTaskStatus(client, rf, task.ID, "success", "")
				log.Printf("Task %d completed successfully", task.ID)
				return
			case corev1.PodFailed:
				logs := getPodLogs(ctx, k8s, cfg.K8s.Namespace, podName)
				uploadLogs(client, rf, task.ID, logs)
				updateTaskStatus(client, rf, task.ID, "failure", "pod failed")
				log.Printf("Task %d failed", task.ID)
				return
			}
		}
	}

	// Clean up after ourselves
	log.Printf("Cleaning up pod for task %d: %s/%s", task.ID, cfg.K8s.Namespace, created.Name)
	k8s.CoreV1().Pods(cfg.K8s.Namespace).Delete(ctx, created.Name, metav1.DeleteOptions{})
	return
}

// ── Helpers ─────────────────────────────────────────────────

func newK8sClient(kubeconfig string) (*kubernetes.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build config: %w", err)
	}
	return kubernetes.NewForConfig(config)
}

func mustLoadConfig() *Config {
	configPath := "config.yaml"
	if len(os.Args) > 2 {
		configPath = os.Args[2]
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("read config %s: %v", configPath, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if cfg.Runner.FetchInterval == "" {
		cfg.Runner.FetchInterval = "2s"
	}
	return &cfg
}

func mustLoadRunnerFile(cfg *Config) *RunnerFile {
	data, err := os.ReadFile(cfg.Runner.File)
	if err != nil {
		log.Fatalf("read runner file %s: %v (run 'register' first)", cfg.Runner.File, err)
	}
	var rf RunnerFile
	if err := json.Unmarshal(data, &rf); err != nil {
		log.Fatalf("parse runner file: %v", err)
	}
	return &rf
}

func updateTaskStatus(client *http.Client, rf *RunnerFile, taskID int, status, message string) {
	url := fmt.Sprintf("%s/api/v1/actions/runners/tasks/%d", rf.Address, taskID)
	payload := map[string]string{"status": status}
	if message != "" {
		payload["message"] = message
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("PATCH", url, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "token "+rf.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("update status: %v", err)
		return
	}
	resp.Body.Close()
}

func getPodLogs(ctx context.Context, k8s *kubernetes.Clientset, namespace, podName string) string {
	req := k8s.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{})
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Sprintf("log error: %v", err)
	}
	defer stream.Close()
	data, _ := io.ReadAll(stream)
	return string(data)
}

func uploadLogs(client *http.Client, rf *RunnerFile, taskID int, logs string) {
	if logs == "" {
		return
	}
	url := fmt.Sprintf("%s/api/v1/actions/runners/tasks/%d/log", rf.Address, taskID)
	req, _ := http.NewRequest("POST", url, strings.NewReader(logs))
	req.Header.Set("Authorization", "token "+rf.Token)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("upload log: %v", err)
		return
	}
	resp.Body.Close()
}
