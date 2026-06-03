package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	runnerv1 "code.gitea.io/actions-proto-go/runner/v1"
	connectcgo "connectrpc.com/connect"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/client"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var k8sClient *kubernetes.Clientset
var k8sCli *client.HTTPClient
var k8sNS string

func initK8s(kc string) error {
	c, e := clientcmd.BuildConfigFromFlags("", kc)
	if e != nil { return e }
	k8sClient, e = kubernetes.NewForConfig(c)
	return e
}
func setK8sContext(cli *client.HTTPClient, ns string) { k8sCli = cli; k8sNS = ns }

type workflowJob struct {
	Name    string
	RunsOn  string            `yaml:"runs-on"`
	Timeout interface{}       `yaml:"timeout-minutes"`
	Steps   []step            `yaml:"steps"`
	Env     map[string]string `yaml:"env"`
	Defaults struct {
		Run struct{ WorkingDirectory string `yaml:"working-directory"` } `yaml:"run"`
	} `yaml:"defaults"`
	Container struct{ Image string `yaml:"image"` } `yaml:"container"`
}
type step struct {
	Name             string
	Uses             string                 `yaml:"uses"`
	Run              string                 `yaml:"run"`
	Env              map[string]string      `yaml:"env"`
	WorkingDirectory string                 `yaml:"working-directory"`
	Shell            string                 `yaml:"shell"`
	With             map[string]interface{} `yaml:"with"`
}
type workflowDoc struct{ Jobs map[string]workflowJob `yaml:"jobs"` }

var templateRe = regexp.MustCompile(`\$\{\{\s*([^}]+)\s*\}\}`)
var stepGroupRe = regexp.MustCompile(`::group::Step (\d+):`)
var errorRe = regexp.MustCompile(`::error::(Step failed|.+)?`)

func k8sTaskHandler(ctx context.Context, task *runnerv1.Task) {
	tid := task.Id
	repo := task.Context.Fields["repository"].GetStringValue()

	// Get secrets from task.Secrets
	secrets := make(map[string]string)
	for k, v := range task.Secrets { secrets[k] = v }

	job, err := parseJob(task.WorkflowPayload)
	if err != nil { log.Printf("[k8s] Parse: %v", err); return }
	log.Printf("[k8s] %s secrets=%d", job.Name, len(secrets))

	token := task.Context.Fields["token"].GetStringValue()
	serverURL := k8sCli.Address()
	wsURL := fmt.Sprintf("%s/%s.git", serverURL, repo)
	if token != "" {
		u, _ := url.Parse(serverURL)
		u.User = url.UserPassword("oauth2", token)
		wsURL = fmt.Sprintf("%s/%s.git", u.String(), repo)
	}

	wsPath := "/workspace/" + repo
	script := generateScript(job, wsURL, wsPath, task, secrets)
	log.Printf("[k8s] %s steps=%d", job.Name, len(job.Steps))

	image := job.Container.Image
	if image == "" { image = "ghcr.io/wyattau/forgejo-runner-nix:latest" }

	name := fmt.Sprintf("forgejo-task-%d", tid)
	root := int64(0)
	dockerGid := int64(999)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k8sNS},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{RunAsUser: &root, RunAsGroup: &dockerGid, SupplementalGroups: []int64{999}},
			Containers: []corev1.Container{{
				Name:            "runner",
				Image:           image,
				SecurityContext: &corev1.SecurityContext{RunAsUser: &root, RunAsGroup: &dockerGid},
				Command:         []string{"/bin/bash", "-c", script},
				Env: []corev1.EnvVar{
					{Name: "PATH", Value: "/nix/var/nix/profiles/default/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/.bun/bin"},
					{Name: "HOME", Value: "/root"},
					{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
					{Name: "GITHUB_PATH", Value: "/dev/null"},
					{Name: "GITHUB_ENV", Value: "/dev/null"},
					{Name: "GITHUB_OUTPUT", Value: "/dev/null"},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "ws", MountPath: "/workspace"},
					{Name: "nix", MountPath: "/nix"},
					{Name: "docker", MountPath: "/var/run/docker.sock"},
					{Name: "dockercfg", MountPath: "/root/.docker"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "ws", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "nix", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: "/mnt/pool_HDD_x2/infra/nix"},
				}},
				{Name: "docker", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: "/var/run/docker.sock"},
				}},
				{Name: "dockercfg", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: "/root/.docker"},
				}},
			},
		},
	}

	_, err = k8sClient.CoreV1().Pods(k8sNS).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil { log.Printf("[k8s] Create: %v", err); return }
	defer k8sClient.CoreV1().Pods(k8sNS).Delete(context.Background(), name, metav1.DeleteOptions{})

	startTime := time.Now()
	// Send initial UpdateTask with empty step states so Forgejo knows step count
	initSteps := make([]*runnerv1.StepState, len(job.Steps))
	for i := range job.Steps {
		initSteps[i] = &runnerv1.StepState{Id: int64(i)}
	}
	if _, err := k8sCli.UpdateTask(ctx, connectcgo.NewRequest(&runnerv1.UpdateTaskRequest{
		State: &runnerv1.TaskState{
			Id: tid, StartedAt: timestamppb.New(startTime),
			Steps: initSteps,
		},
	})); err != nil {
		log.Printf("[k8s] UpdateTask(start) error: %v", err)
	}

	ok := waitForPod(ctx, name)
	logs := getPodLogs(ctx, name)
	log.Printf("[k8s] %s ok=%v log=%d", job.Name, ok, len(logs))

	// Build secret replacer for masking
	var maskPairs []string
	for _, v := range secrets {
		if len(v) > 3 {
			maskPairs = append(maskPairs, v, "***")
		}
	}
	secretReplacer := strings.NewReplacer(maskPairs...)

	// Split logs by line and stream one LogRow per line
	lines := strings.Split(logs, "\n")
	logOffset := 0
	stepBoundaries := make(map[int]int) // step index -> log line index

	for i := 0; i < len(lines); i += 50 {
		end := i + 50
		if end > len(lines) { end = len(lines) }
		rows := make([]*runnerv1.LogRow, 0, end-i)
		for j := i; j < end; j++ {
			content := secretReplacer.Replace(lines[j])
			// Detect step boundaries from ##[group]Step N: annotations
			if m := stepGroupRe.FindStringSubmatch(content); m != nil {
				var stepNum int
				fmt.Sscanf(m[1], "%d", &stepNum)
				if stepNum > 0 && stepNum <= len(job.Steps) {
					stepBoundaries[stepNum-1] = logOffset + (j - i)
				}
			}
			rows = append(rows, &runnerv1.LogRow{
				Time:    timestamppb.Now(),
				Content: content,
			})
		}
		k8sCli.UpdateLog(ctx, connectcgo.NewRequest(&runnerv1.UpdateLogRequest{
			TaskId: tid, Index: int64(i),
			Rows:   rows, NoMore: end == len(lines),
		}))
		logOffset += len(rows)
	}

	log.Printf("[k8s] %s boundaries=%d lines=%d", job.Name, len(stepBoundaries), logOffset)

	// Build step states with per-step failure detection from ::error:: annotations
	now := time.Now()
	duration := now.Sub(startTime)

	// Calculate per-step log ranges for error detection
	stepStarts := make([]int64, len(job.Steps))
	stepEnds := make([]int64, len(job.Steps))
	for i := range job.Steps {
		stepStarts[i] = 0
		stepEnds[i] = int64(logOffset)
		if i > 0 {
			if s, ok := stepBoundaries[i]; ok { stepStarts[i] = int64(s) }
		}
		if i < len(job.Steps)-1 {
			if e, ok := stepBoundaries[i+1]; ok { stepEnds[i] = int64(e) }
		}
	}

	// Detect per-step failure: scan each step's log lines for ::error::
	stepResults := make([]runnerv1.Result, len(job.Steps))
	for i := range stepResults {
		stepResults[i] = runnerv1.Result_RESULT_SUCCESS
	}
	for i := range job.Steps {
		end := stepEnds[i]
		if end > int64(len(lines)) { end = int64(len(lines)) }
		for j := stepStarts[i]; j < end; j++ {
			if errorRe.MatchString(lines[j]) {
				stepResults[i] = runnerv1.Result_RESULT_FAILURE
				break
			}
		}
	}

	steps := make([]*runnerv1.StepState, len(job.Steps))
	for i := range job.Steps {
		logIdx := stepStarts[i]
		logLen := stepEnds[i] - stepStarts[i]
		if logLen < 1 { logLen = 1 }
		// Truncate last step to exclude trailing ::notice::DONE
		if i == len(job.Steps)-1 {
			for j := logIdx + logLen - 1; j >= logIdx; j-- {
				if strings.Contains(lines[j], "::notice::DONE") {
					logLen = j - logIdx
					if logLen < 1 { logLen = 1 }
					break
				}
			}
		}
		// Estimate per-step timing: proportional to log position
		stepStart := startTime
		stepEnd := now
		if logOffset > 0 {
			fraction := float64(logIdx) / float64(logOffset)
			stepStart = startTime.Add(time.Duration(fraction * float64(duration)))
			if i < len(job.Steps)-1 {
				nextFraction := float64(logIdx+logLen) / float64(logOffset)
				stepEnd = startTime.Add(time.Duration(nextFraction * float64(duration)))
			}
		}
		steps[i] = &runnerv1.StepState{
			Id: int64(i), Result: stepResults[i],
			StartedAt: timestamppb.New(stepStart),
			StoppedAt: timestamppb.New(stepEnd),
			LogIndex:  logIdx,
			LogLength: logLen,
		}
		log.Printf("[k8s]   step %d: idx=%d len=%d result=%v start=%v end=%v", i, logIdx, logLen, stepResults[i], stepStart.Format("15:04:05"), stepEnd.Format("15:04:05"))
	}

	// Overall result: failed if pod failed or any step has ::error::
	overallResult := runnerv1.Result_RESULT_SUCCESS
	if !ok { overallResult = runnerv1.Result_RESULT_FAILURE }
	for _, sr := range stepResults {
		if sr == runnerv1.Result_RESULT_FAILURE { overallResult = runnerv1.Result_RESULT_FAILURE }
	}

	k8sCli.UpdateTask(ctx, connectcgo.NewRequest(&runnerv1.UpdateTaskRequest{
		State: &runnerv1.TaskState{
			Id: tid, Result: overallResult,
			StartedAt: timestamppb.New(startTime),
			StoppedAt: timestamppb.New(now),
			Steps: steps,
		},
	}))
	if err != nil {
		log.Printf("[k8s] UpdateTask(final) error: %v", err)
	} else {
		log.Printf("[k8s] %s result=%v steps=%d", job.Name, overallResult, len(steps))
	}
}

func parseJob(payload []byte) (*workflowJob, error) {
	var doc workflowDoc
	if err := yaml.Unmarshal(payload, &doc); err != nil { return nil, err }
	for _, j := range doc.Jobs { return &j, nil }
	return nil, fmt.Errorf("no jobs")
}

func generateScript(job *workflowJob, repoURL, wsPath string, task *runnerv1.Task, secrets map[string]string) string {
	var b strings.Builder
	b.WriteString("set +e\n")  // per-step: steps handle their own errors
	b.WriteString("echo '::group::Clone repo'\n")
	b.WriteString(fmt.Sprintf("mkdir -p %s && git clone --depth 1 %s %s 2>&1\n", wsPath, repoURL, wsPath))
	b.WriteString("echo '::endgroup::'\n")
	for k, v := range job.Env {
		resolved := resolveTemplates(v, task, secrets)
		b.WriteString(fmt.Sprintf("export %s='%s'\n", k, strings.ReplaceAll(resolved, "'", "'\\''")))
		if strings.HasPrefix(v, "${{") { log.Printf("[k8s] env %s: %s -> %s", k, v, resolved) }
	}
	dwd := job.Defaults.Run.WorkingDirectory
	for i, s := range job.Steps {
		nm := s.Name; if nm == "" { nm = s.Uses }
		b.WriteString(fmt.Sprintf("echo '::group::Step %d: %s'\n", i+1, nm))
		for k, v := range s.Env {
			resolved := resolveTemplates(v, task, secrets)
			// Use heredoc to safely pass any value (avoids shell escaping pitfalls)
			b.WriteString(fmt.Sprintf("export %s=\"$(cat <<'ENVEOF'\n%s\nENVEOF\n)\"\n", k, resolved))
			if strings.HasPrefix(v, "${{") { log.Printf("[k8s] step-env %s: -> %s", k, resolved) }
		}
		// Debug: verify env vars are set and have content
		if len(s.Env) > 0 {
			b.WriteString("echo '[DEBUG-ENV]'\n")
			for k := range s.Env {
				b.WriteString(fmt.Sprintf("echo '  %s len='${#%s}\n", k, k))
			}
		}
		swd := s.WorkingDirectory; if swd == "" { swd = dwd }
		tgt := wsPath; if swd != "" { tgt = wsPath + "/" + swd }
		b.WriteString(fmt.Sprintf("cd %s\n", tgt))
		if s.Run != "" {
			sh := s.Shell; if sh == "" { sh = "bash" }
			// Run with set -e, capture exit code, but don't kill the script
			b.WriteString("(\nset -e\n" + resolveTemplates(s.Run, task, secrets) + "\n) || echo '::error::Step failed'\n")
		} else if s.Uses != "" {
			if strings.Contains(s.Uses, "actions/checkout") {
				b.WriteString("echo checkout:done\n")
			} else if strings.Contains(s.Uses, "setup-bun") || strings.Contains(s.Uses, "oven-sh") {
				b.WriteString("echo 'Installing bun...'\n")
				b.WriteString("curl -fsSL https://bun.sh/install | bash 2>&1\n")
				b.WriteString("export BUN_INSTALL=\"$HOME/.bun\"\n")
				b.WriteString("export PATH=\"$HOME/.bun/bin:$PATH\"\n")
			} else {
				b.WriteString(fmt.Sprintf("echo '[WARN] unhandled: %s'\n", s.Uses))
			}
		}
		b.WriteString("echo '::endgroup::'\n")
	}
	b.WriteString("echo ::notice::DONE\n")
	return b.String()
}

func resolveTemplates(s string, task *runnerv1.Task, secrets map[string]string) string {
	return templateRe.ReplaceAllStringFunc(s, func(match string) string {
		inner := templateRe.FindStringSubmatch(match)[1]
		inner = strings.TrimSpace(inner)
		if strings.HasPrefix(inner, "secrets.") {
			key := strings.TrimPrefix(inner, "secrets.")
			parts := strings.SplitN(key, "||", 2)
			if v, ok := secrets[strings.TrimSpace(parts[0])]; ok { return v }
			if len(parts) == 2 {
				return strings.Trim(strings.TrimSpace(parts[1]), "'\"")
			}
			return match
		}
		if strings.HasPrefix(inner, "env.") {
			if v, ok := task.Context.Fields[strings.TrimPrefix(inner, "env.")]; ok {
				return v.GetStringValue()
			}
			return match
		}
		return match
	})
}

func waitForPod(ctx context.Context, name string) bool {
	for i := 0; i < 720; i++ {
		time.Sleep(5 * time.Second)
		p, e := k8sClient.CoreV1().Pods(k8sNS).Get(ctx, name, metav1.GetOptions{})
		if e != nil { continue }
		if p.Status.Phase == corev1.PodSucceeded { return true }
		if p.Status.Phase == corev1.PodFailed { return false }
	}
	return false
}
func getPodLogs(ctx context.Context, name string) string {
	req := k8sClient.CoreV1().Pods(k8sNS).GetLogs(name, &corev1.PodLogOptions{})
	s, e := req.Stream(ctx)
	if e != nil { return fmt.Sprintf("ERR:%v", e) }
	defer s.Close()
	buf := new(bytes.Buffer); buf.ReadFrom(s)
	return buf.String()
}
