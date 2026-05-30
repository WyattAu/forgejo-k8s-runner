package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	runnerv1 "code.gitea.io/actions-proto-go/runner/v1"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/client"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	k8srest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

var (
	k8sClient  *kubernetes.Clientset
	k8sCli     *client.HTTPClient
	k8sNS      string
	k8sRestCfg *k8srest.Config
)

func initK8s(kubeconfig string) error {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil { return fmt.Errorf("kubeconfig: %w", err) }
	k8sRestCfg = cfg
	k8sClient, err = kubernetes.NewForConfig(cfg)
	return err
}

func setK8sContext(cli *client.HTTPClient, namespace string) {
	k8sCli = cli; k8sNS = namespace
}

func k8sTaskHandler(task *runnerv1.Task) {
	ctx := context.Background()
	taskID := task.Id
	podName := fmt.Sprintf("forgejo-task-%d", taskID)
	log.Printf("[k8s] Creating pod %s/%s for task %d", k8sNS, podName, taskID)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: k8sNS},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "runner",
				Image:   "ghcr.io/wyattau/forgejo-runner-image:latest",
				Command: []string{"tail", "-f", "/dev/null"},
			}},
		},
	}

	_, err := k8sClient.CoreV1().Pods(k8sNS).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("[k8s] Pod create failed: %v", err)
		return
	}
	defer k8sClient.CoreV1().Pods(k8sNS).Delete(context.Background(), podName, metav1.DeleteOptions{})

	if !waitForPodReady(ctx, podName) {
		log.Printf("[k8s] Pod not ready"); return
	}

	stdout, stderr, err := execInPod(ctx, podName, "runner", []string{"echo", "K8s exec works!"})
	if err != nil {
		log.Printf("[k8s] Exec failed: %v", err)
		return
	}
	log.Printf("[k8s] Exec OK: %s %s", stdout, stderr)
}

func waitForPodReady(ctx context.Context, name string) bool {
	for i := 0; i < 60; i++ {
		time.Sleep(2 * time.Second)
		pod, err := k8sClient.CoreV1().Pods(k8sNS).Get(ctx, name, metav1.GetOptions{})
		if err != nil { continue }
		for _, c := range pod.Status.ContainerStatuses {
			if c.Name == "runner" && c.Ready { return true }
		}
	}
	return false
}

func execInPod(ctx context.Context, podName, container string, cmd []string) (string, string, error) {
	req := k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(k8sNS).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container, Command: cmd,
			Stdout: true, Stderr: true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(k8sRestCfg, "POST", req.URL())
	if err != nil { return "", "", fmt.Errorf("executor: %w", err) }

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout, Stderr: &stderr,
	})
	return stdout.String(), stderr.String(), err
}
