package main

import (
	"context"
	"fmt"
	"log"
	"time"

	runnerv1 "code.gitea.io/actions-proto-go/runner/v1"
	connectcgo "connectrpc.com/connect"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/client"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	k8sClient  *kubernetes.Clientset
	k8sConfig  *config // placeholder — use k8sCfg
	k8sCli     *client.Client
	k8sCfg     struct {
		Namespace string
	}
)

func initK8s(kubeconfig string) error {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	k8sClient, err = kubernetes.NewForConfig(cfg)
	return err
}

func setK8sContext(cli *client.Client, namespace string) {
	k8sCli = cli
	k8sCfg.Namespace = namespace
}

func k8sTaskHandler(ctx context.Context, task *runnerv1.Task) error {
	taskID := task.Id
	namespace := k8sCfg.Namespace

	podName := fmt.Sprintf("forgejo-task-%d", taskID)
	log.Printf("[k8s] Creating pod %s/%s for task %d", namespace, podName, taskID)

	// Report running
	if k8sCli != nil {
		k8sCli.UpdateTask(ctx, connectcgo.NewRequest(&runnerv1.UpdateTaskRequest{
			State: &runnerv1.TaskState{Id: taskID, Result: runnerv1.Result_RESULT_RUNNING},
		}))
	}

	// Create the pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":     "forgejo-runner",
				"task-id": fmt.Sprintf("%d", taskID),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:    "runner",
					Image:   "ghcr.io/wyattau/forgejo-runner-image:latest",
					Command: []string{"tail", "-f", "/dev/null"},
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

	_, err := k8sClient.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if k8sCli != nil {
			k8sCli.UpdateTask(ctx, connectcgo.NewRequest(&runnerv1.UpdateTaskRequest{
				State: &runnerv1.TaskState{Id: taskID, Result: runnerv1.Result_RESULT_FAILURE},
			}))
		}
		return fmt.Errorf("create pod: %w", err)
	}

	// Wait for pod to complete
	success := waitForPod(ctx, namespace, podName)

	// Clean up
	defer func() {
		k8sClient.CoreV1().Pods(namespace).Delete(context.Background(), podName, metav1.DeleteOptions{})
	}()

	result := runnerv1.Result_RESULT_SUCCESS
	if !success {
		result = runnerv1.Result_RESULT_FAILURE
	}

	if k8sCli != nil {
		k8sCli.UpdateTask(ctx, connectcgo.NewRequest(&runnerv1.UpdateTaskRequest{
			State: &runnerv1.TaskState{Id: taskID, Result: result},
		}))
	}

	log.Printf("[k8s] Task %d completed (result=%v)", taskID, result)
	return nil
}

func waitForPod(ctx context.Context, namespace, name string) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Second):
			pod, err := k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				continue
			}
			switch pod.Status.Phase {
			case corev1.PodSucceeded:
				return true
			case corev1.PodFailed:
				return false
			}
		}
	}
}
