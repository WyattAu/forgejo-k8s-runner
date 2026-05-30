module github.com/WyattAu/forgejo-k8s-runner

go 1.24

require (
	gitea.com/gitea/act_runner v0.2.13
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.32.0
	k8s.io/apimachinery v0.32.0
	k8s.io/client-go v0.32.0
)

// Use local replacement to swap executor
replace gitea.com/gitea/act_runner => ./act_runner
