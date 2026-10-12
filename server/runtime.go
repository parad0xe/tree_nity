type State int

const (
    Running State = iota
	Terminating
	Stopped
)

type Runtime struct {
	ClientManager	*ClientManager,
	TopicManager	*TopicManager,
	State			State,
}
