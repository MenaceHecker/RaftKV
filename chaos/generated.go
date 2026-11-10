package chaos

import (
	"fmt"
	"math/rand"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The generated scenario follows RUNBOOK 8.4 (a randomized nemesis schedule
// drawn from the seed that always ends in a period of full healing, after which
// liveness is checked) and RUNBOOK 8.5 (concurrent clients, 5-20 keys, unique
// write values). The op mix is 50% read and 50% write: the simulated cluster has
// no cas or delete operations, so the 40/40/10/10 mix is not reachable yet.
//
// Every random choice comes from a rand.Rand seeded from the scenario seed
// through its own salt. None of them reads the cluster's or the network's
// generator, so adding this scenario does not move any other scenario's stream.

const (
	generatedNodes = 5

	scheduleSalt int64 = 0x5eed5c4ed01e
	shapeSalt    int64 = 0x5a4e0b7c1d33
	workloadSalt int64 = 0x0a11c11e47a5

	// clientPatience is how many ticks a client waits for an answer before it
	// stops waiting and issues its next operation. The abandoned operation stays
	// in the history as indeterminate (RUNBOOK 8.2).
	clientPatience = 80
)

// StepKind names one nemesis action in a generated schedule (RUNBOOK 8.4).
type StepKind int

// The step kinds a generated schedule draws from.
const (
	// StepPartitionLeader cuts the current leader off from every other node.
	StepPartitionLeader StepKind = iota
	// StepSplit splits the cluster in two; Node is the size of the first group.
	StepSplit
	// StepIsolate cuts Node off from every other node.
	StepIsolate
	// StepCrash stops Node, losing its volatile state.
	StepCrash
	// StepRestart restarts Node if it is down and does nothing otherwise.
	StepRestart
	// StepSetFaults replaces the network's loss, delay and duplication.
	StepSetFaults
	// StepCompact snapshots and compacts Node's log.
	StepCompact
	// StepHeal removes every partition.
	StepHeal
	// StepWork lets the clients run for Ticks ticks.
	StepWork
)

// Step is one entry of a generated schedule. Node is a 1-based node id.
type Step struct {
	Kind     StepKind
	Node     raft.NodeID
	Loss     float64
	Dup      float64
	MinDelay int
	MaxDelay int
	Ticks    int
}

func randomFaults(rng *rand.Rand) Step {
	minDelay := rng.Intn(2)
	return Step{
		Kind:     StepSetFaults,
		Loss:     []float64{0, 0.05, 0.1, 0.2}[rng.Intn(4)],
		Dup:      []float64{0, 0.05, 0.1}[rng.Intn(3)],
		MinDelay: minDelay,
		MaxDelay: minDelay + rng.Intn(6),
	}
}

// Schedule derives the nemesis schedule for a seed (RUNBOOK 8.4). It is a pure
// function of the seed. The schedule always starts with message loss and a
// leader partition, so every run is adverse, and always ends with a heal and a
// restart of every node.
func Schedule(seed int64) []Step {
	rng := rand.New(rand.NewSource(seed ^ scheduleSalt)) //nolint:gosec // seeded simulation RNG, not security

	steps := []Step{
		{Kind: StepSetFaults, Loss: 0.05 + 0.05*float64(rng.Intn(3)), MaxDelay: 1 + rng.Intn(3)},
		{Kind: StepWork, Ticks: 30 + rng.Intn(30)},
	}

	crashed := map[raft.NodeID]bool{}
	anyNode := func() raft.NodeID { return raft.NodeID(1 + rng.Uint64()%generatedNodes) }

	phases := 6 + rng.Intn(5)
	for i := range phases {
		kind := rng.Intn(7)
		if i == 0 {
			kind = 0
		}

		switch kind {
		case 0:
			steps = append(steps, Step{Kind: StepPartitionLeader})
		case 1:
			steps = append(steps, Step{Kind: StepSplit, Node: raft.NodeID(2 + rng.Uint64()%2)})
		case 2:
			steps = append(steps, Step{Kind: StepIsolate, Node: anyNode()})
		case 3:
			if len(crashed) >= 2 {
				steps = append(steps, Step{Kind: StepHeal})
				break
			}
			id := anyNode()
			for crashed[id] {
				id = anyNode()
			}
			crashed[id] = true
			steps = append(steps, Step{Kind: StepCrash, Node: id})
		case 4:
			steps = append(steps, randomFaults(rng))
		case 5:
			steps = append(steps, Step{Kind: StepCompact, Node: anyNode()})
		default:
			restarted := false
			for id := raft.NodeID(1); id <= generatedNodes; id++ {
				if crashed[id] {
					delete(crashed, id)
					steps = append(steps, Step{Kind: StepRestart, Node: id})
					restarted = true
					break
				}
			}
			if !restarted {
				steps = append(steps, Step{Kind: StepHeal})
			}
		}

		steps = append(steps, Step{Kind: StepWork, Ticks: 30 + rng.Intn(50)})
		if rng.Intn(2) == 0 {
			steps = append(steps, Step{Kind: StepHeal}, Step{Kind: StepWork, Ticks: 20 + rng.Intn(30)})
		}
	}

	// Full healing: no faults, no partition, every node running (RUNBOOK 8.4).
	steps = append(steps, Step{Kind: StepSetFaults}, Step{Kind: StepWork, Ticks: 30}, Step{Kind: StepHeal})
	for id := raft.NodeID(1); id <= generatedNodes; id++ {
		steps = append(steps, Step{Kind: StepRestart, Node: id})
	}
	return steps
}

// workloadShape draws the client and key counts for a seed (RUNBOOK 8.5: a few
// concurrent clients over a small key space to force contention).
func workloadShape(seed int64) (clients, keys int) {
	rng := rand.New(rand.NewSource(seed ^ shapeSalt)) //nolint:gosec // seeded simulation RNG, not security
	return 3 + rng.Intn(3), 5 + rng.Intn(16)
}

// workload drives concurrent clients, each with at most one operation it is
// waiting on.
type workload struct {
	rng     *rand.Rand
	keys    int
	current []*Op
	counter int
}

func newWorkload(seed int64, clients, keys int) *workload {
	return &workload{
		rng:     rand.New(rand.NewSource(seed ^ workloadSalt)), //nolint:gosec // seeded simulation RNG, not security
		keys:    keys,
		current: make([]*Op, clients),
	}
}

// tick lets idle clients issue an operation, then advances the cluster.
func (w *workload) tick(c *Cluster) error {
	for client, op := range w.current {
		if op != nil && (op.Status != StatusPending || c.Now()-op.Invoked > clientPatience) {
			op = nil
		}
		if op == nil && w.rng.Intn(3) == 0 {
			key := fmt.Sprintf("k%d", w.rng.Intn(w.keys))
			if w.rng.Intn(2) == 0 {
				w.counter++
				op = c.Write(client+1, key, fmt.Sprintf("c%d-%d", client+1, w.counter))
			} else {
				op = c.Read(client+1, key)
			}
		}
		w.current[client] = op
	}
	return c.Tick()
}

func applyStep(c *Cluster, w *workload, s Step) error {
	ids := c.IDs()
	switch s.Kind {
	case StepPartitionLeader:
		leader, ok, err := c.Leader()
		if err != nil {
			return err
		}
		if !ok {
			leader = ids[0]
		}
		c.Network().Partition([]raft.NodeID{leader}, MajorityWithout(c, leader))
	case StepSplit:
		n := s.Node
		c.Network().Partition(append([]raft.NodeID(nil), ids[:n]...), append([]raft.NodeID(nil), ids[n:]...))
	case StepIsolate:
		c.Network().Partition([]raft.NodeID{s.Node}, MajorityWithout(c, s.Node))
	case StepCrash:
		c.Crash(s.Node)
	case StepRestart:
		return c.Restart(s.Node)
	case StepSetFaults:
		return c.Network().SetFaults(Faults{
			LossRate:      s.Loss,
			DuplicateRate: s.Dup,
			MinDelay:      s.MinDelay,
			MaxDelay:      s.MaxDelay,
		})
	case StepCompact:
		if !c.IsDown(s.Node) {
			return c.Compact(s.Node)
		}
	case StepHeal:
		c.Network().Heal()
	case StepWork:
		for range s.Ticks {
			if err := w.tick(c); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("chaos: unknown step kind %d", s.Kind)
	}
	return nil
}

func requireAdverse(st Stats) error {
	if st.Dropped == 0 && st.Partitions == 0 {
		return fmt.Errorf("no message was dropped or partitioned")
	}
	return nil
}

func scenarioGenerated() Scenario {
	return Scenario{
		Name: "generated nemesis and workload",
		Hypothesis: "a nemesis schedule and client workload drawn from the seed keep " +
			"every safety invariant, and once the cluster is healed and restarted a " +
			"leader is elected and a write succeeds",
		Nodes:         generatedNodes,
		RequireFaults: requireAdverse,

		Run: func(c *Cluster) error {
			seed := c.Seed()
			clients, keys := workloadShape(seed)
			w := newWorkload(seed, clients, keys)

			if _, err := SettleLeader(c, 600); err != nil {
				return err
			}
			for _, step := range Schedule(seed) {
				if err := applyStep(c, w, step); err != nil {
					return err
				}
			}

			if _, err := SettleLeader(c, 1500); err != nil {
				return fmt.Errorf("no leader after the cluster healed: %w", err)
			}
			op, err := WriteWithRetry(c, clients+1, "liveness", "after-heal", 1500)
			if err != nil {
				return fmt.Errorf("no write succeeded after the cluster healed: %w", err)
			}
			if op.Status != StatusOK {
				return fmt.Errorf("the write after healing ended %v, want ok\n%s", op.Status, c.Dump())
			}
			// Let every replica apply the log so Converged compares settled state.
			return c.TickN(400)
		},
	}
}
