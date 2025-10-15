package tx

type DeadlockDetector struct {
	adj map[TxID][]TxID // adj graph
	inc map[TxID][]TxID // incoming edges
}

func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		adj: make(map[TxID][]TxID),
		inc: make(map[TxID][]TxID),
	}
}

func (d *DeadlockDetector) Add(id TxID, waitFor TxID) bool {
	if id == waitFor {
		return true
	}

	d.adj[id] = append(d.adj[id], waitFor)
	d.inc[waitFor] = append(d.inc[waitFor], id)

	if d.hasPath(waitFor, id, make(map[TxID]bool)) {
		d.Remove(id)

		return false
	}

	return true
}

func (d *DeadlockDetector) Remove(id TxID) {
	for i := 0; i < len(d.adj[id]); i++ {
		cur := 0
		deps := d.inc[d.adj[id][i]]
		for j := 0; j < len(deps); j++ {
			if deps[j] != id {
				deps[cur] = deps[j]
				cur++
			}
		}

		if cur == 0 {
			delete(d.inc, d.adj[id][i])
		} else {
			d.inc[d.adj[id][i]] = deps[:cur]
		}
	}
	delete(d.adj, id)

	for i := 0; i < len(d.inc[id]); i++ {
		cur := 0
		deps := d.adj[d.inc[id][i]]
		for j := 0; j < len(deps); j++ {
			if deps[j] != id {
				deps[cur] = deps[j]
				cur++
			}
		}

		if cur == 0 {
			delete(d.adj, d.inc[id][i])
		} else {
			d.adj[d.inc[id][i]] = deps[:cur]
		}
	}
	delete(d.inc, id)
}

func (d *DeadlockDetector) hasPath(from, to TxID, used map[TxID]bool) bool {
	if from == to {
		return true
	}

	if used[from] {
		return false
	}

	used[from] = true

	for i := 0; i < len(d.adj[from]); i++ {
		if d.hasPath(d.adj[from][i], to, used) {
			return true
		}
	}

	return false
}
