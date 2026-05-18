package protocol

// Event is the universal data structure for the Zeno OS Neural Bus.
type Event struct {
	ID        string
	Source    string
	Target    string 
	Payload   string
	Timestamp int64
}

// MemoryNode represents an entity saved in the Sovereign Memory (Neo4j)
type MemoryNode struct {
	EntityID   string
	EntityType string
	Context    string
}