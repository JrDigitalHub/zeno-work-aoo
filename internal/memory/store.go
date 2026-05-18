package memory

import (
	"context"
	"fmt"

	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// SovereignStore now holds the live connection to the Neo4j database
type SovereignStore struct {
	driver neo4j.DriverWithContext
}

// Boot up the connection to the Graph Database
func NewSovereignStore(uri, username, password string) (*SovereignStore, error) {
	// 1. Establish the tunnel
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return nil, err
	}

	// 2. Ping the database to ensure it's actually awake
	err = driver.VerifyConnectivity(context.Background())
	if err != nil {
		return nil, err
	}

	fmt.Println("🧠 [MEMORY] Neural Graph (Neo4j) connected successfully.")
	return &SovereignStore{driver: driver}, nil
}

// Graceful shutdown for the database connection
func (s *SovereignStore) Close() {
	s.driver.Close(context.Background())
}

// Save writes a new node into the Knowledge Graph
func (s *SovereignStore) Save(node protocol.MemoryNode) {
	ctx := context.Background()
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Cypher Query: MERGE creates the node if it doesn't exist, or updates it if it does.
		query := `
			MERGE (e:Entity {id: $id})
			SET e.type = $type, e.context = $context, e.lastUpdated = timestamp()
		`
		params := map[string]any{
			"id":      node.EntityID,
			"type":    node.EntityType,
			"context": node.Context,
		}
		return tx.Run(ctx, query, params)
	})

	if err != nil {
		fmt.Printf("❌ [MEMORY] Failed to write to Graph: %v\n", err)
		return
	}
	fmt.Printf("💾 [MEMORY] Context anchored in Graph for Entity: %s\n", node.EntityID)
}

// Recall pulls a specific memory from the Graph
func (s *SovereignStore) Recall(entityID string) (protocol.MemoryNode, bool) {
	ctx := context.Background()
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Cypher Query: MATCH finds the exact node by its ID
		query := `MATCH (e:Entity {id: $id}) RETURN e.type AS type, e.context AS context`
		
		res, err := tx.Run(ctx, query, map[string]any{"id": entityID})
		if err != nil {
			return nil, err
		}

		if res.Next(ctx) {
			record := res.Record()
			nodeType, _ := record.Get("type")
			contextStr, _ := record.Get("context")
			
			return protocol.MemoryNode{
				EntityID:   entityID,
				EntityType: nodeType.(string),
				Context:    contextStr.(string),
			}, nil
		}
		return nil, nil // Node not found
	})

	if err != nil || result == nil {
		return protocol.MemoryNode{}, false
	}

	return result.(protocol.MemoryNode), true
}