package memory

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/qdrant/go-client/qdrant"
)

type VectorStore struct {
	client  *qdrant.Client
	colName string
}

// NewVectorStore connects to the running Qdrant container
func NewVectorStore(address string, collectionName string) (*VectorStore, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: "localhost",
		Port: 6334,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to qdrant: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := client.CollectionExists(ctx, collectionName)
	if err != nil {
		return nil, fmt.Errorf("failed to check collection status: %w", err)
	}

	if !exists {
		err = client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: collectionName,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     768,
				Distance: qdrant.Distance_Cosine,
			}),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize collection: %w", err)
		}
		fmt.Printf("📐 [VECTOR] Collection '%s' initialized for the first time.\n", collectionName)
	}

	fmt.Println("📐 [VECTOR] Semantic Memory connected successfully.")
	return &VectorStore{client: client, colName: collectionName}, nil
}

func (v *VectorStore) Close() {
	v.client.Close()
}

// UpsertVector pushes a vector embedding along with its source payload metadata to Qdrant
func (v *VectorStore) UpsertVector(id string, vector []float32, payload map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Hash the URL string into a deterministic uint64 integer
	h := fnv.New64a()
	h.Write([]byte(id))
	numID := h.Sum64()

	// 2. Use raw Protobuf bindings to guarantee compilation immunity
	pointID := &qdrant.PointId{
		PointIdOptions: &qdrant.PointId_Num{
			Num: numID,
		},
	}

	point := &qdrant.PointStruct{
		Id:      pointID,
		Vectors: qdrant.NewVectors(vector...),
		Payload: qdrant.NewValueMap(payload),
	}

	_, err := v.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: v.colName,
		Points:         []*qdrant.PointStruct{point},
	})
	if err != nil {
		return fmt.Errorf("failed to upsert point: %w", err)
	}

	fmt.Printf("💾 [VECTOR] Semantic context anchored for Hash: %d | URL: %s\n", numID, id)
	return nil
}
