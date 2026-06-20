package memory

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/qdrant/go-client/qdrant"
)

type VectorStore struct {
	client  *qdrant.Client
	colName string
}

// NewVectorStore connects to Qdrant (Dynamically handles Local Docker or Managed Cloud)
func NewVectorStore(address string, collectionName string) (*VectorStore, error) {
	// 1. Set Local Defaults
	host := address
	port := 6334 // Default gRPC port
	useTLS := false

	// 2. Parse Cloud URLs (Strip protocols and extract ports)
	if strings.HasPrefix(address, "http://") {
		host = strings.TrimPrefix(address, "http://")
	} else if strings.HasPrefix(address, "https://") {
		host = strings.TrimPrefix(address, "https://")
		useTLS = true
	}

	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		host = parts[0]
		if p, err := strconv.Atoi(parts[1]); err == nil {
			// Safety catch: If you accidentally copied the HTTP port (6333) from the Qdrant dashboard,
			// force it to the gRPC port (6334) because the Go client requires gRPC.
			if p == 6333 {
				port = 6334
			} else {
				port = p
			}
		}
	}

	// 3. Inject API Key from Environment
	apiKey := os.Getenv("QDRANT_API_KEY")
	if apiKey != "" {
		useTLS = true // Cloud requires TLS encryption if an API key is used
	}

	// 4. Ignite the Connection
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   host,
		Port:   port,
		APIKey: apiKey,
		UseTLS: useTLS,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure qdrant client: %w", err)
	}

	// 5. Cloud latency buffer (Increased timeout from 5s to 10s for initial cloud handshake)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Hash the URL string into a deterministic uint64 integer
	h := fnv.New64a()
	h.Write([]byte(id))
	numID := h.Sum64()

	// 2. Use raw Protobuf bindings
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