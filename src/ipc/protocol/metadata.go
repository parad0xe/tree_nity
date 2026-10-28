package protocol

// Metadata holds emitter-specific frame information.
// Implementations are provided by ClientMetadata and ServerMetadata.
type Metadata interface {
	toJSON(emitter Emitter) (metadataJSON, error)
	validate() error
}
