package blockchain

import (
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/connector-blockchain/pkg/numbers"
	"github.com/instill-ai/connector/pkg/base"
)

var once sync.Once
var connector base.IConnector

type Connector struct {
	base.BaseConnector
	numbersConnector base.IConnector
}

type ConnectorOptions struct {
	Numbers numbers.ConnectorOptions
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {

		numbersConnector := numbers.Init(logger, options.Numbers)

		connector = &Connector{
			BaseConnector:    base.BaseConnector{Logger: logger},
			numbersConnector: numbersConnector,
		}

		for _, uid := range numbersConnector.ListConnectorDefinitionUids() {
			def, err := numbersConnector.GetConnectorDefinitionByUid(uid)
			if err != nil {
				logger.Error(err.Error())
			}
			err = connector.AddConnectorDefinition(uid, def.GetId(), def)
			if err != nil {
				logger.Warn(err.Error())
			}
		}

	})
	return connector
}

func (c *Connector) CreateConnection(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IConnection, error) {
	switch {

	case c.numbersConnector.HasUid(defUid):
		return c.numbersConnector.CreateConnection(defUid, config, logger)
	default:
		return nil, fmt.Errorf("no blockchainConnector uid: %s", defUid)
	}
}
