package blockchain

import (
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/component/pkg/base"
	"github.com/instill-ai/connector-blockchain/pkg/numbers"

	connectorPB "github.com/instill-ai/protogen-go/vdp/connector/v1alpha"
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

func (c *Connector) CreateExecution(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IExecution, error) {
	switch {

	case c.numbersConnector.HasUid(defUid):
		return c.numbersConnector.CreateExecution(defUid, config, logger)
	default:
		return nil, fmt.Errorf("no blockchainConnector uid: %s", defUid)
	}
}

func (c *Connector) Test(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (connectorPB.ConnectorResource_State, error) {
	switch {
	case c.numbersConnector.HasUid(defUid):
		return c.numbersConnector.Test(defUid, config, logger)
	default:
		return connectorPB.ConnectorResource_STATE_ERROR, fmt.Errorf("no connector uid: %s", defUid)
	}
}
