package main

import (
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/gofrs/uuid"
	connectorBlockchain "github.com/instill-ai/connector-blockchain/pkg"
)

func main() {

	logger, _ := zap.NewDevelopment()
	// It is singleton, should be loaded when connector-backend started
	connector := connectorBlockchain.Init(logger)

	fmt.Println("dee", connector)
	fmt.Println("Xxx", connector.ListConnectorDefinitions())
	// For apis: Get connector definitsion apis
	for _, v := range connector.ListConnectorDefinitions() {
		b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(v)
		fmt.Println(0)
		fmt.Println(string(b), err)
		fmt.Println(1)
		fmt.Println(connector.ListCredentialField(v.Id))
		fmt.Println(2)

	}

	// in connector-backend:
	// if user trigger connectorA
	// ->connectorA.defUid
	// ->connectorA.configuration
	fmt.Println(3)
	execution, _ := connector.CreateExecution(uuid.FromStringOrNil("70d8664a-d512-4517-a5e8-5d4da81756a7"), "default", &structpb.Struct{}, logger)

	fmt.Println(4)
	a, err := execution.ExecuteWithValidation([]*structpb.Struct{})
	fmt.Println(a, err)
	if err != nil {
		fmt.Println(1, err)
	}
	// fmt.Println(4444, connector.GetT())
	// fmt.Println(44441, connector.GetD())

}
