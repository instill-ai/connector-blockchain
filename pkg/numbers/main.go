package numbers

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	_ "embed"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/connector/pkg/base"
	"github.com/instill-ai/connector/pkg/configLoader"

	connectorPB "github.com/instill-ai/protogen-go/vdp/connector/v1alpha"
)

const ApiUrlPin = "https://eoqctv92ahgrcif.m.pipedream.net"
const ApiUrlCommit = "https://eo883tj75azolos.m.pipedream.net"
const ApiUrlMe = "https://api.numbersprotocol.io/api/v3/auth/users/me"

const vendorName = "numbers"

//go:embed config/seed/definitions.json
var definitionJson []byte

var once sync.Once
var connector base.IConnector

type Connector struct {
	base.BaseConnector
	options ConnectorOptions
}

type ConnectorOptions struct{}

type Connection struct {
	base.BaseConnection
	connector *Connector
	defUid    uuid.UUID
	config    *structpb.Struct
}

type CommitCustomLicense struct {
	Name string `json:"name"`
}
type CommitCustom struct {
	GeneratedBy      string              `json:"generatedBy"`
	GeneratedThrough string              `json:"generatedThrough"`
	Prompt           string              `json:"prompt"`
	CreatorWallet    string              `json:"creatorWallet"`
	License          CommitCustomLicense `json:"license"`
}
type Commit struct {
	AssetCid              string       `json:"assetCid"`
	AssetSha256           string       `json:"assetSha256"`
	EncodingFormat        string       `json:"encodingFormat"`
	AssetTimestampCreated int64        `json:"assetTimestampCreated"`
	AssetCreator          string       `json:"assetCreator"`
	Abstract              string       `json:"abstract"`
	Custom                CommitCustom `json:"custom"`
	Testnet               bool         `json:"testnet"`
}

func Init(logger *zap.Logger, options ConnectorOptions) base.IConnector {
	once.Do(func() {

		loader := configLoader.InitJSONSchema(logger)
		connDefs, err := loader.Load(vendorName, connectorPB.ConnectorType_CONNECTOR_TYPE_BLOCKCHAIN, definitionJson)
		if err != nil {
			panic(err)
		}

		connector = &Connector{
			BaseConnector: base.BaseConnector{Logger: logger},
			options:       options,
		}

		for idx := range connDefs {
			err := connector.AddConnectorDefinition(uuid.FromStringOrNil(connDefs[idx].GetUid()), connDefs[idx].GetId(), connDefs[idx])
			if err != nil {
				logger.Warn(err.Error())
			}
		}

	})
	return connector
}

func (con *Connection) getToken() string {
	return fmt.Sprintf("token %s", con.config.GetFields()["captureToken"].GetStringValue())
}

func (con *Connection) getCreatorName() string {
	return con.config.GetFields()["creatorName"].GetStringValue()
}

func (con *Connection) getLicense() string {
	return con.config.GetFields()["license"].GetStringValue()
}

func (con *Connection) pinFile(data []byte) (string, string, error) {

	var b bytes.Buffer

	w := multipart.NewWriter(&b)
	var fw io.Writer

	if _, err := w.CreateFormFile("file", "file.jpg"); err != nil {
		return "", "", err
	}

	if _, err := io.Copy(fw, bytes.NewReader(data)); err != nil {
		return "", "", err
	}

	h := sha256.New()

	if _, err := io.Copy(h, bytes.NewReader(data)); err != nil {
		log.Fatal(err)
	}

	w.Close()
	sha256hash := fmt.Sprintf("%x", h.Sum(nil))

	req, err := http.NewRequest("POST", ApiUrlPin, &b)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", con.getToken())

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return "", "", err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return "", "", err
		}
		var jsonRes map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &jsonRes)
		if cid, ok := jsonRes["cid"]; ok {
			return cid.(string), sha256hash, nil
		} else {
			return "", "", fmt.Errorf("pinFile failed")
		}

	}
	return "", "", fmt.Errorf("pinFile failed")

}

func (con *Connection) commit(commit Commit) (string, string, error) {

	marshalled, err := json.Marshal(commit)
	if err != nil {
		return "", "", nil
	}

	// return "", "", nil
	req, err := http.NewRequest("POST", ApiUrlCommit, bytes.NewReader(marshalled))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", con.getToken())

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return "", "", err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return "", "", err
		}
		var jsonRes map[string]interface{}
		_ = json.Unmarshal(bodyBytes, &jsonRes)

		var assetCid string
		var assetTreeCid string
		if val, ok := jsonRes["assetCid"]; ok {
			assetCid = val.(string)
		} else {
			return "", "", fmt.Errorf("assetCid failed")
		}
		if val, ok := jsonRes["assetTreeCid"]; ok {
			assetTreeCid = val.(string)
		} else {
			return "", "", fmt.Errorf("assetTreeCid failed")
		}
		return assetCid, assetTreeCid, nil

	}
	return "", "", fmt.Errorf("commit failed")

}

func (c *Connector) CreateConnection(defUid uuid.UUID, config *structpb.Struct, logger *zap.Logger) (base.IConnection, error) {
	return &Connection{
		BaseConnection: base.BaseConnection{Logger: logger},
		connector:      c,
		defUid:         defUid,
		config:         config,
	}, nil
}

func (con *Connection) Execute(inputs []*connectorPB.DataPayload) ([]*connectorPB.DataPayload, error) {

	var output []*connectorPB.DataPayload

	for _, dataPayload := range inputs {
		imageGenerationModel := dataPayload.GetMetadata().GetFields()["imageGenerationModel"].GetStringValue()
		prompt := dataPayload.GetTexts()[0]
		var assetCids []*structpb.Value
		var assetUrls []*structpb.Value
		for _, images := range dataPayload.Images {

			cid, sha256hash, err := con.pinFile(images)
			if err != nil {
				return nil, err
			}

			assetCid, _, err := con.commit(Commit{
				AssetCid:              cid,
				AssetSha256:           sha256hash,
				EncodingFormat:        "image/jpeg",
				AssetTimestampCreated: time.Now().Unix(),
				AssetCreator:          con.getCreatorName(),
				Abstract:              "Image Generation",
				Custom: CommitCustom{
					GeneratedBy:      imageGenerationModel,
					GeneratedThrough: "https://console.instill.tech",
					Prompt:           prompt,
					CreatorWallet:    "",
					License: CommitCustomLicense{
						Name: con.getLicense(),
					},
				},
				Testnet: false,
			})

			if err != nil {
				return nil, err
			}
			assetCids = append(assetCids, structpb.NewStringValue(assetCid))
			assetUrls = append(assetUrls, structpb.NewStringValue(fmt.Sprintf("https://nftsearch.site/asset-profile?cid=%s", assetCid)))
		}

		output = append(output, &connectorPB.DataPayload{
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"assetUrls": structpb.NewListValue(&structpb.ListValue{
						Values: assetUrls,
					}),
					"assetCids": structpb.NewListValue(&structpb.ListValue{
						Values: assetCids,
					}),
				},
			},
		})

	}

	return output, nil

}

func (con *Connection) Test() (connectorPB.Connector_State, error) {

	req, err := http.NewRequest("GET", ApiUrlMe, nil)
	if err != nil {
		return connectorPB.Connector_STATE_ERROR, nil
	}
	req.Header.Set("Authorization", con.getToken())

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return connectorPB.Connector_STATE_ERROR, nil
	}
	if res.StatusCode == http.StatusOK {
		return connectorPB.Connector_STATE_CONNECTED, nil
	}
	return connectorPB.Connector_STATE_ERROR, nil
}
