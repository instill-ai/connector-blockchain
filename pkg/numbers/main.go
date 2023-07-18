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
	Name     string `json:"name"`
	Document string `json:"document"`
}
type CommitCustom struct {
	GeneratedThrough string              `json:"generatedThrough"`
	GeneratedBy      string              `json:"generatedBy"`
	CreatorWallet    string              `json:"creatorWallet"`
	License          CommitCustomLicense `json:"license"`
	// Additional Fields
	Texts          []string    `json:"texts"`
	Metadata       interface{} `json:"metadata"`
	StructuredData interface{} `json:"structuredData"`
}
type Commit struct {
	CaptureToken          string       `json:"captureToken"`
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
	return fmt.Sprintf("token %s", con.config.GetFields()["capture_token"].GetStringValue())
}

func (con *Connection) needUploadTextsToMetadata() bool {
	return con.config.GetFields()["metadata_texts"].GetBoolValue()
}
func (con *Connection) needUploadStructuredDataToMetadata() bool {
	return con.config.GetFields()["metadata_structured_data"].GetBoolValue()
}
func (con *Connection) needUploadMetadataToMetadata() bool {
	return con.config.GetFields()["metadata_metadata"].GetBoolValue()
}

func (con *Connection) pinFile(data []byte, token string) (string, string, error) {

	var b bytes.Buffer

	w := multipart.NewWriter(&b)
	var fw io.Writer
	var err error

	if fw, err = w.CreateFormFile("file", "file.jpg"); err != nil {
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
	if token == "" {
		req.Header.Set("Authorization", con.getToken())
	} else {
		req.Header.Set("Authorization", fmt.Sprintf("token %s", token))
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return "", "", err
	}

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

	} else {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return "", "", err
		}
		return "", "", fmt.Errorf(string(bodyBytes))
	}
}

func (con *Connection) commit(commit Commit, token string) (string, string, error) {

	marshalled, err := json.Marshal(commit)
	if err != nil {
		return "", "", err
	}

	req, err := http.NewRequest("POST", ApiUrlCommit, bytes.NewReader(marshalled))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if token == "" {
		req.Header.Set("Authorization", con.getToken())
	} else {
		req.Header.Set("Authorization", fmt.Sprintf("token %s", token))
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return "", "", err
	}

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

	} else {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return "", "", err
		}
		return "", "", fmt.Errorf(string(bodyBytes))
	}

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
		var assetCids []*structpb.Value
		var assetUrls []*structpb.Value
		for _, image := range dataPayload.Images {

			paramFields := dataPayload.Metadata.GetFields()["numbers"].GetStructValue().GetFields()
			paramCustomFields := paramFields["custom"].GetStructValue().GetFields()
			paramCustomLicenseFields := paramCustomFields["license"].GetStructValue().GetFields()
			commitCustom := CommitCustom{
				GeneratedThrough: "https://console.instill.tech",
				GeneratedBy:      paramCustomFields["generated_by"].GetStringValue(),
				CreatorWallet:    paramCustomFields["creator_wallet"].GetStringValue(),
				License: CommitCustomLicense{
					Name:     paramCustomLicenseFields["name"].GetStringValue(),
					Document: paramCustomLicenseFields["document"].GetStringValue(),
				},
			}

			if con.needUploadTextsToMetadata() {
				commitCustom.Texts = dataPayload.Texts
			}
			if con.needUploadStructuredDataToMetadata() {
				commitCustom.StructuredData = dataPayload.StructuredData
			}
			if con.needUploadMetadataToMetadata() {
				metadata := dataPayload.Metadata
				delete(metadata.GetFields(), "numbers")
				commitCustom.Metadata = metadata
			}

			cid, sha256hash, err := con.pinFile(image, paramFields["capture_token"].GetStringValue())
			if err != nil {
				return nil, err
			}

			assetCid, _, err := con.commit(Commit{
				AssetCid:              cid,
				AssetSha256:           sha256hash,
				EncodingFormat:        http.DetectContentType(image),
				AssetTimestampCreated: time.Now().Unix(),
				AssetCreator:          paramFields["asset_creator"].GetStringValue(),
				Abstract:              paramFields["abstract"].GetStringValue(),
				CaptureToken:          paramFields["capture_token"].GetStringValue(),
				Custom:                commitCustom,
				Testnet:               false,
			}, paramFields["capture_token"].GetStringValue())

			if err != nil {
				return nil, err
			}
			assetCids = append(assetCids, structpb.NewStringValue(assetCid))
			assetUrls = append(assetUrls, structpb.NewStringValue(fmt.Sprintf("https://nftsearch.site/asset-profile?cid=%s", assetCid)))
		}

		output = append(output, &connectorPB.DataPayload{
			DataMappingIndex: dataPayload.DataMappingIndex,
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"asset_urls": structpb.NewListValue(&structpb.ListValue{
						Values: assetUrls,
					}),
					"asset_cids": structpb.NewListValue(&structpb.ListValue{
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
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return connectorPB.Connector_STATE_ERROR, nil
	}
	if res.StatusCode == http.StatusOK {
		return connectorPB.Connector_STATE_CONNECTED, nil
	}
	return connectorPB.Connector_STATE_ERROR, nil
}

func (con *Connection) GetTask() (connectorPB.Task, error) {
	return connectorPB.Task_TASK_UNSPECIFIED, nil
}
