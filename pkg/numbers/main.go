package numbers

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	_ "embed"
	b64 "encoding/base64"

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

//go:embed config/definitions.json
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
}

type CommitCustomLicense struct {
	Name     *string `json:"name,omitempty"`
	Document *string `json:"document,omitempty"`
}
type CommitCustom struct {
	DigitalSourceType *string              `json:"digitalSourceType,omitempty"`
	MiningPreference  *string              `json:"miningPreference,omitempty"`
	GeneratedThrough  string               `json:"generatedThrough"`
	GeneratedBy       *string              `json:"generatedBy,omitempty"`
	CreatorWallet     *string              `json:"creatorWallet,omitempty"`
	License           *CommitCustomLicense `json:"license,omitempty"`
}
type Commit struct {
	AssetCid              string        `json:"assetCid"`
	AssetSha256           string        `json:"assetSha256"`
	EncodingFormat        string        `json:"encodingFormat"`
	AssetTimestampCreated int64         `json:"assetTimestampCreated"`
	AssetCreator          *string       `json:"assetCreator,omitempty"`
	Abstract              *string       `json:"abstract,omitempty"`
	Custom                *CommitCustom `json:"custom,omitempty"`
	Testnet               bool          `json:"testnet"`
}

type Input struct {
	Images       []string `json:"images"`
	AssetCreator *string  `json:"asset_creator,omitempty"`
	Abstract     *string  `json:"abstract,omitempty"`
	Custom       *struct {
		DigitalSourceType *string `json:"digital_source_type,omitempty"`
		MiningPreference  *string `json:"mining_preference,omitempty"`
		GeneratedThrough  *string `json:"generated_through,omitempty"`
		GeneratedBy       *string `json:"generated_by,omitempty"`
		CreatorWallet     *string `json:"creator_wallet,omitempty"`
		License           *struct {
			Name     *string `json:"name,omitempty"`
			Document *string `json:"document,omitempty"`
		} `json:"license,omitempty"`
	} `json:"custom,omitempty"`
}

type Output struct {
	AssetUrls []string `json:"asset_urls"`
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
	return fmt.Sprintf("token %s", con.Config.GetFields()["capture_token"].GetStringValue())
}

func (con *Connection) pinFile(data []byte) (string, string, error) {

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
		return "", "", err
	}

	w.Close()
	sha256hash := fmt.Sprintf("%x", h.Sum(nil))

	req, err := http.NewRequest("POST", ApiUrlPin, &b)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", con.getToken())

	tr := &http.Transport{
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: tr}
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

func (con *Connection) commit(commit Commit) (string, string, error) {

	marshalled, err := json.Marshal(commit)
	if err != nil {
		return "", "", err
	}

	req, err := http.NewRequest("POST", ApiUrlCommit, bytes.NewReader(marshalled))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", con.getToken())

	tr := &http.Transport{
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: tr}
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
	def, err := c.GetConnectorDefinitionByUid(defUid)
	if err != nil {
		return nil, err
	}
	return &Connection{
		BaseConnection: base.BaseConnection{
			Logger: logger, DefUid: defUid,
			Config:     config,
			Definition: def,
		},
		connector: c,
	}, nil
}

func (con *Connection) Execute(inputs []*structpb.Struct) ([]*structpb.Struct, error) {

	if err := con.ValidateInput(inputs, "default"); err != nil {
		return nil, err
	}
	var outputs []*structpb.Struct

	for _, input := range inputs {

		assetUrls := []string{}

		inputStruct := Input{}
		err := base.ConvertFromStructpb(input, &inputStruct)
		if err != nil {
			return nil, err
		}

		for _, image := range inputStruct.Images {
			imageBytes, err := b64.StdEncoding.DecodeString(image)
			if err != nil {
				return nil, err
			}

			var commitCustom *CommitCustom
			if inputStruct.Custom != nil {
				var commitCustomLicense *CommitCustomLicense
				if inputStruct.Custom.License != nil {
					commitCustomLicense = &CommitCustomLicense{
						Name:     inputStruct.Custom.License.Name,
						Document: inputStruct.Custom.License.Document,
					}
				}
				commitCustom = &CommitCustom{
					DigitalSourceType: inputStruct.Custom.DigitalSourceType,
					MiningPreference:  inputStruct.Custom.MiningPreference,
					GeneratedThrough:  "https://console.instill.tech",
					GeneratedBy:       inputStruct.Custom.GeneratedBy,
					CreatorWallet:     inputStruct.Custom.CreatorWallet,
					License:           commitCustomLicense,
				}

			}

			cid, sha256hash, err := con.pinFile(imageBytes)
			if err != nil {
				return nil, err
			}

			assetCid, _, err := con.commit(Commit{
				AssetCid:              cid,
				AssetSha256:           sha256hash,
				EncodingFormat:        http.DetectContentType(imageBytes),
				AssetTimestampCreated: time.Now().Unix(),
				AssetCreator:          inputStruct.AssetCreator,
				Abstract:              inputStruct.Abstract,
				Custom:                commitCustom,
				Testnet:               false,
			})

			if err != nil {
				return nil, err
			}
			assetUrls = append(assetUrls, fmt.Sprintf("https://nftsearch.site/asset-profile?cid=%s", assetCid))
		}

		outputStruct := Output{
			AssetUrls: assetUrls,
		}

		output, err := base.ConvertToStructpb(outputStruct)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, output)

	}

	if err := con.ValidateOutput(outputs, "default"); err != nil {
		return nil, err
	}

	return outputs, nil

}

func (con *Connection) Test() (connectorPB.ConnectorResource_State, error) {

	req, err := http.NewRequest("GET", ApiUrlMe, nil)
	if err != nil {
		return connectorPB.ConnectorResource_STATE_ERROR, nil
	}
	req.Header.Set("Authorization", con.getToken())

	tr := &http.Transport{
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return connectorPB.ConnectorResource_STATE_ERROR, nil
	}
	if res.StatusCode == http.StatusOK {
		return connectorPB.ConnectorResource_STATE_CONNECTED, nil
	}
	return connectorPB.ConnectorResource_STATE_ERROR, nil
}
