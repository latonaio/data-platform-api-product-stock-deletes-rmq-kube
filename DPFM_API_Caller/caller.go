package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-product-stock-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-stock-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-product-stock-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	if input.APIType == "deletes" {
		response = c.deleteSqlProcess(input, output, accepter, log)
	}

	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var productStockData *dpfm_api_output_formatter.ProductStock
	productStockAvailabilityData := make([]dpfm_api_output_formatter.ProductStockAvailability, 0)
	for _, a := range accepter {
		switch a {
		case "ProductStock":
			h, i := c.productStockDelete(input, output, log)
			productStockData = h
			if h == nil || i == nil {
				continue
			}
			productStockAvailabilityData = append(productStockAvailabilityData, *i...)
		case "ProductStockAvailability":
			i := c.productStockAvailabilityDelete(input, output, log)
			if i == nil {
				continue
			}
			productStockAvailabilityData = append(productStockAvailabilityData, *i...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		ProductStock:             productStockData,
		ProductStockAvailability: &productStockAvailabilityData,
	}
}

func (c *DPFMAPICaller) productStockDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.ProductStock, *[]dpfm_api_output_formatter.ProductStockAvailability) {
	sessionID := input.RuntimeSessionID
	productStock := c.ProductStockRead(input, log)
	productStock.Product = input.ProductStock.Product
	productStock.BusinessPartner = input.ProductStock.BusinessPartner
	productStock.Plant = input.ProductStock.Plant
	productStock.SupplyChainRelationshipID = input.ProductStock.SupplyChainRelationshipID
	productStock.SupplyChainRelationshipDeliveryID = input.ProductStock.SupplyChainRelationshipDeliveryID
	productStock.SupplyChainRelationshipDeliveryPlantID = input.ProductStock.SupplyChainRelationshipDeliveryPlantID
	productStock.Buyer = input.ProductStock.Buyer
	productStock.Seller = input.ProductStock.Seller
	productStock.DeliverToParty = input.ProductStock.DeliverToParty
	productStock.DeliverFromParty = input.ProductStock.DeliverFromParty
	productStock.DeliverToPlant = input.ProductStock.DeliverToPlant
	productStock.DeliverFromPlant = input.ProductStock.DeliverFromPlant
	productStock.InventoryStockType = input.ProductStock.InventoryStockType
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": productStock, "function": "ProductStockProductStock", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "ProductStock Data cannot delete"
		return nil, nil
	}

	// productStockの削除が取り消された時は子に影響を与えない
	if !*&productStock.IsMarkedForDeletion {
		return productStock, nil
	}

	productStockAvailabilities := c.ProductStockAvailabilities(input, log)
	for i := range *productStockAvailabilities {
		(*productStockAvailabilities)[i].IsMarkedForDeletion = input.ProductStock.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*productStockAvailabilities)[i], "function": "ProductStockProductStockAvailability", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "ProductStockAvailability Data cannot delete"
			return nil, nil
		}
	}

	return productStock, productStockAvailabilities
}

func (c *DPFMAPICaller) productStockAvailabilityDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductStockAvailability {
	sessionID := input.RuntimeSessionID

	productStockAvailabilities := make([]dpfm_api_output_formatter.ProductStockAvailability, 0)
	for _, v := range input.ProductStock.ProductStockAvailability {
		data := dpfm_api_output_formatter.ProductStockAvailability{
			Product:                                input.ProductStock.Product,
			BusinessPartner:                        input.ProductStock.BusinessPartner,
			Plant:                                  input.ProductStock.Plant,
			SupplyChainRelationshipID:              input.ProductStock.SupplyChainRelationshipID,
			SupplyChainRelationshipDeliveryID:      input.ProductStock.SupplyChainRelationshipDeliveryID,
			SupplyChainRelationshipDeliveryPlantID: input.ProductStock.SupplyChainRelationshipDeliveryPlantID,
			Buyer:                                  input.ProductStock.Buyer,
			Seller:                                 input.ProductStock.Seller,
			DeliverToParty:                         input.ProductStock.DeliverToParty,
			DeliverFromParty:                       input.ProductStock.DeliverFromParty,
			DeliverToPlant:                         input.ProductStock.DeliverToPlant,
			DeliverFromPlant:                       input.ProductStock.DeliverFromPlant,
			ProductStockAvailabilityDate:           input.ProductStock.ProductStockAvailabilityDate,
			IsMarkedForDeletion:                    v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{
			"message":            data,
			"function":           "ProductStockProductStockAvailability",
			"runtime_session_id": sessionID,
		})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "ProductStockAvailability Data cannot delete"
			return nil
		}
	}
	// productStockAvailabilityがキャンセル取り消しされた場合、generalのキャンセルも取り消す
	if !*input.ProductStock.ProductStockAvailability[0].IsMarkedForDeletion {
		productStock := c.ProductStockRead(input, log)
		productStock.IsMarkedForDeletion = input.ProductStock.ProductStockAvailability[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": productStock, "function": "ProductStockProductStock", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "ProductStock Data cannot delete"
			return nil
		}
	}

	return &productStockAvailabilities
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
