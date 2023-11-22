package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-product-stock-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-stock-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) ProductStockRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.ProductStock {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE productStock.ProductStock = %d ", input.ProductStock.Product),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	productStock.ProductStock
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_stock_product_stock_data as productStock 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductStock(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ProductStockAvailabilitiesRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductStockAvailability {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE productStockAvailability.ProductStock = %d ", input.ProductStock.Product),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	productStockAvailability.ProductStock
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_stock_product_stock_availability_data as productStockAvailability 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductStockAvailability(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
