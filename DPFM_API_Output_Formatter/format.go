package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToProductStock(rows *sql.Rows) (*ProductStock, error) {
	defer rows.Close()
	productStock := ProductStock{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&productStock.Product,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productStock, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &productStock, nil
	}

	return &productStock, nil
}

func ConvertToProductStockAvailability(rows *sql.Rows) (*[]ProductStockAvailability, error) {
	defer rows.Close()
	productStockAvailabilities := make([]ProductStockAvailability, 0)
	i := 0

	for rows.Next() {
		i++
		productStockAvailability := ProductStockAvailability{}
		err := rows.Scan(
			&productStockAvailability.Product,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productStockAvailabilities, err
		}

		productStockAvailabilities = append(productStockAvailabilities, productStockAvailability)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &productStockAvailabilities, nil
	}

	return &productStockAvailabilities, nil
}
