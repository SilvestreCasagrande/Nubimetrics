SELECT
    sp.CountryRegionCode country_region_code,
    cast(AVG(str.TaxRate) as float) average_taxRate
FROM [Sales].[SalesTaxRate] str
INNER JOIN [Person].[StateProvince] sp
    ON str.StateProvinceID  = sp.StateProvinceID
GROUP BY sp.CountryRegionCode 
;