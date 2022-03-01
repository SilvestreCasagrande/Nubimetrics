with currencies as (
SELECT
	cor.Name country_name
	,cor.CountryRegionCode 
	,c.Name currency_name
	,c.CurrencyCode currency_code
FROM [Person].[CountryRegion] cor
INNER JOIN [Sales].[CountryRegionCurrency] crc
	ON cor.CountryRegionCode = crc.CountryRegionCode
INNER JOIN [Sales].[Currency] c
	ON crc.CurrencyCode = c.CurrencyCode 
INNER JOIN [Sales].[CurrencyRate] cr
	ON crc.CurrencyCode = cr.ToCurrencyCode 
)
,taxes as (
SELECT 
	cor.Name country_name,
	cor.CountryRegionCode,
	str.TaxRate 
FROM [Person].[CountryRegion] cor
INNER JOIN [Person].[StateProvince] sp
	ON cor.CountryRegionCode  = sp.CountryRegionCode 
INNER JOIN [Sales].[SalesTaxRate] str
	ON sp.StateProvinceID = str.StateProvinceID 
)

SELECT 
	c.country_name,
	c.currency_name,
	cast(AVG(c.AverageRate) as numeric(10,2)) currency_rate,
	cast(AVG(t.TaxRate) as numeric(10,2)) average_tax_rate
FROM currencies c
INNER JOIN taxes t
	ON c.CountryRegionCode = t.CountryRegionCode 
GROUP BY c.country_name,c.currency_name 
ORDER BY country_name asc
;
