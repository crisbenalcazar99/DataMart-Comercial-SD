
WITH codigo_articulos AS(
	SELECT codart, nomart, id_codart AS id_articulo
	FROM security_data.articulos
	WHERE codart IN :tuple_codart
), trancxc_abonos AS (
	    SELECT 
	        trancxc.numfac, 
	        SUM(trancxc.importe) AS saldo_factura,
	        SUM(CASE WHEN trancxc.tipo = 'IR' then ABS(trancxc.importe) ELSE 0 END ) AS total_retencion,
	        SUM(case when trancxc.tipo = 'AB' then ABS(trancxc.importe) ELSE 0 END ) AS total_abonado,
			  MAX(trancxc.fecha) AS fecha_abono
	    FROM security_data.trancxc trancxc
	    GROUP BY trancxc.numfac
), facturas_comercial AS (
	SELECT  distinct numdoc FROM security_data.facturas
	WHERE 
		(codven = '410'  AND emision >= DATE '2025-05-01')
		OR (comen3 LIKE 'COMERCIAL%%' AND emision >= DATE '2025-05-01')
		OR (codven IN ('260', '280', '323', '410', '284', '260', '315') AND emision >= DATE '2025-05-01')
		OR (codven IN ('413', '414') AND emision >= DATE '2025-05-26')
		OR (codven ='415' AND emision >= DATE '2025-06-09')
		OR (codven = '211' AND emision >= DATE '2025-05-01' AND emision <= DATE '2025-09-05')
		OR (codven IN ('412', '411') AND emision >= DATE '2025-05-26' AND emision < DATE '2025-08-01')
	   OR (codven = '438' AND emision >= DATE '2025-10-01')
	   OR (id_numfac = 5958542)
		OR (cliente = '1291790310' AND emision >= DATE '2025-09-01') #Agregado por Luis Gomez y su Distribuidor
)


SELECT 

	 f.cliente AS cod_cliente,
	 cli.nomcli AS nom_cliente,
	 cli.cif,
	 TRIM(art.codart) AS cod_articulo,
	 
	 tran.numfac, 
	 f.numdoc,
	 f.comen2, 
    f.comen3, 
    (f.total - f.total_iva) AS subtotal,
    f.total AS total,
    f.iva, 
    tcxc.saldo_factura,
    tcxc.total_retencion,
    tcxc.total_abonado,
    tcxc.fecha_abono,
    f.fecha_hora AS fecha_emision,
    CONCAT(f.numser, f.pedido) AS numero_factura, 
    (tran.cantidad * tran.precio) * (1 - tran.desct / 100) AS 'subtotal_articulo', 
    tran.cantidad AS cantidad_articulo,
    v.id_codven AS id_vendedor,
    tran.unico AS id_transaction,
    CASE 
	    WHEN f.numdoc IN (
	        SELECT numdoc
	        FROM security_data.facturas
	        WHERE numfac LIKE 'DV%'
	    ) 
	    THEN 'ANULADA' 
	    ELSE 'FACT' 
	END AS estado_factura

FROM security_data.facturas f
JOIN facturas_comercial fc ON fc.numdoc = f.numdoc
JOIN security_data.tranfac tran ON tran.numfac = f.numfac
JOIN security_data.clientes cli ON f.cliente = cli.codcli
JOIN codigo_articulos art ON tran.codart = art.codart
LEFT JOIN trancxc_abonos tcxc ON tran.numfac = tcxc.numfac
JOIN security_data.vendedores v ON v.codven = f.codven


ORDER BY f.emision;
