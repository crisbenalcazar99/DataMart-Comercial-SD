
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
	SELECT  distinct numdoc 
	FROM security_data.facturas f 
	JOIN security_data.clientes cli ON f.cliente = cli.codcli
	WHERE 
    (YEAR(f.emision) >= 2023
    AND LEFT(f.numser, 3) <> '000'
    AND f.comen3 LIKE 'TERCERO%'
    AND numdoc NOT IN (
        'F000261350','F000261358','F000261372','F000261419',
		  'F000261456','F000261481','F000261508','F000261521',
		  'F000261534','F000267994','F000268041','F000275706',
		  'F000275737','F000275761','F000275825','F000275837',
		  'F000276373','F000276510','F000276561','F000276727',
		  'F000277311','F000281327','F000281698','F000281720',
		  'F000281753','F000281791','F000281862','F000281879',
		  'F000281912','F000281973','F000284223','F000284243',
		  'F000284256','F000284338','F000284364','F000284374',
		  'F000284406','F000284445','F000284456','F000284494',
		  'F000284805','F000284814','F000284828','F000288449'
    )) or f.numdoc = 'F002575331'
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

WHERE LEFT(cli.cif, 10) IN (
        '0992879955','1790986055','0190153274','1190082446','1790899780',
        '0490000178','1792720990','0390023340','1790085996','0992862211',
        '1792717418','1792500656','1792869889','1790312143','0992821663',
        '1791287541','1790007863','0992936509','0990090866','1792496136',
        '1792459796','0991327371','0991370226','0890032060'
    ) AND art.codart IN (
        '120101001','4830404001','4830404002','4830404003','4830404004',
        '4830404005','4830402059','4830402061','120102001','4880101001',
        '4880201001','4880101002','4880201002','4880102003','4880201003'
    )


ORDER BY f.emision;
