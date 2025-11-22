
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
)

SELECT 
    tran.id_tranfac,
	 f.id_numfac AS id_factura,
	 TRIM(cli.cif) AS cif,
	 TRIM(art.codart) AS cod_articulo,
	 
	 tran.numfac, 
	 f.numdoc,
	 f.comen2, 
    f.comen3, 
    (f.total - f.total_iva) AS subtotal_factura,
    f.total AS total_factura,
    f.iva, 
    tcxc.saldo_factura,
    tcxc.total_retencion,
    tcxc.total_abonado,
    tcxc.fecha_abono,
    f.fecha_hora AS fecha_emision,
    CONCAT(f.numser, f.pedido) AS numero_factura, 

    (tran.cantidad * tran.precio) * (1 - tran.desct / 100) AS 'subtotal_articulo', 
    tran.cantidad AS cantidad_articulo,
    v.id_codven AS id_vendedor


FROM security_data.tranfac tran
JOIN security_data.facturas f 
    ON tran.numfac = f.numfac
JOIN security_data.clientes cli 
    ON f.cliente = cli.codcli
JOIN codigo_articulos art 
    ON tran.codart = art.codart
LEFT JOIN trancxc_abonos tcxc 
    ON tran.numfac = tcxc.numfac
JOIN security_data.vendedores v ON v.codven = f.codven
WHERE  
	(
		(f.codven = '410'  AND f.emision >= DATE '2025-05-01')
		OR (f.comen3 LIKE 'COMERCIAL%%' AND f.emision >= DATE '2025-05-01')
		OR f.id_numfac = 5958542 # Mapeo agregado manual por factura mal ingresada
		OR (f.codven IN ('260', '280', '323', '410', '284', '211', '260', '315') AND f.emision >= DATE '2025-05-01')
		OR (f.codven IN ('413', '412', '411', '414') AND f.emision >= DATE '2025-05-26')
		OR (f.codven ='415' AND f.emision >= DATE '2025-06-09')
	)
	{{filter_date_block}}

ORDER BY f.emision;
