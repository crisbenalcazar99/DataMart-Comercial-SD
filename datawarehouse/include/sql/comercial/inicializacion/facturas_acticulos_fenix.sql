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
    (tran.cantidad * tran.precio) * (1 - tran.desct / 100) AS "subtotal_articulo", 
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
WHERE v.id_codven NOT IN (305, 307) #Codigos de los pruebas del equipo de desarrollo
and f.numdoc not in ('F002452795', 'F002472227') #Eliminacion de factura de Prueba
ORDER BY f.emision;
