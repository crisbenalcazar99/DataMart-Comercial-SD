WITH pagos_registrados AS (
	SELECT idCompra, MAX(FechaIngreso) AS fechaAbono
	FROM dbo.Pago
	GROUP BY idCompra
), facturas_anuladas AS(
	SELECT Comprobante FROM dbo.Compra
	WHERE idTipoFactura  =  5
)

SELECT 
	 
	 com.idCliente AS cod_cliente,
	 cli.Nombre AS nom_cliente,
	 cli.Ruc AS 'cif',
	 det.Codigo AS cod_articulo,
	 
    com.idCompra AS numfac, 
    com.Notas AS comen2,
    com.SubtotalIva AS subtotal,
    com.Total AS total,
    com.Iva AS iva,
    com.saldo AS saldo_factura,
    NULL AS total_retencion, 
    (com.Total - com.saldo) AS total_abonado,
    p.fechaAbono AS fecha_abono,
    det.FechaIngreso AS fecha_emision, 
	 CONCAT(com.SerieFactura, com.Numero) AS numero_factura,
    CASE 
        WHEN det.signo = 1 
        THEN -1 * (det.Cantidad * det.Precio) - (det.Cantidad * det.Precio * det.DescuentoPorc / 100) 
        ELSE (det.Cantidad * det.Precio) - (det.Cantidad * det.Precio * det.DescuentoPorc / 100) 
    END AS subtotal_articulo, 
    det.Cantidad AS cantidad_articulo,
    null AS id_vendedor,
    '' AS cod_transaction,
    CONCAT(det.idDetCompra, 'GK') AS id_transaction ,
    CASE WHEN (com.idTipoFactura = 5 or fa.Comprobante IS NOT  NULL) THEN 'ANULADA' ELSE 'FACT' end AS estado_factura


    
   
FROM GEEKTECH.dbo.DetCompra det
JOIN GEEKTECH.dbo.Compra com 
    ON det.idCompra = com.idCompra
JOIN GEEKTECH.dbo.Cliente cli 
    ON com.idCliente = cli.idCliente
LEFT JOIN pagos_registrados p
	 ON det.idCompra = p.idCompra
LEFT JOIN facturas_anuladas fa
	 ON com.numero = fa.Comprobante

WHERE det.Codigo IN :tuple_codart
AND YEAR(det.FechaIngreso) >= 2023
AND com.Borrar = 0
AND com.idTipoFactura  IN (1, 5)

