WITH pagos_registrados AS (
	SELECT idCompra, MAX(FechaIngreso) AS fechaAbono
	FROM dbo.Pago
	GROUP BY idCompra
), facturas_anuladas AS(
	SELECT Comprobante FROM dbo.Compra
	WHERE idTipoFactura  =  5
)

SELECT 
	 com.idCliente AS id_cliente,
	 det.Codigo AS codart,
    com.idCompra AS numfac, 
    com.Notas AS comen2,
    com.SubtotalIva AS subtotal_factura,
    com.Total AS total_factura,
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
    det.Cantidad AS cantidad_articulo
    
   
FROM GEEKTECH.dbo.DetCompra det
JOIN GEEKTECH.dbo.Compra com 
    ON det.idCompra = com.idCompra
JOIN GEEKTECH.dbo.Cliente cli 
    ON com.idCliente = cli.idCliente
LEFT JOIN pagos_registrados p
	 ON det.idCompra = p.idCompra
LEFT JOIN facturas_anuladas fa
	 ON com.numero = fa.Comprobante

WHERE det.Codigo IN (
    '002005', '002006', 'SBIO', 'CREA01', 'CREA02', 
    'CREA03', 'CREA04', 'CREA05', '0300304', 'NW001', '003004'
)

AND YEAR(det.FechaIngreso) >= 2023
AND com.Borrar = 0
AND com.idTipoFactura  IN (1, 5)

