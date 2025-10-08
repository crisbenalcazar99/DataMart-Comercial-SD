SELECT DISTINCT cli.id_codcli AS id, cli.codcli AS cod_cliente, cli.nomcli AS cliente, cli.cif,
NULLIF(cli.fec_creado, '0000:00:00') AS creation_date, NULLIF(cli.fecha_act, '0000:00:00') AS update_date
FROM security_data.clientes cli
JOIN security_data.facturas f ON f.cliente = cli.codcli
JOIN security_data.tranfac t ON t.numfac = f.numfac
WHERE t.codart IN :tuple_codart
