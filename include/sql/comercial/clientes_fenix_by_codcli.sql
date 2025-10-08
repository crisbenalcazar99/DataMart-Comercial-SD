SELECT DISTINCT cli.id_codcli AS id, cli.codcli AS cod_cliente, cli.nomcli AS cliente, cli.cif,
cli.fec_creado AS creation_date, cli.fecha_act AS update_date
FROM security_data.clientes cli
WHERE cli.cif IN :tuple_cif_clients