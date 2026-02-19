SELECT 
	 CASE WHEN fs.fecha_emision = '' THEN NULL ELSE fs.fecha_emision::TIMESTAMP WITHOUT TIME ZONE END AS fecha_emision,
	 CASE WHEN fs.fecha_expiracion = '' THEN NULL ELSE fs.fecha_expiracion::TIMESTAMP WITHOUT TIME ZONE END AS fecha_caducidad,
	 t.id_tramite,
	 fs.serial_firma
	 
FROM tramite t
JOIN users u ON u.id = t.id_user
JOIN public.datos_solicitante ds ON t.id_tramite = ds.id_tramite
JOIN public.firma_subida fs ON fs.id_tramite = t.id_tramite
WHERE 
fs.estado_registro = TRUE
AND t.estado_registro = TRUE
AND t.id_tramite IN :ids
AND fs.fecha_expiracion IS NOT NULL







